"""
SQL CPI 计算模块 — 直接在 ClickHouse 中用 SQL 计算费雪指数
拉氏 = Σ(Pi/Pi0 × qi0) / Σ(qi0)    基期加权
帕氏 = Σ(Pi/Pi0 × qit) / Σ(qit)    当期加权
费雪 = √(拉氏 × 帕氏)              几何平均
结果写回 CK cpi_trend
"""
import sys, os, re
from datetime import datetime, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from clickhouse_driver import Client
from config import get_database_config


def get_granularity(start_date, end_date):
    days = (end_date - start_date).days
    return 'day' if days < 50 else 'week' if days < 180 else 'month'  # 49天内日粒度


def _existing_cpi_dates(client):
    """获取 CK 中已计算的 CPI 日期"""
    try:
        r = client.execute("SELECT DISTINCT date FROM cpi_trend WHERE category_id = 0 ORDER BY date")
        return {str(row[0]) for row in r}
    except Exception:
        return set()


def compute_cpi_sql(base_date, start_date, end_date, force=False):
    """
    用 SQL 计算 CPI 趋势（全层级：总体 + 二级类目 + 叶子类目）
    结果写入 cpi_trend + 导出到 OSS
    """
    client = Client(
        host=get_database_config()['host'],
        port=get_database_config()['port'],
        user=get_database_config()['user'],
        password=get_database_config()['password'],
        database=get_database_config()['database'],
        connect_timeout=10, send_receive_timeout=300,
        settings={'max_query_size': 10000000, 'allow_experimental_analyzer': 0,
                  'allow_experimental_query_deduplication': 0}
    )

    granularity = get_granularity(start_date, end_date)
    print(f"  粒度: {granularity} (跨度 { (end_date - start_date).days } 天)")

    # 日期列表（按粒度聚合）
    dates = []
    if granularity == 'day':
        d = start_date
        while d <= end_date:
            dates.append(d)
            d += timedelta(days=1)
    elif granularity == 'week':
        d = start_date
        while d <= end_date:
            # 周一开始
            monday = d - timedelta(days=d.weekday())
            dates.append(monday)
            d += timedelta(days=7)
        dates = sorted(set(dates))
    else:  # month
        d = start_date
        while d <= end_date:
            first = d.replace(day=1)
            dates.append(first)
            d = (first + timedelta(days=32)).replace(day=1)

    # 幂等性检查
    existing = set()
    if not force:
        existing = _existing_cpi_dates(client)

    # 过滤
    pending = []
    for d in dates:
        ds = d.isoformat()[:10]
        if granularity == 'week':
            # 对周粒度，用周一的日期
            ds_key = ds
        elif granularity == 'month':
            ds_key = ds
        else:
            ds_key = ds
        if ds_key not in existing:
            pending.append(d)

    print(f"  共 {len(dates)} 个周期，需计算 {len(pending)} 个")
    if not pending:
        print("  [跳过] 所有周期 CPI 已计算")
        client.disconnect()
        return

    # force=True 时先清空旧数据
    if force:
        try:
            client.execute("TRUNCATE TABLE cpi_trend")
            print("  [清空] cpi_trend 旧数据已删除")
        except Exception as e:
            print(f"  [清空失败] {e}")

    base_ds = base_date.isoformat()
    total_periods = 0
    total_rows = 0

    for p_idx, period_date in enumerate(pending):
        # 获取该周期对应的实际数据日期范围
        if granularity == 'month':
            ym = period_date.strftime('%Y-%m')
            date_filter = f"toYYYYMM(sale_date) = {period_date.year * 100 + period_date.month}"
            period_label = period_date.isoformat()  # 用当月第一天 (YYYY-MM-DD)
        elif granularity == 'week':
            start_w = period_date
            end_w = period_date + timedelta(days=6)
            date_filter = f"sale_date BETWEEN '{start_w.isoformat()}' AND '{end_w.isoformat()}'"
            period_label = start_w.isoformat()
        else:
            ds = period_date.isoformat()
            date_filter = f"sale_date = '{ds}'"
            period_label = ds

        # === 总体 CPI（category_id=0）===
        sql_overall = f"""
        INSERT INTO cpi_trend (date, laspeyres, paasche, fisher, product_count, category_id, granularity)
        SELECT
            '{period_label}' AS date,
            sum_l / sum_bq AS laspeyres,
            sum_p / sum_cq AS paasche,
            sqrt((sum_l / sum_bq) * (sum_p / sum_cq)) AS fisher,
            product_count,
            0 AS category_id,
            '{granularity}' AS granularity
        FROM (
            SELECT
                SUM(base_qty) AS sum_bq,
                SUM(cur_qty) AS sum_cq,
                SUM(cur_price / base_price * base_qty) AS sum_l,
                SUM(cur_price / base_price * cur_qty) AS sum_p,
                count() AS product_count
            FROM (
                SELECT b.product_id, b.base_price, b.base_qty, c.cur_price, c.cur_qty
                FROM (
                    SELECT product_id, AVG(price) AS base_price, SUM(sales_volume) AS base_qty
                    FROM sales_clean
                    WHERE sale_date = '{base_ds}' AND is_missing = 0 AND sales_volume > 0
                    GROUP BY product_id
                ) b
                INNER JOIN (
                    SELECT product_id, AVG(price) AS cur_price, SUM(sales_volume) AS cur_qty
                    FROM sales_clean
                    WHERE {date_filter} AND is_missing = 0 AND sales_volume > 0
                    GROUP BY product_id
                ) c ON b.product_id = c.product_id
            ) m
        ) agg
        HAVING product_count > 0
        """
        try:
            client.execute(sql_overall)
            total_periods += 1
            total_rows += 1
        except Exception as e:
            print(f"    [CPI失败] {period_label}: {e}")
            continue

        # === 全层级类目 CPI — 单条 GROUP BY（覆盖 hierarchy=1/2/3）===
        try:
            sql_all_cats = f"""
            INSERT INTO cpi_trend (date, laspeyres, paasche, fisher, product_count, category_id, granularity)
            SELECT
                '{period_label}' AS date,
                SUM(base_qty * cur_price / base_price) / SUM(base_qty) AS laspeyres,
                SUM(cur_qty * cur_price / base_price) / SUM(cur_qty) AS paasche,
                sqrt(laspeyres * paasche) AS fisher,
                count() AS product_count,
                cat.category_id,
                '{granularity}' AS granularity
            FROM (
                SELECT b.product_id, b.base_price, b.base_qty,
                       c.cur_price, c.cur_qty
                FROM (
                    SELECT product_id, AVG(price) AS base_price, SUM(sales_volume) AS base_qty
                    FROM sales_clean
                    WHERE sale_date = '{base_ds}' AND is_missing = 0 AND sales_volume > 0
                    GROUP BY product_id
                ) b
                INNER JOIN (
                    SELECT product_id, AVG(price) AS cur_price, SUM(sales_volume) AS cur_qty
                    FROM sales_clean
                    WHERE {date_filter} AND is_missing = 0 AND sales_volume > 0
                    GROUP BY product_id
                ) c ON b.product_id = c.product_id
            ) m
            INNER JOIN product_category_map cat ON m.product_id = cat.product_id
            GROUP BY cat.category_id
            SETTINGS joined_subquery_requires_alias = 0,
                     allow_experimental_analyzer = 0
            """
            client.execute(sql_all_cats)
            total_rows += 1
        except Exception as e:
            print(f"    [类目CPI失败] {period_label}: {e}")

        if (p_idx + 1) % 10 == 0 or p_idx == 0 or p_idx == len(pending) - 1:
            print(f"    [{p_idx+1}/{len(pending)}] {period_label} OK (累计{total_periods}周期)")

    r = client.execute("SELECT count() FROM cpi_trend")
    print(f"\n  [完成] cpi_trend 共 {r[0][0]} 行 ({total_periods} 周期)")
    client.disconnect()
