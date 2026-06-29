"""
SQL CPI 计算模块 — 用 CK SQL 从 OSS 已清洗 CSV 读取数据，计算费雪指数
拉氏 = Σ(Pi/Pi0 × qi0) / Σ(qi0)    基期加权
帕氏 = Σ(Pi/Pi0 × qit) / Σ(qit)    当期加权
费雪 = √(拉氏 × 帕氏)              几何平均
结果写回 CK cpi_trend

清洗后数据存储在 OSS（CK 不存），CPI 通过 oss() 函数直读计算
"""
import sys, os, re
from datetime import datetime, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.db.connection import get_clickhouse
from config import get_database_config, get_oss_config

# OSS 配置（用于构建已清洗 CSV 的 oss() 读取路径）
OSS_CFG = get_oss_config()
OSS_AK = OSS_CFG['access_key_id']
OSS_SK = OSS_CFG['access_key_secret']
OSS_BUCKET = OSS_CFG['bucket']
OSS_ENDPOINT = OSS_CFG['endpoint'].rstrip('/')
_region_match = re.search(r'oss-([a-z]+-[a-z0-9-]+)', OSS_ENDPOINT)
OSS_REGION = _region_match.group(0) if _region_match else 'oss-cn-hangzhou'
OSS_CLEAN_PREFIX = 'ecommerce/sales_clean/'


def _clean_oss_url(ds):
    """构建已清洗 CSV 的 oss() 内网读取 URL"""
    ym = ds[:7].replace('-', '')
    return f"http://{OSS_BUCKET}.{OSS_REGION}-internal.aliyuncs.com/{OSS_CLEAN_PREFIX}{ym}/{ds}.csv"


def _build_oss_clean_table(period_start, period_end):
    """生成 UNION ALL 从 OSS 已清洗 CSV 读取数据的子查询（用作 FROM 表）"""
    oss_parts = []
    d = period_start
    while d <= period_end:
        ds = d.isoformat()
        url = _clean_oss_url(ds)
        oss_parts.append(
            f"SELECT toUInt64(s.product_id) pid, toDate(s.sale_date) sale_date, "
            f"toFloat64(s.price) price, toInt32(toFloat64OrZero(s.sales_volume)) sales_volume, "
            f"toUInt8(s.is_missing) is_missing "
            f"FROM oss('{url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames', "
            f"'product_id String, sale_date String, sales_volume String, price String, "
            f"revenue String, is_missing String, event_type String, day_type String') s"
        )
        d += timedelta(days=1)
    return ' UNION ALL '.join(oss_parts)


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
    结果写入 cpi_trend
    """
    client = get_clickhouse()
    if client is None:
        raise ConnectionError(f"无法连接 {db['host']}")

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

    # 预构建基期数据的 OSS 子查询（基期固定为 1 天）
    base_oss_table = _build_oss_clean_table(base_date, base_date)

    for p_idx, period_date in enumerate(pending):
        # 获取该周期对应的实际数据日期范围
        if granularity == 'month':
            # 当月第一天到最后一天
            period_start = period_date
            if period_date.month == 12:
                period_end = period_date.replace(year=period_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                period_end = period_date.replace(month=period_date.month + 1, day=1) - timedelta(days=1)
            date_desc = period_date.strftime('%Y-%m')
        elif granularity == 'week':
            period_start = period_date
            period_end = period_date + timedelta(days=6)
            date_desc = period_start.isoformat()
        else:
            period_start = period_date
            period_end = period_date
            date_desc = period_date.isoformat()

        period_label = period_date.isoformat()
        cur_oss_table = _build_oss_clean_table(period_start, period_end)

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
                SELECT b.pid, b.base_price, b.base_qty, c.cur_price, c.cur_qty
                FROM (
                    SELECT pid, AVG(price) AS base_price, SUM(sales_volume) AS base_qty
                    FROM ({base_oss_table})
                    WHERE is_missing = 0 AND sales_volume > 0
                    GROUP BY pid
                ) b
                INNER JOIN (
                    SELECT pid, AVG(price) AS cur_price, SUM(sales_volume) AS cur_qty
                    FROM ({cur_oss_table})
                    WHERE is_missing = 0 AND sales_volume > 0
                    GROUP BY pid
                ) c ON b.pid = c.pid
            ) m
        ) agg
        HAVING product_count > 0
        SETTINGS joined_subquery_requires_alias = 0,
                 allow_experimental_analyzer = 0
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
                SELECT b.pid, b.base_price, b.base_qty,
                       c.cur_price, c.cur_qty
                FROM (
                    SELECT pid, AVG(price) AS base_price, SUM(sales_volume) AS base_qty
                    FROM ({base_oss_table})
                    WHERE is_missing = 0 AND sales_volume > 0
                    GROUP BY pid
                ) b
                INNER JOIN (
                    SELECT pid, AVG(price) AS cur_price, SUM(sales_volume) AS cur_qty
                    FROM ({cur_oss_table})
                    WHERE is_missing = 0 AND sales_volume > 0
                    GROUP BY pid
                ) c ON b.pid = c.pid
            ) m
            INNER JOIN product_category_map cat ON m.pid = cat.product_id
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
