"""
SQL 清洗管道 — OSS → ClickHouse SQL 清洗 → OSS 回写
全部清洗逻辑在 ClickHouse SQL 中完成，Python 仅做流程编排
"""
import sys, os, time, re, math
from datetime import datetime, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from clickhouse_driver import Client
from config import get_database_config, get_oss_config
from src.db.connection import get_oss_bucket

# OSS 配置
OSS_CFG = get_oss_config()
OSS_AK = OSS_CFG['access_key_id']
OSS_SK = OSS_CFG['access_key_secret']
OSS_BUCKET = OSS_CFG['bucket']
# 内网 endpoint 更省流量，但本地测试用外网
OSS_ENDPOINT = OSS_CFG['endpoint'].rstrip('/')

# 从 endpoint 提取 region (https://oss-cn-hangzhou.aliyuncs.com → oss-cn-hangzhou)
_region_match = re.search(r'oss-([a-z]+-[a-z0-9-]+)', OSS_ENDPOINT)
OSS_REGION = _region_match.group(0) if _region_match else 'oss-cn-hangzhou'

# OSS 路径前缀
OSS_SALES_PREFIX = 'ecommerce/sales/'
OSS_CLEAN_PREFIX = 'ecommerce/sales_clean_ck/'
OSS_CPI_PREFIX = 'ecommerce/cpi_results/'

# 阿里云上 CK 实例走内网免流量，外网本地测试用外网
_INTERNAL = 'internal' in OSS_ENDPOINT or 'ads' in OSS_ENDPOINT or 'rds' in OSS_ENDPOINT


def _s3_url(oss_key):
    """构建 S3 函数可用的 OSS URL"""
    if _INTERNAL:
        base = f"https://{OSS_BUCKET}.{OSS_REGION}-internal.aliyuncs.com"
    else:
        base = f"https://{OSS_BUCKET}.{OSS_REGION}.aliyuncs.com"
    return f"{base}/{oss_key}"

# 节日/促销日历（2020-2024）
HOLIDAYS = set()
PROMOTIONS = set()
for y in range(2020, 2026):
    ys = str(y)
    HOLIDAYS.update([f"{ys}-01-01", f"{ys}-10-01", f"{ys}-10-02", f"{ys}-10-03"])
    PROMOTIONS.update([f"{ys}-06-18", f"{ys}-11-11"])

ANOMALY_THRESHOLD = 3.0  # 超过基线 3 倍标记异常
WEEKEND_THRESHOLD = 1.5  # 周末超过基线 1.5 倍标记 weekend

BATCH_SIZE = 30  # 每批处理天数（控制进度输出频率）


# ==================== 日历初始化 ====================

def init_calendar(client):
    """填充日历表 2020-2024"""
    r = client.execute("SELECT count() FROM calendar")
    if r[0][0] > 0:
        print("  [跳过] calendar 表已有数据")
        return

    rows = []
    d = date(2020, 1, 1)
    end = date(2024, 12, 31)
    while d <= end:
        ds = d.isoformat()
        wd = d.weekday()  # 0=Mon, 6=Sun
        if ds in HOLIDAYS:
            dt = 'holiday'
        elif ds in PROMOTIONS:
            dt = 'promotion'
        elif wd >= 5:
            dt = 'weekend'
        else:
            dt = 'weekday'
        rows.append((d, dt, d.year, d.month, d.day, wd))
        d += timedelta(days=1)

    client.execute(
        'INSERT INTO calendar (date, day_type, year, month, day, weekday) VALUES',
        rows
    )
    print(f"  [OK] calendar: {len(rows)} 天")


# ==================== OSS 文件列表 ====================

def list_oss_sales_files():
    """列出 OSS 上所有销量 CSV 文件"""
    bucket = get_oss_bucket()
    import oss2
    files = []
    for obj in oss2.ObjectIteratorV2(bucket, prefix=OSS_SALES_PREFIX):
        key = obj.key
        if key.endswith('.csv') and key != OSS_SALES_PREFIX:
            files.append(key)
    return sorted(files)


# ==================== Step 1: 从 OSS 读入暂存表 ====================

def _load_one_day_via_s3(client, oss_key):
    """
    方式A：用 S3 函数直接从 OSS 加载到 staging（CK 服务器端读取，最快）
    适用于 CK 在阿里云上且 OSS 同区域的场景
    """
    oss_path = f"{OSS_SALES_PREFIX}{oss_key.split('/')[-1]}"
    url = _s3_url(oss_path)
    sql = f"INSERT INTO sales_staging SELECT * FROM s3('{url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames')"
    try:
        client.execute(sql, settings={'max_insert_block_size': 100000})
        return True
    except Exception as e:
        print(f"    [S3加载失败] {oss_key}: {e}")
        return False


def _load_one_day_via_python(client, oss_key):
    """
    方式B：用 oss2 SDK 下载 CSV 通过 Python 写入 staging（后备）
    """
    from datetime import datetime
    try:
        bucket = get_oss_bucket()
        obj = bucket.get_object(oss_key)
        content = obj.read()
        lines = content.decode('utf-8-sig').split('\n')
        if not lines:
            return False

        header = [h.strip() for h in lines[0].strip().split(',')]
        # 期望的列顺序: product_id, sale_date, sales_volume, price, revenue, is_missing
        batch = []
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue
            vals = line.split(',')
            while len(vals) < 6:
                vals.append('')
            if len(vals) > 6:
                vals = vals[:6]
            # 类型转换
            pid = int(float(vals[0])) if vals[0] else 0
            sd = datetime.strptime(vals[1][:10], '%Y-%m-%d').date() if vals[1][:10] else date(2020, 1, 1)
            sv = int(float(vals[2])) if vals[2] and vals[2] not in ('-1', '') else None
            pr = float(vals[3]) if vals[3] else 0.0
            rv = float(vals[4]) if vals[4] else None
            im = 1 if vals[5].lower() in ('true', '1') else 0
            batch.append((pid, sd, sv, pr, rv, im))
            if len(batch) >= 50000:
                client.execute(
                    'INSERT INTO sales_staging (product_id, sale_date, sales_volume, price, revenue, is_missing) VALUES',
                    batch
                )
                batch = []
        if batch:
            client.execute(
                'INSERT INTO sales_staging (product_id, sale_date, sales_volume, price, revenue, is_missing) VALUES',
                batch
            )
        return True
    except Exception as e:
        print(f"    [OSS加载失败] {oss_key}: {e}")
        return False


# ==================== 加载路由 ====================

def _load_one_day_to_staging(client, oss_key):
    """加载单日数据：优先 S3 函数（CK 服务器端直读 OSS），失败降级 Python"""
    if _load_one_day_via_s3(client, oss_key):
        return True
    return _load_one_day_via_python(client, oss_key)


# ==================== Step 2: SQL 清洗 ====================

def _get_14day_baseline_date(sale_date_str):
    """获取基线的 14 天范围"""
    d = datetime.strptime(sale_date_str, '%Y-%m-%d').date()
    start = d - timedelta(days=15)
    end = d - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _sql_clean_day(client, sale_date_str):
    """
    对 sales_staging 中的单日数据执行 SQL 清洗
    - 使用 calendar 表标记日类型
    - 使用 sales_clean 历史数据计算 14 天基线
    - 检测异常值
    结果写入 sales_clean
    """
    bs, be = _get_14day_baseline_date(sale_date_str)

    sql = f"""
    INSERT INTO sales_clean
    SELECT
        s.product_id,
        s.sale_date,
        s.sales_volume,
        s.price,
        s.revenue,
        s.is_missing,
        multiIf(
            s.is_missing = 1 OR s.sales_volume IS NULL OR s.sales_volume <= 0, 'missing',
            c.day_type = 'holiday' AND s.sales_volume > bl.expected * {ANOMALY_THRESHOLD}, 'anomaly',
            c.day_type = 'promotion' AND s.sales_volume > bl.expected * {ANOMALY_THRESHOLD}, 'anomaly',
            c.day_type = 'holiday', 'holiday',
            c.day_type = 'promotion', 'promotion',
            c.day_type = 'weekend' AND s.sales_volume > bl.expected * {WEEKEND_THRESHOLD}, 'weekend',
            s.sales_volume > bl.expected * {ANOMALY_THRESHOLD}, 'anomaly',
            'normal'
        ) AS event_type,
        c.day_type
    FROM sales_staging s
    LEFT JOIN calendar c ON s.sale_date = c.date
    LEFT JOIN (
        SELECT product_id, AVG(sales_volume) AS expected
        FROM sales_clean
        WHERE sale_date BETWEEN '{bs}' AND '{be}'
          AND is_missing = 0 AND sales_volume > 0
        GROUP BY product_id
    ) bl ON s.product_id = bl.product_id
    WHERE s.sale_date = '{sale_date_str}'
    """
    try:
        client.execute(sql)
        return True
    except Exception as e:
        print(f"    [SQL清洗失败] {sale_date_str}: {e}")
        return False


# ==================== Step 3: 导出清洗结果到 OSS ====================

def _export_clean_to_oss(client, sale_date_str):
    """将当日清洗结果写回 OSS"""
    oss_key = f"{OSS_CLEAN_PREFIX}{sale_date_str}.csv"
    url = _s3_url(oss_key)
    struct = "'product_id UInt64, sale_date Date, sales_volume Nullable(Int32), price Float64, revenue Nullable(Float64), is_missing UInt8, event_type String, day_type String'"
    sql = (
        f"INSERT INTO FUNCTION s3('{url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames', {struct}) "
        f"SELECT product_id, sale_date, sales_volume, price, revenue, "
        f"is_missing, event_type, day_type "
        f"FROM sales_clean "
        f"WHERE sale_date = '{sale_date_str}'"
    )
    try:
        client.execute(sql)
        return True
    except Exception as e:
        print(f"    [OSS导出跳过] {sale_date_str}: {e}")
        return False


# ==================== 整日管道 ====================

def _process_one_day(client, oss_key, sale_date_str, existing_dates):
    """处理单日：加载 → SQL清洗 → 导出OSS"""
    if sale_date_str in existing_dates:
        return 0, True  # 已处理，跳过

    ok_load = _load_one_day_to_staging(client, oss_key)
    if not ok_load:
        return 0, False

    ok_clean = _sql_clean_day(client, sale_date_str)
    if not ok_clean:
        # 回滚：清空 staging
        client.execute('TRUNCATE TABLE sales_staging')
        return 0, False

    # 获取清洗后行数
    r = client.execute(f"SELECT count() FROM sales_clean WHERE sale_date = '{sale_date_str}'")
    n = r[0][0]

    if n > 0:
        _export_clean_to_oss(client, sale_date_str)

        # 写入 daily_stats
        try:
            client.execute(f"""
                INSERT INTO daily_stats
                SELECT sale_date, event_type, count() AS record_count,
                       uniqExact(product_id) AS active_products,
                       sumIf(sales_volume, is_missing = 0) AS total_vol,
                       sumIf(revenue, is_missing = 0) AS total_rev,
                       countIf(is_missing = 1) AS missing_cnt,
                       countIf(event_type = 'anomaly') AS anomaly_cnt,
                       countIf(event_type = 'promotion') AS promo_cnt,
                       countIf(event_type = 'holiday') AS holiday_cnt
                FROM sales_clean
                WHERE sale_date = '{sale_date_str}'
                GROUP BY sale_date, event_type
            """)
        except Exception as e:
            print(f"    [daily_stats失败] {sale_date_str}: {e}")

    # 清空 staging
    client.execute('TRUNCATE TABLE sales_staging')
    return n, True


# ==================== 公开接口 ====================

def backfill_daily_stats(client=None):
    """回填所有缺失的 daily_stats"""
    if client is None:
        client = Client(
            host=get_database_config()['host'],
            port=get_database_config()['port'],
            user=get_database_config()['user'],
            password=get_database_config()['password'],
            database=get_database_config()['database'],
            connect_timeout=10, send_receive_timeout=60
        )

    try:
        r = client.execute("""SELECT DISTINCT s.sale_date FROM sales_clean s
            LEFT JOIN daily_stats d ON s.sale_date = d.sale_date
            WHERE d.sale_date IS NULL ORDER BY s.sale_date""")
        missing = [str(row[0]) for row in r]
    except Exception:
        missing = []

    if not missing:
        print("  daily_stats 已全部最新，无需回填")
        return

    print(f"  回填 {len(missing)} 天 daily_stats...")
    for ds in missing:
        try:
            client.execute(f"""
                INSERT INTO daily_stats
                SELECT sale_date, event_type, count() AS record_count,
                       uniqExact(product_id) AS active_products,
                       sumIf(sales_volume, is_missing = 0) AS total_vol,
                       sumIf(revenue, is_missing = 0) AS total_rev,
                       countIf(is_missing = 1) AS missing_cnt,
                       countIf(event_type = 'anomaly') AS anomaly_cnt,
                       countIf(event_type = 'promotion') AS promo_cnt,
                       countIf(event_type = 'holiday') AS holiday_cnt
                FROM sales_clean
                WHERE sale_date = '{ds}'
                GROUP BY sale_date, event_type
            """)
        except Exception as e:
            print(f"    [回填失败] {ds}: {e}")
    print(f"  回填完成")


def run_sql_pipeline(start_date=None, end_date=None, batch_size=30):
    """
    主入口：OSS → CK SQL 清洗 → OSS 回写
    - 自动跳过已有数据
    - 分批处理，输出进度
    """
    t0 = time.time()

    client = Client(
        host=get_database_config()['host'],
        port=get_database_config()['port'],
        user=get_database_config()['user'],
        password=get_database_config()['password'],
        database=get_database_config()['database'],
        connect_timeout=10,
        send_receive_timeout=300,  # S3 批量导入超时
        settings={'max_insert_block_size': 100000}
    )

    # 1. 初始化日历表
    print("\n[1/4] 初始化日历表...")
    init_calendar(client)

    # 2. 检查已有清洗数据
    existing = set()
    try:
        r = client.execute("SELECT DISTINCT sale_date FROM sales_clean ORDER BY sale_date")
        existing = {str(row[0]) for row in r}
    except Exception:
        pass
    print(f"\n[2/4] sales_clean 已有 {len(existing)} 天数据")

    # 3. 列出 OSS 文件
    print("\n[3/4] 扫描 OSS 销量文件...")
    oss_files = list_oss_sales_files()
    if not oss_files:
        print("  [跳过] OSS 上无销量文件")
        client.disconnect()
        return

    # 提取日期
    all_dates = []
    for k in oss_files:
        fname = os.path.basename(k)
        ds = fname.replace('.csv', '')
        if start_date and ds < start_date:
            continue
        if end_date and ds > end_date:
            continue
        all_dates.append((ds, k))

    pending = [(d, k) for d, k in all_dates if d not in existing]
    total_days = len(all_dates)
    pending_days = len(pending)
    print(f"  OSS 共 {total_days} 天，需处理 {pending_days} 天")

    if pending_days == 0:
        print("  [跳过] 全部日期已处理")
        client.disconnect()
        return

    # 4. 逐日处理
    print(f"\n[4/4] SQL 清洗管道运行中...")
    total_rows = 0
    done = 0
    fail = 0

    for i, (ds, oss_key) in enumerate(pending):
        n, ok = _process_one_day(client, oss_key, ds, set())
        if ok and n > 0:
            total_rows += n
            done += 1
        elif not ok:
            fail += 1

        if (i + 1) % batch_size == 0 or i == 0 or i == pending_days - 1:
            elapsed = time.time() - t0
            rate = total_rows / elapsed if elapsed > 0 else 0
            pct = (i + 1) / pending_days * 100
            bar_len = 20
            filled = int(bar_len * (i + 1) / pending_days)
            bar = '█' * filled + '░' * (bar_len - filled)
            print(f"  [{bar}] {i+1}/{pending_days} ({pct:.0f}%) | "
                  f"累计{total_rows:,}行 | 速率{rate:.0f}行/s | "
                  f"成功{done} 失败{fail}")

    # 统计规则分布
    try:
        r = client.execute("""
            SELECT event_type, count() AS cnt
            FROM sales_clean
            WHERE sale_date >= (SELECT min(sale_date) FROM sales_clean
                                WHERE sale_date >= '{}' AND sale_date <= '{}')
            GROUP BY event_type ORDER BY cnt DESC
        """.format(pending[0][0] if pending else '2020-01-01',
                   pending[-1][0] if pending else '2024-12-31'))
        if r:
            print(f"\n【规则检测统计】")
            for ev, cnt in r:
                print(f"  {ev:<12} {cnt:>12,}")
    except Exception:
        pass

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"SQL 管道完成！处理 {done} 天/{pending_days} 天")
    print(f"新增 {total_rows:,} 行，总耗时 {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"清洗结果已写入 OSS: {OSS_CLEAN_PREFIX}")

    # 回填 daily_stats
    backfill_daily_stats(client)

    client.disconnect()


# ==================== 一键初始化维度表 ====================

def _csv_to_ck(client, table, csv_path, columns):
    """将本地 CSV 分批写入 ClickHouse（不依赖 pandas）"""
    import csv, math
    if not os.path.exists(csv_path):
        print(f"  [跳过] 文件不存在: {csv_path}")
        return 0

    # 检查 CK 是否已有
    try:
        r = client.execute(f"SELECT count() FROM {table}")
        if r[0][0] > 0:
            print(f"  [跳过] {table} 已有 {r[0][0]} 条数据")
            return r[0][0]
    except Exception:
        pass

    # 获取列类型映射
    type_map = {}
    try:
        desc = client.execute(f"DESC TABLE {table}")
        for row in desc:
            type_map[row[0]] = row[1]
    except Exception:
        pass

    # 先清空
    try:
        client.execute(f"TRUNCATE TABLE IF EXISTS {table}")
    except Exception:
        pass

    total = 0
    batch = []
    col_list = [c.strip() for c in columns.split(',')]

    from datetime import datetime, date

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vals = []
            for c in col_list:
                raw = row.get(c, '').strip()
                if raw == '' or raw == 'nan':
                    vals.append(None)
                else:
                    ctype = type_map.get(c, 'String')
                    is_date = 'Date' in ctype and 'DateTime' not in ctype
                    if is_date:
                        try:
                            vals.append(datetime.strptime(raw[:10], '%Y-%m-%d').date())
                        except (ValueError, TypeError):
                            vals.append(None)
                    elif 'Int' in ctype or 'UInt' in ctype:
                        try:
                            vals.append(int(float(raw)))
                        except (ValueError, TypeError):
                            vals.append(None)
                    elif 'Float' in ctype:
                        try:
                            vals.append(float(raw))
                        except (ValueError, TypeError):
                            vals.append(None)
                    else:
                        vals.append(raw)
            batch.append(tuple(vals))
            if len(batch) >= 50000:
                client.execute(f'INSERT INTO {table} ({columns}) VALUES', batch)
                total += len(batch)
                batch = []
        if batch:
            client.execute(f'INSERT INTO {table} ({columns}) VALUES', batch)
            total += len(batch)

    print(f"  [OK] {table}: {total} 条")
    return total


def _upload_file_to_oss(local_path, oss_key):
    """上传文件到 OSS"""
    try:
        bucket = get_oss_bucket()
        bucket.put_object_from_file(oss_key, local_path)
        return True
    except Exception as e:
        print(f"  [OSS上传失败] {oss_key}: {e}")
        return False


def init_dimension_tables():
    """初始化维度表（categories, products）"""
    client = Client(
        host=get_database_config()['host'],
        port=get_database_config()['port'],
        user=get_database_config()['user'],
        password=get_database_config()['password'],
        database=get_database_config()['database'],
        connect_timeout=10, send_receive_timeout=60
    )

    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'ecommerce_data')

    # categories
    cat_path = os.path.join(data_dir, 'categories.csv')
    n = _csv_to_ck(client, 'categories', cat_path,
                    'category, category_id, hierarchy, weight, price, parent')
    if n > 0:
        _upload_file_to_oss(cat_path, 'ecommerce_data/categories.csv')

    # products
    prod_path = os.path.join(data_dir, 'products.csv')
    n = _csv_to_ck(client, 'products', prod_path,
                    'product_id, name, category_id, price, weight, status, effective_date, expiration_date')
    if n > 0:
        _upload_file_to_oss(prod_path, 'ecommerce_data/products.csv')

    client.disconnect()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='SQL 清洗管道')
    parser.add_argument('--start', default='2020-01-01', help='开始日期')
    parser.add_argument('--end', default='2024-12-31', help='结束日期')
    parser.add_argument('--batch', type=int, default=30, help='每批天数')
    parser.add_argument('--init-dims', action='store_true', help='仅初始化维度表')
    args = parser.parse_args()

    if args.init_dims:
        init_dimension_tables()
    else:
        run_sql_pipeline(start_date=args.start, end_date=args.end, batch_size=args.batch)
