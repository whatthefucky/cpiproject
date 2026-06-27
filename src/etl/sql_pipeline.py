"""
SQL 清洗管道 — OSS → ClickHouse SQL 清洗 → OSS 回写
全部清洗逻辑在 ClickHouse SQL 中完成，Python 仅做流程编排

一次性运行所有缺失日期，带实时进度条，自动断点续传。
"""
import sys, os, re, time
from datetime import datetime, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from clickhouse_driver import Client
from config import get_database_config, get_oss_config

# OSS 配置
OSS_CFG = get_oss_config()
OSS_AK = OSS_CFG['access_key_id']
OSS_SK = OSS_CFG['access_key_secret']
OSS_BUCKET = OSS_CFG['bucket']
OSS_ENDPOINT = OSS_CFG['endpoint'].rstrip('/')
_region_match = re.search(r'oss-([a-z]+-[a-z0-9-]+)', OSS_ENDPOINT)
OSS_REGION = _region_match.group(0) if _region_match else 'oss-cn-hangzhou'
OSS_SALES_PREFIX = 'ecommerce/sales/'


# 节日/促销日历（2020-2024）
HOLIDAYS = set()
PROMOTIONS = set()
for y in range(2020, 2026):
    ys = str(y)
    HOLIDAYS.update([f"{ys}-01-01", f"{ys}-10-01", f"{ys}-10-02", f"{ys}-10-03"])
    PROMOTIONS.update([f"{ys}-06-18", f"{ys}-11-11"])

ANOMALY_THRESHOLD = 3.0
WEEKEND_THRESHOLD = 1.5

# ==================== 进度条 ====================

class ProgressBar:
    """实时更新进度条（单行刷新）"""
    def __init__(self, total, width=40, prefix=''):
        self.total = total
        self.width = width
        self.prefix = prefix
        self.start = time.time()
        self._last_len = 0

    def update(self, current, extra=''):
        elapsed = time.time() - self.start
        if self.total > 0:
            pct = current / self.total * 100
            filled = int(self.width * current / self.total)
        else:
            pct = 0
            filled = 0
        bar = '#' * filled + '-' * (self.width - filled)
        line = f"\r{self.prefix} [{bar}] {current}/{self.total} ({pct:.0f}%) | {extra} | {elapsed:.0f}s"
        try:
            sys.stdout.write(line + ' ' * max(0, self._last_len - len(line)))
        except UnicodeEncodeError:
            # 降级到纯 ASCII
            sys.stdout.write(f"\r{self.prefix} [{bar}] {current}/{self.total} ({pct:.0f}%) | {elapsed:.0f}s")
        self._last_len = len(line)
        sys.stdout.flush()

    def finish(self, extra=''):
        self.update(self.total, extra)
        sys.stdout.write('\n')


# ==================== 建库 + 建表 ====================

_INIT_DDL = [
    "CREATE TABLE IF NOT EXISTS dataproject.calendar (date Date, day_type String, year UInt16, month UInt8, day UInt8, weekday UInt8) ENGINE = MergeTree() ORDER BY date",
    "CREATE TABLE IF NOT EXISTS dataproject.categories (category String, category_id UInt64, hierarchy UInt8, weight Nullable(Float64), price Nullable(Float64), parent Nullable(UInt64)) ENGINE = MergeTree() ORDER BY category_id",
    "CREATE TABLE IF NOT EXISTS dataproject.products (product_id UInt64, name String, category_id UInt64, price Float64, weight Float64, status UInt8, effective_date Date, expiration_date Nullable(Date)) ENGINE = MergeTree() ORDER BY (product_id, effective_date)",
    "CREATE TABLE IF NOT EXISTS dataproject.sales_staging (product_id UInt64, sale_date Date, sales_volume Nullable(Int32), price Float64, revenue Nullable(Float64), is_missing UInt8) ENGINE = MergeTree() ORDER BY (sale_date, product_id)",
    "CREATE TABLE IF NOT EXISTS dataproject.sales_clean (product_id UInt64, sale_date Date, sales_volume Nullable(Int32), price Float64, revenue Nullable(Float64), is_missing UInt8, event_type String, day_type String) ENGINE = MergeTree() PARTITION BY toYYYYMM(sale_date) ORDER BY (sale_date, product_id)",
    "CREATE TABLE IF NOT EXISTS dataproject.daily_stats (sale_date Date, event_type String, record_count UInt64, active_products UInt64, total_sales_volume Nullable(Int64), total_revenue Nullable(Float64), missing_count UInt64, anomaly_count UInt64, promotion_count UInt64, holiday_count UInt64) ENGINE = MergeTree() ORDER BY (sale_date, event_type)",
    "CREATE TABLE IF NOT EXISTS dataproject.cpi_trend (date Date, laspeyres Nullable(Float64), paasche Nullable(Float64), fisher Float64, product_count UInt32, category_id UInt64 DEFAULT 0, granularity String DEFAULT 'day') ENGINE = MergeTree() ORDER BY (date, category_id)",
    "CREATE TABLE IF NOT EXISTS dataproject.cpi_category (date Date, category_id UInt64, category String, hierarchy UInt8, laspeyres Nullable(Float64), paasche Nullable(Float64), fisher Float64, weight Float64 DEFAULT 0) ENGINE = MergeTree() ORDER BY (date, category_id)",
    "CREATE TABLE IF NOT EXISTS dataproject.product_category_map (product_id UInt64, category_id UInt64, category String, l1_category String, l2_category String, l3_category String) ENGINE = MergeTree() ORDER BY product_id",
    "CREATE TABLE IF NOT EXISTS dataproject.anomaly_events (sale_date Date, product_id UInt64, sales_volume Nullable(Int32), expected_volume Nullable(Float64), ratio Nullable(Float64), event_type String) ENGINE = MergeTree() ORDER BY (sale_date, product_id)",
]


def ensure_database(client):
    """确保 dataproject 数据库存在"""
    client.execute('CREATE DATABASE IF NOT EXISTS dataproject')
    print("  dataproject 数据库就绪")


def ensure_tables(client):
    """确保所有表存在"""
    count = 0
    for ddl in _INIT_DDL:
        try:
            client.execute(ddl)
            count += 1
        except Exception:
            pass
    print(f"  {count} 张表就绪")


def init_database():
    """初始化整个数据库（建库 + 建表 + 维度数据），全自动"""
    t0 = time.time()
    db = get_database_config()
    client = Client(host=db['host'], port=db['port'], user=db['user'],
                    password=db['password'], connect_timeout=30, send_receive_timeout=60)
    ensure_database(client)
    client.execute('USE dataproject')
    ensure_tables(client)

    # 日历表
    r = client.execute("SELECT count() FROM calendar")
    if r[0][0] == 0:
        rows = []
        d = date(2020, 1, 1)
        end = date(2024, 12, 31)
        while d <= end:
            ds = d.isoformat()
            wd = d.weekday()
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
        client.execute('INSERT INTO calendar (date, day_type, year, month, day, weekday) VALUES', rows)
        print(f"  calendar: {len(rows)} 天")

    # 维度表
    import csv
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'ecommerce_data')
    for tname, csv_name, cols in [
        ('categories', 'categories.csv', 'category, category_id, hierarchy, weight, price, parent'),
        ('products', 'products.csv', 'product_id, name, category_id, price, weight, status, effective_date, expiration_date'),
    ]:
        csv_path = os.path.join(data_dir, csv_name)
        if not os.path.exists(csv_path):
            print(f"  [跳过] {csv_name} 不存在")
            continue
        r = client.execute(f"SELECT count() FROM {tname}")
        if r[0][0] > 0:
            print(f"  [跳过] {tname} 已有 {r[0][0]} 条")
            continue
        type_map = {}
        try:
            desc = client.execute(f"DESC TABLE {tname}")
            for row in desc:
                type_map[row[0]] = row[1]
        except Exception:
            pass
        client.execute(f"TRUNCATE TABLE IF EXISTS {tname}")
        total = 0
        batch = []
        col_list = [c.strip() for c in cols.split(',')]
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
                    client.execute(f'INSERT INTO {tname} ({cols}) VALUES', batch)
                    total += len(batch)
                    batch = []
            if batch:
                client.execute(f'INSERT INTO {tname} ({cols}) VALUES', batch)
                total += len(batch)
        print(f"  {tname}: {total} 条")

    client.disconnect()
    print(f"  初始化完成：{time.time()-t0:.0f}s")


# ==================== OSS URL 构建 ====================

def _oss_url(oss_key):
    """oss() 函数专用内网 HTTP URL"""
    return f"http://{OSS_BUCKET}.{OSS_REGION}-internal.aliyuncs.com/{oss_key}"


# ==================== 优化管道：oss() 函数服务端直读（阿里云内网，极速）====================

def _oss_clean_insert(client, ds):
    """
    两步完成：
    1. oss() 直读 OSS → sales_staging（简单 INSERT，服务端毫秒级）
    2. SQL 清洗 staging → sales_clean（含异常检测）
    """
    d = datetime.strptime(ds, '%Y-%m-%d').date()
    bs = (d - timedelta(days=15)).isoformat()
    be = (d - timedelta(days=1)).isoformat()
    url = _oss_url(OSS_SALES_PREFIX + f"{ds}.csv")

    # Step 1: oss() 加载到 staging（不加显式类型，让 CK 自动推断）
    try:
        client.execute(f"""
            INSERT INTO sales_staging (product_id, sale_date, sales_volume, price, revenue, is_missing)
            SELECT
                toUInt64(trim(s.product_id)),
                toDate(trim(s.sale_date)),
                toNullable(toInt32(toFloat64OrZero(s.sales_volume))),
                toFloat64OrZero(s.price),
                toNullable(toFloat64OrZero(s.revenue)),
                toUInt8(s.is_missing = 'true' OR s.is_missing = 'True' OR s.is_missing = '1')
            FROM oss('{url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames',
                'product_id String, sale_date String, sales_volume String, price String, revenue String, is_missing String') s
            SETTINGS max_insert_block_size = 500000
        """)
    except Exception as e:
        print(f"\n  [oss_staging失败] {ds}: {str(e)[:150]}")
        return 0, False

    # Step 2: SQL 清洗
    try:
        client.execute(f"""
            INSERT INTO sales_clean
            SELECT s.product_id, s.sale_date, s.sales_volume, s.price, s.revenue, s.is_missing,
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
                WHERE sale_date BETWEEN '{bs}' AND '{be}' AND is_missing = 0 AND sales_volume > 0
                GROUP BY product_id
            ) bl ON s.product_id = bl.product_id
            WHERE s.sale_date = '{ds}'
            SETTINGS allow_experimental_analyzer = 0
        """)
        client.execute('TRUNCATE TABLE sales_staging')
        r = client.execute(f"SELECT count() FROM sales_clean WHERE sale_date = '{ds}'")
        return r[0][0], True
    except Exception as e:
        print(f"\n  [清洗失败] {ds}: {str(e)[:150]}")
        client.execute('TRUNCATE TABLE sales_staging')
        return 0, False


# ==================== 公开接口：主管道 ====================

def run_sql_pipeline(start_date=None, end_date=None, batch_size=50):
    """
    主入口：OSS → CK SQL 清洗 → OSS 回写
    - 跳过 staging 表（S3 函数直写 sales_clean）
    - 自动跳过已有数据（断点续传）
    - 实时进度条
    """
    t0 = time.time()

    client = Client(
        host=get_database_config()['host'],
        port=get_database_config()['port'],
        user=get_database_config()['user'],
        password=get_database_config()['password'],
        database=get_database_config()['database'],
        connect_timeout=10,
        send_receive_timeout=300,
        settings={'max_insert_block_size': 500000, 'allow_experimental_analyzer': 0}
    )

    # 扫描已有数据
    existing = set()
    try:
        r = client.execute("SELECT DISTINCT sale_date FROM sales_clean")
        existing = {str(row[0]) for row in r}
    except Exception:
        pass

    # 计算缺失的日期
    s = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else date(2020, 1, 1)
    e = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else date(2024, 12, 31)
    missing = []
    d = s
    while d <= e:
        if d.isoformat() not in existing:
            missing.append(d.isoformat())
        d += timedelta(days=1)

    total_pending = len(missing)
    print(f"sales_clean 已有 {len(existing)} 天，需处理 {total_pending} 天")

    if total_pending == 0:
        print("  [跳过] 全部日期已处理")
        client.disconnect()
        return

    # 主循环（oss() 内网直读，单条 SQL 完成加载+清洗+标记）
    print(f"使用 oss() 内网直读 OSS...")
    bar = ProgressBar(total_pending, prefix='  清洗')

    total_rows = 0
    done = 0
    fail = 0

    for i, ds in enumerate(missing):
        n, ok = _oss_clean_insert(client, ds)
        if not ok:
            fail += 1
            bar.update(i + 1, f'失败 x{fail}')
            continue

        total_rows += n
        done += 1

        # 回填 daily_stats
        try:
            client.execute(f"""
                INSERT INTO daily_stats
                SELECT sale_date, event_type, count(), uniqExact(product_id),
                    sumIf(sales_volume, is_missing=0), sumIf(revenue, is_missing=0),
                    countIf(is_missing=1), countIf(event_type='anomaly'),
                    countIf(event_type='promotion'), countIf(event_type='holiday')
                FROM sales_clean WHERE sale_date='{ds}'
                GROUP BY sale_date, event_type
            """)
        except Exception:
            pass

        # 每 20 天更新进度
        if (i + 1) % 20 == 0 or i == 0 or i == total_pending - 1:
            elapsed = time.time() - t0
            rate = total_rows / elapsed if elapsed > 0 else 0
            eta_s = (total_pending - i - 1) * elapsed / max(i + 1, 1)
            bar.update(i + 1, f'+{total_rows:,}行 | {rate:.0f}行/s | ETA {eta_s/60:.0f}min')

    bar.finish(f'成功{done} 失败{fail} 累计{total_rows:,}行')

    # 统计
    try:
        r = client.execute("""
            SELECT event_type, count() AS cnt FROM sales_clean
            WHERE sale_date >= %(s)s AND sale_date <= %(e)s
            GROUP BY event_type ORDER BY cnt DESC
        """, {'s': start_date or '2020-01-01', 'e': end_date or '2024-12-31'})
        if r:
            print("\n【规则检测统计】")
            for ev, cnt in r:
                print(f"  {ev:<12} {cnt:>12,}")
    except Exception:
        pass

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"完成！{elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"新增 {total_rows:,} 行，累计 {len(existing) + done} 天")
    print(f"{'='*50}")

    client.disconnect()
    return done, fail

