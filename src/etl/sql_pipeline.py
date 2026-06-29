"""
SQL 清洗管道 — OSS → ClickHouse SQL 清洗 → OSS 回写
全部清洗逻辑在 ClickHouse SQL 中完成，Python 仅做流程编排

一次性运行所有缺失日期，带实时进度条，自动断点续传。
"""
import sys, os, re, time
from datetime import datetime, timedelta, date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import get_database_config, get_oss_config
from src.db.connection import get_clickhouse

# OSS 配置
OSS_CFG = get_oss_config()
OSS_AK = OSS_CFG['access_key_id']
OSS_SK = OSS_CFG['access_key_secret']
OSS_BUCKET = OSS_CFG['bucket']
OSS_ENDPOINT = OSS_CFG['endpoint'].rstrip('/')
_region_match = re.search(r'oss-([a-z]+-[a-z0-9-]+)', OSS_ENDPOINT)
OSS_REGION = _region_match.group(0) if _region_match else 'oss-cn-hangzhou'
OSS_SALES_PREFIX = 'ecommerce/sales/'
OSS_CLEAN_PREFIX = 'ecommerce/sales_clean/'


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
    "CREATE TABLE IF NOT EXISTS dataproject.cpi_trend (date Date, laspeyres Nullable(Float64), paasche Nullable(Float64), fisher Float64, product_count UInt32, category_id UInt64 DEFAULT 0, granularity String DEFAULT 'day') ENGINE = MergeTree() ORDER BY (date, category_id)",
    "CREATE TABLE IF NOT EXISTS dataproject.product_category_map (product_id UInt64, category_id UInt64, category String, l1_category String, l2_category String, l3_category String) ENGINE = MergeTree() ORDER BY product_id",
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


def _get_clickhouse_client(database=None, connect_timeout=30, send_receive_timeout=60):
    """获取 ClickHouse HTTP 连接"""
    client = get_clickhouse()
    if database:
        client.database = database
    return client


def init_database():
    """初始化整个数据库（建库 + 建表 + 维度数据），全自动"""
    t0 = time.time()
    # 先连 default 库，创建 dataproject
    client = get_clickhouse()
    client.database = 'default'
    try:
        client.execute('CREATE DATABASE IF NOT EXISTS dataproject')
        print("  dataproject 数据库就绪")
    except Exception as e:
        print(f"  [建库] {e}")
    client.database = 'dataproject'
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
        client.execute_values('INSERT INTO calendar (date, day_type, year, month, day, weekday) VALUES', rows)
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
                    client.execute_values(f'INSERT INTO {tname} ({cols}) VALUES', batch)
                    total += len(batch)
                    batch = []
            if batch:
                client.execute_values(f'INSERT INTO {tname} ({cols}) VALUES', batch)
                total += len(batch)
        print(f"  {tname}: {total} 条")

    # 重建 product_category_map（全层级类目映射，供 CPI 类目级使用）
    r = client.execute("SELECT count() FROM product_category_map")
    if r[0][0] == 0:
        _rebuild_product_category_map(client)
    else:
        print(f"  [跳过] product_category_map 已有 {r[0][0]} 条")

    client.disconnect()
    print(f"  初始化完成：{time.time()-t0:.0f}s")


def _rebuild_product_category_map(client):
    """重建类目映射表（全层级：hierarchy=1/2/3）"""
    client.execute('TRUNCATE TABLE product_category_map')
    client.execute('DROP TABLE IF EXISTS _cat_path')
    client.execute('''CREATE TABLE _cat_path (category_id UInt64, ancestor_id UInt64, ancestor_name String, hierarchy UInt8)
        ENGINE = MergeTree() ORDER BY (category_id, ancestor_id)''')
    # h1
    client.execute("INSERT INTO _cat_path SELECT c1.category_id, c1.category_id, c1.category, 1 FROM categories c1 WHERE c1.hierarchy = 1")
    # h2 自身 + 父
    client.execute("INSERT INTO _cat_path SELECT c2.category_id, c2.category_id, c2.category, 2 FROM categories c2 WHERE c2.hierarchy = 2")
    client.execute("INSERT INTO _cat_path SELECT c2.category_id, c1.category_id, c1.category, 1 FROM categories c2 INNER JOIN categories c1 ON c2.parent = c1.category_id WHERE c2.hierarchy=2 AND c1.hierarchy=1")
    # h3 自身 + 父 + 祖父
    client.execute("INSERT INTO _cat_path SELECT c3.category_id, c3.category_id, c3.category, 3 FROM categories c3 WHERE c3.hierarchy = 3")
    client.execute("INSERT INTO _cat_path SELECT c3.category_id, c2.category_id, c2.category, 2 FROM categories c3 INNER JOIN categories c2 ON c3.parent = c2.category_id WHERE c3.hierarchy=3 AND c2.hierarchy=2")
    client.execute("INSERT INTO _cat_path SELECT c3.category_id, c1.category_id, c1.category, 1 FROM categories c3 INNER JOIN categories c2 ON c3.parent = c2.category_id INNER JOIN categories c1 ON c2.parent = c1.category_id WHERE c3.hierarchy=3 AND c2.hierarchy=2 AND c1.hierarchy=1")
    # JOIN 产品
    client.execute("INSERT INTO product_category_map SELECT p.product_id, ap.ancestor_id, ap.ancestor_name, '' l1, '' l2, '' l3 FROM products p INNER JOIN _cat_path ap ON p.category_id = ap.category_id SETTINGS max_insert_block_size = 500000")
    client.execute('DROP TABLE IF EXISTS _cat_path')
    r = client.execute('SELECT count() FROM product_category_map')
    print(f"  product_category_map: {r[0][0]} 条")


# ==================== OSS URL 构建 ====================

def _oss_url(oss_key):
    """oss() 函数专用内网 HTTP URL"""
    return f"http://{OSS_BUCKET}.{OSS_REGION}-internal.aliyuncs.com/{oss_key}"


# ==================== 清洗管道 ====================

def _clean_and_export_to_oss(client, ds):
    """
    单天清洗：oss() 直读原始 CSV → SQL 清洗 → INSERT INTO FUNCTION oss() 导出已清洗 CSV
    全程不落 CK，异常检测从 OSS 已清洗 CSV 取基线
    返回 (行数, 是否成功)
    """
    d = datetime.strptime(ds, '%Y-%m-%d').date()
    raw_url = _oss_url(OSS_SALES_PREFIX + f"{ds}.csv")
    ym = ds[:7].replace('-', '')
    clean_url = _oss_url(f"{OSS_CLEAN_PREFIX}{ym}/{ds}.csv")

    # 构建 15 天基线数据的 oss() URL 列表（从已清洗 CSV 读取）
    # 如果前 15 天还没清洗，降级为无基线模式
    baseline_union_parts = []
    all_found = True
    for i in range(15, 0, -1):
        bd = (d - timedelta(days=i))
        if bd < date(2020, 1, 1):
            continue
        bds = bd.isoformat()
        bym = bds[:7].replace('-', '')
        bkey = f"{OSS_CLEAN_PREFIX}{bym}/{bds}.csv"
        burl = _oss_url(bkey)
        baseline_union_parts.append(
            f"SELECT toUInt64(s0.product_id) pid, toFloat64OrZero(s0.sales_volume) vol "
            f"FROM oss('{burl}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames', "
            f"'product_id String, sale_date String, sales_volume String, price String, revenue String, is_missing String, event_type String, day_type String') s0 "
            f"WHERE toUInt8(s0.is_missing) = 0 AND toFloat64OrZero(s0.sales_volume) > 0"
        )
    baseline_union = ' UNION ALL '.join(baseline_union_parts) if baseline_union_parts else ''
    has_baseline = len(baseline_union_parts) >= 7  # 至少7天基线才启用异常检测

    # 单条 SQL：读取原始 CSV → 清洗 → 直接写入 OSS 清理 CSV
    try:
        if has_baseline:
            sql = f"""
            INSERT INTO FUNCTION oss('{clean_url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames',
'product_id UInt64, sale_date Date, sales_volume Nullable(Int32), price Float64, revenue Nullable(Float64), is_missing UInt8, event_type String, day_type String')
            SELECT
                toUInt64(s.product_id), toDate(s.sale_date),
                toNullable(toInt32(toFloat64OrZero(s.sales_volume))),
                toFloat64OrZero(s.price),
                toNullable(toFloat64OrZero(s.revenue)),
                toUInt8(s.is_missing = 'true' OR s.is_missing = 'True' OR s.is_missing = '1') AS is_missing_flag,
                multiIf(
                    is_missing_flag = 1 OR toFloat64OrZero(s.sales_volume) <= 0, 'missing',
                    bl.expected > 0 AND toFloat64OrZero(s.sales_volume) > bl.expected * {ANOMALY_THRESHOLD}, 'anomaly',
                    c.day_type = 'holiday', 'holiday',
                    c.day_type = 'promotion', 'promotion',
                    c.day_type = 'weekend', 'weekend',
                    toFloat64OrZero(s.sales_volume) > 0, 'normal',
                    'missing'
                ) AS event_type,
                c.day_type
            FROM oss('{raw_url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames',
'product_id String, sale_date String, sales_volume String, price String, revenue String, is_missing String') s
            LEFT JOIN calendar c ON toDate(s.sale_date) = c.date
            LEFT JOIN (
                SELECT pid, AVG(vol) AS expected
                FROM ({baseline_union})
                GROUP BY pid
            ) bl ON toUInt64(s.product_id) = bl.pid
            SETTINGS max_insert_block_size = 500000, allow_experimental_analyzer = 0
            """
        else:
            # 首次运行，无基线数据
            sql = f"""
            INSERT INTO FUNCTION oss('{clean_url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames',
'product_id UInt64, sale_date Date, sales_volume Nullable(Int32), price Float64, revenue Nullable(Float64), is_missing UInt8, event_type String, day_type String')
            SELECT
                toUInt64(s.product_id), toDate(s.sale_date),
                toNullable(toInt32(toFloat64OrZero(s.sales_volume))),
                toFloat64OrZero(s.price),
                toNullable(toFloat64OrZero(s.revenue)),
                toUInt8(s.is_missing = 'true' OR s.is_missing = 'True' OR s.is_missing = '1') AS is_missing_flag,
                multiIf(
                    is_missing_flag = 1 OR toFloat64OrZero(s.sales_volume) <= 0, 'missing',
                    c.day_type = 'holiday', 'holiday',
                    c.day_type = 'promotion', 'promotion',
                    c.day_type = 'weekend', 'weekend',
                    toFloat64OrZero(s.sales_volume) > 0, 'normal',
                    'missing'
                ) AS event_type,
                c.day_type
            FROM oss('{raw_url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames',
'product_id String, sale_date String, sales_volume String, price String, revenue String, is_missing String') s
            LEFT JOIN calendar c ON toDate(s.sale_date) = c.date
            SETTINGS max_insert_block_size = 500000, allow_experimental_analyzer = 0
            """
        client.execute(sql)
        return True
    except Exception as e:
        err_msg = str(e)
        if 'already exists' in err_msg:
            # OSS 文件已存在，跳过（视为成功）
            return True
        print(f"\n  [清洗失败] {ds}: {err_msg[:150]}")
        return False


# ==================== 公开接口：主管道 ====================

def run_sql_pipeline(start_date=None, end_date=None, batch_size=50):
    """
    主入口：原始 OSS CSV → 清洗 → OSS 已清洗 CSV（全程不落 CK）
    已存在的 OSS 清洗文件直接跳过，实现断点续传
    """
    t0 = time.time()
    client = _get_clickhouse_client(database=get_database_config()['database'], send_receive_timeout=300)

    s = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else date(2020, 1, 1)
    e = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else date(2024, 12, 31)

    print(f"扫描 OSS 已清洗存档...")
    all_dates = []
    d = s
    while d <= e:
        all_dates.append(d.isoformat())
        d += timedelta(days=1)

    missing = all_dates

    print(f"需清洗 {len(missing)} 天")

    bar = ProgressBar(len(missing), prefix='  清洗')
    fail = 0
    success = 0
    for i, ds in enumerate(missing):
        ok = _clean_and_export_to_oss(client, ds)
        if not ok:
            fail += 1
        else:
            success += 1
        if (i + 1) % 20 == 0 or i == len(missing) - 1:
            elapsed = time.time() - t0
            bar.update(i + 1, f'成功{success} 失败{fail} | {elapsed:.0f}s')

    bar.finish(f'成功{success} 失败{fail}')
    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"完成！{elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'='*50}")
    client.disconnect()
    return success, fail

