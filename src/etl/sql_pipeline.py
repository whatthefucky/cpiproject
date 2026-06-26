"""
SQL 清洗管道 — OSS → ClickHouse SQL 清洗 → OSS 回写
全部清洗逻辑在 ClickHouse SQL 中完成，Python 仅做流程编排

一次性运行所有缺失日期，带实时进度条，自动断点续传。
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
OSS_ENDPOINT = OSS_CFG['endpoint'].rstrip('/')
_region_match = re.search(r'oss-([a-z]+-[a-z0-9-]+)', OSS_ENDPOINT)
OSS_REGION = _region_match.group(0) if _region_match else 'oss-cn-hangzhou'
OSS_SALES_PREFIX = 'ecommerce/sales/'
OSS_CLEAN_PREFIX = 'ecommerce/sales_clean_ck/'
OSS_CPI_PREFIX = 'ecommerce/cpi_results/'

# 内网（CK 在阿里云上）
_INTERNAL = True

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
        bar = '█' * filled + '░' * (self.width - filled)
        line = f"\r{self.prefix} [{bar}] {current}/{self.total} ({pct:.0f}%) | {extra} | {elapsed:.0f}s"
        # 覆盖上一行
        sys.stdout.write(line + ' ' * max(0, self._last_len - len(line)))
        self._last_len = len(line)
        sys.stdout.flush()

    def finish(self, extra=''):
        self.update(self.total, extra)
        sys.stdout.write('\n')


# ==================== S3 URL 构建 ====================

def _s3_url(oss_key):
    base = f"https://{OSS_BUCKET}.{OSS_REGION}-internal.aliyuncs.com"
    return f"{base}/{oss_key}"


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
    print(f"  [OK] calendar: {len(rows)} 天")


# ==================== Step 1: 从 OSS 加载到暂存表 ====================

def _load_one_day_via_s3(client, oss_key):
    """
    用 S3 函数从 OSS 加载到 staging（CK 服务器端读取，最快）
    """
    fname = oss_key.split('/')[-1]
    oss_path = f"{OSS_SALES_PREFIX}{fname}"
    url = _s3_url(oss_path)
    sql = f"INSERT INTO sales_staging SELECT * FROM s3('{url}', '{OSS_AK}', '{OSS_SK}', 'CSVWithNames')"
    try:
        client.execute(sql, settings={'max_insert_block_size': 100000})
        return True
    except Exception:
        return False


# ==================== Step 2: SQL 清洗 ====================

def _sql_clean_day(client, sale_date_str):
    """SQL 清洗单天数据到 sales_clean"""
    d = datetime.strptime(sale_date_str, '%Y-%m-%d').date()
    bs = (d - timedelta(days=15)).isoformat()
    be = (d - timedelta(days=1)).isoformat()
    sql = f"""
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
    WHERE s.sale_date = '{sale_date_str}'
    """
    try:
        client.execute(sql)
        return True
    except Exception:
        return False


# ==================== 公开接口：主管道 ====================

def run_sql_pipeline(start_date=None, end_date=None, batch_size=50):
    """
    主入口：OSS → CK SQL 清洗 → OSS 回写
    - 自动跳过已有数据（断点续传）
    - 一次性运行所有缺失日期
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
        settings={'max_insert_block_size': 100000, 'allow_experimental_analyzer': 0}
    )

    # 1. 日历
    print("[1/4] 初始化日历表...")
    init_calendar(client)

    # 2. 扫描已有数据
    existing = set()
    try:
        r = client.execute("SELECT DISTINCT sale_date FROM sales_clean")
        existing = {str(row[0]) for row in r}
    except Exception:
        pass

    # 3. 计算缺失的日期
    s = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else date(2020, 1, 1)
    e = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else date(2024, 12, 31)
    missing = []
    d = s
    while d <= e:
        if d.isoformat() not in existing:
            missing.append(d.isoformat())
        d += timedelta(days=1)

    total_pending = len(missing)
    print(f"[2/4] sales_clean 已有 {len(existing)} 天，需处理 {total_pending} 天")

    if total_pending == 0:
        print("  [跳过] 全部日期已处理")
        client.disconnect()
        return

    # 4. 逐日处理（带进度条）
    print(f"[3/4] SQL 清洗管道运行中...")
    bar = ProgressBar(total_pending, prefix='  清洗')

    total_rows = 0
    done = 0
    fail = 0
    TARGET_RATE = 3.0  # 预期每条约 3 秒

    for i, ds in enumerate(missing):
        oss_key = f"ecommerce/sales/{ds}.csv"
        ok_load = _load_one_day_via_s3(client, oss_key)
        if not ok_load:
            # S3 失败时降级 Python
            ok_load = _load_one_day_via_python_fast(client, oss_key)
        if not ok_load:
            fail += 1
            bar.update(i + 1, f'失败{fail} 下载失败')
            continue

        ok_clean = _sql_clean_day(client, ds)
        client.execute('TRUNCATE TABLE sales_staging')

        if not ok_clean:
            fail += 1
            bar.update(i + 1, f'失败{fail} 清洗失败')
            continue

        r = client.execute(f"SELECT count() FROM sales_clean WHERE sale_date='{ds}'")
        n = r[0][0]
        total_rows += n
        done += 1

        # 每5天更新一次进度
        if (i + 1) % 5 == 0 or i == 0 or i == total_pending - 1:
            rate = total_rows / (time.time() - t0) if time.time() > t0 else 0
            eta_s = (total_pending - i - 1) * (time.time() - t0) / max(i + 1, 1)
            bar.update(i + 1, f'+{total_rows:,}行 | {rate:.0f}行/s | ETA {eta_s/60:.0f}min')

    bar.finish(f'成功{done} 失败{fail} 累计{total_rows:,}行')

    # 5. 统计
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
    print(f"SQL 管道完成！{elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"新增 {total_rows:,} 行，累计 {len(existing) + done} 天")

    # 6. 回填 daily_stats
    print("\n[4/4] 回填 daily_stats...")
    try:
        r = client.execute("""SELECT DISTINCT s.sale_date FROM sales_clean s
            LEFT JOIN daily_stats d ON s.sale_date = d.sale_date
            WHERE d.sale_date IS NULL ORDER BY s.sale_date""")
        missing_ds = [str(row[0]) for row in r]
        if missing_ds:
            for ds in missing_ds:
                try:
                    client.execute(f"""
                        INSERT INTO daily_stats
                        SELECT sale_date, event_type, count(), uniqExact(product_id),
                            sumIf(sales_volume, is_missing=0), sumIf(revenue, is_missing=0),
                            countIf(is_missing=1), countIf(event_type='anomaly'),
                            countIf(event_type='promotion'), countIf(event_type='holiday')
                        FROM sales_clean WHERE sale_date='{ds}' GROUP BY sale_date, event_type
                    """)
                except Exception:
                    pass
            print(f"  回填 {len(missing_ds)} 天")
        else:
            print("  daily_stats 已最新")
    except Exception:
        pass

    client.disconnect()
    return done, fail


def _load_one_day_via_python_fast(client, oss_key):
    """
    Python 降级加载（仅当 S3 函数不可用时使用）
    """
    try:
        bucket = get_oss_bucket()
        obj = bucket.get_object(oss_key)
        content = obj.read()
        lines = content.decode('utf-8-sig').split('\n')
        if not lines:
            return False
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
            try:
                pid = int(float(vals[0])) if vals[0] else 0
                sd = datetime.strptime(vals[1][:10], '%Y-%m-%d').date() if vals[1][:10] else date(2020, 1, 1)
                sv = int(float(vals[2])) if vals[2] and vals[2] not in ('-1', '') else None
                pr = float(vals[3]) if vals[3] else 0.0
                rv = float(vals[4]) if vals[4] else None
                im = 1 if vals[5].lower() in ('true', '1') else 0
                batch.append((pid, sd, sv, pr, rv, im))
            except Exception:
                continue
            if len(batch) >= 50000:
                client.execute('INSERT INTO sales_staging (product_id, sale_date, sales_volume, price, revenue, is_missing) VALUES', batch)
                batch = []
        if batch:
            client.execute('INSERT INTO sales_staging (product_id, sale_date, sales_volume, price, revenue, is_missing) VALUES', batch)
        return True
    except Exception:
        return False


# ==================== 一键初始化维度表 ====================

def _csv_to_ck(client, table, csv_path, columns):
    """将本地 CSV 分批写入 ClickHouse"""
    import csv
    if not os.path.exists(csv_path):
        print(f"  [跳过] 文件不存在: {csv_path}")
        return 0
    try:
        r = client.execute(f"SELECT count() FROM {table}")
        if r[0][0] > 0:
            print(f"  [跳过] {table} 已有 {r[0][0]} 条数据")
            return r[0][0]
    except Exception:
        pass
    type_map = {}
    try:
        desc = client.execute(f"DESC TABLE {table}")
        for row in desc:
            type_map[row[0]] = row[1]
    except Exception:
        pass
    try:
        client.execute(f"TRUNCATE TABLE IF EXISTS {table}")
    except Exception:
        pass
    total = 0
    batch = []
    col_list = [c.strip() for c in columns.split(',')]
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
    cat_path = os.path.join(data_dir, 'categories.csv')
    _csv_to_ck(client, 'categories', cat_path, 'category, category_id, hierarchy, weight, price, parent')
    prod_path = os.path.join(data_dir, 'products.csv')
    _csv_to_ck(client, 'products', prod_path, 'product_id, name, category_id, price, weight, status, effective_date, expiration_date')
    client.disconnect()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='SQL 清洗管道')
    parser.add_argument('--start', default='2020-01-01')
    parser.add_argument('--end', default='2024-12-31')
    parser.add_argument('--batch', type=int, default=50)
    parser.add_argument('--init-dims', action='store_true')
    args = parser.parse_args()
    if args.init_dims:
        init_dimension_tables()
    else:
        run_sql_pipeline(start_date=args.start, end_date=args.end, batch_size=args.batch)
