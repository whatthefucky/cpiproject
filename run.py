#!/usr/bin/env python3
"""
电商数据项目 — 统一入口（SQL 管道提速版）
一句话运行：
    .\env\Scripts\python.exe run.py full

步骤：
  full = 建库建表 + 维度数据 + 4年清洗 + CPI计算 + Web
"""
import sys, os, time
from datetime import datetime, date, timedelta

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _PROJECT_ROOT)


def _progress_bar(current, total, prefix='', extra='', width=40):
    """打印单行进度条"""
    if total == 0:
        return
    pct = current / total * 100
    filled = int(width * current / total)
    bar = '#' * filled + '-' * (width - filled)
    try:
        sys.stdout.write(f"\r{prefix} [{bar}] {current}/{total} ({pct:.0f}%) | {extra}    ")
    except UnicodeEncodeError:
        sys.stdout.write(f"\r{prefix} [{bar}] {current}/{total} ({pct:.0f}%)    ")
    sys.stdout.flush()
    if current >= total:
        sys.stdout.write('\n')


def cmd_test(args):
    """测试 ClickHouse + OSS 连接"""
    from src.db.connection import get_clickhouse, get_oss_bucket
    ok = True
    try:
        client = get_clickhouse()
        v = client.execute('SELECT version()')[0][0]
        print(f"  ClickHouse: OK (version {v})")
        client.disconnect()
    except Exception as e:
        print(f"  ClickHouse: FAIL ({e})")
        ok = False
    try:
        bucket = get_oss_bucket()
        info = bucket.get_bucket_info()
        print(f"  OSS: OK (bucket {info.name})")
    except Exception as e:
        print(f"  OSS: FAIL ({e})")
        ok = False
    sys.exit(0 if ok else 1)


def cmd_init_dims(args):
    """初始化整个数据库（建库 + 建表 + 维度数据）"""
    print("=" * 50)
    print("初始化数据库结构与维度数据")
    print("=" * 50)
    from src.etl.sql_pipeline import init_database
    init_database()
    print("[完成] 初始化")


def cmd_pipeline(args):
    """SQL 清洗管道：oss() 内网直读 OSS，单条 SQL 完成清洗"""
    print("=" * 50)
    print("SQL 清洗管道 (oss() 内网直读 + 异常检测)")
    print("=" * 50)
    from src.etl.sql_pipeline import run_sql_pipeline
    run_sql_pipeline(
        start_date=args.start or '2020-01-01',
        end_date=args.end or '2024-12-31',
        batch_size=args.batch or 50,
    )


def cmd_cpi(args):
    """SQL CPI 计算"""
    print("=" * 50)
    print("SQL CPI 费雪指数计算（全层级类目）")
    print("=" * 50)
    from src.cpi.sql_calculator import compute_cpi_sql
    start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else date(2020, 1, 1)
    end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else date.today()
    base = datetime.strptime(args.base, '%Y-%m-%d').date() if args.base else start
    compute_cpi_sql(base, start, end, force=args.force)


def cmd_web(args):
    from src.web.app import start
    start(host=args.host or '0.0.0.0', port=args.port or 5050, debug=args.debug)


def cmd_full(args):
    """
    一键全流程（推荐）：
    1. 自动建库 + 建表 + 维度数据
    2. SQL 清洗全部缺失天数（跳过 staging，极速）
    3. CPI 计算（全层级类目）
    4. 启动 Web 界面
    """
    t0 = time.time()
    print("=" * 60)
    print("一键全流程启动")
    print("=" * 60)

    # === Step 1: 初始化数据库（建库 + 建表 + 维度数据）===
    print("\n【步骤 1/4】初始化数据库")
    from src.etl.sql_pipeline import init_database
    init_database()

    # === Step 2: SQL 清洗 ===
    print("\n【步骤 2/4】SQL 清洗管道")
    from src.etl.sql_pipeline import run_sql_pipeline
    start_str = args.start or '2020-01-01'
    end_str = args.end or '2024-12-31'
    run_sql_pipeline(start_date=start_str, end_date=end_str, batch_size=50)

    # === Step 3: CPI 计算 ===
    print("\n【步骤 3/4】SQL CPI 计算")
    from src.cpi.sql_calculator import compute_cpi_sql
    start_d = datetime.strptime(start_str, '%Y-%m-%d').date()
    end_d = datetime.strptime(end_str, '%Y-%m-%d').date()
    compute_cpi_sql(base_date=start_d, start_date=start_d, end_date=end_d, force=True)

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"全流程完成！总耗时 {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'='*60}")

    # === Step 4: Web ===
    print("\n【步骤 4/4】启动 Web 界面")
    from src.web.app import start
    start(host='0.0.0.0', port=args.port or 5050)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='电商数据项目 — 一键运行（新实例只需改 config.ini + run.py full）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  run.py test                       # 测试连接
  run.py init-dims                  # 初始化数据库（建库+建表+维度数据）
  run.py pipeline                   # SQL 清洗全部（S3 直写，跳过 staging）
  run.py cpi --start 2020-01-01     # CPI 计算
  run.py web --port 5050            # 启动 Web
  run.py full                       # ★ 一键全流程（新实例只需这个）
        """
    )
    subparsers = parser.add_subparsers(dest='command', help='子命令')

    subparsers.add_parser('test', help='测试 ClickHouse + OSS 连接')

    subparsers.add_parser('init-dims', help='初始化数据库（建库 + 建表 + 维度数据）')

    pipe_p = subparsers.add_parser('pipeline', help='SQL 清洗管道（oss() 内网直读 OSS）')
    pipe_p.add_argument('--start', default='2020-01-01')
    pipe_p.add_argument('--end', default='2024-12-31')
    pipe_p.add_argument('--batch', type=int, default=50)

    cpi_p = subparsers.add_parser('cpi', help='SQL CPI 费雪指数计算（全层级类目）')
    cpi_p.add_argument('--base', default='2020-01-01')
    cpi_p.add_argument('--start', required=True)
    cpi_p.add_argument('--end', required=True)
    cpi_p.add_argument('--force', action='store_true', help='强制重新计算')

    web_p = subparsers.add_parser('web', help='启动 Web 可视化界面')
    web_p.add_argument('--host', default='0.0.0.0')
    web_p.add_argument('--port', type=int, default=5050)
    web_p.add_argument('--debug', action='store_true')

    full_p = subparsers.add_parser('full', help='★ 一键全流程：建库→建表→维度→清洗→CPI→Web')
    full_p.add_argument('--start', default='2020-01-01')
    full_p.add_argument('--end', default='2024-12-31')
    full_p.add_argument('--port', type=int, default=5050)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        sys.exit(1)

    commands = {
        'test': cmd_test,
        'init-dims': cmd_init_dims,
        'pipeline': cmd_pipeline,
        'cpi': cmd_cpi,
        'web': cmd_web,
        'full': cmd_full,
    }
    commands[args.command](args)


if __name__ == '__main__':
    main()
