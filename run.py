#!/usr/bin/env python3
"""
电商数据项目 — 统一入口（SQL 管道版）
用法：
    .\env\Scripts\python.exe run.py test                           # 测试连接
    .\env\Scripts\python.exe run.py init-dims                      # 初始化维度表
    .\env\Scripts\python.exe run.py pipeline --start 2020-01-01    # SQL 清洗（自动续传）
    .\env\Scripts\python.exe run.py cpi --start 2020-01-01 --end 2024-12-31  # CPI 计算
    .\env\Scripts\python.exe run.py web --port 5050                # 启动 Web
    .\env\Scripts\python.exe run.py full                           # 一键全流程（清洗→CPI→Web）
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
    bar = '█' * filled + '░' * (width - filled)
    sys.stdout.write(f"\r{prefix} [{bar}] {current}/{total} ({pct:.0f}%) | {extra}    ")
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
    """初始化维度表（categories, products）"""
    print("=" * 50)
    print("初始化维度表（categories + products）")
    print("=" * 50)
    from src.etl.sql_pipeline import init_dimension_tables
    init_dimension_tables()
    print("[完成] 维度表初始化")


def cmd_pipeline(args):
    """SQL 清洗管道：OSS → CK SQL 清洗 → OSS 回写"""
    print("=" * 50)
    print("SQL 清洗管道 (支持断点续传)")
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
    一键全流程：
    1. 初始化维度表（如果未初始化）
    2. SQL 清洗全部缺失天数
    3. CPI 计算（日粒度）
    4. 启动 Web 界面
    """
    t0 = time.time()
    print("=" * 60)
    print("一键全流程启动")
    print("=" * 60)

    # === Step 1: 初始化维度表 ===
    print("\n【步骤 1/4】初始化维度表")
    from src.etl.sql_pipeline import init_dimension_tables
    init_dimension_tables()

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
    force = args.force_cpi or True
    compute_cpi_sql(base_date=start_d, start_date=start_d, end_date=end_d, force=force)

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
        description='电商数据项目 — SQL 管道版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s init-dims                  # 初始化维度表
  %(prog)s pipeline --start 2020-01-01  # SQL 清洗全量（自动续传）
  %(prog)s cpi --start 2020-01-01 --end 2024-12-31  # CPI 计算
  %(prog)s web --port 5050            # 启动 Web
  %(prog)s full                       # 一键全流程（推荐）
        """
    )
    subparsers = parser.add_subparsers(dest='command', help='子命令')

    subparsers.add_parser('test', help='测试 ClickHouse + OSS 连接')

    subparsers.add_parser('init-dims', help='初始化维度表（categories, products）')

    pipe_p = subparsers.add_parser('pipeline', help='SQL 清洗管道（OSS→CK→OSS，自动续传）')
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

    full_p = subparsers.add_parser('full', help='一键全流程：维度表→清洗→CPI→Web')
    full_p.add_argument('--start', default='2020-01-01')
    full_p.add_argument('--end', default='2024-12-31')
    full_p.add_argument('--port', type=int, default=5050)
    full_p.add_argument('--force-cpi', action='store_true')

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
