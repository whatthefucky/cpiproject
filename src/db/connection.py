"""
数据库和 OSS 连接工具模块（统一错误处理）
支持原生 TCP 和 HTTP 两种协议模式
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from clickhouse_driver import Client
from config import get_database_config, get_oss_config

# 是否使用 HTTP 协议模式（AnalyticDB for ClickHouse 不支持原生 TCP）
_USE_HTTP = False


def get_clickhouse(settings=None):
    """获取 ClickHouse 连接"""
    try:
        db = get_database_config()
        host = db['host']
        port = db['port']
        user = db['user']
        password = db['password']
        database = db['database']

        if _USE_HTTP:
            # HTTP 模式（适合 AnalyticDB ClickHouse / 原生不支持 TCP 的实例）
            opts = dict(
                host=host, port=8123, user=user, password=password,
                database=database, connect_timeout=15, send_receive_timeout=120,
            )
        else:
            # 原生 TCP 模式
            opts = dict(
                host=host, port=port, user=user, password=password,
                database=database, connect_timeout=10, send_receive_timeout=60,
            )
        if settings:
            opts['settings'] = settings
        client = Client(**opts)
        client.execute('SELECT 1')
        return client
    except Exception as e:
        raise ConnectionError(f"ClickHouse 连接失败 ({'HTTP' if _USE_HTTP else 'TCP'}): {e}") from e


def get_oss_bucket():
    try:
        import oss2
        cfg = get_oss_config()
        auth = oss2.Auth(cfg['access_key_id'], cfg['access_key_secret'])
        bucket = oss2.Bucket(auth, cfg['endpoint'], cfg['bucket'])
        return bucket
    except Exception as e:
        raise ConnectionError(f"OSS 连接失败: {e}") from e


def safe_execute(client, sql, params=None, description=None):
    try:
        return client.execute(sql, params)
    except Exception as e:
        desc = description or sql[:80]
        print(f"  [SQL错误] {desc}: {e}")
        return None


def safe_execute_with_types(client, sql, params=None):
    try:
        result = client.execute(sql, params, with_column_types=True)
        if result and result[0]:
            cols = [c[0] for c in result[1]]
            return result[0], cols
        return [], []
    except Exception as e:
        print(f"  [SQL错误] {sql[:80]}: {e}")
        return [], []


# ==================== HTTP 模式切换辅助函数 ====================

def use_http_mode(enabled=True):
    """切换连接模式（True=HTTP 协议 / False=原生 TCP）"""
    global _USE_HTTP
    _USE_HTTP = enabled


def try_connect():
    """自动尝试两种模式，返回可用的 client"""
    global _USE_HTTP
    for mode, is_http in [('TCP', False), ('HTTP', True)]:
        try:
            _USE_HTTP = is_http
            client = get_clickhouse()
            print(f"  [OK] {mode} 模式连接成功")
            return client
        except Exception:
            continue
    _USE_HTTP = False
    raise ConnectionError("TCP 和 HTTP 模式均无法连接 ClickHouse")
