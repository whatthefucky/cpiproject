"""
数据库和 OSS 连接工具模块（统一错误处理）
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from clickhouse_driver import Client
from config import get_database_config, get_oss_config


def get_clickhouse(settings=None):
    """获取 ClickHouse TCP 连接"""
    db = get_database_config()
    opts = dict(
        host=db['host'], port=db['port'], user=db['user'],
        password=db['password'], database=db['database'],
        connect_timeout=10, send_receive_timeout=60,
    )
    if settings:
        opts['settings'] = settings
    client = Client(**opts)
    client.execute('SELECT 1')
    return client


def get_oss_bucket():
    import oss2
    cfg = get_oss_config()
    auth = oss2.Auth(cfg['access_key_id'], cfg['access_key_secret'])
    return oss2.Bucket(auth, cfg['endpoint'], cfg['bucket'])
