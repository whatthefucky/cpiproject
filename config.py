"""配置文件读取工具
从 env/config.ini 读取项目所有私密配置。
使用示例：
    from config import get_config
    cfg = get_config()
    oss_key = cfg['oss']['access_key_id']
"""
import configparser
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'env', 'config.ini')
_cache = None


def get_config():
    """读取并缓存配置，返回一个类字典对象"""
    global _cache
    if _cache is not None:
        return _cache

    if not os.path.exists(_CONFIG_PATH):
        raise FileNotFoundError(
            f"配置文件不存在: {_CONFIG_PATH}\n"
            f"请复制 env/config.ini 并填写实际的密钥信息。"
        )

    cfg = configparser.ConfigParser()
    cfg.read(_CONFIG_PATH, encoding='utf-8-sig')
    _cache = cfg
    return cfg


def get_database_config():
    """快捷获取数据库配置"""
    cfg = get_config()
    section = 'database'
    return {
        'host': cfg.get(section, 'host'),
        'port': cfg.getint(section, 'port'),
        'user': cfg.get(section, 'user'),
        'password': cfg.get(section, 'password'),
        'database': cfg.get(section, 'database'),
    }


def get_oss_config():
    """快捷获取 OSS 配置"""
    cfg = get_config()
    section = 'oss'
    return {
        'endpoint': cfg.get(section, 'endpoint'),
        'bucket': cfg.get(section, 'bucket'),
        'access_key_id': cfg.get(section, 'access_key_id'),
        'access_key_secret': cfg.get(section, 'access_key_secret'),
        'prefix': cfg.get(section, 'prefix'),
    }
