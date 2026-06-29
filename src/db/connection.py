"""
ClickHouse HTTP 客户端（替代 clickhouse-driver，适配阿里云 CK 新版）
使用原生 HTTP 协议（端口 8123），支持所有 ClickHouse SQL 语法
"""
import sys, os, urllib.request, urllib.parse, urllib.error, json
from datetime import date, datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import get_database_config, get_oss_config


class HttpClient:
    """ClickHouse HTTP 客户端包装器"""

    def __init__(self, host, port, user, password, database='default',
                 connect_timeout=10, send_receive_timeout=60):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = send_receive_timeout

    def execute(self, sql, params=None):
        """执行 SQL，返回行列表（认证在 URL，SQL 在 POST body）"""
        final_sql = sql
        if isinstance(params, dict):
            for k, v in params.items():
                final_sql = final_sql.replace(f'%({k})s', f"'{v}'")

        # 认证参数放 URL（CK HTTP 协议要求）
        url = (f"http://{self.host}:{self.port}/"
               f"?user={urllib.parse.quote(self.user)}"
               f"&password={urllib.parse.quote(self.password)}"
               f"&database={urllib.parse.quote(self.database)}"
               f"&default_format=TabSeparatedWithNamesAndTypes")

        # SQL 放 POST body
        req = urllib.request.Request(url, data=final_sql.encode('utf-8'), method='POST')
        try:
            resp = urllib.request.urlopen(req, timeout=self.timeout)
            body = resp.read().decode('utf-8')
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='replace')[:300]
            raise Exception(f"Code: {e.code}. {err_body}") from e
        except Exception as e:
            raise Exception(f"HTTP 请求失败: {e}") from e

        return self._parse_tsv(body)

    def execute_values(self, sql_template, rows):
        """批量写入：将行列表拼成 VALUES 语句执行"""
        if not rows:
            return []
        # sql_template: "INSERT INTO t (a,b) VALUES"
        # 提取 INSERT 部分
        parts = sql_template.split('VALUES')
        prefix = parts[0].strip()
        # 拼接 values
        def _fmt(v):
            if v is None:
                return 'NULL'
            if isinstance(v, int):
                return str(v)
            if isinstance(v, float):
                return str(v)
            if isinstance(v, date):
                return f"'{v.isoformat()}'"
            if isinstance(v, datetime):
                return f"'{v.isoformat()}'"
            return f"'{v}'"
        val_strs = []
        for row in rows:
            val_strs.append('(' + ','.join(_fmt(v) for v in row) + ')')
        sql = prefix + ' VALUES ' + ','.join(val_strs)
        return self.execute(sql)

    def _parse_tsv(self, body):
        """解析 TabSeparatedWithNamesAndTypes 格式返回列表，无类型信息"""
        lines = body.strip().split('\n')
        if len(lines) <= 2:
            return []  # 只有列名和类型，没有数据行
        # 解析类型行，决定每列的类型
        type_names = lines[1].split('\t') if len(lines) >= 2 else []
        rows = []
        for line in lines[2:]:
            if not line.strip():
                continue
            vals = line.split('\t')
            row = []
            for i, v in enumerate(vals):
                if i < len(type_names):
                    t = type_names[i]
                    if 'Int' in t or 'UInt' in t:
                        try: row.append(int(v))
                        except: row.append(0)
                    elif 'Float' in t or 'Decimal' in t:
                        try: row.append(float(v))
                        except: row.append(0.0)
                    elif t in ('Date', 'DateTime'):
                        row.append(v)
                    else:
                        row.append(v)
                else:
                    row.append(v)
            rows.append(row)
        return rows

    def disconnect(self):
        pass


def get_clickhouse(settings=None):
    """获取 ClickHouse HTTP 连接（端口从 config.ini 读取）"""
    db = get_database_config()
    return HttpClient(
        host=db['host'],
        port=db['port'],
        user=db['user'],
        password=db['password'],
        database=db.get('database', 'default'),
        send_receive_timeout=300,
    )


def get_oss_bucket():
    import oss2
    cfg = get_oss_config()
    auth = oss2.Auth(cfg['access_key_id'], cfg['access_key_secret'])
    return oss2.Bucket(auth, cfg['endpoint'], cfg['bucket'])
