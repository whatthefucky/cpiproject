"""
Flask Web 可视化应用
- /api/categories — 分类树
- /api/cpi_trend — 直接从 cpi_trend 表读取已计算的 CPI 趋势（SQL 管道输出）
"""
import sys, os, traceback
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime, date
from src.db.connection import get_clickhouse

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(__file__), 'static'),
            static_url_path='')


def _get_client():
    """获取 ClickHouse HTTP 连接"""
    return get_clickhouse()


def get_granularity(start_date, end_date):
    days = (end_date - start_date).days
    return 'day' if days < 30 else 'week' if days < 180 else 'month'


# ==================== API ====================

@app.route('/api/categories')
def api_categories():
    """返回分类树（从 categories 表）"""
    client = None
    try:
        client = _get_client()
        rows = client.execute(
            "SELECT category_id, category, hierarchy, parent FROM categories ORDER BY category_id"
        )
        tree = [{'id': r[0], 'name': r[1], 'level': r[2],
                 'parent': int(r[3]) if r[3] else None} for r in rows]
        return jsonify(tree)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if client:
            try: client.disconnect()
            except: pass


@app.route('/api/cpi_trend')
def api_cpi_trend():
    """
    从 cpi_trend 表读取已计算的 CPI 趋势
    参数: base, start, end, category_id（默认0=总体）
    """
    client = None
    try:
        start_str = request.args.get('start')
        end_str = request.args.get('end')
        cat_id = request.args.get('category_id', '0')

        end = datetime.strptime(end_str, '%Y-%m-%d').date() if end_str else date.today()
        start = datetime.strptime(start_str, '%Y-%m-%d').date() if start_str else date(2020, 1, 1)

        granularity = get_granularity(start, end)
        cid = int(cat_id) if cat_id else 0

        client = _get_client()
        rows = client.execute(f"""
            SELECT c.date, c.laspeyres, c.paasche, c.fisher, c.product_count,
                   cat.hierarchy
            FROM cpi_trend c
            LEFT JOIN categories cat ON c.category_id = cat.category_id
            WHERE c.date >= %(start)s AND c.date <= %(end)s
              AND c.category_id = %(cid)s
            ORDER BY c.date
        """, {'start': start.isoformat(), 'end': end.isoformat(), 'cid': cid})

        data = []
        for r in rows:
            data.append({
                'date': str(r[0]),
                'laspeyres': r[1],
                'paasche': r[2],
                'fisher': r[3],
                'product_count': r[4],
                'hierarchy': r[5],
            })

        if data:
            bf = data[0]['fisher']
            for d in data:
                d['change_pct'] = round((d['fisher'] - bf) * 100 / bf, 4) if bf else 0

        return jsonify({'granularity': granularity, 'data': data})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        if client:
            try: client.disconnect()
            except: pass


@app.route('/api/daily_stats')
def api_daily_stats():
    """返回每日聚合统计（从 OSS 已清洗 CSV 读取）"""
    return jsonify({"note": "清洗数据存储在 OSS，通过 CPI 趋势查看数据"})


@app.route('/api/info')
def api_info():
    """返回系统中实际数据的日期范围"""
    client = None
    try:
        client = _get_client()
        r2 = client.execute("SELECT min(date), max(date) FROM cpi_trend WHERE category_id = 0")
        cpi_min, cpi_max = r2[0]
        cpi_min_valid = str(cpi_min) if cpi_min and str(cpi_min) != '1970-01-01' else None
        cpi_max_valid = str(cpi_max) if cpi_max and str(cpi_max) != '1970-01-01' else None
        return jsonify({
            'cpi_trend': {
                'min_date': cpi_min_valid,
                'max_date': cpi_max_valid,
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if client:
            try: client.disconnect()
            except: pass


@app.route('/')
def index():
    try:
        return send_from_directory(app.static_folder, 'index.html')
    except Exception as e:
        return f"Static file not found: {e}", 404


def start(host='0.0.0.0', port=5050, debug=False):
    static_dir = app.static_folder
    os.makedirs(static_dir, exist_ok=True)
    index_path = os.path.join(static_dir, 'index.html')
    if not os.path.exists(index_path):
        print(f"[警告] index.html 不存在: {index_path}")
    print(f"Web 服务启动: http://{host}:{port}")
    app.run(host=host, port=port, debug=debug, use_reloader=False)
