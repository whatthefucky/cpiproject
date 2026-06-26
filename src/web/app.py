"""
Flask Web 可视化应用
- /api/categories — 分类树
- /api/cpi_trend — 直接从 cpi_trend 表读取已计算的 CPI 趋势（SQL 管道输出）
"""
import sys, os, math, traceback
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from flask import Flask, jsonify, request, send_from_directory
from datetime import datetime, date
from clickhouse_driver import Client
from config import get_database_config

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(__file__), 'static'),
            static_url_path='')


def _get_client():
    db = get_database_config()
    return Client(host=db['host'], port=db['port'], user=db['user'],
                  password=db['password'], database=db['database'],
                  connect_timeout=10, send_receive_timeout=30)


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
            SELECT date, laspeyres, paasche, fisher, product_count
            FROM cpi_trend
            WHERE date >= %(start)s AND date <= %(end)s
              AND category_id = %(cid)s
            ORDER BY date
        """, {'start': start.isoformat(), 'end': end.isoformat(), 'cid': cid})

        data = []
        for r in rows:
            data.append({
                'date': str(r[0]),
                'laspeyres': r[1],
                'paasche': r[2],
                'fisher': r[3],
                'product_count': r[4],
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
    """返回每日聚合统计"""
    client = None
    try:
        start = request.args.get('start', '2020-01-01')
        end = request.args.get('end', '2024-12-31')
        client = _get_client()
        rows = client.execute(f"""
            SELECT sale_date, event_type, record_count, active_products,
                   total_sales_volume, total_revenue, missing_count,
                   anomaly_count, promotion_count, holiday_count
            FROM daily_stats
            WHERE sale_date >= '{start}' AND sale_date <= '{end}'
            ORDER BY sale_date
        """)
        data = []
        for r in rows:
            data.append({
                'date': str(r[0]), 'event_type': r[1],
                'count': r[2], 'products': r[3],
                'volume': r[4], 'revenue': r[5],
                'missing': r[6], 'anomaly': r[7],
                'promotion': r[8], 'holiday': r[9],
            })
        return jsonify(data)
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
