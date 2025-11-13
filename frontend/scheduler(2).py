# scheduler.py - Automatización de recolección y API REST
import schedule
import time
import threading
from datetime import datetime, timedelta, timezone
import logging
from data_collector import AirQualityCollector
import json
import os
import sys

# Configurar logging
if not os.path.exists('data'):
    os.makedirs('data')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/air_quality.log'),
        logging.StreamHandler()
    ]
)

class DataScheduler:
    def __init__(self, api_key):
        self.collector = AirQualityCollector(api_key)
        self.running = False

    def collect_data_job(self):
        """Job que ejecuta la recolección de datos"""
        try:
            logging.info("Iniciando recolección programada de datos")
            successful, failed = self.collector.collect_all_locations()
            logging.info(f"Recolección completada - Exitosos: {successful}, Fallidos: {failed}")
        except Exception as e:
            logging.error(f"Error en recolección programada: {e}")

    def start_scheduler(self):
        """Iniciar el scheduler"""
        schedule.every(4).hours.do(self.collect_data_job)
        schedule.every().day.at("06:00").do(self.collect_data_job)
        schedule.every().day.at("18:00").do(self.collect_data_job)
        logging.info("Scheduler iniciado - Recolectando cada 4 horas")
        logging.info("Horarios especiales: 06:00 y 18:00")
        self.running = True
        self.collect_data_job()
        while self.running:
            schedule.run_pending()
            time.sleep(60)

    def stop_scheduler(self):
        self.running = False
        logging.info("Scheduler detenido")

# ===== API REST =====
from flask import Flask, jsonify, request
from flask_cors import CORS
from database_setup import get_historical_data, get_monthly_statistics
import sqlite3
import calendar

app = Flask(__name__)
CORS(app)

# Utilidades de tiempo
BOGOTA_OFFSET = -5  # Colombia (sin DST)
def yesterday_local_bounds_utc():
    """Devuelve (start_utc_iso, end_utc_iso) que cubren AYER local en América/Bogotá [00:00, 24:00)."""
    now_utc = datetime.now(timezone.utc)
    # Convertir a hora local manualmente (UTC-5)
    now_local = now_utc + timedelta(hours=BOGOTA_OFFSET)
    # Ayer local
    y_local_date = (now_local.date() - timedelta(days=1))
    start_local = datetime(y_local_date.year, y_local_date.month, y_local_date.day, 0, 0, 0)
    end_local = start_local + timedelta(days=1)
    # Volver a UTC
    start_utc = start_local - timedelta(hours=BOGOTA_OFFSET)
    end_utc = end_local - timedelta(hours=BOGOTA_OFFSET)
    return (
        start_utc.replace(tzinfo=timezone.utc).isoformat(timespec='seconds'),
        end_utc.replace(tzinfo=timezone.utc).isoformat(timespec='seconds'),
    )

@app.route('/api/current/<location_id>')
def get_current_data(location_id):
    try:
        data = get_historical_data(location_id=location_id, limit=1)
        if data:
            return jsonify({'success': True, 'data': data[0]})
        else:
            return jsonify({'success': False, 'error': 'No data found'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/live/<location_id>')
def get_live(location_id):
    """Devuelve dato en tiempo real directamente desde OpenWeather para 'ahora'."""
    try:
        # Buscar lat/lon de la ubicación en el collector
        api_key = os.getenv('OPENWEATHER_API_KEY')
        if not api_key and os.path.exists('config.json'):
            try:
                with open('config.json','r',encoding='utf-8') as f:
                    api_key = json.load(f).get('openweather_api_key')
            except Exception:
                pass
        if not api_key:
            return jsonify({'success': False, 'error': 'OPENWEATHER_API_KEY missing'})
        collector = AirQualityCollector(api_key)
        location = next((l for l in collector.locations if l['id'] == location_id), None)
        if not location:
            return jsonify({'success': False, 'error': 'location not found'})
        api_data = collector.get_air_quality_data(location['lat'], location['lon'])
        return jsonify({'success': api_data.get('success', False), 'data': api_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/historical/<location_id>')
def get_historical(location_id):
    try:
        days = request.args.get('days', 7, type=int)
        limit = request.args.get('limit', 100, type=int)
        end_date = datetime.now().isoformat()
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        data = get_historical_data(
            location_id=location_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        return jsonify({'success': True, 'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/monthly-stats/<location_id>/<int:year>/<int:month>')
def get_monthly_stats(location_id, year, month):
    try:
        stats = get_monthly_statistics(location_id, year, month)
        if stats:
            return jsonify({'success': True, 'data': stats})
        else:
            return jsonify({'success': False, 'error': 'No data for this month'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/boxplot-data/<location_id>/<int:year>')
def get_boxplot_data(location_id, year):
    try:
        current_month = datetime.now().month
        boxplot_data = []
        for month in range(1, current_month):
            stats = get_monthly_statistics(location_id, year, month)
            if stats and stats['count'] >= 10:
                month_name = calendar.month_name[month]
                conn = sqlite3.connect('data/air_quality.db')
                cursor = conn.cursor()
                cursor.execute('''
                SELECT pm2_5, pm10 FROM air_quality_data 
                WHERE location_id = ? 
                AND strftime('%Y', timestamp) = ? 
                AND strftime('%m', timestamp) = ?
                ORDER BY pm2_5
                ''', (location_id, str(year), f"{month:02d}"))
                values = cursor.fetchall()
                conn.close()
                if values:
                    pm25_values = [v[0] for v in values if v[0] is not None]
                    pm10_values = [v[1] for v in values if v[1] is not None]
                    if pm25_values and pm10_values:
                        pm25_values.sort()
                        pm10_values.sort()
                        n = len(pm25_values)
                        pm25_stats = {
                            'min': round(pm25_values[0], 2),
                            'q1': round(pm25_values[int(n * 0.25)], 2),
                            'median': round(pm25_values[int(n * 0.5)], 2),
                            'q3': round(pm25_values[int(n * 0.75)], 2),
                            'max': round(pm25_values[-1], 2)
                        }
                        n = len(pm10_values)
                        pm10_stats = {
                            'min': round(pm10_values[0], 2),
                            'q1': round(pm10_values[int(n * 0.25)], 2),
                            'median': round(pm10_values[int(n * 0.5)], 2),
                            'q3': round(pm10_values[int(n * 0.75)], 2),
                            'max': round(pm10_values[-1], 2)
                        }
                        boxplot_data.append({
                            'month': month_name,
                            'month_number': month,
                            'pm25': pm25_stats,
                            'pm10': pm10_stats,
                            'data_count': stats['count']
                        })
        return jsonify({'success': True, 'data': boxplot_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/locations')
def get_locations():
    try:
        conn = sqlite3.connect('data/air_quality.db')
        cursor = conn.cursor()
        cursor.execute('''
        SELECT DISTINCT location_id, location_name, latitude, longitude,
               COUNT(*) as data_count,
               MAX(timestamp) as last_update
        FROM air_quality_data 
        GROUP BY location_id
        ORDER BY location_name
        ''')
        locations = []
        for row in cursor.fetchall():
            locations.append({
                'id': row[0], 'name': row[1], 'latitude': row[2], 'longitude': row[3],
                'data_count': row[4], 'last_update': row[5]
            })
        conn.close()
        return jsonify({'success': True, 'data': locations})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/status')
def get_status():
    try:
        conn = sqlite3.connect('data/air_quality.db')
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM air_quality_data')
        total_records = cursor.fetchone()[0]
        cursor.execute('SELECT MAX(timestamp) FROM air_quality_data')
        last_update = cursor.fetchone()[0]
        cursor.execute('SELECT location_id, COUNT(*) FROM air_quality_data GROUP BY location_id')
        location_counts = dict(cursor.fetchall())
        conn.close()
        return jsonify({
            'success': True,
            'data': {
                'total_records': total_records,
                'last_update': last_update,
                'location_counts': location_counts,
                'database_status': 'active'
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ======= Tendencias y distribución =======
@app.route('/api/trends/<location_id>')
def get_trends(location_id):
    """Devuelve series de AYER local (Bogotá) 00:00–23:59 para PM2.5, PM10, O3, NO2,
    y la distribución de AQI de los últimos 7 días. Si day=last24h, usa ventana móvil 24h.
    """
    try:
        mode = request.args.get('day', 'yesterday')  # yesterday | last24h
        conn = sqlite3.connect('data/air_quality.db')
        cursor = conn.cursor()

        if mode == 'last24h':
            cursor.execute(
                """SELECT timestamp, pm2_5, pm10, o3, no2
                     FROM air_quality_data
                     WHERE location_id = ?
                     AND timestamp >= datetime('now','-1 day')
                     ORDER BY timestamp ASC""",
                (location_id,)
            )
        else:
            # Ayer local convertido a UTC
            start_utc, end_utc = yesterday_local_bounds_utc()
            cursor.execute(
                """SELECT timestamp, pm2_5, pm10, o3, no2
                     FROM air_quality_data
                     WHERE location_id = ?
                     AND timestamp >= ?
                     AND timestamp < ?
                     ORDER BY timestamp ASC""",
                (location_id, start_utc, end_utc)
            )

        rows = cursor.fetchall()

        # Distribución AQI 7 días (siempre sobre 7d móviles)
        cursor.execute(
            """SELECT aqi
                 FROM air_quality_data
                 WHERE location_id = ?
                 AND timestamp >= datetime('now','-7 day')""",
            (location_id,)
        )
        rows_7d = cursor.fetchall()
        conn.close()

        pm25 = [{'t': r[0], 'v': round(r[1],2)} for r in rows if r[1] is not None]
        pm10 = [{'t': r[0], 'v': round(r[2],2)} for r in rows if r[2] is not None]
        o3   = [{'t': r[0], 'v': round(r[3],2)} for r in rows if r[3] is not None]
        no2  = [{'t': r[0], 'v': round(r[4],2)} for r in rows if r[4] is not None]

        dist = {'1':0,'2':0,'3':0,'4':0,'5':0}
        for (aqi,) in rows_7d:
            if aqi is None: 
                continue
            k = str(int(aqi))
            if k in dist: dist[k] += 1

        if sum(len(x) for x in [pm25, pm10, o3, no2]) < 4 and sum(dist.values()) < 1:
            return jsonify({'success': False, 'error': 'not_enough_data'})

        return jsonify({
            'success': True,
            'pm25_24h': pm25,
            'pm10_24h': pm10,
            'o3_24h':   o3,
            'no2_24h':  no2,
            'aqi_distribution_7d': dist
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

def run_api_server():
    app.run(host='127.0.0.1', port=5000, debug=False)

def main():
    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                api_key = config.get('openweather_api_key')
        except FileNotFoundError:
            pass
    if not api_key:
        print("❌ API key no configurada. Crea un archivo config.json con tu API key:")
        print('{"openweather_api_key": "tu_api_key_aqui"}')
        return
    if len(sys.argv) > 1:
        if sys.argv[1] == 'scheduler':
            scheduler = DataScheduler(api_key)
            try: scheduler.start_scheduler()
            except KeyboardInterrupt: scheduler.stop_scheduler()
        elif sys.argv[1] == 'api':
            print("🚀 Iniciando servidor API en http://127.0.0.1:5000")
            run_api_server()
        elif sys.argv[1] == 'both':
            scheduler = DataScheduler(api_key)
            scheduler_thread = threading.Thread(target=scheduler.start_scheduler, daemon=True)
            scheduler_thread.start()
            api_thread = threading.Thread(target=run_api_server, daemon=True)
            api_thread.start()
            print("🚀 Sistema completo iniciado:\n   📡 Scheduler: Recolectando datos cada 4 horas\n   🌐 API Server: http://127.0.0.1:5000")
            try:
                while True: time.sleep(1)
            except KeyboardInterrupt:
                scheduler.stop_scheduler()
                print("\n✅ Sistema detenido")
    else:
        print("Uso: python scheduler.py [scheduler|api|both]")

if __name__ == "__main__":
    main()
