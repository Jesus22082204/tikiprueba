# scheduler.py - Automatización de recolección y API REST
import schedule
import time
import threading
from datetime import datetime, timedelta
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
        # Programar recolecciones cada 4 horas
        schedule.every(4).hours.do(self.collect_data_job)
        
        # Recolección diaria a las 6:00 AM
        schedule.every().day.at("06:00").do(self.collect_data_job)
        
        # Recolección diaria a las 18:00 PM
        schedule.every().day.at("18:00").do(self.collect_data_job)
        
        logging.info("Scheduler iniciado - Recolectando cada 4 horas")
        logging.info("Horarios especiales: 06:00 y 18:00")
        
        self.running = True
        
        # Ejecutar primera recolección inmediatamente
        self.collect_data_job()
        
        # Loop principal del scheduler
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Verificar cada minuto
    
    def stop_scheduler(self):
        """Detener el scheduler"""
        self.running = False
        logging.info("Scheduler detenido")

# API REST usando Flask
from flask import Flask, jsonify, request
from flask_cors import CORS
from database_setup import get_historical_data, get_monthly_statistics
import sqlite3
import calendar

app = Flask(__name__)
CORS(app)  # Permitir requests desde el frontend

@app.route('/api/current/<location_id>')
def get_current_data(location_id):
    """Obtener datos más recientes de una ubicación"""
    try:
        data = get_historical_data(location_id=location_id, limit=1)
        if data:
            return jsonify({'success': True, 'data': data[0]})
        else:
            return jsonify({'success': False, 'error': 'No data found'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/historical/<location_id>')
def get_historical(location_id):
    """Obtener datos históricos de una ubicación"""
    try:
        # Parámetros opcionales
        days = request.args.get('days', 7, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        # Calcular fecha de inicio
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        data = get_historical_data(
            location_id=location_id,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            limit=limit
        )
        
        return jsonify({'success': True, 'data': data, 'count': len(data)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/monthly-stats/<location_id>/<int:year>/<int:month>')
def get_monthly_stats(location_id, year, month):
    """Obtener estadísticas mensuales para boxplots"""
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
    """Obtener datos para boxplots de todo el año"""
    try:
        current_month = datetime.now().month
        boxplot_data = []
        
        for month in range(1, current_month):
            stats = get_monthly_statistics(location_id, year, month)
            if stats and stats['count'] >= 10:  # Mínimo 10 datos por mes
                month_name = calendar.month_name[month]
                
                # Calcular percentiles para boxplot (aproximación)
                conn = sqlite3.connect('data/air_quality.db')
                cursor = conn.cursor()
                
                # Obtener todos los valores del mes para calcular percentiles
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
    """Obtener todas las ubicaciones disponibles"""
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
                'id': row[0],
                'name': row[1],
                'latitude': row[2],
                'longitude': row[3],
                'data_count': row[4],
                'last_update': row[5]
            })
        
        conn.close()
        return jsonify({'success': True, 'data': locations})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/status')
def get_status():
    """Obtener estado general del sistema"""
    try:
        conn = sqlite3.connect('data/air_quality.db')
        cursor = conn.cursor()
        
        # Contar total de registros
        cursor.execute('SELECT COUNT(*) FROM air_quality_data')
        total_records = cursor.fetchone()[0]
        
        # Última actualización
        cursor.execute('SELECT MAX(timestamp) FROM air_quality_data')
        last_update = cursor.fetchone()[0]
        
        # Registros por ubicación
        cursor.execute('''
        SELECT location_id, COUNT(*) 
        FROM air_quality_data 
        GROUP BY location_id
        ''')
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

def run_api_server():
    """Ejecutar el servidor API"""
    app.run(host='127.0.0.1', port=5000, debug=False)

def main():
    """Función principal para ejecutar scheduler y API"""
    
    # Obtener API key
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
            # Ejecutar solo el scheduler
            scheduler = DataScheduler(api_key)
            try:
                scheduler.start_scheduler()
            except KeyboardInterrupt:
                scheduler.stop_scheduler()
                
        elif sys.argv[1] == 'api':
            # Ejecutar solo el servidor API
            print("🚀 Iniciando servidor API en http://127.0.0.1:5000")
            run_api_server()
            
        elif sys.argv[1] == 'both':
            # Ejecutar ambos en threads separados
            scheduler = DataScheduler(api_key)
            
            # Thread para el scheduler
            scheduler_thread = threading.Thread(target=scheduler.start_scheduler)
            scheduler_thread.daemon = True
            scheduler_thread.start()
            
            # Thread para el API server
            api_thread = threading.Thread(target=run_api_server)
            api_thread.daemon = True
            api_thread.start()
            
            print("🚀 Sistema completo iniciado:")
            print("   📡 Scheduler: Recolectando datos cada 4 horas")
            print("   🌐 API Server: http://127.0.0.1:5000")
            
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                scheduler.stop_scheduler()
                print("\n✅ Sistema detenido")
    else:
        print("Uso: python scheduler.py [scheduler|api|both]")

if __name__ == "__main__":
    main()