import sqlite3

from datetime import datetime
import os
import psycopg2

def get_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL no está configurada")
    return psycopg2.connect(db_url)



def create_database():
    """Crear la base de datos y tabla para datos de calidad del aire"""
    
    # Crear carpeta para la base de datos si no existe
    if not os.path.exists('data'):
        os.makedirs('data')
    

    ##local
    # Conectar a la base de datos (se crea si no existe)
    #conn = sqlite3.connect('data/air_quality.db')
    #cursor = conn.cursor()


    conn = get_connection()
    cursor = conn.cursor()

    
    # Crear tabla para datos de calidad del aire
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS air_quality_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id TEXT NOT NULL,
        location_name TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        timestamp DATETIME NOT NULL,
        pm2_5 REAL,
        pm10 REAL,
        o3 REAL,
        no2 REAL,
        aqi INTEGER,
        temperature REAL,
        humidity REAL,
        pressure REAL,
        wind_speed REAL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(location_id, timestamp)
    )
    ''')
    
    # Crear índices para búsquedas rápidas
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_location_timestamp 
    ON air_quality_data(location_id, timestamp)
    ''')
    
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_timestamp 
    ON air_quality_data(timestamp)
    ''')
    
    # Confirmar cambios
    conn.commit()
    conn.close()
    
    print("Base de datos creada exitosamente en: data/air_quality.db")

def insert_air_quality_data(location_id, location_name, lat, lon, timestamp, 
                           pm2_5, pm10, o3, no2, aqi, temp=None, humidity=None, 
                           pressure=None, wind_speed=None):
    """Insertar datos de calidad del aire en la base de datos (UPSERT que preserva valores existentes)"""
    
    ##local
    # conn = sqlite3.connect('data/air_quality.db')
    #cursor = conn.cursor()

    conn = get_connection()
    cursor = conn.cursor()

    
    try:
        cursor.execute('''
        INSERT INTO air_quality_data
        (location_id, location_name, latitude, longitude, timestamp, 
         pm2_5, pm10, o3, no2, aqi, temperature, humidity, pressure, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id, timestamp) DO UPDATE SET
          location_name = excluded.location_name,
          latitude = excluded.latitude,
          longitude = excluded.longitude,
          pm2_5 = COALESCE(excluded.pm2_5, air_quality_data.pm2_5),
          pm10 = COALESCE(excluded.pm10, air_quality_data.pm10),
          o3 = COALESCE(excluded.o3, air_quality_data.o3),
          no2 = COALESCE(excluded.no2, air_quality_data.no2),
          aqi = COALESCE(excluded.aqi, air_quality_data.aqi),
          temperature = COALESCE(excluded.temperature, air_quality_data.temperature),
          humidity = COALESCE(excluded.humidity, air_quality_data.humidity),
          pressure = COALESCE(excluded.pressure, air_quality_data.pressure),
          wind_speed = COALESCE(excluded.wind_speed, air_quality_data.wind_speed)
        ''', (location_id, location_name, lat, lon, timestamp, 
              pm2_5, pm10, o3, no2, aqi, temp, humidity, pressure, wind_speed))
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error al insertar datos: {e}")
        return False
    finally:
        conn.close()

def get_historical_data(location_id=None, start_date=None, end_date=None, limit=None):
    """Obtener datos históricos de la base de datos"""
    ##local
    #conn = sqlite3.connect('data/air_quality.db')
    #cursor = conn.cursor()

    conn = get_connection()
    cursor = conn.cursor()

    
    query = "SELECT * FROM air_quality_data WHERE 1=1"
    params = []
    
    if location_id:
        query += " AND location_id = ?"
        params.append(location_id)
    
    if start_date:
        query += " AND timestamp >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND timestamp <= ?"
        params.append(end_date)
    
    query += " ORDER BY timestamp DESC"
    
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Convertir a lista de diccionarios
    columns = [description[0] for description in cursor.description]
    results = []
    for row in rows:
        results.append(dict(zip(columns, row)))
    
    conn.close()
    return results

def get_monthly_statistics(location_id, year, month):
    """Obtener estadísticas mensuales para boxplots"""
    
    #conn = sqlite3.connect('data/air_quality.db')
    #cursor = conn.cursor()

    conn = get_connection()
    cursor = conn.cursor()

    
    cursor.execute('''
    SELECT 
        COUNT(*) as count,
        AVG(pm2_5) as avg_pm25,
        MIN(pm2_5) as min_pm25,
        MAX(pm2_5) as max_pm25,
        AVG(pm10) as avg_pm10,
        MIN(pm10) as min_pm10,
        MAX(pm10) as max_pm10
    FROM air_quality_data 
    WHERE location_id = ? 
    AND strftime('%Y', timestamp) = ? 
    AND strftime('%m', timestamp) = ?
    ''', (location_id, str(year), f"{month:02d}"))
    
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0] > 0:
        columns = [description[0] for description in cursor.description]
        return dict(zip(columns, result))
    return None

if __name__ == "__main__":
    create_database()
    print("Configuración de base de datos completada.")
