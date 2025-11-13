import sqlite3
import psycopg2
from psycopg2.extras import execute_batch

# --- Rutas y conexión ---
sqlite_path = "data/air_quality.db"
postgres_url = "sapo"

# --- Conectar a SQLite ---
sqlite_conn = sqlite3.connect(sqlite_path)
sqlite_cur = sqlite_conn.cursor()

# --- Conectar a PostgreSQL ---
pg_conn = psycopg2.connect(postgres_url)
pg_cur = pg_conn.cursor()

# --- Leer todas las filas ---
sqlite_cur.execute("SELECT * FROM air_quality_data")
rows = sqlite_cur.fetchall()

# --- Crear tabla en PostgreSQL ---
pg_cur.execute("""
CREATE TABLE IF NOT EXISTS air_quality_data (
    id SERIAL PRIMARY KEY,
    location_id TEXT NOT NULL,
    location_name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    pm2_5 REAL,
    pm10 REAL,
    o3 REAL,
    no2 REAL,
    aqi INTEGER,
    temperature REAL,
    humidity REAL,
    pressure REAL,
    wind_speed REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, timestamp)
)
""")

# --- Insertar datos ---
query = """
INSERT INTO air_quality_data (
    id, location_id, location_name, latitude, longitude, timestamp,
    pm2_5, pm10, o3, no2, aqi, temperature, humidity, pressure, wind_speed, created_at
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (location_id, timestamp) DO NOTHING
"""

execute_batch(pg_cur, query, rows)
pg_conn.commit()

print(f"✅ Migración completada: {len(rows)} filas copiadas.")

sqlite_conn.close()
pg_conn.close()
