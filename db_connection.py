# db_connection.py
import os
import psycopg2

def get_connection():
    db_url = os.getenv("DATABASE_URL")  # Render te da esta variable
    if not db_url:
        raise Exception("DATABASE_URL no configurada.")
    return psycopg2.connect(db_url)
