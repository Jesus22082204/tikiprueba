# backfill_last5days.py
import os, json
from data_collector import AirQualityCollector

def load_api_key():
    k = os.getenv("OPENWEATHER_API_KEY")
    if k: return k
    try:
        with open("config.json","r",encoding="utf-8") as f:
            return json.load(f).get("openweather_api_key")
    except FileNotFoundError:
        return None

def main():
    api_key = load_api_key()
    if not api_key:
        print("❌ Falta OPENWEATHER_API_KEY (o config.json).")
        return
    c = AirQualityCollector(api_key)
    results = c.collect_last5days_all_locations()
    total = sum((r[1].get("saved",0) if isinstance(r[1],dict) else 0) for r in results)
    print(f"✅ Histórico 5 días completado. Filas guardadas: {total}")

if __name__ == "__main__":
    main()
