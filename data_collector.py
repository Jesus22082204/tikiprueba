import requests
import time
from datetime import datetime, timezone, timedelta
import json
from database_setup import insert_air_quality_data

class AirQualityCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"
        
        # Puntos de interés de Aguachica
        self.locations = [
            {
                'id': 'aguachica_general',
                'name': 'Aguachica - Vista General',
                'lat': 8.312,
                'lon': -73.626
            },
            {
                'id': 'parque_central',
                'name': 'Parque Central',
                'lat': 8.310675833008426,
                'lon': -73.62363665855918
            },
            {
                'id': 'universidad',
                'name': 'Universidad Popular del Cesar',
                'lat': 8.314789098234467,
                'lon': -73.59638568793966
            },
            {
                'id': 'parque_morrocoy',
                'name': 'Parque Morrocoy',
                'lat': 8.310373774726447,
                'lon': -73.61670782048647
            },
            {
                'id': 'patinodromo',
                'name': 'Patinódromo',
                'lat': 8.297149888853758,
                'lon': -73.62335200184627
            },
            {
                'id': 'ciudadela_paz',
                'name': 'Ciudadela de la Paz',
                'lat': 8.312099985681844,
                'lon': -73.63467832511535
            },
            {
                'id': 'bosque',
                'name': 'Bosque',
                'lat': 8.312303609676293,
                'lon': -73.61448867800057
            },
            {
                'id': 'estadio',
                'name': 'Estadio',
                'lat': 8.30159931733102,
                'lon': -73.622763654179
            }
        ]

        # Caché simple por día para meteo histórica (lat, lon, 'YYYY-MM-DD') -> { dt: {temp,humidity,pressure,wind_speed} }
        self._wx_cache = {}

    def get_air_quality_data(self, lat, lon):
        """Obtener datos de calidad del aire de OpenWeather API"""
        try:
            # URL para calidad del aire
            air_url = f"{self.base_url}/air_pollution"
            air_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key
            }
            
            # URL para datos meteorológicos
            weather_url = f"{self.base_url}/weather"
            weather_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            # Hacer las peticiones
            air_response = requests.get(air_url, params=air_params, timeout=10)
            weather_response = requests.get(weather_url, params=weather_params, timeout=10)
            
            if air_response.status_code == 200 and weather_response.status_code == 200:
                air_data = air_response.json()
                weather_data = weather_response.json()
                
                return {
                    'air_quality': air_data,
                    'weather': weather_data,
                    'success': True
                }
            else:
                print(f"Error en API: Air={air_response.status_code}, Weather={weather_response.status_code}")
                return {'success': False, 'error': 'API request failed'}
                
        except requests.exceptions.Timeout:
            print("Timeout en la petición a la API")
            return {'success': False, 'error': 'Timeout'}
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            return {'success': False, 'error': str(e)}
    
    def process_and_save_data(self, location, api_data):
        """Procesar y guardar datos en la base de datos"""
        try:
            air_quality = api_data['air_quality']
            weather = api_data['weather']
            
            # Extraer datos de calidad del aire
            components = air_quality['list'][0]['components']
            aqi = air_quality['list'][0]['main']['aqi']
            
            # Extraer datos meteorológicos
            temp = weather['main']['temp']
            humidity = weather['main']['humidity']
            pressure = weather['main']['pressure']
            wind_speed = weather.get('wind', {}).get('speed', None)
            
            # Timestamp de la observación (usar 'dt' de la API si existe; fallback a ahora)
            obs_dt = air_quality['list'][0].get('dt') if air_quality.get('list') else None
            if obs_dt:
                timestamp = datetime.fromtimestamp(obs_dt, tz=timezone.utc).isoformat(timespec='seconds')
            else:
                timestamp = datetime.now(timezone.utc).isoformat()
            # Guardar en base de datos
            success = insert_air_quality_data(
                location_id=location['id'],
                location_name=location['name'],
                lat=location['lat'],
                lon=location['lon'],
                timestamp=timestamp,
                pm2_5=components.get('pm2_5'),
                pm10=components.get('pm10'),
                o3=components.get('o3'),
                no2=components.get('no2'),
                aqi=aqi,
                temp=temp,
                humidity=humidity,
                pressure=pressure,
                wind_speed=wind_speed
            )
            
            if success:
                print(f"✅ Datos guardados para {location['name']}")
                return True
            else:
                print(f"❌ Error guardando datos para {location['name']}")
                return False
                
        except Exception as e:
            print(f"Error procesando datos para {location['name']}: {e}")
            return False

    # ======= NUEVO: utilidades para meteo histórica =======
    def _weather_day_cache(self, lat, lon, unix_ts):
        """Descarga y cachea las horas de meteo del día (UTC) de unix_ts."""
        day_key = (lat, lon, datetime.utcfromtimestamp(unix_ts).strftime('%Y-%m-%d'))
        if day_key in self._wx_cache:
            return self._wx_cache[day_key]

        self._wx_cache[day_key] = {}
        noon = int(datetime.utcfromtimestamp(unix_ts).replace(hour=12, minute=0, second=0, microsecond=0).timestamp())

        # Intentar v3 primero y luego v2.5 (según plan de API)
        for base in [
            "https://api.openweathermap.org/data/3.0/onecall/timemachine",
            "https://api.openweathermap.org/data/2.5/onecall/timemachine"
        ]:
            try:
                params = {'lat': lat, 'lon': lon, 'dt': noon, 'appid': self.api_key, 'units': 'metric'}
                r = requests.get(base, params=params, timeout=15)
                if r.status_code != 200:
                    continue
                data = r.json()

                def norm(obj):
                    return {
                        'temp': obj.get('temp'),
                        'humidity': obj.get('humidity'),
                        'pressure': obj.get('pressure'),
                        'wind_speed': obj.get('wind_speed') or (obj.get('wind') or {}).get('speed')
                    }

                m = {}
                for h in (data.get('hourly') or []):
                    if 'dt' in h:
                        m[h['dt']] = norm(h)
                cur = data.get('current') or {}
                if 'dt' in cur:
                    m[cur['dt']] = norm(cur)
                for h in (data.get('data') or []):
                    if 'dt' in h:
                        m[h['dt']] = {
                            'temp': h.get('temp') or (h.get('main') or {}).get('temp'),
                            'humidity': h.get('humidity') or (h.get('main') or {}).get('humidity'),
                            'pressure': h.get('pressure') or (h.get('main') or {}).get('pressure'),
                            'wind_speed': h.get('wind_speed') or (h.get('wind') or {}).get('speed')
                        }

                self._wx_cache[day_key] = m
                break
            except requests.exceptions.RequestException:
                pass

        return self._wx_cache.get(day_key, {})

    def _weather_at(self, lat, lon, unix_ts):
        """Devuelve meteo de la hora más cercana (±1h) al unix_ts para lat/lon."""
        day_map = self._weather_day_cache(lat, lon, unix_ts)
        if not day_map:
            return None
        nearest = min(day_map.keys(), key=lambda k: abs(int(k) - int(unix_ts))) if day_map else None
        if nearest is None or abs(int(nearest) - int(unix_ts)) > 3600:
            return None
        return day_map[nearest]
    # =======================================================
    
    def collect_all_locations(self):
        """Recolectar datos de todas las ubicaciones"""
        print(f"🔄 Iniciando recolección de datos - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        successful = 0
        failed = 0
        
        for location in self.locations:
            print(f"📡 Recolectando datos para {location['name']}...")
            
            # Obtener datos de la API
            api_data = self.get_air_quality_data(location['lat'], location['lon'])
            
            if api_data['success']:
                # Procesar y guardar
                if self.process_and_save_data(location, api_data):
                    successful += 1
                else:
                    failed += 1
            else:
                print(f"❌ Error en API para {location['name']}: {api_data.get('error', 'Unknown')}")
                failed += 1
            
            # Esperar entre peticiones para evitar límites de rate
            time.sleep(2)
        
        print(f"\n📊 Resumen de recolección:")
        print(f"   ✅ Exitosos: {successful}")
        print(f"   ❌ Fallidos: {failed}")
        print(f"   📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return successful, failed
    
    def collect_single_location(self, location_id):
        """Recolectar datos de una ubicación específica"""
        location = next((loc for loc in self.locations if loc['id'] == location_id), None)
        
        if not location:
            print(f"❌ Ubicación '{location_id}' no encontrada")
            return False
        
        print(f"📡 Recolectando datos para {location['name']}...")
        
        api_data = self.get_air_quality_data(location['lat'], location['lon'])
        
        if api_data['success']:
            return self.process_and_save_data(location, api_data)
        else:
            print(f"❌ Error en API: {api_data.get('error', 'Unknown')}")
            return False


    def collect_history_window(self, location, days=5, backfill_buffer_seconds=3600):
        """
        Descargar y guardar el histórico de 'days' días para una ubicación
        usando /air_pollution/history. OpenWeather permite hasta 5 días hacia atrás.
        Además, se completa con meteorología histórica (temp, humidity, pressure, wind_speed).
        """
        try:
            end = int(time.time())
            start = end - days * 24 * 3600 - int(backfill_buffer_seconds)

            url = f"{self.base_url}/air_pollution/history"
            params = {
                'lat': location['lat'],
                'lon': location['lon'],
                'start': start,
                'end': end,
                'appid': self.api_key
            }

            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            payload = r.json()
            items = payload.get('list', []) or []

            saved = 0
            for item in items:
                dt_val = item.get('dt')
                if not dt_val:
                    continue
                timestamp = datetime.fromtimestamp(dt_val, tz=timezone.utc).isoformat(timespec='seconds')
                comps = item.get('components', {}) or {}
                aqi = (item.get('main') or {}).get('aqi')

                # NUEVO: buscar meteo histórica cercana
                wx = self._weather_at(location['lat'], location['lon'], dt_val)
                temp = wx.get('temp') if wx else None
                humidity = wx.get('humidity') if wx else None
                pressure = wx.get('pressure') if wx else None
                wind_speed = wx.get('wind_speed') if wx else None

                ok = insert_air_quality_data(
                    location_id=location['id'],
                    location_name=location['name'],
                    lat=location['lat'],
                    lon=location['lon'],
                    timestamp=timestamp,
                    pm2_5=comps.get('pm2_5'),
                    pm10=comps.get('pm10'),
                    o3=comps.get('o3'),
                    no2=comps.get('no2'),
                    aqi=aqi,
                    temp=temp, humidity=humidity, pressure=pressure, wind_speed=wind_speed
                )
                if ok:
                    saved += 1

            print(f"✅ Histórico guardado para {location['name']}: {saved}/{len(items)} filas")
            return {'success': True, 'saved': saved, 'count': len(items)}
        except requests.exceptions.Timeout:
            print(f"Timeout al consultar histórico para {location['name']}")
            return {'success': False, 'error': 'Timeout'}
        except requests.exceptions.RequestException as e:
            print(f"Error histórico para {location['name']}: {e}")
            return {'success': False, 'error': str(e)}

    def collect_last5days_all_locations(self):
        """Ejecuta collect_history_window(days=5) para todas las ubicaciones."""
        results = []
        for i, loc in enumerate(self.locations, start=1):
            print(f"[{i}/{len(self.locations)}] Histórico 5 días → {loc['name']} ...", flush=True)
            res = self.collect_history_window(loc, days=5, backfill_buffer_seconds=3600)
            results.append((loc['id'], res))
            time.sleep(1)  # ir suave con la API
        return results

def main():
    """Función principal para ejecutar la recolección"""
    import os
    
    # Obtener API key del archivo de configuración o variable de entorno
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        # Intentar leer de archivo de configuración
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                api_key = config.get('openweather_api_key')
        except FileNotFoundError:
            print("❌ No se encontró config.json")
    
    if not api_key:
        print("❌ API key no configurada. Crea un archivo config.json con tu API key:")
        print('{"openweather_api_key": "tu_api_key_aqui"}')
        return
    
    # Crear instancia del recolector
    collector = AirQualityCollector(api_key)
    
    # Modo CLI opcional: backfill de 5 días
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "last5days":
        collector.collect_last5days_all_locations()
    else:
        # Recolectar datos de todas las ubicaciones
        collector.collect_all_locations()

if __name__ == "__main__":
    main()
