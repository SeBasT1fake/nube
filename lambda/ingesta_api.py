import json
import boto3
import requests
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda para ingesta automática de datos climáticos desde Open-Meteo API
    """
    try:
        BUCKET_RAW = 'proyecto3-eafit-raw'
        
        # Ciudades colombianas principales
        cities = [
            {"name": "Medellin", "lat": 6.25, "lon": -75.56},
            {"name": "Bogota", "lat": 4.71, "lon": -74.07},
            {"name": "Cali", "lat": 3.44, "lon": -76.52},
            {"name": "Barranquilla", "lat": 10.96, "lon": -74.80},
            {"name": "Cartagena", "lat": 10.39, "lon": -75.51}
        ]
        
        # Fechas para datos históricos (último año)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        
        all_weather_data = []
        
        for city in cities:
            logger.info(f"Obteniendo datos para {city['name']}")
            
            # URL de Open-Meteo API (sin autenticación requerida)
            url = f"https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': city['lat'],
                'longitude': city['lon'],
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max',
                'timezone': 'America/Bogota'
            }
            
            # Hacer petición a la API
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Procesar datos
            if 'daily' in data:
                daily_data = data['daily']
                dates = daily_data['time']
                temps_max = daily_data['temperature_2m_max']
                temps_min = daily_data['temperature_2m_min']
                precipitation = daily_data['precipitation_sum']
                windspeed = daily_data['windspeed_10m_max']
                
                # Crear registros estructurados
                for i, date in enumerate(dates):
                    record = {
                        'city': city['name'],
                        'latitude': city['lat'],
                        'longitude': city['lon'],
                        'date': date,
                        'temp_max': temps_max[i] if temps_max[i] is not None else 0,
                        'temp_min': temps_min[i] if temps_min[i] is not None else 0,
                        'precipitation': precipitation[i] if precipitation[i] is not None else 0,
                        'windspeed_max': windspeed[i] if windspeed[i] is not None else 0,
                        'extraction_timestamp': datetime.now().isoformat()
                    }
                    all_weather_data.append(record)
        
        # Guardar en S3 como JSON
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"weather_data/weather_data_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=BUCKET_RAW,
            Key=s3_key,
            Body=json.dumps(all_weather_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"✅ Datos guardados: s3://{BUCKET_RAW}/{s3_key}")
        logger.info(f"📊 Total registros: {len(all_weather_data)}")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Ingesta de datos climáticos completada exitosamente',
                's3_location': f"s3://{BUCKET_RAW}/{s3_key}",
                'records_count': len(all_weather_data),
                'cities_processed': len(cities),
                'timestamp': timestamp
            }
        }
        
    except requests.RequestException as e:
        logger.error(f"Error en API request: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': f'Error al consultar API: {str(e)}'}
        }
    except Exception as e:
        logger.error(f"Error general: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

# Para probar localmente
if __name__ == "__main__":
    test_event = {}
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))