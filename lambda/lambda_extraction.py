import json
import boto3
import pymysql
import csv
import io
from datetime import datetime
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda para extracción de datos desde EC2/MySQL
    """
    try:
        BUCKET_RAW = 'proyecto3-eafit-raw'
        
        # Configuración de conexión a EC2/MySQL
        DB_CONFIG = {
            'host': os.environ.get('DB_HOST', ''),  # IP del EC2
            'port': int(os.environ.get('DB_PORT', '3306')),
            'user': os.environ.get('DB_USER', 'weather_user'),
            'password': os.environ.get('DB_PASSWORD', ''),
            'database': os.environ.get('DB_NAME', 'weather_db')
        }
        
        tables_to_extract = event.get('tables', ['cities', 'stations', 'weather_alerts'])
        
        # Conectar a la base de datos
        logger.info(f"Conectando a MySQL en {DB_CONFIG['host']}")
        
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset='utf8mb4',
            connect_timeout=30,
            read_timeout=30,
            write_timeout=30
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_files = []
        
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            for table_name in tables_to_extract:
                logger.info(f"Extrayendo tabla: {table_name}")
                
                # Consultar datos de la tabla
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning(f"No se encontraron datos en la tabla {table_name}")
                    continue
                
                # Convertir a CSV
                csv_buffer = io.StringIO()
                
                # Obtener nombres de columnas
                fieldnames = rows[0].keys() if rows else []
                writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
                writer.writeheader()
                
                # Escribir datos
                for row in rows:
                    # Convertir valores datetime a string
                    clean_row = {}
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            clean_row[key] = value.isoformat()
                        else:
                            clean_row[key] = value
                    writer.writerow(clean_row)
                
                # Subir a S3
                s3_key = f"database_exports/{table_name}_{timestamp}.csv"
                s3_client.put_object(
                    Bucket=BUCKET_RAW,
                    Key=s3_key,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv'
                )
                
                uploaded_files.append({
                    'table': table_name,
                    's3_location': f"s3://{BUCKET_RAW}/{s3_key}",
                    'records_count': len(rows)
                })
                
                logger.info(f"✅ Tabla {table_name}: {len(rows)} registros -> S3")
        
        connection.close()
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Extracción desde EC2/MySQL completada exitosamente',
                'uploaded_files': uploaded_files,
                'total_tables': len(uploaded_files),
                'timestamp': timestamp
            }
        }
        
    except pymysql.Error as e:
        logger.error(f"Error de MySQL: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': f'Error de base de datos: {str(e)}'}
        }
    except Exception as e:
        logger.error(f"Error general: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

def test_connection():
    """
    Función para probar la conexión desde EMR o localmente
    """
    try:
        DB_CONFIG = {
            'host': '54.221.155.180',  # Reemplazar con IP real
            'port': 3306,
            'user': 'weather_user',
            'password': 'Weather123!',
            'database': 'weather_db'
        }
        
        connection = pymysql.connect(**DB_CONFIG)
        
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as total FROM cities")
            result = cursor.fetchone()
            print(f"✅ Conexión exitosa. Total ciudades: {result[0]}")
            
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"✅ Tablas disponibles: {[table[0] for table in tables]}")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return False

if __name__ == "__main__":
    test_connection()