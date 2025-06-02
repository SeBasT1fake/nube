import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda para crear tablas en Athena apuntando a datos en S3
    """
    try:
        database_name = event.get('database_name', 'weather_analytics_db')
        refined_bucket = event.get('refined_bucket', 'proyecto3-eafit-refined')
        tables_to_create = event.get('tables_to_create', [])
        
        # Configuración de Athena
        output_location = f"s3://{refined_bucket}/athena-results/"
        
        # Crear base de datos si no existe
        create_database_query = f"""
        CREATE DATABASE IF NOT EXISTS {database_name}
        COMMENT 'Base de datos para analytics de clima'
        LOCATION 's3://{refined_bucket}/database/'
        """
        
        query_execution_id = execute_athena_query(
            create_database_query, 
            output_location
        )
        
        wait_for_query_completion(query_execution_id)
        logger.info(f"✅ Base de datos {database_name} creada/verificada")
        
        created_tables = []
        
        # Crear cada tabla
        for table_config in tables_to_create:
            table_name = table_config['table_name']
            location = table_config['location']
            format_type = table_config.get('format', 'PARQUET')
            
            # DDL específico para cada tabla
            if table_name == 'city_temperature_stats':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    city string,
                    year int,
                    month int,
                    avg_temp_max double,
                    avg_temp_min double,
                    total_precipitation double,
                    avg_windspeed double,
                    days_with_rain int,
                    extreme_temp_max double,
                    extreme_temp_min double
                )
                STORED AS {format_type}
                LOCATION '{location}'
                """
            
            elif table_name == 'monthly_patterns':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    month int,
                    city string,
                    avg_temperature double,
                    total_precipitation double,
                    precipitation_days int,
                    temperature_variance double,
                    seasonal_pattern string
                )
                STORED AS {format_type}
                LOCATION '{location}'
                """
            
            elif table_name == 'extreme_events':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    city string,
                    event_date date,
                    event_type string,
                    temperature_max double,
                    precipitation double,
                    windspeed_max double,
                    severity_score double,
                    duration_days int
                )
                STORED AS {format_type}
                LOCATION '{location}'
                """
            
            elif table_name == 'ml_predictions':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    city string,
                    prediction_date date,
                    predicted_temp_max double,
                    predicted_temp_min double,
                    predicted_precipitation double,
                    confidence_score double,
                    model_version string,
                    prediction_timestamp timestamp
                )
                STORED AS {format_type}
                LOCATION '{location}'
                """
            
            else:
                # Tabla genérica
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    data string
                )
                STORED AS {format_type}
                LOCATION '{location}'
                """
            
            # Ejecutar creación de tabla
            query_execution_id = execute_athena_query(
                create_table_query, 
                output_location, 
                database_name
            )
            
            wait_for_query_completion(query_execution_id)
            
            created_tables.append({
                'table_name': table_name,
                'location': location,
                'status': 'created',
                'query_execution_id': query_execution_id
            })
            
            logger.info(f"✅ Tabla {table_name} creada en Athena")
        
        # Crear vista de resumen
        summary_view_query = f"""
        CREATE OR REPLACE VIEW {database_name}.weather_summary AS
        SELECT 
            city,
            year,
            month,
            avg_temp_max,
            avg_temp_min,
            total_precipitation,
            CASE 
                WHEN avg_temp_max > 30 THEN 'Hot'
                WHEN avg_temp_max < 15 THEN 'Cold'
                ELSE 'Moderate'
            END as temperature_category,
            CASE 
                WHEN total_precipitation > 200 THEN 'High'
                WHEN total_precipitation > 100 THEN 'Medium'
                ELSE 'Low'
            END as precipitation_category
        FROM {database_name}.city_temperature_stats
        WHERE year >= 2023
        """
        
        query_execution_id = execute_athena_query(
            summary_view_query, 
            output_location, 
            database_name
        )
        
        wait_for_query_completion(query_execution_id)
        logger.info("✅ Vista de resumen creada")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Tablas de Athena configuradas exitosamente',
                'database_name': database_name,
                'created_tables': created_tables,
                'summary_view': 'weather_summary',
                'query_location': f"s3://{refined_bucket}/athena-results/",
                'timestamp': datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error configurando Athena: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

def execute_athena_query(query, output_location, database=None):
    """Ejecutar query en Athena"""
    params = {
        'QueryString': query,
        'ResultConfiguration': {
            'OutputLocation': output_location
        }
    }
    
    if database:
        params['QueryExecutionContext'] = {'Database': database}
    
    response = athena_client.start_query_execution(**params)
    return response['QueryExecutionId']

def wait_for_query_completion(query_execution_id, max_wait=300):
    """Esperar a que termine la query"""
    import time
    
    waited = 0
    while waited < max_wait:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        
        status = response['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED']:
            return True
        elif status in ['FAILED', 'CANCELLED']:
            error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Query failed: {error_msg}")
        
        time.sleep(10)
        waited += 10
    
    raise Exception(f"Query timeout after {max_wait} seconds")

# Función para testing
def test_athena_setup():
    """Probar configuración de Athena"""
    test_event = {
        'database_name': 'weather_analytics_db',
        'refined_bucket': 'proyecto3-eafit-refined',
        'tables_to_create': [
            {
                'table_name': 'city_temperature_stats',
                'location': 's3://proyecto3-eafit-refined/descriptive_analytics/city_temperature_stats/',
                'format': 'PARQUET'
            }
        ]
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    test_athena_setup()