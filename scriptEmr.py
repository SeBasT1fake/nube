from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, when, isnan, isnull
import sys

def create_spark_session():
    """Crear sesión Spark con configuración para MySQL"""
    return SparkSession.builder \
        .appName("WeatherETL-EC2MySQL") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def read_from_mysql(spark, table_name, db_config):
    """Leer datos desde MySQL en EC2"""
    jdbc_url = f"jdbc:mysql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

def main():
    try:
        spark = create_spark_session()
        
        # Configuración de base de datos EC2
        # IMPORTANTE: Reemplazar con la IP real de tu EC2
        DB_CONFIG = {
            'host': '54.123.45.67',  # ⚠️ CAMBIAR POR TU IP EC2
            'port': '3306',
            'database': 'weather_db',
            'user': 'weather_user',
            'password': 'Weather123!'
        }
        
        print("🔗 Conectando a MySQL en EC2...")
        
        # Leer datos weather desde S3 (de la API)
        print("📊 Leyendo datos weather desde S3")
        weather_raw = spark.read.json("s3://proyecto3-eafit-raw/weather_data/*.json")
        
        weather_clean = weather_raw.select(
            col("latitude").alias("lat"),
            col("longitude").alias("lon"), 
            col("date"),
            col("temp_max"),
            col("temp_min"),
            col("precipitation"),
            col("city").alias("weather_city")
        )
        
        print("🏙️ Leyendo cities desde MySQL/EC2")
        cities_df = read_from_mysql(spark, "cities", DB_CONFIG)
        cities_df = cities_df.select(
            col("name").alias("city_name"),
            col("department"),
            col("latitude").alias("city_lat"),
            col("longitude").alias("city_lon"),
            col("population")
        )
        
        print("📡 Leyendo stations desde MySQL/EC2")
        stations_df = read_from_mysql(spark, "stations", DB_CONFIG) 
        stations_df = stations_df.select(
            col("station_name"),
            col("city").alias("station_city"),
            col("latitude").alias("station_lat"),
            col("longitude").alias("station_lon"),
            col("altitude"),
            col("status")
        )
        
        print("🚨 Leyendo weather_alerts desde MySQL/EC2")
        alerts_df = read_from_mysql(spark, "weather_alerts", DB_CONFIG)
        alerts_df = alerts_df.select(
            col("city").alias("alert_city"),
            col("alert_type"),
            col("severity"),
            col("threshold_value"),
            col("is_active")
        )
        
        # Mostrar datos de muestra
        print("🔍 Datos de muestra:")
        print("Weather data:")
        weather_clean.show(3)
        print("Cities data:")
        cities_df.show(3)
        print("Stations data:")
        stations_df.show(3)
        print("Alerts data:")
        alerts_df.show(3)
        
        # Realizar joins
        print("🔗 Realizando joins...")
        
        # Join weather con cities por coordenadas aproximadas
        weather_cities = weather_clean.join(
            cities_df,
            (abs(weather_clean.lat - cities_df.city_lat) < 0.1) &
            (abs(weather_clean.lon - cities_df.city_lon) < 0.1),
            "left"
        )
        
        # Join con stations
        weather_stations = weather_cities.join(
            stations_df,
            (abs(weather_cities.lat - stations_df.station_lat) < 0.1) &
            (abs(weather_cities.lon - stations_df.station_lon) < 0.1),
            "left"
        )
        
        # Join con alerts
        final_data = weather_stations.join(
            alerts_df,
            weather_stations.city_name == alerts_df.alert_city,
            "left"
        )
        
        # Limpiar datos nulos y agregar métricas
        processed_data = final_data.withColumn(
            "temp_avg", (col("temp_max") + col("temp_min")) / 2
        ).withColumn(
            "alert_triggered", 
            when(
                (col("alert_type") == "temperatura") & (col("temp_max") > col("threshold_value")), True
            ).when(
                (col("alert_type") == "lluvia") & (col("precipitation") > col("threshold_value")), True
            ).otherwise(False)
        ).withColumn(
            "data_quality_score",
            when(
                col("city_name").isNull() | col("station_name").isNull(), 0.5
            ).otherwise(1.0)
        )
        
        print("💾 Guardando en zona Trusted...")
        processed_data.write \
            .mode("overwrite") \
            .partitionBy("date") \
            .parquet("s3://proyecto3-eafit-trusted/weather_processed/")
        
        # Estadísticas finales
        total_records = processed_data.count()
        valid_records = processed_data.filter(col("data_quality_score") == 1.0).count()
        
        print(f"✅ ETL completado exitosamente!")
        print(f"📊 Total registros: {total_records}")
        print(f"✅ Registros válidos: {valid_records}")
        print(f"📈 Calidad de datos: {(valid_records/total_records)*100:.1f}%")
        
        return {
            'status': 'success',
            'total_records': total_records,
            'valid_records': valid_records,
            'data_quality': (valid_records/total_records)*100
        }
        
    except Exception as e:
        print(f"❌ Error en ETL: {str(e)}")
        raise e
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    result = main()
    print(f"🎯 Resultado final: {result}")