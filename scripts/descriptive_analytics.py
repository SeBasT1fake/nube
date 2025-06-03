import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Crear sesión de Spark optimizada para análisis"""
    return SparkSession.builder \
        .appName("WeatherDescriptiveAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .getOrCreate()

def load_weather_data(spark, s3_path):
    """Cargar datos meteorológicos procesados"""
    try:
        logger.info(f"Cargando datos desde: {s3_path}")
        
        # Cargar datos principales
        weather_df = spark.read.parquet(f"{s3_path}/weather_data/")
        
        # Verificar que los datos se cargaron correctamente
        total_records = weather_df.count()
        logger.info(f"✅ Datos cargados: {total_records:,} registros")
        
        if total_records == 0:
            raise ValueError("No se encontraron datos meteorológicos")
            
        return weather_df
        
    except Exception as e:
        logger.error(f"❌ Error cargando datos: {str(e)}")
        raise

def generate_summary_statistics(df):
    """Generar estadísticas descriptivas generales"""
    logger.info("📊 Generando estadísticas descriptivas...")
    
    # Estadísticas básicas por ciudad
    summary_stats = df.groupBy("city") \
        .agg(
            count("*").alias("total_observations"),
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            stddev("temperature").alias("std_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("pressure").alias("avg_pressure"),
            avg("wind_speed").alias("avg_wind_speed"),
            countDistinct("date").alias("days_with_data")
        ) \
        .orderBy("city")
    
    logger.info("✅ Estadísticas por ciudad calculadas")
    return summary_stats

def analyze_temperature_trends(df):
    """Analizar tendencias de temperatura"""
    logger.info("🌡️ Analizando tendencias de temperatura...")
    
    # Agregar columnas de tiempo
    df_with_time = df.withColumn("year", year("date")) \
                    .withColumn("month", month("date")) \
                    .withColumn("day_of_year", dayofyear("date"))
    
    # Tendencias mensuales por ciudad
    monthly_trends = df_with_time.groupBy("city", "year", "month") \
        .agg(
            avg("temperature").alias("avg_monthly_temp"),
            min("temperature").alias("min_monthly_temp"),
            max("temperature").alias("max_monthly_temp"),
            count("*").alias("observations_count")
        ) \
        .orderBy("city", "year", "month")
    
    # Temperaturas extremas
    extreme_temps = df.groupBy("city") \
        .agg(
            max("temperature").alias("highest_temp"),
            min("temperature").alias("lowest_temp")
        ) \
        .withColumn("temperature_range", col("highest_temp") - col("lowest_temp"))
    
    logger.info("✅ Análisis de tendencias completado")
    return monthly_trends, extreme_temps

def analyze_weather_patterns(df):
    """Analizar patrones meteorológicos"""
    logger.info("🌤️ Analizando patrones meteorológicos...")
    
    # Clasificar días por temperatura
    weather_patterns = df.withColumn(
        "temp_category",
        when(col("temperature") < 15, "Frío")
        .when(col("temperature") < 25, "Templado")
        .when(col("temperature") < 30, "Cálido")
        .otherwise("Muy Cálido")
    )
    
    # Contar días por categoría
    temp_distribution = weather_patterns.groupBy("city", "temp_category") \
        .agg(count("*").alias("days_count")) \
        .orderBy("city", "temp_category")
    
    # Análisis de humedad
    humidity_analysis = df.groupBy("city") \
        .agg(
            avg("humidity").alias("avg_humidity"),
            expr("percentile_approx(humidity, 0.25)").alias("humidity_q1"),
            expr("percentile_approx(humidity, 0.5)").alias("humidity_median"),
            expr("percentile_approx(humidity, 0.75)").alias("humidity_q3")
        )
    
    logger.info("✅ Análisis de patrones completado")
    return temp_distribution, humidity_analysis

def generate_city_rankings(df):
    """Generar rankings de ciudades"""
    logger.info("🏆 Generando rankings de ciudades...")
    
    # Ranking por temperatura promedio
    temp_ranking = df.groupBy("city") \
        .agg(avg("temperature").alias("avg_temperature")) \
        .orderBy(desc("avg_temperature")) \
        .withColumn("temp_rank", row_number().over(
            Window.orderBy(desc("avg_temperature"))
        ))
    
    # Ranking por humedad
    humidity_ranking = df.groupBy("city") \
        .agg(avg("humidity").alias("avg_humidity")) \
        .orderBy(desc("avg_humidity")) \
        .withColumn("humidity_rank", row_number().over(
            Window.orderBy(desc("avg_humidity"))
        ))
    
    # Combinar rankings
    combined_ranking = temp_ranking.select("city", "avg_temperature", "temp_rank") \
        .join(
            humidity_ranking.select("city", "avg_humidity", "humidity_rank"),
            "city"
        )
    
    logger.info("✅ Rankings generados")
    return combined_ranking

def save_results_to_s3(spark, results_dict, output_path):
    """Guardar resultados en S3"""
    logger.info(f"💾 Guardando resultados en: {output_path}")
    
    try:
        for name, df in results_dict.items():
            result_path = f"{output_path}/descriptive_analytics/{name}"
            
            # Guardar en formato Parquet para eficiencia
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(result_path)
            
            logger.info(f"✅ Guardado: {name} -> {result_path}")
            
            # También guardar como CSV para fácil lectura
            csv_path = f"{output_path}/descriptive_analytics_csv/{name}"
            df.coalesce(1) \
              .write \
              .mode("overwrite") \
              .option("header", "true") \
              .csv(csv_path)
    
    except Exception as e:
        logger.error(f"❌ Error guardando resultados: {str(e)}")
        raise

def create_summary_report(results_dict):
    """Crear reporte resumen"""
    logger.info("📋 Creando reporte resumen...")
    
    try:
        # Obtener spark session
        spark = SparkSession.getActiveSession()
        
        # Crear DataFrame de resumen
        summary_data = []
        
        for name, df in results_dict.items():
            count = df.count()
            columns = len(df.columns)
            summary_data.append((name, count, columns))
        
        summary_schema = StructType([
            StructField("analysis_type", StringType(), True),
            StructField("records_count", IntegerType(), True),
            StructField("columns_count", IntegerType(), True)
        ])
        
        summary_df = spark.createDataFrame(summary_data, summary_schema)
        
        # Agregar timestamp
        summary_df = summary_df.withColumn("generated_at", current_timestamp())
        
        logger.info("✅ Reporte resumen creado")
        return summary_df
        
    except Exception as e:
        logger.error(f"❌ Error creando reporte: {str(e)}")
        raise

def main():
    """Función principal del análisis descriptivo"""
    try:
        logger.info("🚀 Iniciando análisis descriptivo de datos meteorológicos")
        
        # Configuración
        INPUT_PATH = "s3://proyecto3-eafit-trusted"
        OUTPUT_PATH = "s3://proyecto3-eafit-refined"
        
        # Crear sesión Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Cargar datos
        weather_df = load_weather_data(spark, INPUT_PATH)
        
        # Generar análisis
        logger.info("🔬 Ejecutando análisis descriptivos...")
        
        # 1. Estadísticas generales
        summary_stats = generate_summary_statistics(weather_df)
        
        # 2. Análisis de temperatura
        monthly_trends, extreme_temps = analyze_temperature_trends(weather_df)
        
        # 3. Patrones meteorológicos
        temp_distribution, humidity_analysis = analyze_weather_patterns(weather_df)
        
        # 4. Rankings de ciudades
        city_rankings = generate_city_rankings(weather_df)
        
        # Preparar resultados
        results = {
            "summary_statistics": summary_stats,
            "monthly_temperature_trends": monthly_trends,
            "extreme_temperatures": extreme_temps,
            "temperature_distribution": temp_distribution,
            "humidity_analysis": humidity_analysis,
            "city_rankings": city_rankings
        }
        
        # Crear reporte resumen
        summary_report = create_summary_report(results)
        results["execution_summary"] = summary_report
        
        # Guardar resultados
        save_results_to_s3(spark, results, OUTPUT_PATH)
        
        # Mostrar algunas estadísticas en logs
        logger.info("📊 RESUMEN DEL ANÁLISIS:")
        logger.info("-" * 50)
        
        # Mostrar estadísticas básicas
        logger.info("🌡️ Estadísticas por ciudad:")
        summary_stats.show(truncate=False)
        
        logger.info("🏆 Ranking de temperaturas:")
        city_rankings.select("city", "avg_temperature", "temp_rank").show()
        
        logger.info("✅ Análisis descriptivo completado exitosamente")
        
        # Retornar información para Step Functions
        return {
            "status": "success",
            "message": "Análisis descriptivo completado",
            "output_location": OUTPUT_PATH,
            "analyses_generated": len(results),
            "total_records_processed": weather_df.count()
        }
        
    except Exception as e:
        logger.error(f"❌ Error en análisis descriptivo: {str(e)}")
        raise
    
    finally:
        # Limpiar recursos
        if 'spark' in locals():
            spark.stop()
            logger.info("🔚 Sesión Spark cerrada")

if __name__ == "__main__":
    try:
        result = main()
        logger.info(f"🎉 Proceso completado: {result}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fallo crítico: {str(e)}")
        sys.exit(1)