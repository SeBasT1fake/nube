from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, sum, stddev, when, month, year
import sys

def create_spark_session():
    """Crear sesión Spark optimizada"""
    return SparkSession.builder \
        .appName("WeatherDescriptiveAnalytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def load_trusted_data(spark):
    """Cargar datos desde zona Trusted"""
    print("📊 Cargando datos desde zona Trusted...")
    
    df = spark.read.parquet("s3://proyecto3-eafit-trusted/weather_processed/")
    
    # Registrar como vista temporal para usar SparkSQL
    df.createOrReplaceTempView("weather_data")
    
    print(f"🔢 Total registros cargados: {df.count()}")
    return df

def city_temperature_analysis(spark):
    """Análisis descriptivo por ciudad usando SparkSQL"""
    print("🏙️ Análisis de temperatura por ciudad...")
    
    # Usar SparkSQL como requiere el proyecto
    city_stats = spark.sql("""
        SELECT 
            city_name,
            COUNT(*) as total_records,
            ROUND(AVG(temp_max), 2) as avg_temp_max,
            ROUND(AVG(temp_min), 2) as avg_temp_min,
            ROUND(MAX(temp_max), 2) as max_temp_recorded,
            ROUND(MIN(temp_min), 2) as min_temp_recorded,
            ROUND(AVG(precipitation), 2) as avg_precipitation,
            ROUND(MAX(precipitation), 2) as max_precipitation,
            ROUND(STDDEV(temp_max), 2) as temp_variability
        FROM weather_data 
        WHERE city_name IS NOT NULL
        GROUP BY city_name
        ORDER BY avg_temp_max DESC
    """)
    
    print("📈 Estadísticas por ciudad:")
    city_stats.show(truncate=False)
    
    # Guardar resultados
    city_stats.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/city_temperature_stats/")
    
    return city_stats

def monthly_climate_patterns(spark):
    """Análisis de patrones climáticos mensuales usando SparkSQL"""
    print("📅 Análisis de patrones mensuales...")
    
    monthly_patterns = spark.sql("""
        SELECT 
            MONTH(date) as month,
            CASE 
                WHEN MONTH(date) IN (12, 1, 2) THEN 'Verano'
                WHEN MONTH(date) IN (3, 4, 5) THEN 'Otoño'
                WHEN MONTH(date) IN (6, 7, 8) THEN 'Invierno'
                ELSE 'Primavera'
            END as season,
            COUNT(*) as records_count,
            ROUND(AVG(temp_max), 2) as avg_temp_max,
            ROUND(AVG(temp_min), 2) as avg_temp_min,
            ROUND(AVG(precipitation), 2) as avg_precipitation,
            ROUND(SUM(precipitation), 2) as total_precipitation
        FROM weather_data 
        WHERE date IS NOT NULL
        GROUP BY MONTH(date)
        ORDER BY month
    """)
    
    print("🌡️ Patrones climáticos mensuales:")
    monthly_patterns.show(truncate=False)
    
    # Guardar resultados
    monthly_patterns.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/monthly_patterns/")
    
    return monthly_patterns

def extreme_weather_events(spark):
    """Identificar eventos climáticos extremos usando SparkSQL"""
    print("⚠️ Análisis de eventos climáticos extremos...")
    
    extreme_events = spark.sql("""
        SELECT 
            city_name,
            date,
            temp_max,
            temp_min,
            precipitation,
            CASE 
                WHEN temp_max > 35 THEN 'Calor Extremo'
                WHEN temp_min < 5 THEN 'Frío Extremo'
                WHEN precipitation > 50 THEN 'Lluvia Intensa'
                ELSE 'Normal'
            END as event_type
        FROM weather_data 
        WHERE temp_max > 35 OR temp_min < 5 OR precipitation > 50
        ORDER BY date DESC, temp_max DESC
    """)
    
    print("🌪️ Eventos climáticos extremos:")
    extreme_events.show(20, truncate=False)
    
    # Contar eventos por tipo
    event_summary = spark.sql("""
        SELECT 
            event_type,
            COUNT(*) as event_count,
            ROUND(AVG(temp_max), 2) as avg_temp_during_event
        FROM (
            SELECT 
                temp_max,
                CASE 
                    WHEN temp_max > 35 THEN 'Calor Extremo'
                    WHEN temp_min < 5 THEN 'Frío Extremo'
                    WHEN precipitation > 50 THEN 'Lluvia Intensa'
                    ELSE 'Normal'
                END as event_type
            FROM weather_data 
        ) events
        WHERE event_type != 'Normal'
        GROUP BY event_type
        ORDER BY event_count DESC
    """)
    
    print("📊 Resumen de eventos extremos:")
    event_summary.show()
    
    # Guardar resultados
    extreme_events.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/extreme_events/")
    event_summary.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/event_summary/")
    
    return extreme_events, event_summary

def precipitation_analysis(spark):
    """Análisis detallado de precipitaciones usando SparkSQL"""
    print("🌧️ Análisis de precipitaciones...")
    
    precip_analysis = spark.sql("""
        SELECT 
            city_name,
            COUNT(*) as total_days,
            COUNT(CASE WHEN precipitation > 0 THEN 1 END) as rainy_days,
            ROUND(COUNT(CASE WHEN precipitation > 0 THEN 1 END) * 100.0 / COUNT(*), 2) as rainy_days_percentage,
            ROUND(SUM(precipitation), 2) as total_precipitation,
            ROUND(AVG(precipitation), 2) as avg_daily_precipitation,
            ROUND(MAX(precipitation), 2) as max_daily_precipitation,
            COUNT(CASE WHEN precipitation > 10 THEN 1 END) as heavy_rain_days
        FROM weather_data 
        WHERE city_name IS NOT NULL
        GROUP BY city_name
        ORDER BY total_precipitation DESC
    """)
    
    print("☔ Análisis de precipitaciones por ciudad:")
    precip_analysis.show(truncate=False)
    
    # Guardar resultados
    precip_analysis.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/precipitation_analysis/")
    
    return precip_analysis

def data_quality_report(spark):
    """Reporte de calidad de datos usando SparkSQL"""
    print("🔍 Reporte de calidad de datos...")
    
    quality_report = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN city_name IS NULL THEN 1 END) as missing_city,
            COUNT(CASE WHEN temp_max IS NULL THEN 1 END) as missing_temp_max,
            COUNT(CASE WHEN temp_min IS NULL THEN 1 END) as missing_temp_min,
            COUNT(CASE WHEN precipitation IS NULL THEN 1 END) as missing_precipitation,
            COUNT(CASE WHEN station_name IS NOT NULL THEN 1 END) as records_with_station,
            COUNT(CASE WHEN population IS NOT NULL THEN 1 END) as records_with_population,
            ROUND(COUNT(CASE WHEN city_name IS NOT NULL AND temp_max IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as data_completeness_percentage
        FROM weather_data
    """)
    
    print("📋 Reporte de calidad de datos:")
    quality_report.show(truncate=False)
    
    # Guardar resultados
    quality_report.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/data_quality_report/")
    
    return quality_report

def generate_executive_summary(spark):
    """Generar resumen ejecutivo usando SparkSQL"""
    print("📄 Generando resumen ejecutivo...")
    
    executive_summary = spark.sql("""
        SELECT 
            'Resumen Ejecutivo - Análisis Climático' as report_title,
            COUNT(DISTINCT city_name) as cities_analyzed,
            COUNT(*) as total_weather_records,
            MIN(date) as period_start,
            MAX(date) as period_end,
            ROUND(AVG(temp_max), 2) as overall_avg_temp_max,
            ROUND(AVG(temp_min), 2) as overall_avg_temp_min,
            ROUND(AVG(precipitation), 2) as overall_avg_precipitation,
            COUNT(CASE WHEN temp_max > 35 THEN 1 END) as extreme_heat_days,
            COUNT(CASE WHEN precipitation > 50 THEN 1 END) as heavy_rain_days,
            CURRENT_TIMESTAMP() as report_generated_at
        FROM weather_data
    """)
    
    print("📊 Resumen Ejecutivo:")
    executive_summary.show(truncate=False)
    
    # Guardar resultados
    executive_summary.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/descriptive_analytics/executive_summary/")
    
    return executive_summary

def main():
    """Función principal del análisis descriptivo"""
    try:
        spark = create_spark_session()
        
        print("📊 Iniciando análisis descriptivo con SparkSQL...")
        
        # Cargar datos
        df = load_trusted_data(spark)
        
        # Ejecutar análisis descriptivos
        results = {}
        
        # 1. Análisis por ciudad
        results['city_stats'] = city_temperature_analysis(spark)
        
        # 2. Patrones mensuales
        results['monthly_patterns'] = monthly_climate_patterns(spark)
        
        # 3. Eventos extremos
        extreme_events, event_summary = extreme_weather_events(spark)
        results['extreme_events'] = extreme_events
        results['event_summary'] = event_summary
        
        # 4. Análisis de precipitaciones
        results['precipitation'] = precipitation_analysis(spark)
        
        # 5. Reporte de calidad
        results['quality_report'] = data_quality_report(spark)
        
        # 6. Resumen ejecutivo
        results['executive_summary'] = generate_executive_summary(spark)
        
        print("✅ Análisis descriptivo completado exitosamente!")
        print("📁 Resultados guardados en: s3://proyecto3-eafit-refined/descriptive_analytics/")
        
        return {
            'status': 'success',
            'analyses_completed': len(results),
            'output_location': 's3://proyecto3-eafit-refined/descriptive_analytics/'
        }
        
    except Exception as e:
        print(f"❌ Error en análisis descriptivo: {str(e)}")
        raise e
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    result = main()
    print(f"🎯 Resultado final: {result}")