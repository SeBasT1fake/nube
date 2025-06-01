from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs
import sys

try:
    spark = SparkSession.builder.appName("WeatherETL").getOrCreate()

    print("✅ Leyendo datos weather JSON")
    weather_raw = spark.read.json("s3://proyecto3-eafit-raw/weather_data/file.json")

    # Validar esquema y mostrar datos
    weather_raw.printSchema()
    weather_raw.show(5)

    weather_clean = weather_raw.select(
        col("latitude").alias("lat"),
        col("longitude").alias("lon"),
        col("date"),
        col("temp_max"),
        col("temp_min"),
        col("precipitation")
    )

    print("✅ Leyendo cities.csv")
    cities_df = spark.read.option("header", True).csv("s3://proyecto3-eafit-raw/database_exports/cities.csv")
    cities_df = cities_df.withColumn("population", col("population").cast("int")) \
                         .select(col("name").alias("city_name"),
                                 col("latitude").alias("city_lat"),
                                 col("longitude").alias("city_lon"),
                                 col("population"))

    print("✅ Leyendo stations.csv")
    stations_df = spark.read.option("header", True).csv("s3://proyecto3-eafit-raw/database_exports/stations.csv")
    stations_df = stations_df.withColumn("altitude", col("altitude").cast("int")) \
                             .select(col("station_name"),
                                     col("latitude").alias("station_lat"),
                                     col("longitude").alias("station_lon"),
                                     col("altitude"))

    print("✅ Leyendo weather_alerts.csv")
    alerts_df = spark.read.option("header", True).csv("s3://proyecto3-eafit-raw/database_exports/weather_alerts.csv")
    alerts_df = alerts_df.withColumn("threshold_value", col("threshold_value").cast("double"))

    print("✅ Haciendo joins")
    joined_data = weather_clean.join(
        cities_df,
        (abs(weather_clean.lat - cities_df.city_lat) < 0.5) &
        (abs(weather_clean.lon - cities_df.city_lon) < 0.5),
        "left"
    ).join(
        stations_df,
        (abs(weather_clean.lat - stations_df.station_lat) < 0.5) &
        (abs(weather_clean.lon - stations_df.station_lon) < 0.5),
        "left"
    )

    print("✅ Escribiendo resultados a trusted (S3)")
    joined_data.write.mode("overwrite").parquet("s3://proyecto3-eafit-trusted/weather_processed/")

    print("✅ ETL completado con éxito")

except Exception as e:
    print("❌ Error encontrado:", str(e))
    sys.exit(1)

finally:
    spark.stop()
