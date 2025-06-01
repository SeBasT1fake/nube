from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder.appName("CheckParquet").getOrCreate()

    print("✅ Leyendo datos Parquet de S3")
    df = spark.read.parquet("s3://proyecto3-eafit-trusted/weather_processed/")

    print("✅ Mostrando datos de muestra")
    df.show(truncate=False)

    print("✅ Esquema de los datos")
    df.printSchema()

    print(f"✅ Total de registros: {df.count()}")

    print("✅ Validación completada con éxito")

except Exception as e:
    print("❌ Error durante la validación:", str(e))

finally:
    spark.stop()
