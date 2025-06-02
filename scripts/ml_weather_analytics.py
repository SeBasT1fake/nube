from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, when, isnan, isnull, dayofyear, month, year
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import sys

def create_spark_session():
    """Crear sesión Spark con configuración para ML"""
    return SparkSession.builder \
        .appName("WeatherML-Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def load_and_prepare_data(spark):
    """Cargar y preparar datos para análisis ML"""
    print("📊 Cargando datos desde zona Trusted...")
    
    # Cargar datos procesados
    df = spark.read.parquet("s3://proyecto3-eafit-trusted/weather_processed/")
    
    print(f"🔢 Total registros cargados: {df.count()}")
    
    # Crear características adicionales
    df_ml = df.withColumn("day_of_year", dayofyear(col("date"))) \
              .withColumn("month", month(col("date"))) \
              .withColumn("year", year(col("date"))) \
              .withColumn("temp_range", col("temp_max") - col("temp_min")) \
              .withColumn("temp_avg", (col("temp_max") + col("temp_min")) / 2) \
              .withColumn("high_precipitation", when(col("precipitation") > 10, 1).otherwise(0)) \
              .withColumn("extreme_temp", when(col("temp_max") > 35, 1).otherwise(0))
    
    # Filtrar datos válidos (sin nulos en variables críticas)
    df_clean = df_ml.filter(
        col("temp_max").isNotNull() & 
        col("temp_min").isNotNull() & 
        col("precipitation").isNotNull() &
        col("city_name").isNotNull()
    )
    
    print(f"🧹 Registros después de limpieza: {df_clean.count()}")
    
    return df_clean

def temperature_prediction_model(df, spark):
    """Modelo de predicción de temperatura máxima"""
    print("🌡️ Entrenando modelo de predicción de temperatura...")
    
    # Preparar features para predicción de temperatura
    feature_cols = ["temp_min", "precipitation", "day_of_year", "month", "altitude", "population"]
    
    # Filtrar columnas que existen y no son nulas
    available_cols = [col_name for col_name in feature_cols if col_name in df.columns]
    df_temp = df.select(available_cols + ["temp_max"]).filter(
        col("temp_max").isNotNull()
    )
    
    # Llenar valores nulos con medias
    for col_name in available_cols:
        if col_name in ["altitude", "population"]:
            # Para columnas que pueden ser nulas, llenar con 0
            df_temp = df_temp.fillna({col_name: 0})
    
    # Ensamblar características
    assembler = VectorAssembler(inputCols=available_cols, outputCol="features")
    
    # Escalador
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # Modelo de regresión
    rf_regressor = RandomForestRegressor(
        featuresCol="scaled_features", 
        labelCol="temp_max",
        numTrees=50,
        maxDepth=10
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf_regressor])
    
    # División train/test
    train_data, test_data = df_temp.randomSplit([0.8, 0.2], seed=42)
    
    print(f"📈 Datos de entrenamiento: {train_data.count()}")
    print(f"🧪 Datos de prueba: {test_data.count()}")
    
    # Entrenar modelo
    model = pipeline.fit(train_data)
    
    # Predicciones
    predictions = model.transform(test_data)
    
    # Evaluación
    evaluator = RegressionEvaluator(labelCol="temp_max", predictionCol="prediction")
    
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    
    print(f"🎯 RMSE: {rmse:.2f}")
    print(f"📊 MAE: {mae:.2f}")
    print(f"📈 R²: {r2:.3f}")
    
    # Guardar resultados
    results = predictions.select("temp_max", "prediction", "city_name", "date") \
                        .withColumn("error", abs(col("temp_max") - col("prediction")))
    
    results.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/ml_results/temperature_predictions/")
    
    return {
        "model_type": "temperature_prediction",
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "predictions_count": predictions.count()
    }

def precipitation_classification_model(df, spark):
    """Modelo de clasificación de precipitación (alta/baja)"""
    print("🌧️ Entrenando modelo de clasificación de precipitación...")
    
    # Crear variable objetivo: precipitación alta (>5mm) o baja (<=5mm)
    df_precip = df.withColumn("precip_class", when(col("precipitation") > 5, 1).otherwise(0))
    
    # Features
    feature_cols = ["temp_max", "temp_min", "temp_range", "day_of_year", "month"]
    available_cols = [col_name for col_name in feature_cols if col_name in df.columns]
    
    df_class = df_precip.select(available_cols + ["precip_class"]).filter(
        col("precip_class").isNotNull()
    )
    
    # Ensamblar características
    assembler = VectorAssembler(inputCols=available_cols, outputCol="features")
    
    # Escalador
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # Clasificador
    rf_classifier = RandomForestClassifier(
        featuresCol="scaled_features",
        labelCol="precip_class",
        numTrees=50,
        maxDepth=10
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf_classifier])
    
    # División train/test
    train_data, test_data = df_class.randomSplit([0.8, 0.2], seed=42)
    
    # Entrenar
    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)
    
    # Evaluación
    evaluator = MulticlassClassificationEvaluator(
        labelCol="precip_class", 
        predictionCol="prediction"
    )
    
    accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
    precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
    
    print(f"🎯 Accuracy: {accuracy:.3f}")
    print(f"📊 Precision: {precision:.3f}")
    print(f"📈 Recall: {recall:.3f}")
    
    # Guardar resultados
    class_results = predictions.select("precip_class", "prediction", "probability")
    class_results.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/ml_results/precipitation_classification/")
    
    return {
        "model_type": "precipitation_classification",
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "predictions_count": predictions.count()
    }

def weather_clustering_analysis(df, spark):
    """Análisis de clustering de patrones climáticos"""
    print("🔍 Realizando clustering de patrones climáticos...")
    
    # Features para clustering
    feature_cols = ["temp_avg", "temp_range", "precipitation"]
    
    df_cluster = df.select(feature_cols + ["city_name", "date"]).filter(
        col("temp_avg").isNotNull() & 
        col("precipitation").isNotNull()
    )
    
    # Ensamblar características
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Escalador
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # KMeans clustering
    kmeans = KMeans(k=4, featuresCol="scaled_features", predictionCol="cluster")
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    # Entrenar
    model = pipeline.fit(df_cluster)
    clustered_data = model.transform(df_cluster)
    
    # Análisis de clusters
    cluster_stats = clustered_data.groupBy("cluster") \
        .agg(
            count("*").alias("count"),
            avg("temp_avg").alias("avg_temp"),
            avg("temp_range").alias("avg_temp_range"),
            avg("precipitation").alias("avg_precipitation")
        ).orderBy("cluster")
    
    print("📊 Estadísticas por cluster:")
    cluster_stats.show()
    
    # Guardar resultados
    clustered_data.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/ml_results/weather_clusters/")
    cluster_stats.write.mode("overwrite").parquet("s3://proyecto3-eafit-refined/ml_results/cluster_statistics/")
    
    return {
        "model_type": "weather_clustering",
        "clusters_count": 4,
        "total_records": clustered_data.count()
    }

def generate_ml_summary(results_list, spark):
    """Generar resumen ejecutivo de todos los modelos ML"""
    print("📋 Generando resumen ejecutivo de ML...")
    
    summary_data = []
    for result in results_list:
        summary_data.append(result)
    
    # Crear DataFrame con resultados
    summary_df = spark.createDataFrame(summary_data)
    
    # Guardar resumen
    summary_df.write.mode("overwrite").json("s3://proyecto3-eafit-refined/ml_results/summary/")
    
    print("✅ Resumen de modelos ML guardado")
    return summary_data

def main():
    """Función principal del análisis ML"""
    try:
        spark = create_spark_session()
        
        print("🤖 Iniciando análisis de Machine Learning...")
        
        # Cargar datos
        df = load_and_prepare_data(spark)
        
        # Lista para almacenar resultados
        ml_results = []
        
        # 1. Modelo de predicción de temperatura
        temp_result = temperature_prediction_model(df, spark)
        ml_results.append(temp_result)
        
        # 2. Modelo de clasificación de precipitación
        precip_result = precipitation_classification_model(df, spark)
        ml_results.append(precip_result)
        
        # 3. Análisis de clustering
        cluster_result = weather_clustering_analysis(df, spark)
        ml_results.append(cluster_result)
        
        # 4. Generar resumen
        summary = generate_ml_summary(ml_results, spark)
        
        print("✅ Análisis de Machine Learning completado exitosamente!")
        print("📊 Modelos entrenados:")
        for result in ml_results:
            print(f"  - {result['model_type']}")
        
        return {
            'status': 'success',
            'models_trained': len(ml_results),
            'results': ml_results
        }
        
    except Exception as e:
        print(f"❌ Error en análisis ML: {str(e)}")
        raise e
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    result = main()
    print(f"🎯 Resultado final ML: {result}")