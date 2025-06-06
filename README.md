# Proyecto 3 - Automatización Big Data Pipeline
## ST0263: Tópicos Especiales en Telemática - Universidad EAFIT

### 📋 Descripción del Proyecto

Este proyecto implementa una arquitectura batch completa para big data que automatiza el proceso de **Captura, Ingesta, Procesamiento y Salida** de datos meteorológicos para análisis en tiempo real. El sistema procesa datos de múltiples fuentes (APIs y bases de datos) utilizando servicios de AWS para crear un pipeline robusto y escalable.

### 🎯 Objetivos

- Automatizar la captura e ingesta de datos desde APIs y bases de datos relacionales
- Implementar procesamiento ETL distribuido con Apache Spark en EMR
- Realizar análisis descriptivos y predictivos con SparkSQL y SparkML
- Proporcionar acceso a resultados via Amazon Athena y API Gateway
- Demostrar una arquitectura de big data completamente automatizada

### 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   AWS Lambda     │───▶│   S3 Raw Zone   │
│                 │    │   (Ingestion)    │    │                 │
│ • Weather APIs  │    └──────────────────┘    └─────────────────┘
│ • MySQL/Postgres│                                      │
└─────────────────┘                                      ▼
                                               ┌─────────────────┐
┌─────────────────┐    ┌──────────────────┐    │ EMR Cluster     │
│   Results       │◀───│  S3 Trusted Zone │◀───│ (Spark ETL)     │
│                 │    │                  │    └─────────────────┘
│ • Athena Tables │    └──────────────────┘              │
│ • API Gateway   │                                      ▼
└─────────────────┘    ┌──────────────────┐    ┌─────────────────┐
                       │ S3 Refined Zone  │◀───│EMR Analytics    │
                       │                  │    │(SparkSQL+ML)    │
                       └──────────────────┘    └─────────────────┘
```

### 📊 Fuentes de Datos Utilizadas

**APIs de Clima:**
- **Open-Meteo API**: Datos históricos y actuales de temperatura, precipitación, viento
- **Ciudades objetivo**: Medellín, Bogotá, Cali, Barranquilla, Cartagena
- **Período**: Junio 2023 - Mayo 2024

**Base de Datos Relacional (Creada en una instancia EC2):**
- Tabla `cities`: Información demográfica y geográfica
- Tabla `weather_stations`: Estaciones meteorológicas
- Tabla `weather_alerts`: Alertas y umbrales de clima
- Tabla `historical_events`: Eventos climáticos extremos

### 🛠️ Tecnologías y Servicios

| Capa | Tecnología | Propósito |
|------|------------|-----------|
| **Orquestación** | AWS Step Functions | Automatización del pipeline completo |
| **Ingesta** | AWS Lambda | Captura de datos desde APIs y BBDD |
| **Almacenamiento** | Amazon S3 | Data Lake (Raw, Trusted, Refined) |
| **Procesamiento** | Amazon EMR + Apache Spark | ETL y análisis distribuido |
| **Analytics** | SparkSQL + SparkML | Análisis descriptivo y predictivo |
| **Consulta** | Amazon Athena | Consultas SQL sobre S3 |
| **API** | API Gateway + Lambda | Acceso programático a resultados |
| **Notificaciones** | Amazon SNS | Alertas del pipeline |

### 🚀 Despliegue y Configuración

#### Prerrequisitos

- Cuenta de AWS Academy o créditos en GCP/Azure (máx $50 USD)
- AWS CLI configurado
- Permisos para crear recursos EMR, Lambda, S3, Step Functions
- Base de datos MySQL/PostgreSQL configurada

#### Paso 1: Configuración de Infraestructura

```bash
# Crear buckets S3
aws s3 mb s3://proyecto3-eafit-raw
aws s3 mb s3://proyecto3-eafit-trusted  
aws s3 mb s3://proyecto3-eafit-refined
aws s3 mb s3://proyecto3-eafit-scripts

# Subir scripts de Spark
aws s3 cp spark-jobs/ s3://proyecto3-eafit-scripts/ --recursive
aws s3 cp infrastructure/bootstrap.sh s3://proyecto3-eafit-scripts/
```

#### Paso 2: Desplegar Funciones Lambda

```bash
# Comprimir y desplegar cada función Lambda
cd lambda-functions/weather-api-ingestion
zip -r weather-api-ingestion.zip .
aws lambda create-function --function-name weather-api-ingestion \
    --runtime python3.9 --role arn:aws:iam::ACCOUNT:role/lambda-execution-role \
    --handler lambda_function.lambda_handler --zip-file fileb://weather-api-ingestion.zip
```

#### Paso 3: Crear Step Function

```bash
aws stepfunctions create-state-machine \
    --name weather-analytics-pipeline \
    --definition file://step-functions/weather-pipeline.json \
    --role-arn arn:aws:iam::ACCOUNT:role/StepFunctions-WeatherPipeline-role
```

#### Paso 4: Configurar Base de Datos

```sql
-- Ejecutar sql/database-setup.sql en tu instancia RDS
mysql -h your-rds-endpoint -u username -p < sql/database-setup.sql
```

### ▶️ Ejecución del Pipeline

#### Ejecución Automática (Recomendada)

```bash
# Iniciar pipeline completo
aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:region:account:stateMachine:weather-analytics-pipeline \
    --name "execution-$(date +%Y%m%d-%H%M%S)"
```

#### Monitoreo de Ejecución

```bash
# Verificar estado del pipeline
aws stepfunctions list-executions \
    --state-machine-arn arn:aws:states:region:account:stateMachine:weather-analytics-pipeline

# Ver logs detallados
aws logs filter-log-events --log-group-name /aws/stepfunctions/weather-analytics-pipeline
```

### 📈 Análisis y Resultados

#### Análisis Descriptivo (SparkSQL)

- **Estadísticas por ciudad**: Temperatura promedio, máxima, mínima
- **Patrones mensuales**: Tendencias estacionales de precipitación
- **Eventos extremos**: Identificación de días con condiciones atípicas
- **Correlaciones**: Relación entre variables meteorológicas

#### Machine Learning (SparkML)

- **Predicción de temperatura**: Modelo de regresión lineal
- **Clasificación de precipitación**: Algoritmo de bosques aleatorios  
- **Clustering climático**: Agrupación k-means de patrones

#### Acceso a Resultados

**Via Amazon Athena:**
```sql
-- Consulta ejemplo en Athena
SELECT city_name, avg_temperature, max_temperature 
FROM weather_analytics_db.city_temperature_stats 
WHERE avg_temperature > 25
ORDER BY avg_temperature DESC;
```

**Via API Gateway:**
```bash
# Consultar via API REST
curl -X GET "https://api-gateway-url/prod/weather/cities?city=Medellin"
curl -X GET "https://api-gateway-url/prod/weather/ml-predictions"
```

### 📊 Endpoints de la API

| Endpoint | Método | Descripción |
|----------|--------|-------------|
| `/weather/cities` | GET | Estadísticas por ciudad |
| `/weather/monthly` | GET | Patrones mensuales |
| `/weather/extreme-events` | GET | Eventos climáticos extremos |
| `/weather/precipitation` | GET | Análisis de precipitación |
| `/weather/ml-predictions` | GET | Predicciones de ML |
| `/weather/summary` | GET | Resumen general |
| `/weather/health` | GET | Estado del sistema |

### 🔍 Casos de Uso y Análisis

1. **Análisis Climatológico**: Identificar tendencias de temperatura y precipitación
2. **Predicción Meteorológica**: Modelos de forecasting basados en datos históricos  
3. **Alertas Tempranas**: Sistema de notificación para eventos extremos
4. **Planificación Urbana**: Información para gestión de recursos hídricos
5. **Agricultura**: Apoyo en decisiones de cultivo y riego

### 🚨 Manejo de Errores y Reintentos

El pipeline incluye manejo robusto de errores:

- **Reintentos automáticos**: 3 intentos con backoff exponencial
- **Ejecución parcial**: Continúa aunque fallen componentes individuales
- **Notificaciones**: Alertas SNS para fallos críticos  
- **Logs detallados**: Trazabilidad completa en CloudWatch

### 💰 Costos Estimados

**AWS Academy (Limitado):**
- EMR Cluster (3 horas): ~$8 USD
- Lambda (1000 ejecuciones): ~$0.20 USD
- S3 Storage (10 GB): ~$0.23 USD
- **Total estimado**: ~$10 USD por ejecución

**GCP/Azure (Sin límites):**
- Costo similar pero con mayor flexibilidad de servicios

### 🧪 Testing y Validación

```bash
# Ejecutar tests unitarios
python -m pytest tests/

# Validar calidad de datos
python scripts/data_quality_check.py --bucket proyecto3-eafit-trusted

# Test de API endpoints
python scripts/api_integration_test.py --base-url https://api-gateway-url/prod
```

### 📚 Documentación Adicional

- [Documentación de la API](docs/api-documentation.md)
- [Guía de Desarrollo](docs/development-guide.md)
- [Troubleshooting](docs/troubleshooting.md)

### 👥 Equipo de Desarrollo

- **Estudiantes**:Juan Sebastian Aguilar Carballo, Jeronimo Cardona Osorio, Juan Fernadno Romero Rosero,Tomas Zuleta Cardona
- **Curso**: ST0263 - Tópicos Especiales en Telemática
- **Universidad**: EAFIT
- **Período**: 2025-1

### 📝 Referencias

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [AWS EMR Best Practices](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan.html)
- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Ejemplo EMR con Steps](https://github.com/airscholar/EMR-for-data-engineers)

### 📄 Licencia

Este proyecto está desarrollado con fines académicos para el curso ST0263 de la Universidad EAFIT.