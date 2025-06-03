#!/bin/bash

# Bootstrap script para EMR Cluster - Weather Analytics Pipeline
# Este script se ejecuta en cada nodo del cluster durante la inicialización

set -e
set -x

echo "🚀 Iniciando bootstrap para Weather Analytics EMR Cluster..."

# Actualizar sistema
sudo yum update -y

# Instalar dependencias del sistema
echo "📦 Instalando dependencias del sistema..."
sudo yum install -y gcc gcc-c++ make git wget curl

# Configurar Python 3.9 (si no está disponible, usar Python 3.8)
echo "🐍 Configurando Python..."
python3 --version
pip3 --version

# Actualizar pip
sudo pip3 install --upgrade pip

# Instalar dependencias de Python para Spark y análisis
echo "📊 Instalando librerías de Python para análisis..."
sudo pip3 install \
    pandas==1.5.3 \
    numpy==1.24.3 \
    boto3==1.26.137 \
    botocore==1.29.137 \
    pyarrow==12.0.0 \
    fastparquet==0.8.3 \
    scikit-learn==1.2.2 \
    matplotlib==3.7.1 \
    seaborn==0.12.2 \
    requests==2.31.0 \
    sqlalchemy==2.0.15 \
    pymysql==1.0.3

# Instalar MySQL connector para Java (para Spark JDBC)
echo "🔌 Configurando MySQL connector para Spark..."
MYSQL_CONNECTOR_VERSION="8.0.33"
MYSQL_JAR_PATH="/usr/lib/spark/jars/"

sudo wget -O ${MYSQL_JAR_PATH}mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar

# Configurar variables de entorno
echo "🌐 Configurando variables de entorno..."
cat << 'EOF' | sudo tee -a /etc/environment
SPARK_HOME=/usr/lib/spark
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
PATH=$SPARK_HOME/bin:$PATH
EOF

# Configurar Spark para optimización
echo "⚡ Configurando optimizaciones de Spark..."
sudo mkdir -p /etc/spark/conf
cat << 'EOF' | sudo tee /etc/spark/conf/spark-env.sh
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_DIRS="/tmp/spark"
export SPARK_WORKER_DIR="/tmp/spark-worker"
EOF

# Configurar logging de Spark
cat << 'EOF' | sudo tee /etc/spark/conf/log4j.properties
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Configurar nivel específico para algunos loggers
log4j.logger.org.apache.spark.repl.Main=WARN
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
EOF

# Crear directorios temporales para Spark
echo "📁 Creando directorios temporales..."
sudo mkdir -p /tmp/spark /tmp/spark-worker /tmp/spark-events
sudo chmod 777 /tmp/spark /tmp/spark-worker /tmp/spark-events

# Configurar límites del sistema
echo "⚙️ Configurando límites del sistema..."
cat << 'EOF' | sudo tee -a /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536
* soft nproc 32768
* hard nproc 32768
EOF

# Configurar memoria virtual
echo "🧠 Configurando memoria virtual..."
echo 'vm.swappiness=1' | sudo tee -a /etc/sysctl.conf
echo 'vm.dirty_ratio=15' | sudo tee -a /etc/sysctl.conf
echo 'vm.dirty_background_ratio=5' | sudo tee -a /etc/sysctl.conf

# Instalar herramientas de monitoreo adicionales
echo "📈 Instalando herramientas de monitoreo..."
sudo yum install -y htop iotop nethogs

# Configurar AWS CLI si no está configurado
echo "☁️ Verificando configuración de AWS CLI..."
aws --version

# Crear script de verificación de salud del cluster
cat << 'EOF' | sudo tee /opt/cluster-health-check.sh
#!/bin/bash
echo "=== CLUSTER HEALTH CHECK ===" 
echo "Fecha: $(date)"
echo "Memoria libre:"
free -h
echo "Espacio en disco:"
df -h
echo "Procesos de Spark:"
ps aux | grep spark | head -5
echo "Conectividad S3:"
aws s3 ls s3://proyecto3-eafit-raw/ --region us-east-1 | head -3
echo "=========================="
EOF

sudo chmod +x /opt/cluster-health-check.sh

# Test rápido de conectividad
echo "🔍 Verificando conectividad..."
aws s3 ls s3://proyecto3-eafit-raw/ --region us-east-1 > /tmp/s3-test.log 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Conectividad S3 OK"
else
    echo "❌ Problema con conectividad S3"
    cat /tmp/s3-test.log
fi

# Verificar instalaciones
echo "🧪 Verificando instalaciones..."
python3 -c "import pandas, numpy, boto3, sklearn; print('✅ Librerías Python OK')"
ls -la /usr/lib/spark/jars/mysql-connector-java-*.jar

# Log de finalización
echo "✅ Bootstrap completado exitosamente en $(date)"
echo "🎯 Cluster listo para procesar datos meteorológicos"

# Guardar información del nodo
cat << EOF > /tmp/bootstrap-info.txt
Bootstrap completed: $(date)
Hostname: $(hostname)
IP: $(hostname -I)
Python version: $(python3 --version)
Spark version: $(spark-submit --version 2>&1 | head -1)
Available memory: $(free -h | grep Mem)
Disk space: $(df -h / | tail -1)
EOF

echo "📝 Información del bootstrap guardada en /tmp/bootstrap-info.txt"