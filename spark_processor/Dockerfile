# Dockerfile untuk Spark Processor
FROM openjdk:11-jre-slim

# Set environment variables
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt/ \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/jobs /app/logs /app/checkpoints

# Set environment variables for Spark
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin

# Expose ports
EXPOSE 4040 4041 4042

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD pgrep -f "java.*spark" > /dev/null || exit 1

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Wait for dependencies\n\
echo "Waiting for Kafka..."\n\
while ! nc -z kafka 9092; do sleep 1; done\n\
echo "Kafka is ready"\n\
\n\
echo "Waiting for MinIO..."\n\
while ! nc -z minio 9000; do sleep 1; done\n\
echo "MinIO is ready"\n\
\n\
# Start Spark worker if in cluster mode\n\
if [ "${SPARK_MODE}" = "worker" ]; then\n\
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL\n\
elif [ "${SPARK_MODE}" = "master" ]; then\n\
    exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master\n\
else\n\
    # Run Spark jobs\n\
    echo "Starting Spark jobs..."\n\
    \n\
    # Run bronze to silver job\n\
    echo "Running bronze to silver transformation..."\n\
    spark-submit \\\n\
        --master $SPARK_MASTER_URL \\\n\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261 \\\n\
        --conf spark.sql.adaptive.enabled=true \\\n\
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \\\n\
        jobs/1_bronze_to_silver.py\n\
    \n\
    # Run silver to gold job\n\
    echo "Running silver to gold transformation..."\n\
    spark-submit \\\n\
        --master $SPARK_MASTER_URL \\\n\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261 \\\n\
        --conf spark.sql.adaptive.enabled=true \\\n\
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \\\n\
        jobs/2_silver_to_gold.py\n\
    \n\
    # Run model training job\n\
    echo "Running model training..."\n\
    spark-submit \\\n\
        --master $SPARK_MASTER_URL \\\n\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261 \\\n\
        --conf spark.sql.adaptive.enabled=true \\\n\
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \\\n\
        jobs/3_train_model.py\n\
    \n\
    echo "All Spark jobs completed successfully!"\n\
    \n\
    # Keep container running for streaming jobs\n\
    tail -f /dev/null\n\
fi\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

# Run entrypoint
ENTRYPOINT ["/entrypoint.sh"]