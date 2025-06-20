from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.376") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read raw data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert to readable format
raw_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

# Write raw data to MinIO bucket `bronze`
query = raw_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://bronze/ecommerce-events") \
    .option("checkpointLocation", "s3a://checkpoints/kafka-to-bronze") \
    .start()

query.awaitTermination()