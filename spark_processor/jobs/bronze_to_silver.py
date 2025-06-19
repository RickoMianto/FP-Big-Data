from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeToSilverProcessor:
    def __init__(self):
        self.spark = None
        self.setup_spark()
    
    def setup_spark(self):
        """Initialize Spark session with Kafka support"""
        try:
            self.spark = SparkSession.builder \
                .appName("EcommerceBronzeToSilver") \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                       "com.amazonaws:aws-java-sdk-bundle:1.12.376") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def read_from_kafka(self, kafka_servers="kafka:29092", topic="ecommerce-events"):
        """Read streaming data from Kafka"""
        try:
            logger.info(f"Reading from Kafka topic: {topic}")
            
            # Define schema for incoming data
            schema = StructType([
                StructField("event_time", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("category_code", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("user_id", StringType(), True),
                StructField("user_session", StringType(), True),
                StructField("processed_timestamp", StringType(), True)
            ])
            
            # Read from Kafka
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()
            
            # Parse JSON data
            parsed_df = df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("json_data"),
                col("timestamp").alias("kafka_timestamp")
            ).select(
                col("kafka_key"),
                col("kafka_timestamp"),
                from_json(col("json_data"), schema).alias("data")
            ).select(
                col("kafka_key"),
                col("kafka_timestamp"),
                col("data.*")
            )
            
            return parsed_df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
    
    def clean_and_transform(self, df):
        """Clean and transform the data for Silver layer"""
        try:
            logger.info("Cleaning and transforming data...")
            
            # Data cleaning and transformations
            cleaned_df = df \
                .filter(col("event_type").isin(["view", "cart", "purchase"])) \
                .filter(col("product_id").isNotNull()) \
                .filter(col("user_id").isNotNull()) \
                .withColumn("event_timestamp", 
                           to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")) \
                .withColumn("price_cleaned", 
                           when(col("price").isNull(), 0.0).otherwise(col("price"))) \
                .withColumn("brand_cleaned", 
                           when(col("brand").isNull(), "unknown").otherwise(lower(trim(col("brand"))))) \
                .withColumn("category_code_cleaned", 
                           when(col("category_code").isNull(), "unknown").otherwise(lower(trim(col("category_code"))))) \
                .withColumn("processing_date", current_date()) \
                .withColumn("processing_timestamp", current_timestamp())
            
            # Select final columns for Silver layer
            silver_df = cleaned_df.select(
                col("product_id"),
                col("category_id"),
                col("category_code_cleaned").alias("category_code"),
                col("brand_cleaned").alias("brand"),
                col("price_cleaned").alias("price"),
                col("user_id"),
                col("user_session"),
                col("event_type"),
                col("event_timestamp"),
                col("processing_date"),
                col("processing_timestamp")
            )
            
            return silver_df
            
        except Exception as e:
            logger.error(f"Error in data transformation: {e}")
            raise
    
    def write_to_silver(self, df, output_path="s3a://silver/ecommerce-events"):
        """Write cleaned data to Silver layer"""
        try:
            logger.info(f"Writing to Silver layer: {output_path}")
            
            # Write to Silver layer with partitioning
            query = df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", output_path) \
                .option("checkpointLocation", "s3a://checkpoints/bronze-to-silver") \
                .partitionBy("processing_date", "event_type") \
                .trigger(processingTime="30 seconds") \
                .start()
            
            return query
            
        except Exception as e:
            logger.error(f"Error writing to Silver layer: {e}")
            raise
    
    def write_to_console(self, df):
        """Write data to console for debugging"""
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        return query
    
    def run_batch_processing(self, input_path="s3a://bronze/ecommerce-events"):
        """Run batch processing on existing data"""
        try:
            logger.info("Running batch processing...")
            
            # Read batch data
            df = self.spark.read \
                .format("parquet") \
                .load(input_path)
            
            # Clean and transform
            silver_df = self.clean_and_transform(df)
            
            # Write to Silver layer
            silver_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .partitionBy("processing_date", "event_type") \
                .save("s3a://silver/ecommerce-events")
            
            logger.info("Batch processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            raise
    
    def run_streaming(self):
        """Run the streaming pipeline"""
        try:
            logger.info("Starting streaming pipeline...")
            
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Clean and transform
            silver_df = self.clean_and_transform(kafka_df)
            
            # Write to Silver layer (uncomment for production)
            # query = self.write_to_silver(silver_df)
            
            # Write to console for debugging
            query = self.write_to_console(silver_df)
            
            # Await termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming pipeline: {e}")
            raise
        finally:
            self.close()
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")

def main():
    processor = BronzeToSilverProcessor()
    
    try:
        # Run streaming pipeline
        processor.run_streaming()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        processor.close()

if __name__ == "__main__":
    main()