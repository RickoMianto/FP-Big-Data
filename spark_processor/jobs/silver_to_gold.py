from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverToGoldProcessor:
    def __init__(self):
        self.spark = None
        self.setup_spark()
    
    def setup_spark(self):
        """Initialize Spark session"""
        try:
            self.spark = SparkSession.builder \
.appName("EcommerceBronzeToSilver") \
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                       "org.apache.hadoop:hadoop-aws:3.3.4,"
                       "com.amazonaws:aws-java-sdk-bundle:1.12.376") \
                .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
                .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session for Gold layer initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def read_silver_data(self, silver_path="s3a://silver/ecommerce-events"):
        """Read data from Silver layer"""
        try:
            logger.info(f"Reading Silver data from: {silver_path}")
            
            df = self.spark.read \
                .format("parquet") \
                .load(silver_path)
            
            logger.info(f"Loaded {df.count()} records from Silver layer")
            return df
            
        except Exception as e:
            logger.error(f"Error reading Silver data: {e}")
            raise
    
    def create_product_statistics(self, df):
        """Create product-level statistics"""
        try:
            logger.info("Creating product statistics...")
            
            # Product-level aggregations
            product_stats = df.groupBy(
                "product_id", 
                "category_id", 
                "category_code", 
                "brand"
            ).agg(
                # Event counts
                count("*").alias("total_events"),
                sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                
                # User engagement
                countDistinct("user_id").alias("unique_users"),
                countDistinct("user_session").alias("unique_sessions"),
                
                # Price statistics
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
                
                # Time statistics
                min("event_timestamp").alias("first_seen"),
                max("event_timestamp").alias("last_seen")
            )
            
            # Calculate conversion rates
            product_stats_with_rates = product_stats \
                .withColumn("view_to_cart_rate", 
                           when(col("view_count") > 0, 
                                col("cart_count") / col("view_count")).otherwise(0.0)) \
                .withColumn("cart_to_purchase_rate", 
                           when(col("cart_count") > 0, 
                                col("purchase_count") / col("cart_count")).otherwise(0.0)) \
                .withColumn("view_to_purchase_rate", 
                           when(col("view_count") > 0, 
                                col("purchase_count") / col("view_count")).otherwise(0.0))
            
            # Calculate popularity scores
            # Weighted score: views (1x) + carts (3x) + purchases (10x)
            product_stats_final = product_stats_with_rates \
                .withColumn("popularity_score", 
                           col("view_count") + (col("cart_count") * 3) + (col("purchase_count") * 10)) \
                .withColumn("engagement_score", 
                           col("unique_users") + (col("unique_sessions") * 0.5)) \
                .withColumn("created_timestamp", current_timestamp())
            
            return product_stats_final
            
        except Exception as e:
            logger.error(f"Error creating product statistics: {e}")
            raise
    
    def create_category_statistics(self, df):
        """Create category-level statistics"""
        try:
            logger.info("Creating category statistics...")
            
            category_stats = df.groupBy("category_id", "category_code").agg(
                countDistinct("product_id").alias("product_count"),
                countDistinct("brand").alias("brand_count"),
                count("*").alias("total_events"),
                sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
                sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_carts"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
                countDistinct("user_id").alias("unique_users"),
                avg("price").alias("avg_category_price"),
                min("event_timestamp").alias("first_activity"),
                max("event_timestamp").alias("last_activity")
            ).withColumn("created_timestamp", current_timestamp())
            
            return category_stats
            
        except Exception as e:
            logger.error(f"Error creating category statistics: {e}")
            raise
    
    def create_brand_statistics(self, df):
        """Create brand-level statistics"""
        try:
            logger.info("Creating brand statistics...")
            
            brand_stats = df.filter(col("brand") != "unknown").groupBy("brand").agg(
                countDistinct("product_id").alias("product_count"),
                countDistinct("category_id").alias("category_count"),
                count("*").alias("total_events"),
                sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
                sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_carts"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
                countDistinct("user_id").alias("unique_users"),
                avg("price").alias("avg_brand_price"),
                min("event_timestamp").alias("first_activity"),
                max("event_timestamp").alias("last_activity")
            ).withColumn("created_timestamp", current_timestamp())
            
            return brand_stats
            
        except Exception as e:
            logger.error(f"Error creating brand statistics: {e}")
            raise
    
    def create_top_rankings(self, product_stats):
        """Create top product rankings"""
        try:
            logger.info("Creating top product rankings...")
            
            # Top products by views
            top_views = product_stats.select(
                col("product_id"),
                col("category_code"),
                col("brand"),
                col("view_count"),
                col("avg_price")
            ).orderBy(desc("view_count")).limit(100) \
            .withColumn("rank_type", lit("most_viewed")) \
            .withColumn("rank_value", col("view_count"))
            
            # Top products by cart additions
            top_carts = product_stats.select(
                col("product_id"),
                col("category_code"),
                col("brand"),
                col("cart_count"),
                col("avg_price")
            ).orderBy(desc("cart_count")).limit(100) \
            .withColumn("rank_type", lit("most_carted")) \
            .withColumn("rank_value", col("cart_count"))
            
            # Top products by purchases
            top_purchases = product_stats.select(
                col("product_id"),
                col("category_code"),
                col("brand"),
                col("purchase_count"),
                col("avg_price")
            ).orderBy(desc("purchase_count")).limit(100) \
            .withColumn("rank_type", lit("most_purchased")) \
            .withColumn("rank_value", col("purchase_count"))
            
            # Top products by popularity score
            top_popular = product_stats.select(
                col("product_id"),
                col("category_code"),
                col("brand"),
                col("popularity_score"),
                col("avg_price")
            ).orderBy(desc("popularity_score")).limit(100) \
            .withColumn("rank_type", lit("most_popular")) \
            .withColumn("rank_value", col("popularity_score"))
            
            # Union all rankings
            rankings = top_views.select("product_id", "category_code", "brand", "avg_price", "rank_type", "rank_value") \
                .union(top_carts.select("product_id", "category_code", "brand", "avg_price", "rank_type", "rank_value")) \
                .union(top_purchases.select("product_id", "category_code", "brand", "avg_price", "rank_type", "rank_value")) \
                .union(top_popular.select("product_id", "category_code", "brand", "avg_price", "rank_type", "rank_value")) \
                .withColumn("created_timestamp", current_timestamp())
            
            return rankings
            
            raise
            
        except Exception as e:
            logger.error(f"Error creating rankings: {e}")
            raise
    
    def create_recommendation_features(self, product_stats):
        """Create features for recommendation system"""
        try:
            logger.info("Creating recommendation features...")
            
            # Calculate percentile ranks for normalization
            window_spec = Window.orderBy("popularity_score")
            
            recommendation_features = product_stats.select(
                col("product_id"),
                col("category_id"),
                col("category_code"),
                col("brand"),
                col("avg_price"),
                col("view_count"),
                col("cart_count"),
                col("purchase_count"),
                col("popularity_score"),
                col("view_to_cart_rate"),
                col("cart_to_purchase_rate"),
                col("view_to_purchase_rate"),
                col("unique_users"),
                col("engagement_score")
            ).withColumn("popularity_percentile", 
                        percent_rank().over(window_spec)) \
            .withColumn("price_tier", 
                       when(col("avg_price") < 50, "budget")
                       .when(col("avg_price") < 200, "mid_range")
                       .when(col("avg_price") < 500, "premium")
                       .otherwise("luxury")) \
            .withColumn("engagement_tier",
                       when(col("engagement_score") < 10, "low")
                       .when(col("engagement_score") < 50, "medium")
                       .when(col("engagement_score") < 100, "high")
                       .otherwise("very_high")) \
            .withColumn("created_timestamp", current_timestamp())
            
            return recommendation_features
            
        except Exception as e:
            logger.error(f"Error creating recommendation features: {e}")
            raise
    
    def write_to_gold(self, df, table_name, gold_path="s3a://gold"):
        """Write data to Gold layer"""
        try:
            output_path = f"{gold_path}/{table_name}"
            logger.info(f"Writing {table_name} to Gold layer: {output_path}")
            
            df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(output_path)
            
            logger.info(f"Successfully wrote {df.count()} records to {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing {table_name} to Gold layer: {e}")
            raise
    
    def run_gold_processing(self):
        """Run the complete Gold layer processing"""
        try:
            logger.info("Starting Gold layer processing...")
            
            # Read Silver data
            silver_df = self.read_silver_data()
            
            # Create product statistics
            product_stats = self.create_product_statistics(silver_df)
            self.write_to_gold(product_stats, "product_statistics")
            
            # Create category statistics
            category_stats = self.create_category_statistics(silver_df)
            self.write_to_gold(category_stats, "category_statistics")
            
            # Create brand statistics
            brand_stats = self.create_brand_statistics(silver_df)
            self.write_to_gold(brand_stats, "brand_statistics")
            
            # Create rankings
            rankings = self.create_top_rankings(product_stats)
            self.write_to_gold(rankings, "product_rankings")
            
            # Create recommendation features
            rec_features = self.create_recommendation_features(product_stats)
            self.write_to_gold(rec_features, "recommendation_features")
            
            logger.info("Gold layer processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error in Gold layer processing: {e}")
            raise
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")

def main():
    processor = SilverToGoldProcessor()
    
    try:
        processor.run_gold_processing()
        
    except Exception as e:
        logger.error(f"Gold processing failed: {e}")
    finally:
        processor.close()

if __name__ == "__main__":
    main()