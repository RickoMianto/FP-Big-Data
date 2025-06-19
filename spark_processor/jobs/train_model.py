from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.recommendation import ALS
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLModelTrainer:
    def __init__(self):
        self.spark = None
        self.setup_spark()
    
    def setup_spark(self):
        """Initialize Spark session with ML support"""
        try:
            self.spark = SparkSession.builder \
                .appName("EcommerceMLTraining") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session for ML training initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def read_silver_data(self, silver_path="s3a://silver/ecommerce-events"):
        """Read data from Silver layer for ML training"""
        try:
            logger.info("Reading Silver data for ML training...")
            
            df = self.spark.read \
                .format("parquet") \
                .load(silver_path)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading Silver data: {e}")
            raise
    
    def prepare_collaborative_filtering_data(self, df):
        """Prepare data for collaborative filtering (ALS)"""
        try:
            logger.info("Preparing collaborative filtering data...")
            
            # Create user-item interaction matrix with implicit ratings
            # Rating calculation: view=1, cart=3, purchase=10
            user_item_interactions = df.withColumn(
                "rating",
                when(col("event_type") == "view", 1.0)
                .when(col("event_type") == "cart", 3.0)
                .when(col("event_type") == "purchase", 10.0)
                .otherwise(0.0)
            ).groupBy("user_id", "product_id").agg(
                sum("rating").alias("total_rating"),
                count("*").alias("interaction_count")
            ).withColumn("final_rating", 
                        col("total_rating") + log(col("interaction_count") + 1))
            
            # Convert string IDs to numeric for ALS
            user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
            product_indexer = StringIndexer(inputCol="product_id", outputCol="product_index")
            
            # Index users and products
            indexed_data = user_indexer.fit(user_item_interactions).transform(user_item_interactions)
            indexed_data = product_indexer.fit(indexed_data).transform(indexed_data)
            
            # Select final columns for ALS
            als_data = indexed_data.select(
                col("user_index").cast("int").alias("user"),
                col("product_index").cast("int").alias("item"),
                col("final_rating").cast("float").alias("rating")
            )
            
            return als_data, user_indexer.fit(user_item_interactions), product_indexer.fit(indexed_data)
            
        except Exception as e:
            logger.error(f"Error preparing collaborative filtering data: {e}")
            raise
    
    def train_als_model(self, als_data):
        """Train ALS collaborative filtering model"""
        try:
            logger.info("Training ALS collaborative filtering model...")
            
            # Split data
            train_data, test_data = als_data.randomSplit([0.8, 0.2], seed=42)
            
            # Configure ALS
            als = ALS(
                maxIter=10,
                regParam=0.1,
                userCol="user",
                itemCol="item",
                ratingCol="rating",
                coldStartStrategy="drop",
                implicitPrefs=True,  # For implicit feedback
                alpha=1.0
            )
            
            # Train model
            als_model = als.fit(train_data)
            
            # Make predictions on test data
            predictions = als_model.transform(test_data)
            
            # Evaluate model
            evaluator = RegressionEvaluator(
                metricName="rmse",
                labelCol="rating",
                predictionCol="prediction"
            )
            rmse = evaluator.evaluate(predictions)
            logger.info(f"ALS Model RMSE: {rmse}")
            
            return als_model
            
        except Exception as e:
            logger.error(f"Error training ALS model: {e}")
            raise
    
    def prepare_content_based_features(self, df):
        """Prepare features for content-based filtering"""
        try:
            logger.info("Preparing content-based features...")
            
            # Get product features
            product_features = df.groupBy(
                "product_id", "category_id", "category_code", "brand"
            ).agg(
                avg("price").alias("avg_price"),
                count("*").alias("interaction_count"),
                sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
                sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
                sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
                countDistinct("user_id").alias("unique_users")
            )
            
            # Calculate popularity and engagement scores
            product_features = product_features.withColumn(
                "popularity_score",
                col("view_count") + (col("cart_count") * 3) + (col("purchase_count") * 10)
            ).withColumn(
                "engagement_rate",
                col("unique_users") / col("interaction_count")
            )
            
            return product_features
            
        except Exception as e:
            logger.error(f"Error preparing content-based features: {e}")
            raise
    
    def train_product_clustering(self, product_features):
        """Train K-means clustering for product similarity"""
        try:
            logger.info("Training product clustering model...")
            
            # Index categorical features
            brand_indexer = StringIndexer(inputCol="brand", outputCol="brand_index", handleInvalid="keep")
            category_indexer = StringIndexer(inputCol="category_code", outputCol="category_index", handleInvalid="keep")
            
            # Create feature vector
            feature_cols = [
                "avg_price", "popularity_score", "engagement_rate",
                "brand_index", "category_index"
            ]
            
            assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
            scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
            
            # K-means clustering
            kmeans = KMeans(
                featuresCol="scaled_features",
                predictionCol="cluster",
                k=20,  # Number of clusters
                seed=42
            )
            
            # Create pipeline
            pipeline = Pipeline(stages=[
                brand_indexer,
                category_indexer,
                assembler,
                scaler,
                kmeans
            ])
            
            # Train pipeline
            clustering_model = pipeline.fit(product_features)
            
            # Apply clustering
            clustered_products = clustering_model.transform(product_features)
            
            # Show cluster distribution
            cluster_distribution = clustered_products.groupBy("cluster").count().orderBy("cluster")
            logger.info("Cluster distribution:")
            cluster_distribution.show()
            
            return clustering_model, clustered_products
            
        except Exception as e:
            logger.error(f"Error training clustering model: {e}")
            raise
    
    def generate_product_recommendations(self, als_model, user_indexer, product_indexer, num_recommendations=10):
        """Generate product recommendations for all users"""
        try:
            logger.info("Generating product recommendations...")
            
            # Get all users
            users = self.spark.range(als_model.userFactors.count()).withColumnRenamed("id", "user")
            
            # Generate recommendations for all users
            user_recommendations = als_model.recommendForAllUsers(num_recommendations)
            
            # Create lookup tables for reverse indexing
            user_lookup = user_indexer.labels
            product_lookup = product_indexer.labels
            
            # Convert recommendations back to original IDs
            recommendations_with_ids = user_recommendations.select(
                col("user"),
                explode(col("recommendations")).alias("recommendation")
            ).select(
                col("user"),
                col("recommendation.item").alias("item"),
                col("recommendation.rating").alias("score")
            )
            
            return recommendations_with_ids
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            raise
    
    def create_similar_products(self, clustered_products):
        """Create similar products mapping based on clustering"""
        try:
            logger.info("Creating similar products mapping...")
            
            # Self-join to find products in same cluster
            similar_products = clustered_products.alias("p1").join(
                clustered_products.alias("p2"),
                col("p1.cluster") == col("p2.cluster")
            ).where(
                col("p1.product_id") != col("p2.product_id")
            ).select(
                col("p1.product_id").alias("product_id"),
                col("p2.product_id").alias("similar_product_id"),
                col("p1.cluster"),
                abs(col("p1.popularity_score") - col("p2.popularity_score")).alias("score_diff")
            ).withColumn(
                "similarity_score",
                1.0 / (1.0 + col("score_diff") / 1000.0)  # Normalize similarity
            )
            
            # Rank similar products by similarity
            from pyspark.sql.window import Window
            window_spec = Window.partitionBy("product_id").orderBy(desc("similarity_score"))
            
            ranked_similar = similar_products.withColumn(
                "rank",
                row_number().over(window_spec)
            ).filter(col("rank") <= 10)  # Top 10 similar products
            
            return ranked_similar
            
        except Exception as e:
            logger.error(f"Error creating similar products: {e}")
            raise
    
    def save_models_and_results(self, als_model, clustering_model, recommendations, similar_products, gold_path="s3a://gold"):
        """Save trained models and results to Gold layer"""
        try:
            logger.info("Saving models and results...")
            
            # Save ALS model
            als_model.write().overwrite().save(f"{gold_path}/models/als_model")
            
            # Save clustering model
            clustering_model.write().overwrite().save(f"{gold_path}/models/clustering_model")
            
            # Save recommendations
            recommendations.write \
                .mode("overwrite") \
                .format("parquet") \
                .save(f"{gold_path}/user_recommendations")
            
            # Save similar products
            similar_products.write \
                .mode("overwrite") \
                .format("parquet") \
                .save(f"{gold_path}/similar_products")
            
            logger.info("Models and results saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving models: {e}")
            raise
    
    def run_ml_training(self):
        """Run the complete ML training pipeline"""
        try:
            logger.info("Starting ML training pipeline...")
            
            # Read Silver data
            silver_df = self.read_silver_data()
            
            # Train collaborative filtering
            als_data, user_indexer, product_indexer = self.prepare_collaborative_filtering_data(silver_df)
            als_model = self.train_als_model(als_data)
            
            # Generate recommendations
            recommendations = self.generate_product_recommendations(als_model, user_indexer, product_indexer)
            
            # Train content-based filtering
            product_features = self.prepare_content_based_features(silver_df)
            clustering_model, clustered_products = self.train_product_clustering(product_features)
            
            # Create similar products
            similar_products = self.create_similar_products(clustered_products)
            
            # Save everything
            self.save_models_and_results(als_model, clustering_model, recommendations, similar_products)
            
            logger.info("ML training pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Error in ML training pipeline: {e}")
            raise
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")

def main():
    trainer = MLModelTrainer()
    
    try:
        trainer.run_ml_training()
        
    except Exception as e:
        logger.error(f"ML training failed: {e}")
    finally:
        trainer.close()

if __name__ == "__main__":
    main()