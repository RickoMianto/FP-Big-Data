import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EcommerceDataProducer:
    def __init__(self, kafka_servers='localhost:9092', topic='ecommerce-events'):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.producer = None
        self.setup_producer()
    
    def setup_producer(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None
            )
            logger.info(f"Kafka producer connected to {self.kafka_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def load_and_send_data(self, csv_file_path, batch_size=1000, delay=0.1):
        """Load CSV data and send to Kafka topic"""
        try:
            # Load CSV data
            logger.info(f"Loading data from {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            
            # Clean and validate data
            df = self.clean_data(df)
            
            total_records = len(df)
            logger.info(f"Loaded {total_records} records from CSV")
            
            # Send data in batches
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Convert row to dictionary
                    event_data = {
                        'event_time': row['event_time'],
                        'event_type': row['event_type'],
                        'product_id': str(row['product_id']),
                        'category_id': str(row['category_id']),
                        'category_code': row['category_code'] if pd.notna(row['category_code']) else None,
                        'brand': row['brand'] if pd.notna(row['brand']) else None,
                        'price': float(row['price']) if pd.notna(row['price']) else 0.0,
                        'user_id': str(row['user_id']),
                        'user_session': row['user_session'],
                        'processed_timestamp': datetime.now().isoformat()
                    }
                    
                    # Send to Kafka
                    self.producer.send(
                        self.topic,
                        key=row['product_id'],
                        value=event_data
                    )
                
                # Flush batch
                self.producer.flush()
                
                # Log progress
                processed = min(i + batch_size, total_records)
                logger.info(f"Sent {processed}/{total_records} records to Kafka")
                
                # Add delay between batches
                if delay > 0:
                    time.sleep(delay)
            
            logger.info("Successfully sent all data to Kafka")
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
    
    def clean_data(self, df):
        """Clean and validate the dataset"""
        logger.info("Cleaning data...")
        
        # Remove rows with missing critical fields
        initial_count = len(df)
        df = df.dropna(subset=['event_time', 'event_type', 'product_id', 'user_id'])
        
        # Filter valid event types
        valid_events = ['view', 'cart', 'purchase']
        df = df[df['event_type'].isin(valid_events)]
        
        # Convert data types
        df['product_id'] = df['product_id'].astype(str)
        df['category_id'] = df['category_id'].astype(str)
        df['user_id'] = df['user_id'].astype(str)
        
        # Fill missing prices with 0
        df['price'] = df['price'].fillna(0.0)
        
        final_count = len(df)
        logger.info(f"Data cleaned: {initial_count} -> {final_count} records")
        
        return df
    
    def send_sample_data(self, num_samples=100):
        """Send sample data for testing"""
        logger.info(f"Sending {num_samples} sample events...")
        
        sample_data = [
            {
                'event_time': datetime.now().isoformat(),
                'event_type': 'view',
                'product_id': '1005105',
                'category_id': '2232732093077520756',
                'category_code': 'construction.tools.light',
                'brand': 'apple',
                'price': 1302.48,
                'user_id': '556695836',
                'user_session': 'sample-session-001',
                'processed_timestamp': datetime.now().isoformat()
            },
            {
                'event_time': datetime.now().isoformat(),
                'event_type': 'cart',
                'product_id': '2402273',
                'category_id': '2232732100769874463',
                'category_code': 'appliances.personal.massager',
                'brand': 'bosch',
                'price': 313.52,
                'user_id': '539453785',
                'user_session': 'sample-session-002',
                'processed_timestamp': datetime.now().isoformat()
            }
        ]
        
        for i in range(num_samples):
            event = sample_data[i % len(sample_data)]
            event['product_id'] = f"product_{i}"
            event['user_id'] = f"user_{i}"
            
            self.producer.send(self.topic, value=event)
        
        self.producer.flush()
        logger.info(f"Sent {num_samples} sample events")
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")

def main():
    # Configuration
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    csv_file = '../data/ecommerce-events.csv'
    
    # Create producer
    producer = EcommerceDataProducer(kafka_servers=kafka_servers)
    
    try:
        # Check if CSV file exists
        if os.path.exists(csv_file):
            logger.info(f"Processing CSV file: {csv_file}")
            producer.load_and_send_data(csv_file)
        else:
            logger.warning(f"CSV file not found: {csv_file}")
            logger.info("Sending sample data instead...")
            producer.send_sample_data(1000)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()