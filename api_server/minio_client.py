import os
import pandas as pd
from minio import Minio
from minio.error import S3Error
import io
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self, endpoint='localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False):
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        logger.info(f"MinIO client initialized for endpoint: {endpoint}")
    
    def bucket_exists(self, bucket_name):
        """Check if bucket exists"""
        try:
            return self.client.bucket_exists(bucket_name)
        except S3Error as e:
            logger.error(f"Error checking bucket {bucket_name}: {e}")
            return False
    
    def create_bucket(self, bucket_name):
        """Create bucket if it doesn't exist"""
        try:
            if not self.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                logger.info(f"Bucket {bucket_name} already exists")
        except S3Error as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            raise
    
    def list_objects(self, bucket_name, prefix=""):
        """List objects in bucket with optional prefix"""
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket_name}: {e}")
            return []
    
    def read_parquet(self, bucket_name, object_name):
        """Read parquet file from MinIO and return pandas DataFrame"""
        try:
            # Get object from MinIO
            response = self.client.get_object(bucket_name, object_name)
            
            # Read parquet data
            parquet_data = response.read()
            
            # Convert to pandas DataFrame
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            response.close()
            response.release_conn()
            
            logger.info(f"Successfully read {len(df)} records from {bucket_name}/{object_name}")
            return df
            
        except S3Error as e:
            logger.error(f"Error reading parquet from {bucket_name}/{object_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing parquet data: {e}")
            return None
    
    def write_parquet(self, df, bucket_name, object_name):
        """Write pandas DataFrame to MinIO as parquet file"""
        try:
            # Convert DataFrame to parquet bytes
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, compression='snappy')
            parquet_data = parquet_buffer.getvalue()
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(parquet_data),
                length=len(parquet_data),
                content_type='application/octet-stream'
            )
            
            logger.info(f"Successfully wrote {len(df)} records to {bucket_name}/{object_name}")
            
        except S3Error as e:
            logger.error(f"Error writing parquet to {bucket_name}/{object_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing DataFrame for upload: {e}")
            raise
    
    def read_csv(self, bucket_name, object_name):
        """Read CSV file from MinIO and return pandas DataFrame"""
        try:
            response = self.client.get_object(bucket_name, object_name)
            csv_data = response.read()
            
            df = pd.read_csv(io.BytesIO(csv_data))
            
            response.close()
            response.release_conn()
            
            logger.info(f"Successfully read {len(df)} records from {bucket_name}/{object_name}")
            return df
            
        except S3Error as e:
            logger.error(f"Error reading CSV from {bucket_name}/{object_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing CSV data: {e}")
            return None
    
    def write_csv(self, df, bucket_name, object_name):
        """Write pandas DataFrame to MinIO as CSV file"""
        try:
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue().encode('utf-8')
            
            self.client.put_object(
                bucket_name,
                object_name,
                io.BytesIO(csv_data),
                length=len(csv_data),
                content_type='text/csv'
            )
            
            logger.info(f"Successfully wrote {len(df)} records to {bucket_name}/{object_name}")
            
        except S3Error as e:
            logger.error(f"Error writing CSV to {bucket_name}/{object_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing DataFrame for CSV upload: {e}")
            raise
    
    def delete_object(self, bucket_name, object_name):
        """Delete object from MinIO"""
        try:
            self.client.remove_object(bucket_name, object_name)
            logger.info(f"Deleted object: {bucket_name}/{object_name}")
        except S3Error as e:
            logger.error(f"Error deleting object {bucket_name}/{object_name}: {e}")
            raise
    
    def get_object_info(self, bucket_name, object_name):
        """Get object information"""
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            return {
                'size': stat.size,
                'last_modified': stat.last_modified,
                'content_type': stat.content_type,
                'etag': stat.etag
            }
        except S3Error as e:
            logger.error(f"Error getting object info for {bucket_name}/{object_name}: {e}")
            return None
    
    def read_parquet_folder(self, bucket_name, folder_prefix):
        """Read all parquet files from a folder in MinIO and return a concatenated DataFrame"""
        try:
            object_names = self.list_objects(bucket_name, prefix=folder_prefix)
            parquet_files = [name for name in object_names if name.endswith(".parquet")]

            dataframes = []
            for object_name in parquet_files:
                df = self.read_parquet(bucket_name, object_name)
                if df is not None:
                    dataframes.append(df)

            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                logger.info(f"Combined {len(parquet_files)} parquet files into DataFrame with {len(combined_df)} rows")
                return combined_df
            else:
                logger.warning(f"No parquet files found in {bucket_name}/{folder_prefix}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error reading parquet folder {bucket_name}/{folder_prefix}: {e}")
            return pd.DataFrame()
