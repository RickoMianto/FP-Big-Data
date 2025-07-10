from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import json
import logging
from minio_client import MinIOClient
import numpy as np
from datetime import datetime
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Initialize MinIO client
minio_client = MinIOClient()

class EcommerceAPI:
    def __init__(self):
        self.minio_client = minio_client
        self.cache = {}
        self.cache_timeout = 300  # 5 minutes
    
    def get_cached_data(self, key):
        """Get data from cache if valid"""
        if key in self.cache:
            data, timestamp = self.cache[key]
            if (datetime.now() - timestamp).seconds < self.cache_timeout:
                return data
        return None
    
    def set_cached_data(self, key, data):
        """Set data in cache"""
        self.cache[key] = (data, datetime.now())
    
    def load_parquet_data(self, bucket, object_name):
        """Load parquet data from MinIO"""
        try:
            # Check cache first
            cache_key = f"{bucket}/{object_name}"
            cached_data = self.get_cached_data(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Load from MinIO
            data = self.minio_client.read_parquet_folder(bucket, object_name)

            # data = self.minio_client.read_parquet(bucket, object_name)
            
            # Cache the data
            self.set_cached_data(cache_key, data)
            
            return data
            
        except Exception as e:
            logger.error(f"Error loading data from {bucket}/{object_name}: {e}")
            return None
    
    def get_product_statistics(self):
        """Get product statistics from models layer"""
        try:
            data = self.load_parquet_data("models", "product_statistics")
            if data is not None:
                return data.to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting product statistics: {e}")
            return []
    
    def get_top_products(self, metric="most_viewed", limit=10):
        """Get top products by specific metric"""
        try:
            data = self.load_parquet_data("models", "product_rankings")
            if data is not None:
                filtered_data = data[data['rank_type'] == metric].head(limit)
                return filtered_data.to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting top products: {e}")
            return []
    
    def search_products(self, query, filters=None, limit=20):
        """Search products with optional filters"""
        try:
            data = self.load_parquet_data("models", "recommendation_features")
            if data is None:
                return []
            
            # Apply text search
            if query:
                query_lower = query.lower()
                mask = (
                    data['category_code'].str.contains(query_lower, na=False) |
                    data['brand'].str.contains(query_lower, na=False)
                )
                data = data[mask]
            
            # Apply filters
            if filters:
                if 'category' in filters and filters['category']:
                    data = data[data['category_code'].str.contains(filters['category'], na=False)]
                
                if 'brand' in filters and filters['brand']:
                    data = data[data['brand'].str.contains(filters['brand'], na=False)]
                
                if 'price_min' in filters and filters['price_min']:
                    data = data[data['avg_price'] >= float(filters['price_min'])]
                
                if 'price_max' in filters and filters['price_max']:
                    data = data[data['avg_price'] <= float(filters['price_max'])]
                
                if 'price_tier' in filters and filters['price_tier']:
                    data = data[data['price_tier'] == filters['price_tier']]
            
            # Sort by popularity score
            data = data.sort_values('popularity_score', ascending=False)
            
            return data.head(limit).to_dict('records')
            
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return []
    
    def get_product_details(self, product_id):
        """Get detailed information about a specific product"""
        try:
            data = self.load_parquet_data("models", "product_statistics")
            if data is not None:
                product = data[data['product_id'] == product_id]
                if not product.empty:
                    return product.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.error(f"Error getting product details: {e}")
            return None
    
    def get_similar_products(self, product_id, limit=5):
        """Get similar products for a given product"""
        try:
            data = self.load_parquet_data("models", "similar_products")
            if data is not None:
                similar = data[data['product_id'] == product_id].sort_values('similarity_score', ascending=False)
                return similar.head(limit).to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting similar products: {e}")
            return []
    
    def get_recommendations_for_user(self, user_id, limit=10):
        """Get recommendations for a specific user"""
        try:
            data = self.load_parquet_data("models", "user_recommendations")
            if data is not None:
                user_recs = data[data['user'] == int(user_id)]
                if not user_recs.empty:
                    return user_recs.head(limit).to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting user recommendations: {e}")
            return []
    
    def get_category_statistics(self):
        """Get category-level statistics"""
        try:
            data = self.load_parquet_data("models", "category_statistics")
            if data is not None:
                return data.to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting category statistics: {e}")
            return []
    
    def get_brand_statistics(self):
        """Get brand-level statistics"""
        try:
            data = self.load_parquet_data("models", "brand_statistics")
            if data is not None:
                return data.to_dict('records')
            return []
        except Exception as e:
            logger.error(f"Error getting brand statistics: {e}")
            return []
        
    def get_user_recommendations_data(self):
        return self.load_parquet_data("models", "user_recommendations")

    def get_clustering_model_data(self):
        dfs = []
        for sub in ["metadata", "stages"]:
            df = self.load_parquet_data("models", f"clustering_model/{sub}")
            if df is not None:
                df["source"] = sub
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    
# Initialize API instance
api = EcommerceAPI()

# API Routes
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api/products/search', methods=['GET'])
def search_products():
    """Search products with filters"""
    try:
        query = request.args.get('q', '')
        limit = int(request.args.get('limit', 20))
        
        # Parse filters
        filters = {}
        if request.args.get('category'):
            filters['category'] = request.args.get('category')
        if request.args.get('brand'):
            filters['brand'] = request.args.get('brand')
        if request.args.get('price_min'):
            filters['price_min'] = request.args.get('price_min')
        if request.args.get('price_max'):
            filters['price_max'] = request.args.get('price_max')
        if request.args.get('price_tier'):
            filters['price_tier'] = request.args.get('price_tier')
        
        results = api.search_products(query, filters, limit)
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results),
            "query": query,
            "filters": filters
        })
        
    except Exception as e:
        logger.error(f"Error in search endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/products/top', methods=['GET'])
def get_top_products():
    """Get top products by metric"""
    try:
        metric = request.args.get('metric', 'most_viewed')
        limit = int(request.args.get('limit', 10))
        
        valid_metrics = ['most_viewed', 'most_carted', 'most_purchased', 'most_popular']
        if metric not in valid_metrics:
            return jsonify({"success": False, "error": "Invalid metric"}), 400
        
        results = api.get_top_products(metric, limit)
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results),
            "metric": metric
        })
        
    except Exception as e:
        logger.error(f"Error in top products endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/products/<product_id>', methods=['GET'])
def get_product_details(product_id):
    """Get detailed product information"""
    try:
        product = api.get_product_details(product_id)
        
        if product:
            return jsonify({
                "success": True,
                "data": product
            })
        else:
            return jsonify({"success": False, "error": "Product not found"}), 404
            
    except Exception as e:
        logger.error(f"Error in product details endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/products/<product_id>/similar', methods=['GET'])
def get_similar_products(product_id):
    """Get similar products"""
    try:
        limit = int(request.args.get('limit', 5))
        results = api.get_similar_products(product_id, limit)
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results),
            "product_id": product_id
        })
        
    except Exception as e:
        logger.error(f"Error in similar products endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/recommendations/<user_id>', methods=['GET'])
def get_user_recommendations(user_id):
    """Get recommendations for a user"""
    try:
        limit = int(request.args.get('limit', 10))
        results = api.get_recommendations_for_user(user_id, limit)
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results),
            "user_id": user_id
        })
        
    except Exception as e:
        logger.error(f"Error in recommendations endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/stats/categories', methods=['GET'])
def get_categories():
    """Get category statistics"""
    try:
        results = api.get_category_statistics()
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results)
        })
        
    except Exception as e:
        logger.error(f"Error in categories endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/stats/brands', methods=['GET'])
def get_brands():
    """Get brand statistics"""
    try:
        results = api.get_brand_statistics()
        
        return jsonify({
            "success": True,
            "data": results,
            "count": len(results)
        })
        
    except Exception as e:
        logger.error(f"Error in brands endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/dashboard/overview', methods=['GET'])
def get_dashboard_overview():
    """Get overview data for dashboard"""
    try:
        # Get top products for each category
        top_viewed = api.get_top_products('most_viewed', 5)
        top_carted = api.get_top_products('most_carted', 5)
        top_purchased = api.get_top_products('most_purchased', 5)
        
        # Get category and brand stats
        categories = api.get_category_statistics()
        brands = api.get_brand_statistics()
        
        return jsonify({
            "success": True,
            "data": {
                "top_viewed": top_viewed,
                "top_carted": top_carted,
                "top_purchased": top_purchased,
                "total_categories": len(categories),
                "total_brands": len(brands),
                "categories": categories[:10],  # Top 10 categories
                "brands": brands[:10]  # Top 10 brands
            }
        })
        
    except Exception as e:
        logger.error(f"Error in dashboard overview endpoint: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)