#!/usr/bin/env python3
"""
E-Commerce Recommender System - Project Runner
Automation script untuk menjalankan keseluruhan pipeline big data project
"""

import os
import sys
import time
import subprocess
import signal
import threading
from pathlib import Path

class ProjectRunner:
    def __init__(self):
        self.project_root = Path(__file__).parent.absolute()
        self.processes = []
        self.running = True
        
    def log(self, message, level="INFO"):
        """Logging dengan timestamp"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")
        
    def run_command(self, command, cwd=None, background=False):
        """Menjalankan command dengan error handling"""
        try:
            self.log(f"Running: {command}")
            if background:
                process = subprocess.Popen(
                    command, 
                    shell=True, 
                    cwd=cwd or self.project_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                self.processes.append(process)
                return process
            else:
                result = subprocess.run(
                    command, 
                    shell=True, 
                    cwd=cwd or self.project_root,
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    self.log(f"Error executing: {command}", "ERROR")
                    self.log(f"Error output: {result.stderr}", "ERROR")
                    return False
                return True
        except Exception as e:
            self.log(f"Exception running command {command}: {str(e)}", "ERROR")
            return False
            
    def check_dependencies(self):
        """Check apakah semua dependencies tersedia"""
        self.log("Checking dependencies...")
        
        # Check Docker
        if not self.run_command("docker --version"):
            self.log("Docker tidak ditemukan. Install Docker terlebih dahulu.", "ERROR")
            return False
            
        # Check Docker Compose
        if not self.run_command("docker-compose --version"):
            self.log("Docker Compose tidak ditemukan. Install Docker Compose terlebih dahulu.", "ERROR")
            return False
            
        # Check required directories
        required_dirs = ['data', 'kafka_producer', 'spark_processor', 'api_server', 'frontend']
        for dir_name in required_dirs:
            if not (self.project_root / dir_name).exists():
                self.log(f"Directory {dir_name} tidak ditemukan!", "ERROR")
                return False
                
        # Check dataset
        dataset_path = self.project_root / 'data' / 'ecommerce-behavior-data.csv'
        if not dataset_path.exists():
            self.log("Dataset ecommerce-events.csv tidak ditemukan di folder data/", "ERROR")
            return False
            
        self.log("All dependencies check passed!")
        return True
        
    def setup_environment(self):
        """Setup environment dan install dependencies"""
        self.log("Setting up environment...")
        
        # Install requirements untuk setiap service
        services = ['kafka_producer', 'spark_processor', 'api_server']
        
        for service in services:
            requirements_path = self.project_root / service / 'requirements.txt'
            if requirements_path.exists():
                self.log(f"Installing requirements for {service}...")
                if not self.run_command(f"pip install -r {service}/requirements.txt"):
                    self.log(f"Failed to install requirements for {service}", "ERROR")
                    return False
            else:
                self.log(f"No requirements.txt found for {service}", "WARNING")
                
        return True
        
    def start_infrastructure(self):
        """Start Docker services (Kafka, Spark, MinIO)"""
        self.log("Starting infrastructure services...")
        
        # Stop any existing containers
        self.run_command("docker-compose down")
        
        # Start services
        if not self.run_command("docker-compose up -d"):
            self.log("Failed to start Docker services", "ERROR")
            return False
            
        self.log("Waiting for services to be ready...")
        time.sleep(30)  # Wait for services to start
        
         # ===> Tambahkan inisialisasi bucket MinIO di sini <===
        from minio_client import MinIOClient
        mc = MinIOClient(
            endpoint=os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            secure=False
        )
        for bucket in ["bronze", "silver", "gold", "checkpoints"]:
            mc.create_bucket(bucket)
        self.log("MinIO buckets ensured: bronze, silver, gold, checkpoints")

        # Check if services are running
        result = subprocess.run("docker-compose ps", shell=True, capture_output=True, text=True)
        self.log("Docker services status:")
        print(result.stdout)
        
        return True
        
    def run_data_pipeline(self):
        """Menjalankan data pipeline secara berurutan"""
        self.log("Starting data pipeline...")
        
        # Step 1: Start Structured Data Producer
        self.log("Step 1a: Running Structured Kafka Producer (ecommerce-events)...")
        producer_a = self.run_command(
            "python3 kafka_producer/producer.py",
            background=True
        )

        # Step 1b: Start Unstructured Image Producer
        self.log("Step 1b: Running Image Kafka Producer (ecommerce-images)...")
        producer_b = self.run_command(
            "python3 kafka_producer/producer_images.py",
            background=True
        )

        if not (producer_a and producer_b):
            self.log("Failed to start one of the Kafka Producers", "ERROR")
            return False

        self.log("Both producers are running in background.")
        time.sleep(10)
        
        # Step 2: Run Spark Jobs
        spark_jobs = [
            'spark_processor/jobs/bronze_to_silver.py',
            'spark_processor/jobs/silver_to_gold.py',
            'spark_processor/jobs/train_model.py'
        ]
        
        for i, job in enumerate(spark_jobs, 2):
            self.log(f"Step {i}: Running {job}...")
            if not self.run_command(f"python3 {job}"):
                self.log(f"Failed to run {job}", "ERROR")
                return False
            time.sleep(5)  # Wait between jobs
            
        return True
        
    def start_api_server(self):
        """Start Flask API Server"""
        self.log("Starting API Server...")
        
        api_process = self.run_command(
            "python3 api_server/app.py",
            background=True
        )
        
        if not api_process:
            self.log("Failed to start API Server", "ERROR")
            return False
            
        time.sleep(5)
        return True
        
    def start_frontend(self):
        """Start Frontend (simple HTTP server)"""
        self.log("Starting Frontend...")
        
        # Simple HTTP server untuk frontend
        frontend_process = self.run_command(
            "python3 -m http.server 8080",
            cwd=self.project_root / 'frontend',
            background=True
        )
        
        if not frontend_process:
            self.log("Failed to start Frontend server", "ERROR")
            return False
            
        return True
               
    def monitor_services(self):
        """Monitor running services"""
        self.log("=== PROJECT STARTED SUCCESSFULLY ===")
        self.log("Services running:")
        self.log("- Infrastructure: Docker Compose services")
        self.log("- API Server: http://localhost:5000")
        self.log("- Frontend: http://localhost:8080")
        self.log("- Kafka Producer: Running in background")
        self.log("")
        self.log("Press Ctrl+C to stop all services")
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.log("Stopping services...")
            self.stop_all_services()
            
    def stop_all_services(self):
        """Stop all running services"""
        self.running = False
        
        # Stop Python processes
        for process in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
                    
        # Stop Docker services
        self.run_command("docker-compose down")
        self.log("All services stopped.")
        
    def run_full_pipeline(self):
        """Menjalankan full pipeline"""
        try:
            if not self.check_dependencies():
                return False
                
            if not self.setup_environment():
                return False
                
            if not self.start_infrastructure():
                return False
                
            if not self.run_data_pipeline():
                return False
                
            if not self.start_api_server():
                return False
                
            if not self.start_frontend():
                return False
                
            self.monitor_services()
            return True
            
        except Exception as e:
            self.log(f"Error in pipeline: {str(e)}", "ERROR")
            self.stop_all_services()
            return False

def main():
    """Main function dengan command line options"""
    import argparse
    
    parser = argparse.ArgumentParser(description='E-Commerce Recommender System Runner')
    parser.add_argument('--mode', choices=['full', 'infra', 'pipeline', 'api', 'frontend'], 
                       default='full', help='Mode to run')
    parser.add_argument('--skip-deps', action='store_true', help='Skip dependency check')
    
    args = parser.parse_args()
    
    runner = ProjectRunner()
    
    # Setup signal handler
    def signal_handler(sig, frame):
        print("\nReceived interrupt signal...")
        runner.stop_all_services()
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.mode == 'full':
        success = runner.run_full_pipeline()
    elif args.mode == 'infra':
        success = runner.check_dependencies() and runner.start_infrastructure()
    elif args.mode == 'pipeline':
        success = runner.run_data_pipeline()
    elif args.mode == 'api':
        success = runner.start_api_server() and runner.monitor_services()
    elif args.mode == 'frontend':
        success = runner.start_frontend() and runner.monitor_services()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()