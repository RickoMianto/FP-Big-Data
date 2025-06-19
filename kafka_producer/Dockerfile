# Dockerfile untuk Kafka Producer
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directory for data if needed
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC_EVENTS=ecommerce-events
ENV KAFKA_TOPIC_IMAGES=ecommerce-images
ENV DATA_FILE=/app/data/ecommerce-events.csv

# Expose port (if needed for health checks)
EXPOSE 8001

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import kafka; kafka.KafkaProducer(bootstrap_servers='kafka:9092')" || exit 1

# Run the producer
# CMD ["python", "producer.py"]
CMD ["sh", "-c", "python producer.py & python producer_images.py"]
