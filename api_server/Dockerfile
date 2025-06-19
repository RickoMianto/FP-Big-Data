# Dockerfile untuk API Server
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/models

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV FLASK_DEBUG=0
ENV PYTHONPATH=/app
ENV MINIO_ENDPOINT=minio:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV MINIO_SECURE=false

# Expose port
EXPOSE 5000

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5000/health || exit 1

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# Wait for MinIO to be ready\n\
echo "Waiting for MinIO..."\n\
while ! nc -z minio 9000; do\n\
    echo "MinIO is not ready yet, waiting..."\n\
    sleep 2\n\
done\n\
echo "MinIO is ready!"\n\
\n\
# Wait a bit more to ensure MinIO is fully initialized\n\
sleep 5\n\
\n\
# Start the Flask application\n\
echo "Starting Flask API server..."\n\
exec python app.py\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# Add user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# Run entrypoint
ENTRYPOINT ["/entrypoint.sh"]