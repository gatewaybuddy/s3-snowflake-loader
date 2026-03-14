# Multi-stage build for S3-Snowflake Fargate processor
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

# Production stage
FROM python:3.11-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    gnupg2 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r fargate \
    && useradd -r -g fargate fargate

# Copy Python packages from builder
COPY --from=builder /root/.local /home/fargate/.local

# Copy application code
COPY splitter/ /app/splitter/
COPY --from=/mnt/d/Projects/s3-snowflake-loader/lambda/loader/config_loader.py /app/lambda/loader/
COPY --from=/mnt/d/Projects/s3-snowflake-loader/lambda/loader/snowflake_client.py /app/lambda/loader/
COPY --from=/mnt/d/Projects/s3-snowflake-loader/lambda/loader/path_parser.py /app/lambda/loader/

# Create required directories
RUN mkdir -p /tmp/gnupg \
    && chown -R fargate:fargate /app /tmp/gnupg \
    && chmod 755 /app/splitter/handler.py

# Set up environment
ENV PATH=/home/fargate/.local/bin:$PATH
ENV PYTHONPATH=/app
ENV GNUPGHOME=/tmp/gnupg

# Switch to non-root user
USER fargate
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Entry point
ENTRYPOINT ["python", "/app/splitter/handler.py"]