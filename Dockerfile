# Dockerfile - 使用python slim镜像，支持ARM64
FROM docker.m.daocloud.io/library/python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    libsasl2-dev \
    libsasl2-modules \
    libkrb5-dev \
    openssh-client \
    curl \
    netcat \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data/uploads /app/data/sample_data /app/config /app/ssh_keys

# Create bigdata user
RUN groupadd -g 1000 bigdata && \
    useradd -u 1000 -g bigdata -m -s /bin/bash bigdata && \
    chown -R bigdata:bigdata /app && \
    chmod 700 /app/ssh_keys

USER bigdata

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/overview/health || exit 1

CMD ["python3", "startup.py"]