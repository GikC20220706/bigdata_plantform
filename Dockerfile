# Dockerfile - 修复版本
FROM debian:bullseye

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# 使用阿里云镜像源加速
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list

# Install system dependencies including SASL requirements
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    gcc \
    g++ \
    make \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    libsasl2-dev \
    libsasl2-modules \
    libsasl2-modules-gssapi-mit \
    libkrb5-dev \
    libldap2-dev \
    openssh-client \
    curl \
    netcat \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 创建python3软链接
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/data/uploads /app/data/sample_data
RUN mkdir -p /app/config /app/ssh_keys

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser
RUN chown -R appuser:appuser /app
RUN chown -R atguigu:atguigu /app/config /app/ssh_keys
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/overview/health || exit 1

# Start command
CMD ["python3", "startup.py"]