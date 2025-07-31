# Dockerfile - 优化版本，适用于aarch64离线部署
FROM docker.m.daocloud.io/library/python:3.9-slim

# 设置镜像元数据
LABEL maintainer="bigdata-platform" \
      version="3.2.1" \
      architecture="aarch64" \
      description="Big Data Platform for offline deployment"

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Shanghai

# 安装系统依赖（一次性安装，减少层数）
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 编译工具
    gcc \
    g++ \
    make \
    pkg-config \
    #datax 相关 \
    openjdk-8-jdk \
    # SSL和加密相关
    libssl-dev \
    libffi-dev \
    # SASL认证（Hive连接需要）
    libsasl2-dev \
    libsasl2-modules \
    libkrb5-dev \
    # 网络工具
    openssh-client \
    curl \
    netcat-traditional \
    # 时区数据
    tzdata \
    # 清理缓存
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 优化pip配置（预先配置，避免后续网络问题）
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
    && pip config set global.trusted-host mirrors.aliyun.com \
    && pip config set global.timeout 60 \
    && pip config set global.retries 3

# 先复制requirements.txt并安装依赖（利用Docker层缓存）
COPY requirements.txt .

# 安装Python依赖（添加更多错误处理和优化）
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt \
    # 清理pip缓存
    && pip cache purge \
    # 验证关键包安装
    && python -c "import fastapi, uvicorn, sqlalchemy, redis, paramiko; print('Core packages imported successfully')"

# 复制libkci.so.5文件到镜像中
COPY libkci.so.5 /usr/lib/libkci.so.5

# 设置libkci.so.5的权限并更新库缓存
RUN chmod 755 /usr/lib/libkci.so.5 \
    && ldconfig \
    && echo "libkci.so.5 installed successfully"

# 验证libkci.so.5安装
RUN ldconfig -p | grep kci || echo "Warning: libkci.so.5 not found in ldconfig cache" \
    && ls -la /usr/lib/libkci.so.5

# 复制应用代码（放在依赖安装之后，优化构建缓存）
COPY . .

# 创建必要的目录并设置权限
RUN mkdir -p \
    /app/logs \
    /app/data/uploads \
    /app/data/sample_data \
    /app/config \
    /app/ssh_keys \
    /app/uploads \
    # 创建临时目录
    /app/tmp

# 创建bigdata用户并设置权限
RUN groupadd -g 1000 bigdata \
    && useradd -u 1000 -g bigdata -m -s /bin/bash bigdata \
    # 设置目录权限
    && chown -R bigdata:bigdata /app \
    # SSH密钥目录需要严格权限
    && chmod 700 /app/ssh_keys \
    # 日志目录权限
    && chmod 755 /app/logs \
    # 上传目录权限
    && chmod 755 /app/uploads \
    # 临时目录权限
    && chmod 1777 /app/tmp

# 复制启动脚本并设置执行权限
COPY startup.py /app/
RUN chmod +x /app/startup.py \
    && chown bigdata:bigdata /app/startup.py

# 切换到非root用户
USER bigdata

# 暴露端口
EXPOSE 8000

# 健康检查（优化检查逻辑）
HEALTHCHECK --interval=30s --timeout=30s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/overview/health || exit 1

# 设置启动命令
CMD ["python3", "startup.py"]

# 添加一些运行时标签（便于管理）
LABEL build.date="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
      build.environment="production" \
      deployment.type="offline"
