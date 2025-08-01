# Dockerfile - aarch64优化版本，解决Java安装问题
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

# 更新软件源并安装基础工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release \
    wget \
    curl

# 安装系统依赖（分步安装，更好地处理aarch64兼容性）
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 编译工具
    gcc \
    g++ \
    make \
    pkg-config \
    build-essential \
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
    tzdata

# 尝试安装Java - 使用多个备选方案
RUN apt-get update && \
    # 方案1: 尝试安装default-jdk（最兼容的方式）
    (apt-get install -y --no-install-recommends default-jdk && echo "✓ Installed default-jdk") || \
    # 方案2: 尝试安装openjdk-11-jdk
    (apt-get install -y --no-install-recommends openjdk-11-jdk && echo "✓ Installed openjdk-11-jdk") || \
    # 方案3: 尝试安装openjdk-17-jdk（更新的版本）
    (apt-get install -y --no-install-recommends openjdk-17-jdk && echo "✓ Installed openjdk-17-jdk") || \
    # 方案4: 安装headless版本
    (apt-get install -y --no-install-recommends default-jdk-headless && echo "✓ Installed default-jdk-headless") || \
    # 如果都失败，报错退出
    (echo "❌ Failed to install any Java version" && exit 1)

# 设置Java环境变量（自动检测Java路径）
RUN JAVA_HOME_CANDIDATE=$(find /usr/lib/jvm -name "java-*" -type d | head -n 1) && \
    if [ -n "$JAVA_HOME_CANDIDATE" ]; then \
        echo "export JAVA_HOME=$JAVA_HOME_CANDIDATE" >> /etc/environment && \
        echo "JAVA_HOME=$JAVA_HOME_CANDIDATE" && \
        ln -sf $JAVA_HOME_CANDIDATE/bin/java /usr/bin/java 2>/dev/null || true && \
        ln -sf $JAVA_HOME_CANDIDATE/bin/javac /usr/bin/javac 2>/dev/null || true; \
    fi

# 验证Java安装
RUN java -version && echo "✓ Java installation verified"

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 清理APT缓存
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* && \
    rm -rf /var/tmp/*

# 优化pip配置
RUN pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ && \
    pip config set global.trusted-host mirrors.aliyun.com && \
    pip config set global.timeout 60 && \
    pip config set global.retries 3

# 先复制requirements.txt并安装依赖
COPY requirements.txt .

# 安装Python依赖
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt && \
    pip cache purge && \
    python -c "import fastapi, uvicorn, sqlalchemy, redis, paramiko; print('✓ Core Python packages imported successfully')"

# 复制libkci.so.5文件（如果存在）
COPY libkci.so.5 /usr/lib/libkci.so.5

# 设置libkci.so.5的权限（如果存在）
RUN if [ -f /usr/lib/libkci.so.5 ]; then \
      chmod 755 /usr/lib/libkci.so.5 && \
      ldconfig && \
      echo "✓ libkci.so.5 installed successfully"; \
    else \
      echo "ℹ libkci.so.5 not found, skipping"; \
    fi

# 复制应用代码
COPY . .

# 创建必要的目录
RUN mkdir -p \
    /app/logs \
    /app/data/uploads \
    /app/data/sample_data \
    /app/config \
    /app/ssh_keys \
    /app/uploads \
    /app/tmp \
    /app/datax/jobs \
    /app/datax/logs \
    /app/datax/scripts

# 创建bigdata用户并设置权限
RUN groupadd -g 1000 bigdata && \
    useradd -u 1000 -g bigdata -m -s /bin/bash bigdata && \
    chown -R bigdata:bigdata /app && \
    chmod 700 /app/ssh_keys && \
    chmod 755 /app/logs && \
    chmod 755 /app/uploads && \
    chmod 1777 /app/tmp

# 复制启动脚本并设置执行权限
COPY startup.py /app/
RUN chmod +x /app/startup.py && \
    chown bigdata:bigdata /app/startup.py

# 切换到非root用户
USER bigdata

# 暴露端口
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8000/api/v1/overview/health || exit 1

# 设置启动命令
CMD ["python3", "startup.py"]

# 添加构建信息标签
LABEL build.date="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
      build.environment="production" \
      deployment.type="offline" \
      java.version="auto-detected"
