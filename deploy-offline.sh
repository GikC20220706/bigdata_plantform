#!/bin/bash

# Big Data Platform Offline Deployment Script
# 适用于无网络环境的离线部署流程

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
IMAGE_NAME="bigdata-platform"
IMAGE_TAG="latest"
EXPORT_FILE="bigdata-platform-${IMAGE_TAG}.tar"
PACKAGE_NAME="bigdata-platform-deployment.tar.gz"

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Function to check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    log "Docker and Docker Compose are available"
}

# Function to create necessary directories
create_directories() {
    log "Creating necessary directories..."

    mkdir -p logs
    mkdir -p uploads/excel
    mkdir -p data
    mkdir -p ssh_keys
    mkdir -p config
    mkdir -p nginx
    mkdir -p deployment_package

    # Set proper permissions
    chmod 755 logs uploads data
    chmod 700 ssh_keys

    log "Directories created successfully"
}

# Function to create environment file for development
create_dev_env_file() {
    if [ ! -f ".env" ]; then
        log "Creating .env file for development environment..."

        cat > .env << 'EOF'
# 首信云数据底座 - 开发环境配置

# 应用基础配置
DEBUG=false
HOST=0.0.0.0
PORT=8000
APP_NAME=首信云数据底座
VERSION=3.2.1

# 环境标识 - 开发环境使用模拟数据
IS_LOCAL_DEV=false
MOCK_DATA_MODE=false

# 数据库配置
DATABASE_URL=sqlite:///./data/bigdata_platform.db

# Hadoop配置 - 实际生产环境IP
HADOOP_HOME=/opt/module/hadoop
HADOOP_CONF_DIR=/opt/module/hadoop/etc/hadoop
HDFS_NAMENODE=hdfs://192.142.76.242:8020
HDFS_USER=bigdata

# Hive配置
HIVE_SERVER_HOST=192.142.76.242
HIVE_SERVER_PORT=10000
HIVE_DATABASE=default
HIVE_USERNAME=bigdata
HIVE_PASSWORD=gqdw8862

# Flink配置
FLINK_JOBMANAGER_HOST=192.142.76.242
FLINK_JOBMANAGER_PORT=8081

# Doris配置
DORIS_FE_HOST=192.142.76.242
DORIS_FE_PORT=8060
DORIS_USERNAME=root
DORIS_PASSWORD=1qaz@WSX3edc

# Redis配置
REDIS_URL=redis://redis:6379/0

# 缓存配置
CACHE_TTL=300
METRICS_CACHE_TTL=30
METRICS_SSH_TIMEOUT=5
METRICS_MAX_CONCURRENT=5
METRICS_ENABLE_CACHE=true

# SSH配置
SSH_KEY_PATH=/app/ssh_keys/id_rsa
SSH_USERNAME=bigdata
SSH_PASSWORD=
SSH_PORT=22
SSH_TIMEOUT=10

# 集群节点配置
CLUSTER_NODES_CONFIG_FILE=/app/config/nodes.json

# 数据集成配置
DATA_INTEGRATION_ENABLED=true
MAX_QUERY_RESULTS=1000
CONNECTION_TIMEOUT=30
DEFAULT_POOL_SIZE=10

# 文件上传配置
UPLOAD_DIR=/app/uploads
MAX_UPLOAD_SIZE=52428800

# 日志配置
LOG_LEVEL=INFO
LOG_FILE=/app/logs/bigdata_platform.log

# 安全配置
SECRET_KEY=change-this-secret-key-in-production

# 监控配置
METRICS_COLLECTION_INTERVAL=60
EOF

        warn "Please edit .env file with your actual configuration values"
    else
        log ".env file already exists"
    fi
}

# Function to create production environment template
create_production_env_template() {
    log "Creating production environment template..."

    cat > .env.production << 'EOF'
# 首信云数据底座 - 生产环境配置模板
# 请根据生产环境实际情况修改以下配置

# 应用基础配置
DEBUG=false
HOST=0.0.0.0
PORT=8000
APP_NAME=首信云数据底座
VERSION=3.2.1

# 环境标识 - 生产环境
IS_LOCAL_DEV=false
MOCK_DATA_MODE=false

# 数据库配置
DATABASE_URL=sqlite:///./data/bigdata_platform.db

# Hadoop配置 - 请修改为生产环境实际IP
HADOOP_HOME=/opt/module/hadoop
HADOOP_CONF_DIR=/opt/module/hadoop/etc/hadoop
HDFS_NAMENODE=hdfs://192.142.76.242:8020
HDFS_USER=bigdata

# Hive配置 - 请修改为生产环境实际IP
HIVE_SERVER_HOST=192.142.76.242
HIVE_SERVER_PORT=10000
HIVE_DATABASE=default
HIVE_USERNAME=bigdata
HIVE_PASSWORD=请修改为实际密码

# Flink配置 - 请修改为生产环境实际IP
FLINK_JOBMANAGER_HOST=192.142.76.242
FLINK_JOBMANAGER_PORT=8081

# Doris配置 - 请修改为生产环境实际IP
DORIS_FE_HOST=192.142.76.242
DORIS_FE_PORT=8060
DORIS_USERNAME=root
DORIS_PASSWORD=请修改为实际密码

# Redis配置
REDIS_URL=redis://redis:6379/0

# 缓存配置
CACHE_TTL=300
METRICS_CACHE_TTL=30
METRICS_SSH_TIMEOUT=5
METRICS_MAX_CONCURRENT=5
METRICS_ENABLE_CACHE=true

# SSH配置 - 请确保SSH密钥正确
SSH_KEY_PATH=/app/ssh_keys/id_rsa
SSH_USERNAME=bigdata
SSH_PASSWORD=
SSH_PORT=22
SSH_TIMEOUT=10

# 集群节点配置
CLUSTER_NODES_CONFIG_FILE=/app/config/nodes.json

# 数据集成配置
DATA_INTEGRATION_ENABLED=true
MAX_QUERY_RESULTS=1000
CONNECTION_TIMEOUT=30
DEFAULT_POOL_SIZE=10

# 文件上传配置
UPLOAD_DIR=/app/uploads
MAX_UPLOAD_SIZE=52428800

# 日志配置
LOG_LEVEL=INFO
LOG_FILE=/app/logs/bigdata_platform.log

# 安全配置 - 生产环境请务必修改
SECRET_KEY=请修改为安全的密钥

# 监控配置
METRICS_COLLECTION_INTERVAL=60
EOF

    log "Production environment template created as .env.production"
}

# Function to build Docker image
build_image() {
    log "Building Docker image..."

    # Pull base images first (if online)
    if ping -c 1 google.com &> /dev/null; then
        log "Network available, pulling base images..."
        docker pull debian:bullseye
        docker pull redis:7-alpine
    else
        warn "No network connection, using local base images"
    fi

    # Build the application image
    docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

    if [ $? -eq 0 ]; then
        log "Docker image built successfully: ${IMAGE_NAME}:${IMAGE_TAG}"
    else
        error "Failed to build Docker image"
        exit 1
    fi
}

# Function to export Docker images
export_images() {
    log "Exporting Docker images for offline deployment..."

    # Create deployment package directory
    mkdir -p deployment_package

    # Export application image
    log "Exporting application image..."
    docker save ${IMAGE_NAME}:${IMAGE_TAG} > deployment_package/${IMAGE_NAME}-${IMAGE_TAG}.tar

    # Export Redis image if available
    if docker images redis:7-alpine --format "table {{.Repository}}:{{.Tag}}" | grep -q "redis:7-alpine"; then
        log "Exporting Redis image..."
        docker save redis:7-alpine > deployment_package/redis-7-alpine.tar
    else
        warn "Redis image not found, will need to be pulled in production"
    fi

    log "Docker images exported successfully"
}

# Function to create deployment package
create_deployment_package() {
    log "Creating deployment package..."

    # Create deployment structure
    mkdir -p deployment_package/app

    # Copy necessary files
    cp docker-compose.yml deployment_package/
    cp .env.production deployment_package/.env
    cp -r config deployment_package/
    cp -r nginx deployment_package/ 2>/dev/null || mkdir -p deployment_package/nginx

    # Copy deployment scripts
    cat > deployment_package/deploy-production.sh << 'EOF'
#!/bin/bash
# Production deployment script

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    error "Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    error "Docker Compose is not installed"
    exit 1
fi

case "${1:-deploy}" in
    "load")
        log "Loading Docker images..."

        # Load application image
        if [ -f "bigdata-platform-latest.tar" ]; then
            docker load < bigdata-platform-latest.tar
            log "Application image loaded"
        else
            error "Application image file not found"
            exit 1
        fi

        # Load Redis image
        if [ -f "redis-7-alpine.tar" ]; then
            docker load < redis-7-alpine.tar
            log "Redis image loaded"
        else
            warn "Redis image not found, trying to pull..."
            docker pull redis:7-alpine || {
                error "Failed to load or pull Redis image"
                exit 1
            }
        fi
        ;;

    "deploy")
        log "Deploying application..."

        # Load images first
        $0 load

        # Create necessary directories
        mkdir -p logs uploads data ssh_keys
        chmod 700 ssh_keys

        # Check SSH key
        if [ ! -f "ssh_keys/id_rsa" ]; then
            error "SSH private key not found at ssh_keys/id_rsa"
            error "Please copy your SSH private key to ssh_keys/id_rsa and set permissions: chmod 600 ssh_keys/id_rsa"
            exit 1
        fi

        # Check configuration
        if [ ! -f ".env" ]; then
            error "Environment file .env not found"
            error "Please copy and configure .env.production to .env"
            exit 1
        fi

        # Start services
        docker-compose up -d

        log "Application deployed successfully"
        log "Access the platform at: http://localhost:8000"
        ;;

    "status")
        docker-compose ps
        ;;

    "logs")
        docker-compose logs -f bigdata-platform
        ;;

    "stop")
        log "Stopping services..."
        docker-compose down
        ;;

    "restart")
        log "Restarting services..."
        docker-compose down
        sleep 5
        docker-compose up -d
        ;;

    *)
        echo "Usage: $0 [load|deploy|status|logs|stop|restart]"
        echo ""
        echo "Commands:"
        echo "  load     - Load Docker images from tar files"
        echo "  deploy   - Deploy the application (default)"
        echo "  status   - Show service status"
        echo "  logs     - Show application logs"
        echo "  stop     - Stop all services"
        echo "  restart  - Restart all services"
        ;;
esac
EOF

    chmod +x deployment_package/deploy-production.sh

    # Create deployment instructions
    cat > deployment_package/README.md << 'EOF'
# 首信云数据底座 - 生产环境部署说明

## 部署前准备

1. 确保生产服务器已安装 Docker 和 Docker Compose
2. 将整个部署包传输到生产服务器
3. 解压部署包到目标目录

## 配置步骤

1. **配置环境变量**：
   ```bash
   cp .env.production .env
   vi .env  # 修改为生产环境实际配置
   ```

2. **配置SSH密钥**：
   ```bash
   # 将SSH私钥复制到ssh_keys目录
   cp /path/to/your/private_key ssh_keys/id_rsa
   chmod 600 ssh_keys/id_rsa
   ```

3. **配置集群节点信息**：
   ```bash
   vi config/nodes.json  # 根据实际集群情况修改
   ```

## 部署命令

```bash
# 加载Docker镜像
./deploy-production.sh load

# 部署应用
./deploy-production.sh deploy

# 查看状态
./deploy-production.sh status

# 查看日志
./deploy-production.sh logs
```

## 访问地址

- 应用地址: http://localhost:8000
- API文档: http://localhost:8000/docs

## 故障排除

1. 检查服务状态: `./deploy-production.sh status`
2. 查看日志: `./deploy-production.sh logs`
3. 重启服务: `./deploy-production.sh restart`
4. 检查网络连通性: `ping 192.142.76.242`
5. 测试SSH连接: `ssh -i ssh_keys/id_rsa bigdata@192.142.76.242`
EOF

    # Create the final package
    tar -czf ${PACKAGE_NAME} -C deployment_package --transform 's,^,bigdata-platform-deployment/,' .

    log "Deployment package created: ${PACKAGE_NAME}"
}

# Function to test deployment locally
test_deployment() {
    log "Testing deployment locally..."

    # Create test environment
    mkdir -p test_deployment
    tar -xzf ${PACKAGE_NAME} -C test_deployment

    cd test_deployment

    # Load images
    ./deploy-production.sh load

    # Check if images are loaded
    if docker images | grep -q ${IMAGE_NAME}; then
        log "Test successful: Images loaded correctly"
    else
        error "Test failed: Images not loaded"
        exit 1
    fi

    cd ..
    rm -rf test_deployment

    log "Local test completed successfully"
}

# Main function
main() {
    case "${1:-build-package}" in
        "init")
            log "Initializing development environment..."
            check_docker
            create_directories
            create_dev_env_file
            log "Development environment initialized!"
            info "Next steps:"
            info "1. Edit .env file with your configuration"
            info "2. Add SSH keys to ssh_keys/ directory"
            info "3. Run: $0 build-package"
            ;;

        "build")
            log "Building Docker image..."
            check_docker
            build_image
            log "Build completed!"
            ;;

        "build-package")
            log "Building deployment package for offline deployment..."
            check_docker
            create_directories
            create_dev_env_file
            create_production_env_template
            build_image
            export_images
            create_deployment_package
            log "Deployment package ready: ${PACKAGE_NAME}"
            info "Transfer this file to your production server and follow the instructions in the package"
            ;;

        "test")
            log "Testing deployment package..."
            test_deployment
            ;;

        "clean")
            log "Cleaning up..."
            rm -rf deployment_package
            rm -f ${PACKAGE_NAME}
            docker rmi ${IMAGE_NAME}:${IMAGE_TAG} 2>/dev/null || true
            log "Cleanup completed"
            ;;

        "help"|"-h"|"--help")
            echo "Big Data Platform Offline Deployment Script"
            echo ""
            echo "Usage: $0 [command]"
            echo ""
            echo "Commands:"
            echo "  init           - Initialize development environment"
            echo "  build          - Build Docker image only"
            echo "  build-package  - Build complete deployment package (default)"
            echo "  test           - Test deployment package locally"
            echo "  clean          - Clean up build artifacts"
            echo "  help           - Show this help message"
            echo ""
            echo "Offline Deployment Workflow:"
            echo "1. Run '$0 build-package' on machine with internet"
            echo "2. Transfer generated .tar.gz file to production server"
            echo "3. Extract and run './deploy-production.sh deploy' on production server"
            ;;

        *)
            error "Unknown command: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"