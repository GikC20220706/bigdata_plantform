#!/bin/bash
# deploy.sh - Deployment script for BigData Platform

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="bigdata-platform"
COMPOSE_FILE="docker-compose.yml"
ENV_FILE=".env"

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker and Docker Compose are installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    log_success "Docker and Docker Compose are available"
}

# Create environment file if it doesn't exist
create_env_file() {
    if [ ! -f "$ENV_FILE" ]; then
        log_info "Creating environment file..."
        cat > "$ENV_FILE" << 'EOL'
# BigData Platform Configuration

# Application Settings
DEBUG=false
HOST=0.0.0.0
PORT=8000
IS_LOCAL_DEV=false
MOCK_DATA_MODE=false

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Cache Configuration
CACHE_DEFAULT_EXPIRE=300
CACHE_OVERVIEW_EXPIRE=60
CACHE_CLUSTER_EXPIRE=30
CACHE_METRICS_EXPIRE=15

# Database Configuration (MySQL/PostgreSQL)
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=bigdata_platform

# Hadoop Cluster Configuration
HADOOP_NAMENODE_HOST=192.168.1.100
HADOOP_NAMENODE_PORT=9000
HADOOP_WEBHDFS_PORT=9870
HADOOP_RM_HOST=192.168.1.100
HADOOP_RM_PORT=8088

# Hive Configuration
HIVE_HOST=192.168.1.100
HIVE_PORT=10000
HIVE_USERNAME=hive
HIVE_PASSWORD=your_hive_password

# Flink Configuration
FLINK_JOBMANAGER_HOST=192.168.1.100
FLINK_JOBMANAGER_PORT=8081

# Doris Configuration
DORIS_FE_HOST=192.168.1.100
DORIS_FE_PORT=8060
DORIS_USERNAME=root
DORIS_PASSWORD=your_doris_password

# Monitoring Configuration
METRICS_CACHE_TTL=30
METRICS_SSH_TIMEOUT=5
METRICS_MAX_CONCURRENT=5

# Cluster Auto Discovery
CLUSTER_AUTO_DISCOVERY=false

# Security (CHANGE IN PRODUCTION!)
SECRET_KEY=your-super-secret-key-change-this-in-production

# Logging
LOG_LEVEL=INFO

# Grafana (for monitoring profile)
GRAFANA_PASSWORD=admin
EOL
        log_success "Environment file created: $ENV_FILE"
        log_warning "Please edit $ENV_FILE to configure your cluster settings!"
    else
        log_info "Environment file already exists: $ENV_FILE"
    fi
}

# Create necessary directories
create_directories() {
    log_info "Creating necessary directories..."

    directories=(
        "logs"
        "data/uploads"
        "data/sample_data"
        "config"
        "ssh_keys"
        "nginx"
        "monitoring/grafana/dashboards"
        "monitoring/grafana/datasources"
    )

    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log_success "Created directory: $dir"
    done
}

# Create nginx configuration
create_nginx_config() {
    log_info "Creating Nginx configuration..."

    mkdir -p nginx
    cat > nginx/nginx.conf << 'EOL'
events {
    worker_connections 1024;
}

http {
    upstream bigdata_backend {
        server bigdata-platform:8000;
    }

    server {
        listen 80;
        server_name _;

        client_max_body_size 100M;

        location / {
            proxy_pass http://bigdata_backend;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # WebSocket support
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";

            # Timeouts
            proxy_connect_timeout 60s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;
        }

        # Health check endpoint
        location /health {
            access_log off;
            return 200 "healthy\n";
        }
    }
}
EOL
    log_success "Nginx configuration created"
}

# Create monitoring configuration
create_monitoring_config() {
    log_info "Creating monitoring configuration..."

    # Prometheus configuration
    mkdir -p monitoring
    cat > monitoring/prometheus.yml << 'EOL'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'bigdata-platform'
    static_configs:
      - targets: ['bigdata-platform:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s

  - job_name: 'redis'
    static_configs:
      - targets: ['redis:6379']
EOL

    # Grafana datasource
    mkdir -p monitoring/grafana/datasources
    cat > monitoring/grafana/datasources/prometheus.yml << 'EOL'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
EOL

    log_success "Monitoring configuration created"
}

# Create sample cluster nodes configuration
create_sample_nodes_config() {
    log_info "Creating sample cluster nodes configuration..."

    cat > config/nodes.json << 'EOL'
{
  "cluster_info": {
    "cluster_name": "bigdata-cluster",
    "version": "3.3.4",
    "description": "大数据平台集群配置"
  },
  "discovery": {
    "auto_discovery": false,
    "discovery_interval": 300
  },
  "nodes": [
    {
      "node_name": "bigdata-master-01",
      "ip_address": "192.168.1.100",
      "hostname": "master01.bigdata.local",
      "role": "NameNode",
      "node_type": "master",
      "ssh_config": {
        "port": 22,
        "username": "bigdata",
        "password": null,
        "key_path": "/app/ssh_keys/id_rsa",
        "timeout": 10
      },
      "services": [
        {
          "name": "hadoop-namenode",
          "port": 9000,
          "web_port": 9870,
          "status_check_url": "http://192.168.1.100:9870/jmx"
        },
        {
          "name": "yarn-resourcemanager",
          "port": 8032,
          "web_port": 8088,
          "status_check_url": "http://192.168.1.100:8088/ws/v1/cluster/info"
        }
      ],
      "resources": {
        "cpu_cores": 8,
        "memory_gb": 32,
        "disk_gb": 500
      },
      "monitoring": {
        "enabled": true,
        "collection_interval": 30
      },
      "enabled": true
    },
    {
      "node_name": "bigdata-worker-01",
      "ip_address": "192.168.1.101",
      "hostname": "worker01.bigdata.local",
      "role": "DataNode",
      "node_type": "worker",
      "ssh_config": {
        "port": 22,
        "username": "bigdata",
        "password": null,
        "key_path": "/app/ssh_keys/id_rsa",
        "timeout": 10
      },
      "services": [
        {
          "name": "hadoop-datanode",
          "port": 9866,
          "web_port": 9864,
          "status_check_url": "http://192.168.1.101:9864/jmx"
        },
        {
          "name": "yarn-nodemanager",
          "port": 8041,
          "web_port": 8042,
          "status_check_url": "http://192.168.1.101:8042/ws/v1/node/info"
        }
      ],
      "resources": {
        "cpu_cores": 16,
        "memory_gb": 64,
        "disk_gb": 2000
      },
      "monitoring": {
        "enabled": true,
        "collection_interval": 30
      },
      "enabled": true
    }
  ]
}
EOL
    log_success "Sample cluster nodes configuration created"
    log_warning "Please edit config/nodes.json to match your actual cluster setup!"
}

# Build and start services
deploy() {
    log_info "Building and starting BigData Platform..."

    # Build the application
    log_info "Building Docker image..."
    docker-compose build --no-cache

    # Start core services (without optional profiles)
    log_info "Starting core services..."
    docker-compose up -d redis bigdata-platform

    # Wait for services to be healthy
    log_info "Waiting for services to be ready..."
    sleep 30

    # Check service health
    if docker-compose ps | grep -q "Up (healthy)"; then
        log_success "Services are running and healthy!"
    else
        log_warning "Some services may not be fully ready. Check with: docker-compose ps"
    fi

    # Show service status
    docker-compose ps
}

# Deploy with monitoring
deploy_with_monitoring() {
    log_info "Deploying with monitoring stack..."

    # Start all services including monitoring
    docker-compose --profile monitoring up -d

    log_success "Deployed with monitoring stack!"
    log_info "Grafana is available at: http://localhost:3000 (admin/admin)"
    log_info "Prometheus is available at: http://localhost:9090"
}

# Deploy with production setup
deploy_production() {
    log_info "Deploying production setup with Nginx..."

    # Start all services including production profile
    docker-compose --profile production --profile monitoring up -d

    log_success "Production deployment completed!"
    log_info "Application is available at: http://localhost"
    log_info "Grafana is available at: http://localhost:3000"
}

# Show deployment information
show_info() {
    echo ""
    log_success "BigData Platform deployment completed!"
    echo ""
    echo "🌐 Application URLs:"
    echo "   - Main Application: http://localhost:8000"
    echo "   - API Documentation: http://localhost:8000/docs"
    echo "   - Health Check: http://localhost:8000/api/v1/overview/health"
    echo ""
    echo "🔧 Management Commands:"
    echo "   - View logs: docker-compose logs -f"
    echo "   - Stop services: docker-compose down"
    echo "   - Update services: docker-compose pull && docker-compose up -d"
    echo "   - View status: docker-compose ps"
    echo ""
    echo "📁 Important Files:"
    echo "   - Environment: .env"
    echo "   - Cluster Config: config/nodes.json"
    echo "   - Logs: logs/"
    echo ""
    log_warning "Remember to:"
    echo "   1. Configure your cluster settings in .env and config/nodes.json"
    echo "   2. Add your SSH keys to ssh_keys/ directory"
    echo "   3. Update cluster IP addresses and credentials"
    echo "   4. Test connectivity to your Hadoop/Flink/Doris clusters"
    echo ""
}

# Stop services
stop() {
    log_info "Stopping BigData Platform services..."
    docker-compose down
    log_success "Services stopped"
}

# Clean up everything
clean() {
    log_warning "This will remove all containers, images, and volumes!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cleaning up..."
        docker-compose down -v --rmi all --remove-orphans
        docker system prune -f
        log_success "Cleanup completed"
    else
        log_info "Cleanup cancelled"
    fi
}

# Show logs
logs() {
    docker-compose logs -f "${1:-bigdata-platform}"
}

# Update services
update() {
    log_info "Updating BigData Platform..."

    # Pull latest images
    docker-compose pull

    # Rebuild application
    docker-compose build --no-cache bigdata-platform

    # Restart services
    docker-compose up -d

    log_success "Update completed"
}

# Backup configuration and data
backup() {
    backup_dir="backup_$(date +%Y%m%d_%H%M%S)"
    log_info "Creating backup in $backup_dir..."

    mkdir -p "$backup_dir"

    # Backup configuration files
    cp -r config "$backup_dir/"
    cp -r ssh_keys "$backup_dir/" 2>/dev/null || true
    cp .env "$backup_dir/" 2>/dev/null || true

    # Backup data
    cp -r data "$backup_dir/"

    # Backup logs
    cp -r logs "$backup_dir/"

    # Create archive
    tar -czf "${backup_dir}.tar.gz" "$backup_dir"
    rm -rf "$backup_dir"

    log_success "Backup created: ${backup_dir}.tar.gz"
}

# Health check
health_check() {
    log_info "Performing health check..."

    # Check if containers are running
    if ! docker-compose ps | grep -q "Up"; then
        log_error "Some services are not running!"
        docker-compose ps
        return 1
    fi

    # Check application health endpoint
    if curl -f http://localhost:8000/api/v1/overview/health >/dev/null 2>&1; then
        log_success "Application health check passed"
    else
        log_error "Application health check failed"
        return 1
    fi

    # Check Redis
    if docker-compose exec redis redis-cli ping | grep -q "PONG"; then
        log_success "Redis health check passed"
    else
        log_error "Redis health check failed"
        return 1
    fi

    log_success "All health checks passed!"
}

# Show help
show_help() {
    echo "BigData Platform Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  setup           - Initial setup (create configs, directories)"
    echo "  deploy          - Deploy core services"
    echo "  deploy-monitor  - Deploy with monitoring (Grafana, Prometheus)"
    echo "  deploy-prod     - Deploy production setup (with Nginx)"
    echo "  stop            - Stop all services"
    echo "  restart         - Restart services"
    echo "  logs [service]  - Show logs (default: bigdata-platform)"
    echo "  update          - Update and restart services"
    echo "  backup          - Backup configuration and data"
    echo "  clean           - Remove all containers and images"
    echo "  health          - Perform health check"
    echo "  help            - Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 setup && $0 deploy"
    echo "  $0 deploy-monitor"
    echo "  $0 logs redis"
    echo "  $0 health"
}

# Setup function - create all necessary files and directories
setup() {
    log_info "Setting up BigData Platform deployment..."

    check_docker
    create_directories
    create_env_file
    create_nginx_config
    create_monitoring_config
    create_sample_nodes_config

    log_success "Setup completed successfully!"
    echo ""
    log_info "Next steps:"
    echo "1. Edit .env file with your cluster configuration"
    echo "2. Edit config/nodes.json with your cluster nodes"
    echo "3. Add SSH keys to ssh_keys/ directory"
    echo "4. Run: $0 deploy"
}

# Main script logic
case "${1:-help}" in
    setup)
        setup
        ;;
    deploy)
        check_docker
        deploy
        show_info
        ;;
    deploy-monitor)
        check_docker
        deploy_with_monitoring
        show_info
        ;;
    deploy-prod)
        check_docker
        deploy_production
        show_info
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 5
        deploy
        ;;
    logs)
        logs "$2"
        ;;
    update)
        update
        ;;
    backup)
        backup
        ;;
    clean)
        clean
        ;;
    health)
        health_check
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac