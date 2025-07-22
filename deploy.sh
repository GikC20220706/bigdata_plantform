#!/bin/bash

# Big Data Platform Deployment Script
# This script helps deploy the platform with proper configuration

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
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

    # Set proper permissions
    chmod 755 logs uploads data
    chmod 700 ssh_keys

    log "Directories created successfully"
}

# Function to create environment file
create_env_file() {
    if [ ! -f ".env" ]; then
        log "Creating .env file from template..."

        cat > .env << EOF
# Application Configuration
DEBUG=false
HOST=0.0.0.0
PORT=8000
IS_LOCAL_DEV=false
MOCK_DATA_MODE=false

# Database Configuration
DATABASE_URL=sqlite:///./data/bigdata_platform.db

# Hadoop Configuration
HADOOP_HOME=/opt/hadoop
HDFS_NAMENODE=hdfs://hadoop101:8020
HDFS_USER=bigdata

# Hive Configuration
HIVE_SERVER_HOST=hadoop101
HIVE_SERVER_PORT=10000
HIVE_USERNAME=bigdata
HIVE_PASSWORD=gqdw8862

# Flink Configuration
FLINK_JOBMANAGER_HOST=hadoop101
FLINK_JOBMANAGER_PORT=8081

# Doris Configuration
DORIS_FE_HOST=hadoop101
DORIS_FE_PORT=8060
DORIS_USERNAME=root
DORIS_PASSWORD=1qaz@WSX3edc

# Redis Configuration
REDIS_URL=redis://redis:6379/0

# Cache Configuration
CACHE_TTL=300
METRICS_CACHE_TTL=30
METRICS_SSH_TIMEOUT=5
METRICS_MAX_CONCURRENT=5

# SSH Configuration
SSH_USERNAME=bigdata
SSH_PASSWORD=
SSH_PORT=22
SSH_TIMEOUT=10

# Security
SECRET_KEY=change-this-secret-key-in-production

# Logging
LOG_LEVEL=INFO

# File Upload
MAX_UPLOAD_SIZE=52428800
EOF

        warn "Please edit .env file with your actual configuration values"

    else
        log ".env file already exists"
    fi
}

# Function to create nodes configuration file
create_nodes_config() {
    if [ ! -f "config/nodes.json" ]; then
        log "Creating nodes configuration file..."

        # Copy the template if it exists, otherwise create a minimal config
        if [ -f "config/nodes-template.json" ]; then
            cp config/nodes-template.json config/nodes.json
            log "Copied nodes-template.json to nodes.json"
        else
            cat > config/nodes.json << EOF
{
  "cluster_info": {
    "cluster_name": "production-cluster",
    "version": "3.3.4",
    "description": "请修改此配置文件以匹配您的实际集群环境"
  },
  "ssh_defaults": {
    "port": 22,
    "username": "bigdata",
    "timeout": 10,
    "key_path": "/app/ssh_keys/id_rsa"
  },
  "nodes": [
    {
      "node_name": "hadoop101",
      "ip_address": "192.168.1.101",
      "hostname": "hadoop101.cluster.local",
      "role": "NameNode,ResourceManager",
      "cluster_types": ["hadoop"],
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
          "status_check_url": "http://192.168.1.101:9870/jmx"
        }
      ],
      "monitoring": {
        "enabled": true,
        "node_exporter_port": 9100
      },
      "tags": ["namenode", "resourcemanager", "master"],
      "enabled": true
    }
  ],
  "monitoring_settings": {
    "collection_interval": 30,
    "cache_ttl": 30,
    "ssh_timeout": 5,
    "max_concurrent_connections": 5,
    "retry_attempts": 3,
    "retry_delay": 2
  }
}
EOF
        fi

        warn "Please edit config/nodes.json with your actual cluster node information"

    else
        log "nodes.json configuration file already exists"
    fi
}

# Function to create SSH key directory
setup_ssh_keys() {
    log "Setting up SSH keys directory..."

    if [ ! -d "ssh_keys" ]; then
        mkdir -p ssh_keys
        chmod 700 ssh_keys
    fi

    if [ ! -f "ssh_keys/id_rsa" ]; then
        warn "SSH private key not found at ssh_keys/id_rsa"
        warn "Please copy your SSH private key to ssh_keys/id_rsa"
        warn "and set proper permissions: chmod 600 ssh_keys/id_rsa"

        # Create a placeholder
        cat > ssh_keys/README.md << EOF
# SSH Keys Directory

This directory should contain the SSH private key for accessing your cluster nodes.

Required files:
- id_rsa: Private key file (chmod 600)
- id_rsa.pub: Public key file (optional, chmod 644)

Make sure the public key is added to ~/.ssh/authorized_keys on all cluster nodes.

Example setup:
1. Copy your private key: cp ~/.ssh/id_rsa ./ssh_keys/
2. Set permissions: chmod 600 ./ssh_keys/id_rsa
3. Verify access: ssh -i ./ssh_keys/id_rsa bigdata@hadoop101
EOF
    else
        log "SSH key found"
    fi
}

# Function to create nginx configuration
create_nginx_config() {
    log "Creating nginx configuration..."

    mkdir -p nginx

    if [ ! -f "nginx/nginx.conf" ]; then
        cat > nginx/nginx.conf << EOF
events {
    worker_connections 1024;
}

http {
    upstream bigdata_backend {
        server bigdata-platform:8000;
    }

    server {
        listen 80;
        server_name localhost;

        client_max_body_size 50M;

        location / {
            proxy_pass http://bigdata_backend;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;

            # WebSocket support
            proxy_http_version 1.1;
            proxy_set_header Upgrade \$http_upgrade;
            proxy_set_header Connection "upgrade";

            # Timeouts
            proxy_connect_timeout 60s;
            proxy_send_timeout 60s;
            proxy_read_timeout 60s;
        }

        # Static files (if any)
        location /static/ {
            proxy_pass http://bigdata_backend;
        }

        # Health check
        location /health {
            access_log off;
            return 200 "healthy\n";
        }
    }
}
EOF
        log "Nginx configuration created"
    else
        log "Nginx configuration already exists"
    fi
}

# Function to validate configuration
validate_config() {
    log "Validating configuration..."

    # Check if required files exist
    required_files=(".env" "config/nodes.json")

    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            error "Required file $file not found"
            exit 1
        fi
    done

    # Check if SSH key exists (warning only)
    if [ ! -f "ssh_keys/id_rsa" ]; then
        warn "SSH private key not found. Cluster monitoring will not work."
    fi

    log "Configuration validation completed"
}

# Function to build and start services
deploy() {
    log "Building and starting services..."

    # Pull latest images
    docker-compose -f docker-compose.production.yml pull redis

    # Build application
    docker-compose -f docker-compose.production.yml build bigdata-platform

    # Start services
    docker-compose -f docker-compose.production.yml up -d

    log "Services started successfully"
}

# Function to show status
show_status() {
    log "Checking service status..."
    docker-compose -f docker-compose.production.yml ps
}

# Function to show logs
show_logs() {
    log "Showing application logs..."
    docker-compose -f docker-compose.production.yml logs -f bigdata-platform
}

# Function to stop services
stop() {
    log "Stopping services..."
    docker-compose -f docker-compose.production.yml down
}

# Function to cleanup
cleanup() {
    log "Cleaning up..."
    docker-compose -f docker-compose.production.yml down -v
    docker system prune -f
}

# Main function
main() {
    case "${1:-deploy}" in
        "init")
            log "Initializing deployment environment..."
            check_docker
            create_directories
            create_env_file
            create_nodes_config
            setup_ssh_keys
            create_nginx_config
            log "Initialization completed!"
            info "Next steps:"
            info "1. Edit .env file with your configuration"
            info "2. Edit config/nodes.json with your cluster nodes"
            info "3. Copy SSH keys to ssh_keys/ directory"
            info "4. Run: ./deploy.sh deploy"
            ;;
        "deploy")
            log "Deploying Big Data Platform..."
            check_docker
            validate_config
            deploy
            sleep 10
            show_status
            log "Deployment completed!"
            info "Access the platform at: http://localhost:8000"
            info "API documentation at: http://localhost:8000/docs"
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs
            ;;
        "stop")
            stop
            ;;
        "restart")
            log "Restarting services..."
            stop
            sleep 5
            deploy
            show_status
            ;;
        "cleanup")
            cleanup
            ;;
        "help"|"-h"|"--help")
            echo "Big Data Platform Deployment Script"
            echo ""
            echo "Usage: $0 [command]"
            echo ""
            echo "Commands:"
            echo "  init     - Initialize deployment environment"
            echo "  deploy   - Deploy the platform (default)"
            echo "  status   - Show service status"
            echo "  logs     - Show application logs"
            echo "  stop     - Stop all services"
            echo "  restart  - Restart all services"
            echo "  cleanup  - Stop services and cleanup"
            echo "  help     - Show this help message"
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