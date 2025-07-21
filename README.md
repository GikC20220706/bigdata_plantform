# 大数据平台部署指南

## 概述

这是一个企业级大数据平台，提供数据总览、集群监控、数据集成等功能。本指南将帮助您快速部署和配置这个平台。

## 架构优化

### 性能优化特性

1. **Redis 缓存层**
   - 多级缓存策略
   - 自动缓存失效
   - 并发请求优化

2. **异步处理**
   - 并发数据收集
   - 异步 API 调用
   - 背景任务处理

3. **可配置节点管理**
   - 外部配置文件支持
   - 动态节点发现
   - SSH 连接池

## 快速开始

### 前置要求

- Docker 20.10+
- Docker Compose 2.0+
- 8GB+ 内存
- 网络访问到 Hadoop/Flink/Doris 集群

### 一键部署

```bash
# 1. 克隆项目
git clone <repository-url>
cd bigdata-platform

# 2. 执行部署脚本
chmod +x deploy.sh
./deploy.sh setup
./deploy.sh deploy

# 3. 访问应用
open http://localhost:8000
```

## 详细配置

### 1. 环境配置

编辑 `.env` 文件：

```bash
# 核心配置
DEBUG=false
MOCK_DATA_MODE=false

# Redis 缓存
REDIS_HOST=redis
REDIS_PORT=6379

# 缓存策略 (秒)
CACHE_OVERVIEW_EXPIRE=60    # 总览数据缓存1分钟
CACHE_CLUSTER_EXPIRE=30     # 集群状态缓存30秒
CACHE_METRICS_EXPIRE=15     # 指标数据缓存15秒

# Hadoop 集群
HADOOP_NAMENODE_HOST=192.168.1.100
HADOOP_NAMENODE_PORT=9000
HIVE_HOST=192.168.1.100
HIVE_PORT=10000
HIVE_USERNAME=hive
HIVE_PASSWORD=your_password

# Flink 集群
FLINK_JOBMANAGER_HOST=192.168.1.100
FLINK_JOBMANAGER_PORT=8081

# Doris 集群
DORIS_FE_HOST=192.168.1.100
DORIS_FE_PORT=8060
DORIS_USERNAME=root
DORIS_PASSWORD=your_password
```

### 2. 集群节点配置

编辑 `config/nodes.json` 文件：

```json
{
  "cluster_info": {
    "cluster_name": "production-cluster",
    "version": "3.3.4"
  },
  "nodes": [
    {
      "node_name": "master-01",
      "ip_address": "192.168.1.100",
      "role": "NameNode",
      "ssh_config": {
        "port": 22,
        "username": "bigdata",
        "key_path": "/app/ssh_keys/id_rsa"
      },
      "services": [
        {
          "name": "hadoop-namenode",
          "port": 9000,
          "web_port": 9870
        }
      ],
      "enabled": true
    }
  ]
}
```

### 3. SSH 密钥配置

```bash
# 复制 SSH 密钥到部署目录
mkdir -p ssh_keys
cp ~/.ssh/id_rsa ssh_keys/
cp ~/.ssh/id_rsa.pub ssh_keys/

# 确保密钥权限
chmod 600 ssh_keys/id_rsa
chmod 644 ssh_keys/id_rsa.pub
```

## 部署选项

### 基础部署
```bash
./deploy.sh deploy
```
- 应用: http://localhost:8000
- Redis 缓存

### 监控部署
```bash
./deploy.sh deploy-monitor
```
- 应用: http://localhost:8000
- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090

### 生产部署
```bash
./deploy.sh deploy-prod
```
- Nginx 反向代理: http://localhost
- 完整监控栈
- 生产级配置

## 性能优化说明

### 缓存策略

| 数据类型 | 缓存时间 | 说明 |
|---------|---------|------|
| 完整总览 | 60秒 | 主页面数据 |
| 集群状态 | 30秒 | 集群监控数据 |
| 节点指标 | 15秒 | 系统资源数据 |
| 业务系统 | 1小时 | 相对稳定的配置数据 |
| 表统计 | 30分钟 | 数据库元信息 |

### API 性能

优化后的 API 响应时间：

- 缓存命中: < 50ms
- 缓存未命中: < 500ms
- 集群监控: < 200ms

### 并发处理

- 支持并发 SSH 连接（可配置最大并发数）
- 异步数据收集
- 连接池管理

## 运维管理

### 常用命令

```bash
# 查看服务状态
./deploy.sh health

# 查看日志
./deploy.sh logs
./deploy.sh logs redis

# 重启服务
./deploy.sh restart

# 更新服务
./deploy.sh update

# 备份配置
./deploy.sh backup

# 清理缓存
curl -X POST http://localhost:8000/api/v1/overview/cache/clear

# 手动刷新数据
curl -X POST http://localhost:8000/api/v1/overview/refresh
```

### 监控指标

访问 http://localhost:8000/api/v1/overview/performance 查看：

- 缓存命中率
- API 响应时间
- 系统资源使用
- 集群连接状态

### 故障排查

#### 1. 连接超时
```bash
# 检查网络连通性
docker-compose exec bigdata-platform ping 192.168.1.100

# 检查 SSH 连接
docker-compose exec bigdata-platform ssh -i /app/ssh_keys/id_rsa bigdata@192.168.1.100
```

#### 2. 缓存问题
```bash
# 检查 Redis 连接
docker-compose exec redis redis-cli ping

# 查看缓存状态
curl http://localhost:8000/api/v1/overview/cache/status

# 清除缓存
curl -X POST http://localhost:8000/api/v1/overview/cache/clear
```

#### 3. 性能问题
```bash
# 查看容器资源使用
docker stats

# 查看应用日志
./deploy.sh logs | grep "response time"

# 检查集群配置
curl http://localhost:8000/api/v1/overview/performance
```

## 配置管理

### 动态配置重载

```bash
# 重新加载集群配置
curl -X POST http://localhost:8000/api/v1/overview/config/reload
```

### 环境变量说明

| 变量名 | 默认值 | 说明 |
|-------|--------|------|
| CACHE_OVERVIEW_EXPIRE | 60 | 总览数据缓存秒数 |
| METRICS_CACHE_TTL | 30 | 集群指标缓存秒数 |
| METRICS_MAX_CONCURRENT | 5 | 最大并发 SSH 连接数 |
| CLUSTER_AUTO_DISCOVERY | false | 是否启用自动节点发现 |

## 安全配置

### 1. SSH 密钥管理
```bash
# 生成专用密钥
ssh-keygen -t rsa -b 4096 -f ssh_keys/bigdata_platform_key

# 配置到所有集群节点
ssh-copy-id -i ssh_keys/bigdata_platform_key.pub bigdata@192.168.1.100
```

### 2. 网络安全
```bash
# 限制访问 IP (生产环境)
# 在 nginx/nginx.conf 中添加:
# allow 192.168.1.0/24;
# deny all;
```

### 3. 密码管理
```bash
# 使用 Docker secrets (生产环境)
echo "your_secret_password" | docker secret create db_password -

# 在 docker-compose.yml 中引用
# secrets:
#   - db_password
```

## 扩展配置

### 添加新集群类型

1. 在 `config/nodes.json` 中添加节点
2. 修改 `app/utils/metrics_collector.py` 添加集群类型
3. 重启服务

### 自定义缓存策略

1. 修改 `.env` 中的缓存时间
2. 或在代码中调整 `cache_ttl` 配置

### 添加监控指标

1. 在 `monitoring/prometheus.yml` 中添加抓取目标
2. 在 Grafana 中创建新仪表板

## 备份和恢复

### 备份
```bash
# 自动备份
./deploy.sh backup

# 手动备份配置
tar -czf config_backup.tar.gz config/ ssh_keys/ .env
```

### 恢复
```bash
# 恢复配置
tar -xzf config_backup.tar.gz

# 重新部署
./deploy.sh restart
```

## 技术支持

### 日志位置
- 应用日志: `logs/bigdata_platform.log`
- Docker 日志: `docker-compose logs`
- Nginx 日志: 容器内 `/var/log/nginx/`

### 性能调优

1. **增加缓存时间** (适用于数据变化不频繁的场景)
2. **增加并发连接数** (适用于大集群)
3. **启用数据压缩** (适用于大量数据传输)

### 联系方式

如有问题，请查看：
1. 日志文件: `logs/bigdata_platform.log`
2. 健康检查: http://localhost:8000/api/v1/overview/health
3. API 文档: http://localhost:8000/docs

---

© 2024 首信云数据平台. 版本 3.2.1