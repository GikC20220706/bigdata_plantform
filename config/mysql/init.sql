-- config/mysql/init.sql
-- MySQL数据库初始化脚本

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS bigdata_platform
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_unicode_ci;

-- 创建用户（如果不存在）
CREATE USER IF NOT EXISTS 'bigdata'@'%' IDENTIFIED BY 'bigdata123';

-- 授权
GRANT ALL PRIVILEGES ON bigdata_platform.* TO 'bigdata'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 使用数据库
USE bigdata_platform;

-- 设置时区
SET time_zone = '+08:00';

-- 优化配置
SET GLOBAL innodb_buffer_pool_size = 128M;
SET GLOBAL max_connections = 200;
SET GLOBAL query_cache_size = 32M;
SET GLOBAL query_cache_type = 1;

-- 创建索引（如果表已存在）
-- 这些索引将在应用启动时通过SQLAlchemy创建