-- config/mysql/init_airflow.sql
-- MySQL数据库初始化脚本 - 增加Airflow支持

-- 创建主数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS bigdata_platform
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_unicode_ci;

-- 创建Airflow数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS airflow_db
DEFAULT CHARACTER SET utf8mb4
DEFAULT COLLATE utf8mb4_unicode_ci;

-- 创建主用户（如果不存在）
CREATE USER IF NOT EXISTS 'bigdata'@'%' IDENTIFIED BY 'bigdata123';

-- 创建Airflow用户（如果不存在）
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow123';

-- 授权主用户
GRANT ALL PRIVILEGES ON bigdata_platform.* TO 'bigdata'@'%';
GRANT SELECT ON mysql.* TO 'bigdata'@'%';

-- 授权Airflow用户
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';
-- 给Airflow用户一些额外权限（Airflow需要）
GRANT SELECT, CREATE, DROP, ALTER ON *.* TO 'airflow'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 使用主数据库
USE bigdata_platform;

-- 设置时区
SET time_zone = '+08:00';

-- 优化MySQL配置
SET GLOBAL innodb_buffer_pool_size = 256M;
SET GLOBAL max_connections = 300;
SET GLOBAL query_cache_size = 64M;
SET GLOBAL query_cache_type = 1;

-- 针对Airflow的优化设置
SET GLOBAL innodb_lock_wait_timeout = 300;
SET GLOBAL innodb_rollback_on_timeout = ON;
SET GLOBAL max_allowed_packet = 32M;

-- 设置Airflow数据库的字符集
ALTER DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 验证数据库创建
SHOW DATABASES;
SELECT User, Host FROM mysql.user WHERE User IN ('bigdata', 'airflow');

-- 记录初始化完成
INSERT INTO bigdata_platform.system_logs (log_level, message, created_at)
VALUES ('INFO', 'MySQL数据库初始化完成（含Airflow支持）', NOW())
ON DUPLICATE KEY UPDATE message = VALUES(message), created_at = NOW();