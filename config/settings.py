"""
TODO 大数据平台的应用程序配置设置。 该模块包含应用程序的所有配置变量和环境特定设置，包括数据库、Hadoop和服务配置。
"""

import os
import platform
import pathlib
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application configuration settings.

    Loads configuration from environment variables with fallback defaults.
    Supports different modes for local development and production environments.
    """

    # Application Basic Configuration
    APP_NAME: str = "首信云数据底座"
    VERSION: str = "3.2.1"
    DEBUG: bool = Field(default=False, env="DEBUG")
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8000, env="PORT")

    # Environment Detection
    IS_WINDOWS: bool = platform.system() == "Windows"
    IS_LOCAL_DEV: bool = Field(default=True, env="IS_LOCAL_DEV")
    MOCK_DATA_MODE: bool = Field(default=True, env="MOCK_DATA_MODE")

    # 数据集成配置
    DATA_INTEGRATION_ENABLED: bool = Field(default=True, env="DATA_INTEGRATION_ENABLED")
    MAX_QUERY_RESULTS: int = Field(default=1000, env="MAX_QUERY_RESULTS")
    CONNECTION_TIMEOUT: int = Field(default=30, env="CONNECTION_TIMEOUT")
    DEFAULT_POOL_SIZE: int = Field(default=10, env="DEFAULT_POOL_SIZE")

    # Database Configuration
    DATABASE_URL: str = Field(
        default="sqlite:///./bigdata_platform.db",
        env="DATABASE_URL"
    )

    # Hadoop Configuration (only for non-Windows or production environments)
    HADOOP_HOME: Optional[str] = Field(default=None, env="HADOOP_HOME")
    HADOOP_CONF_DIR: Optional[str] = Field(default=None, env="HADOOP_CONF_DIR")

    # HDFS Configuration
    HDFS_NAMENODE: str = Field(default="hdfs://hadoop101:8020", env="HDFS_NAMENODE")
    HDFS_USER: str = Field(default="bigdata", env="HDFS_USER")

    # Hive Configuration
    HIVE_SERVER_HOST: str = Field(default="hadoop101", env="HIVE_SERVER_HOST")
    HIVE_SERVER_PORT: int = Field(default=10000, env="HIVE_SERVER_PORT")
    HIVE_DATABASE: str = Field(default="default", env="HIVE_DATABASE")
    HIVE_USERNAME: str = Field(default="bigdata", env="HIVE_USERNAME")
    HIVE_PASSWORD: str = Field(default="gqdw8862", env="HIVE_PASSWORD")

    # Flink Configuration
    FLINK_JOBMANAGER_HOST: str = Field(default="hadoop101", env="FLINK_JOBMANAGER_HOST")
    FLINK_JOBMANAGER_PORT: int = Field(default=8081, env="FLINK_JOBMANAGER_PORT")

    # Doris Configuration
    DORIS_FE_HOST: str = Field(default="hadoop101", env="DORIS_FE_HOST")
    DORIS_FE_PORT: int = Field(default=8060, env="DORIS_FE_PORT")
    DORIS_USERNAME: str = Field(default="root", env="DORIS_USERNAME")
    DORIS_PASSWORD: str = Field(default="1qaz@WSX3edc", env="DORIS_PASSWORD")

    # Redis Configuration
    REDIS_URL: str = Field(default="redis://hadoop101:6379/0", env="REDIS_URL")

    # Cache Configuration
    CACHE_TTL: int = Field(default=300, env="CACHE_TTL")  # 5 minutes

    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FILE: str = Field(default="logs/bigdata_platform.log", env="LOG_FILE")

    # Monitoring Configuration
    METRICS_COLLECTION_INTERVAL: int = Field(
        default=60,
        env="METRICS_COLLECTION_INTERVAL"
    )  # seconds
    METRICS_CACHE_TTL: int = Field(default=30, env="METRICS_CACHE_TTL")  # seconds
    METRICS_SSH_TIMEOUT: int = Field(default=5, env="METRICS_SSH_TIMEOUT")  # seconds
    METRICS_MAX_CONCURRENT: int = Field(default=5, env="METRICS_MAX_CONCURRENT")
    METRICS_ENABLE_CACHE: bool = Field(default=True, env="METRICS_ENABLE_CACHE")

    # Security Configuration
    SECRET_KEY: str = Field(default="your-secret-key-here", env="SECRET_KEY")

    # Environment-specific Properties
    @property
    def use_real_clusters(self) -> bool:
        """
        Determine whether to use real cluster connections.

        Returns:
            bool: True if should connect to real clusters, False for mock mode.
        """
        return not self.IS_LOCAL_DEV and not self.MOCK_DATA_MODE

    @property
    def hadoop_available(self) -> bool:
        """
        Check if Hadoop is available in the current environment.

        Returns:
            bool: True if Hadoop is properly configured and available.
        """
        if self.IS_WINDOWS or self.IS_LOCAL_DEV:
            return False
        return (
                self.HADOOP_HOME is not None and
                os.path.exists(self.HADOOP_HOME or "")
        )

    class Config:
        """Pydantic configuration for Settings class."""
        env_file = ".env"
        env_file_encoding = 'utf-8'
        env_ignore_empty = True
        extra = "ignore"  # Ignore extra environment variables


def create_settings() -> Settings:
    """
    Create and configure the settings instance.

    Returns:
        Settings: Configured settings instance.
    """
    settings_instance = Settings()

    # Ensure log directory exists
    log_dir = pathlib.Path(settings_instance.LOG_FILE).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    # Output environment information in debug mode
    if settings_instance.DEBUG:
        _print_environment_info(settings_instance)

    return settings_instance


def _print_environment_info(settings_instance: Settings) -> None:
    """Print environment information for debugging."""
    env_type = "Windows本地开发" if settings_instance.IS_WINDOWS else "Linux服务器"
    data_mode = "模拟数据" if settings_instance.MOCK_DATA_MODE else "真实数据"
    cluster_status = "禁用" if not settings_instance.use_real_clusters else "启用"

    print(f"🔧 运行环境: {env_type}")
    print(f"📊 数据模式: {data_mode}")
    print(f"🖥️ 集群连接: {cluster_status}")


# Create global settings instance
settings = create_settings()