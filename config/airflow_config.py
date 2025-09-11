"""
Airflow集成配置
配置Airflow与数据底座的集成参数
"""

import os
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any, ClassVar


class AirflowConfig(BaseSettings):
    """Airflow集成配置类"""

    # Airflow连接配置
    AIRFLOW_HOST: str = Field(default="airflow-webserver", env="AIRFLOW_HOST")
    AIRFLOW_PORT: int = Field(default=8080, env="AIRFLOW_PORT")
    AIRFLOW_USERNAME: str = Field(default="admin", env="AIRFLOW_USERNAME")
    AIRFLOW_PASSWORD: str = Field(default="admin", env="AIRFLOW_PASSWORD")
    AIRFLOW_API_VERSION: str = Field(default="v1", env="AIRFLOW_API_VERSION")

    # Airflow数据库配置（使用统一的MySQL）
    AIRFLOW_DB_HOST: str = Field(default="mysql", env="AIRFLOW_DB_HOST")
    AIRFLOW_DB_PORT: int = Field(default=3306, env="AIRFLOW_DB_PORT")
    AIRFLOW_DB_USER: str = Field(default="airflow", env="AIRFLOW_DB_USER")
    AIRFLOW_DB_PASSWORD: str = Field(default="airflow123", env="AIRFLOW_DB_PASSWORD")
    AIRFLOW_DB_NAME: str = Field(default="airflow_db", env="AIRFLOW_DB_NAME")

    # Airflow Web UI配置
    AIRFLOW_WEB_URL: str = Field(default="http://airflow-webserver:8080", env="AIRFLOW_WEB_URL")

    # DAG配置
    DAG_FOLDER: str = Field(default="/opt/airflow/dags", env="DAG_FOLDER")
    DAG_TEMPLATES_FOLDER: str = Field(default="/opt/airflow/dag_templates", env="DAG_TEMPLATES_FOLDER")

    # 任务执行配置
    DEFAULT_TASK_TIMEOUT: int = Field(default=3600, env="DEFAULT_TASK_TIMEOUT")  # 1小时
    DEFAULT_RETRY_COUNT: int = Field(default=3, env="DEFAULT_RETRY_COUNT")
    DEFAULT_RETRY_DELAY: int = Field(default=300, env="DEFAULT_RETRY_DELAY")  # 5分钟

    # DataX配置
    DATAX_HOME: str = Field(default="/opt/datax", env="DATAX_HOME")
    DATAX_JOB_PATH: str = Field(default="./datax/jobs", env="DATAX_JOB_PATH")
    DATAX_LOG_PATH: str = Field(default="./datax/logs", env="DATAX_LOG_PATH")

    # SQL执行配置
    SQL_SCRIPTS_PATH: str = Field(default="./scripts/sql", env="SQL_SCRIPTS_PATH")
    SHELL_SCRIPTS_PATH: str = Field(default="./scripts/shell", env="SHELL_SCRIPTS_PATH")

    # 数据底座API配置
    BIGDATA_PLATFORM_API: str = Field(default="http://bigdata-platform:8000", env="BIGDATA_PLATFORM_API")
    BIGDATA_PLATFORM_AUTH_TOKEN: Optional[str] = Field(default=None, env="BIGDATA_PLATFORM_AUTH_TOKEN")

    # 任务分类配置
    TASK_CATEGORIES: ClassVar[Dict[str, Dict[str, str]]] = {
        "etl": {
            "display_name": "ETL数据处理",
            "color": "#1890ff",
            "icon": ""
        },
        "sync": {
            "display_name": "数据同步",
            "color": "#52c41a",
            "icon": ""
        },
        "sql": {
            "display_name": "SQL查询",
            "color": "#722ed1",
            "icon": ""
        },
        "shell": {
            "display_name": "Shell脚本",
            "color": "#fa541c",
            "icon": ""
        },
        "datax": {
            "display_name": "DataX任务",
            "color": "#13c2c2",
            "icon": ""
        }
    }

    # DAG默认参数
    DEFAULT_DAG_ARGS: ClassVar[Dict[str, Any]] = {
        'owner': 'bigdata-platform',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay_seconds': 300,
        'max_active_tasks': 16,
        'max_active_runs': 1
    }

    # 调度器配置
    SCHEDULER_HEARTBEAT_SEC: int = Field(default=5, env="SCHEDULER_HEARTBEAT_SEC")
    SCHEDULER_MAX_THREADS: int = Field(default=2, env="SCHEDULER_MAX_THREADS")

    # 日志配置
    ENABLE_TASK_LOGS: bool = Field(default=True, env="ENABLE_TASK_LOGS")
    LOG_RETENTION_DAYS: int = Field(default=30, env="LOG_RETENTION_DAYS")

    @property
    def airflow_api_url(self) -> str:
        """Airflow API基础URL"""
        return f"http://{self.AIRFLOW_HOST}:{self.AIRFLOW_PORT}/api/{self.AIRFLOW_API_VERSION}"

    @property
    def airflow_db_url(self) -> str:
        """Airflow数据库连接URL"""
        return f"mysql+pymysql://{self.AIRFLOW_DB_USER}:{self.AIRFLOW_DB_PASSWORD}@{self.AIRFLOW_DB_HOST}:{self.AIRFLOW_DB_PORT}/{self.AIRFLOW_DB_NAME}?charset=utf8mb4"

    @property
    def auth_headers(self) -> Dict[str, str]:
        """API认证头"""
        import base64
        credentials = f"{self.AIRFLOW_USERNAME}:{self.AIRFLOW_PASSWORD}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }

    def ensure_directories(self):
        """确保必要的目录存在"""
        directories = [
            self.DAG_FOLDER,
            self.DAG_TEMPLATES_FOLDER,
            self.DATAX_JOB_PATH,
            self.DATAX_LOG_PATH,
            self.SQL_SCRIPTS_PATH,
            self.SHELL_SCRIPTS_PATH,
            f"{self.DAG_FOLDER}/generated",  # 动态生成的DAG目录
            "./airflow/logs",
            "./airflow/config",
            "./airflow/plugins"
        ]

        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


# 创建全局配置实例
airflow_config = AirflowConfig()

# 确保目录存在
airflow_config.ensure_directories()