"""
执行器模块初始化
注册所有可用的执行器
"""
from .base_executor import JobExecutor, ExecutorManager, executor_manager
from .bash_executor import BashExecutor
from .smart_sync_executor import SmartSyncExecutor
from .python_executor import PythonExecutor
from .http_executor import HttpExecutor
from .api_executor import ApiExecutor
from .jdbc_executor_enhanced import JDBCExecutorEnhanced


def init_executors():
    """初始化并注册所有执行器"""

    # 注册JDBC增强版执行器（带连接池）
    jdbc_executor_enhanced = JDBCExecutorEnhanced()
    executor_manager.register_executor('jdbc_executor_enhanced', jdbc_executor_enhanced)

    # 注册Bash执行器
    bash_executor = BashExecutor()
    executor_manager.register_executor('bash_executor', bash_executor)

    # 注册Python执行器
    python_executor = PythonExecutor()
    executor_manager.register_executor('python_executor', python_executor)

    # 注册HTTP执行器
    http_executor = HttpExecutor()
    executor_manager.register_executor('http_executor', http_executor)

    # 注册API执行器
    api_executor = ApiExecutor()
    executor_manager.register_executor('api_executor', api_executor)

    # 注册数据同步执行器（对接smart-sync）
    sync_executor = SmartSyncExecutor()
    executor_manager.register_executor('smart_sync_executor', sync_executor)

    # TODO: 注册其他执行器
    # - spark_sql_executor: Spark SQL执行器
    # - spark_jar_executor: Spark Jar执行器
    # - flink_sql_executor: Flink SQL执行器
    # - flink_jar_executor: Flink Jar执行器
    # - excel_sync_executor: Excel导入执行器
    # - db_migrate_executor: 整库迁移执行器

    from loguru import logger
    logger.info(f"已注册 {len(executor_manager.list_executors())} 个执行器")
    logger.info(f"可用执行器: {list(executor_manager.list_executors().keys())}")


__all__ = [
    "JobExecutor",
    "ExecutorManager",
    "executor_manager",
    "init_executors",
    "JDBCExecutorEnhanced",
    "BashExecutor",
    "PythonExecutor",
    "HttpExecutor",
    "ApiExecutor",
    "SmartSyncExecutor"
]