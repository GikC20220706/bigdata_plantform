"""
任务执行器模块
提供SQL、Shell、DataX等任务类型的执行器实现
"""

from .base_executor import (
    BaseExecutor,
    ExecutionContext,
    ExecutionResult,
    ExecutionStatus,
    executor_registry
)

from .sql_executor import sql_executor
from .shell_executor import shell_executor
from .datax_executor import datax_executor

# 导入执行器服务
from app.services.executor_service import executor_service

__all__ = [
    "BaseExecutor",
    "ExecutionContext",
    "ExecutionResult",
    "ExecutionStatus",
    "executor_registry",
    "sql_executor",
    "shell_executor",
    "datax_executor",
    "executor_service"
]


def get_registered_executors():
    """获取所有已注册的执行器"""
    return executor_registry.list_executors()


def get_executor_by_type(task_type: str):
    """根据任务类型获取执行器"""
    return executor_registry.get_executor(task_type)


# 在模块初始化时确保所有执行器都已注册
def _ensure_executors_registered():
    """确保所有执行器都已正确注册"""
    registered = executor_registry.list_executors()

    expected_executors = {
        "sql": "SQLExecutor",
        "shell": "ShellExecutor",
        "datax": "DataXExecutor"
    }

    for task_type, expected_name in expected_executors.items():
        if task_type in registered:
            actual_name = registered[task_type]
            if actual_name != expected_name:
                print(f"Warning: Expected executor {expected_name} for {task_type}, got {actual_name}")
        else:
            print(f"Warning: Executor for {task_type} not registered")

    print(f"Registered executors: {registered}")


# 执行注册检查
_ensure_executors_registered()