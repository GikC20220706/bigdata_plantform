"""
基础执行器抽象类
定义所有任务执行器的通用接口和行为
"""

import os
import asyncio
import signal
import psutil
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Callable
from enum import Enum
from dataclasses import dataclass, field
from loguru import logger
from pathlib import Path
import json
import tempfile


class ExecutionStatus(Enum):
    """执行状态枚举"""
    PENDING = "pending"  # 等待执行
    RUNNING = "running"  # 正在执行
    SUCCESS = "success"  # 执行成功
    FAILED = "failed"  # 执行失败
    TIMEOUT = "timeout"  # 执行超时
    CANCELLED = "cancelled"  # 执行取消
    RETRY = "retry"  # 重试中


@dataclass
class ExecutionContext:
    """执行上下文"""
    task_id: str
    dag_id: str
    execution_date: str
    task_instance_id: Optional[str] = None
    retry_number: int = 0
    max_retries: int = 3
    timeout: int = 3600  # 默认1小时超时

    # 执行环境
    working_directory: Optional[str] = None
    environment_variables: Dict[str, str] = field(default_factory=dict)

    # 资源限制
    max_memory_mb: Optional[int] = None
    max_cpu_cores: Optional[float] = None

    # 回调函数
    on_success: Optional[Callable] = None
    on_failure: Optional[Callable] = None
    on_retry: Optional[Callable] = None

    # 日志配置
    log_level: str = "INFO"
    log_file: Optional[str] = None
    capture_output: bool = True


@dataclass
class ExecutionResult:
    """执行结果"""
    status: ExecutionStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None

    # 输出信息
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    logs: List[str] = field(default_factory=list)

    # 执行统计
    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    files_created: List[str] = field(default_factory=list)

    # 错误信息
    error_message: Optional[str] = None
    exception_details: Optional[Dict[str, Any]] = None

    # 任务特定结果
    task_result: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "logs": self.logs,
            "cpu_usage_percent": self.cpu_usage_percent,
            "memory_usage_mb": self.memory_usage_mb,
            "files_created": self.files_created,
            "error_message": self.error_message,
            "exception_details": self.exception_details,
            "task_result": self.task_result
        }


class BaseExecutor(ABC):
    """基础执行器抽象类"""

    def __init__(self, executor_name: str):
        self.executor_name = executor_name
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.process_registry: Dict[str, psutil.Process] = {}

        # 执行器配置
        self.default_timeout = 3600
        self.default_retries = 3
        self.cleanup_temp_files = True

        logger.info(f"初始化执行器: {executor_name}")

    @abstractmethod
    async def execute_task(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        执行任务的抽象方法

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        pass

    @abstractmethod
    def validate_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证任务配置的抽象方法

        Args:
            task_config: 任务配置

        Returns:
            bool: 配置是否有效
        """
        pass

    async def execute_with_retry(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        带重试机制的任务执行

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        last_result = None

        for attempt in range(context.max_retries + 1):
            context.retry_number = attempt

            try:
                logger.info(f"执行任务 {context.task_id}, 尝试 {attempt + 1}/{context.max_retries + 1}")

                # 执行任务
                result = await self.execute_task(task_config, context)

                # 如果成功，直接返回
                if result.status == ExecutionStatus.SUCCESS:
                    logger.info(f"任务 {context.task_id} 执行成功")
                    if context.on_success:
                        await self._safe_callback(context.on_success, result)
                    return result

                last_result = result

                # 如果是最后一次尝试，不再重试
                if attempt == context.max_retries:
                    break

                # 重试回调
                if context.on_retry:
                    await self._safe_callback(context.on_retry, result)

                # 重试延迟
                retry_delay = min(60 * (2 ** attempt), 300)  # 指数退避，最大5分钟
                logger.warning(f"任务 {context.task_id} 第 {attempt + 1} 次执行失败，{retry_delay}秒后重试")
                await asyncio.sleep(retry_delay)

            except Exception as e:
                logger.error(f"任务 {context.task_id} 执行异常: {e}")
                last_result = ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    error_message=str(e),
                    exception_details={"type": type(e).__name__, "message": str(e)}
                )

                if attempt == context.max_retries:
                    break

        # 所有重试都失败了
        logger.error(f"任务 {context.task_id} 经过 {context.max_retries + 1} 次尝试后最终失败")
        if context.on_failure:
            await self._safe_callback(context.on_failure, last_result)

        return last_result or ExecutionResult(
            status=ExecutionStatus.FAILED,
            start_time=datetime.now(),
            end_time=datetime.now(),
            error_message="未知执行错误"
        )

    async def execute_with_timeout(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        带超时机制的任务执行

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        task_key = f"{context.dag_id}.{context.task_id}.{context.execution_date}"

        try:
            # 创建执行任务
            exec_task = asyncio.create_task(
                self.execute_with_retry(task_config, context)
            )
            self.running_tasks[task_key] = exec_task

            # 等待执行完成或超时
            result = await asyncio.wait_for(
                exec_task,
                timeout=context.timeout
            )

            return result

        except asyncio.TimeoutError:
            logger.warning(f"任务 {context.task_id} 执行超时 ({context.timeout}秒)")

            # 取消任务
            await self.cancel_task(task_key)

            return ExecutionResult(
                status=ExecutionStatus.TIMEOUT,
                start_time=datetime.now() - timedelta(seconds=context.timeout),
                end_time=datetime.now(),
                duration_seconds=context.timeout,
                error_message=f"任务超时 ({context.timeout}秒)"
            )

        finally:
            # 清理运行记录
            self.running_tasks.pop(task_key, None)

    async def cancel_task(self, task_key: str) -> bool:
        """
        取消正在执行的任务

        Args:
            task_key: 任务键

        Returns:
            bool: 取消是否成功
        """
        try:
            # 取消异步任务
            if task_key in self.running_tasks:
                task = self.running_tasks[task_key]
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # 终止进程
            if task_key in self.process_registry:
                process = self.process_registry[task_key]
                if process.is_running():
                    process.terminate()

                    # 等待进程终止
                    try:
                        process.wait(timeout=10)
                    except psutil.TimeoutExpired:
                        process.kill()

                self.process_registry.pop(task_key, None)

            logger.info(f"任务 {task_key} 已取消")
            return True

        except Exception as e:
            logger.error(f"取消任务 {task_key} 失败: {e}")
            return False

    async def get_task_status(self, task_key: str) -> Optional[ExecutionStatus]:
        """
        获取任务执行状态

        Args:
            task_key: 任务键

        Returns:
            ExecutionStatus: 任务状态
        """
        if task_key in self.running_tasks:
            task = self.running_tasks[task_key]
            if task.done():
                try:
                    result = await task
                    return result.status
                except:
                    return ExecutionStatus.FAILED
            else:
                return ExecutionStatus.RUNNING

        return None

    def create_temp_file(self, content: str, suffix: str = ".tmp") -> str:
        """
        创建临时文件

        Args:
            content: 文件内容
            suffix: 文件后缀

        Returns:
            str: 临时文件路径
        """
        with tempfile.NamedTemporaryFile(
                mode='w',
                suffix=suffix,
                delete=False,
                encoding='utf-8'
        ) as f:
            f.write(content)
            return f.name

    def create_temp_script(self, script_content: str, executable: bool = True) -> str:
        """
        创建临时脚本文件

        Args:
            script_content: 脚本内容
            executable: 是否设置为可执行

        Returns:
            str: 脚本文件路径
        """
        script_path = self.create_temp_file(script_content, suffix=".sh")

        if executable:
            os.chmod(script_path, 0o755)

        return script_path

    def cleanup_temp_file(self, file_path: str):
        """
        清理临时文件

        Args:
            file_path: 文件路径
        """
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.debug(f"已清理临时文件: {file_path}")
        except Exception as e:
            logger.warning(f"清理临时文件失败 {file_path}: {e}")

    async def _safe_callback(self, callback: Callable, result: ExecutionResult):
        """
        安全执行回调函数

        Args:
            callback: 回调函数
            result: 执行结果
        """
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(result)
            else:
                callback(result)
        except Exception as e:
            logger.error(f"回调函数执行失败: {e}")

    def _setup_logging(self, context: ExecutionContext) -> Optional[str]:
        """
        设置日志记录

        Args:
            context: 执行上下文

        Returns:
            str: 日志文件路径
        """
        if context.log_file:
            # 确保日志目录存在
            log_dir = os.path.dirname(context.log_file)
            os.makedirs(log_dir, exist_ok=True)

            return context.log_file

        return None

    def _monitor_resource_usage(self, process: psutil.Process) -> Dict[str, float]:
        """
        监控进程资源使用情况

        Args:
            process: 进程对象

        Returns:
            Dict[str, float]: 资源使用情况
        """
        try:
            memory_info = process.memory_info()
            cpu_percent = process.cpu_percent()

            return {
                "memory_mb": memory_info.rss / 1024 / 1024,  # 转换为MB
                "cpu_percent": cpu_percent
            }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return {"memory_mb": 0, "cpu_percent": 0}

    def get_executor_info(self) -> Dict[str, Any]:
        """
        获取执行器信息

        Returns:
            Dict[str, Any]: 执行器信息
        """
        return {
            "executor_name": self.executor_name,
            "running_tasks_count": len(self.running_tasks),
            "registered_processes_count": len(self.process_registry),
            "default_timeout": self.default_timeout,
            "default_retries": self.default_retries,
            "supported_task_types": self.get_supported_task_types()
        }

    @abstractmethod
    def get_supported_task_types(self) -> List[str]:
        """
        获取支持的任务类型列表

        Returns:
            List[str]: 支持的任务类型
        """
        pass

    async def health_check(self) -> Dict[str, Any]:
        """
        执行器健康检查

        Returns:
            Dict[str, Any]: 健康状态信息
        """
        try:
            # 基础健康检查
            health_status = {
                "healthy": True,
                "executor_name": self.executor_name,
                "running_tasks": len(self.running_tasks),
                "last_check": datetime.now().isoformat()
            }

            # 检查运行中的任务状态
            unhealthy_tasks = []
            for task_key, task in self.running_tasks.items():
                if task.done():
                    try:
                        result = await task
                        if result.status == ExecutionStatus.FAILED:
                            unhealthy_tasks.append(task_key)
                    except:
                        unhealthy_tasks.append(task_key)

            if unhealthy_tasks:
                health_status["unhealthy_tasks"] = unhealthy_tasks
                health_status["healthy"] = False

            return health_status

        except Exception as e:
            return {
                "healthy": False,
                "executor_name": self.executor_name,
                "error": str(e),
                "last_check": datetime.now().isoformat()
            }


class ExecutorRegistry:
    """执行器注册表"""

    def __init__(self):
        self._executors: Dict[str, BaseExecutor] = {}

    def register(self, task_type: str, executor: BaseExecutor):
        """
        注册执行器

        Args:
            task_type: 任务类型
            executor: 执行器实例
        """
        self._executors[task_type] = executor
        logger.info(f"注册执行器: {task_type} -> {executor.executor_name}")

    def get_executor(self, task_type: str) -> Optional[BaseExecutor]:
        """
        获取执行器

        Args:
            task_type: 任务类型

        Returns:
            BaseExecutor: 执行器实例
        """
        return self._executors.get(task_type)

    def list_executors(self) -> Dict[str, str]:
        """
        列出所有注册的执行器

        Returns:
            Dict[str, str]: 任务类型到执行器名称的映射
        """
        return {
            task_type: executor.executor_name
            for task_type, executor in self._executors.items()
        }

    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """
        检查所有执行器的健康状态

        Returns:
            Dict[str, Dict[str, Any]]: 所有执行器的健康状态
        """
        health_results = {}

        for task_type, executor in self._executors.items():
            try:
                health_results[task_type] = await executor.health_check()
            except Exception as e:
                health_results[task_type] = {
                    "healthy": False,
                    "error": str(e),
                    "executor_name": executor.executor_name
                }

        return health_results


# 创建全局执行器注册表
executor_registry = ExecutorRegistry()