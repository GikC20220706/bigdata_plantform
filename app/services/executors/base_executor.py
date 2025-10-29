"""
作业执行器基类和管理器
定义统一的执行器接口，支持不同类型作业的执行
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import asyncio
from loguru import logger

from sqlalchemy.ext.asyncio import AsyncSession


class JobExecutor(ABC):
    """作业执行器抽象基类"""

    def __init__(self, executor_name: str):
        self.executor_name = executor_name

    @abstractmethod
    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        执行作业

        Args:
            db: 数据库会话
            work_config: 作业配置
            instance_id: 作业实例ID
            context: 执行上下文（可包含变量、参数等）

        Returns:
            执行结果字典
            {
                "success": bool,
                "message": str,
                "data": Any,
                "error": Optional[str]
            }
        """
        pass

    @abstractmethod
    async def validate_config(
            self,
            config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        验证作业配置

        Returns:
            {
                "valid": bool,
                "errors": List[str]
            }
        """
        pass

    async def on_start(self, instance_id: str):
        """作业开始前的钩子"""
        logger.info(f"[{self.executor_name}] 作业开始: {instance_id}")

    async def on_success(self, instance_id: str, result: Dict[str, Any]):
        """作业成功后的钩子"""
        logger.info(f"[{self.executor_name}] 作业成功: {instance_id}")

    async def on_failure(self, instance_id: str, error: Exception):
        """作业失败后的钩子"""
        logger.error(f"[{self.executor_name}] 作业失败: {instance_id}, 错误: {error}")

    async def on_complete(self, instance_id: str):
        """作业完成后的钩子（无论成功还是失败）"""
        logger.info(f"[{self.executor_name}] 作业完成: {instance_id}")


class ExecutorManager:
    """执行器管理器 - 注册和获取执行器"""

    def __init__(self):
        self._executors: Dict[str, JobExecutor] = {}

    def register_executor(self, executor_type: str, executor: JobExecutor):
        """注册执行器"""
        self._executors[executor_type] = executor
        logger.info(f"注册执行器: {executor_type} -> {executor.executor_name}")

    def get_executor(self, executor_type: str) -> Optional[JobExecutor]:
        """获取执行器"""
        return self._executors.get(executor_type)

    def list_executors(self) -> Dict[str, str]:
        """列出所有已注册的执行器"""
        return {
            executor_type: executor.executor_name
            for executor_type, executor in self._executors.items()
        }

    async def execute_work(
            self,
            db: AsyncSession,
            executor_type: str,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        通过管理器执行作业
        """
        executor = self.get_executor(executor_type)
        if not executor:
            raise ValueError(f"找不到执行器: {executor_type}")

        try:
            # 执行前钩子
            await executor.on_start(instance_id)

            # 执行作业
            result = await executor.execute(db, work_config, instance_id, context)

            # 执行后钩子
            if result.get("success"):
                await executor.on_success(instance_id, result)
            else:
                await executor.on_failure(instance_id, Exception(result.get("error", "未知错误")))

            await executor.on_complete(instance_id)

            return result

        except Exception as e:
            await executor.on_failure(instance_id, e)
            await executor.on_complete(instance_id)
            raise


# 创建全局执行器管理器
executor_manager = ExecutorManager()