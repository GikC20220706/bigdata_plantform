"""
执行器管理服务
统一管理所有任务执行器，提供任务执行的统一接口
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from app.executors.base_executor import (
    executor_registry, ExecutionContext, ExecutionResult, ExecutionStatus
)


class ExecutorService:
    """执行器管理服务"""

    def __init__(self):
        self.registry = executor_registry
        self.execution_history: Dict[str, ExecutionResult] = {}
        self.active_executions: Dict[str, asyncio.Task] = {}

        # 服务配置
        self.max_concurrent_tasks = 50
        self.history_retention_days = 30
        self.default_timeout = 3600

        logger.info("执行器管理服务初始化完成")

    async def execute_task(
            self,
            task_type: str,
            task_config: Dict[str, Any],
            context: Optional[ExecutionContext] = None,
            timeout: Optional[int] = None
    ) -> ExecutionResult:
        """
        执行任务

        Args:
            task_type: 任务类型
            task_config: 任务配置
            context: 执行上下文
            timeout: 超时时间(秒)

        Returns:
            ExecutionResult: 执行结果
        """
        # 获取执行器
        executor = self.registry.get_executor(task_type)
        if not executor:
            logger.error(f"不支持的任务类型: {task_type}")
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message=f"不支持的任务类型: {task_type}"
            )

        # 创建默认执行上下文
        if context is None:
            context = ExecutionContext(
                task_id=task_config.get("task_id", "unknown"),
                dag_id=task_config.get("dag_id", "manual"),
                execution_date=datetime.now().strftime("%Y-%m-%d"),
                timeout=timeout or self.default_timeout
            )

        # 验证任务配置
        if not executor.validate_config(task_config):
            logger.error(f"任务配置验证失败: {task_config.get('task_id', 'unknown')}")
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message="任务配置验证失败"
            )

        # 检查并发限制
        if len(self.active_executions) >= self.max_concurrent_tasks:
            logger.warning("达到最大并发任务限制，任务被拒绝")
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message="达到最大并发任务限制"
            )

        # 执行任务
        execution_key = f"{context.dag_id}.{context.task_id}.{context.execution_date}.{context.retry_number}"

        try:
            logger.info(f"开始执行任务: {execution_key}")

            # 创建执行任务
            exec_task = asyncio.create_task(
                executor.execute_with_timeout(task_config, context)
            )
            self.active_executions[execution_key] = exec_task

            # 等待执行完成
            result = await exec_task

            # 记录执行历史
            self.execution_history[execution_key] = result

            logger.info(f"任务执行完成: {execution_key}, 状态: {result.status.value}")
            return result

        except asyncio.CancelledError:
            logger.warning(f"任务被取消: {execution_key}")
            return ExecutionResult(
                status=ExecutionStatus.CANCELLED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message="任务被取消"
            )
        except Exception as e:
            logger.error(f"任务执行异常: {execution_key}, 错误: {e}")
            return ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=datetime.now(),
                end_time=datetime.now(),
                error_message=str(e),
                exception_details={"type": type(e).__name__, "message": str(e)}
            )
        finally:
            # 清理活跃执行记录
            await self.active_executions.pop(execution_key, None)

    async def cancel_task(self, execution_key: str) -> bool:
        """
        取消正在执行的任务

        Args:
            execution_key: 执行键

        Returns:
            bool: 取消是否成功
        """
        try:
            if execution_key in self.active_executions:
                task = self.active_executions[execution_key]
                task.cancel()

                # 等待任务取消完成
                try:
                    await asyncio.wait_for(task, timeout=10.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

                await self.active_executions.pop(execution_key, None)
                logger.info(f"任务已取消: {execution_key}")
                return True
            else:
                logger.warning(f"未找到活跃的执行任务: {execution_key}")
                return False

        except Exception as e:
            logger.error(f"取消任务失败: {execution_key}, 错误: {e}")
            return False

    async def batch_execute_tasks(
            self,
            task_definitions: List[Dict[str, Any]],
            max_concurrent: int = 5
    ) -> List[ExecutionResult]:
        """
        批量执行任务

        Args:
            task_definitions: 任务定义列表
            max_concurrent: 最大并发数

        Returns:
            List[ExecutionResult]: 执行结果列表
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def execute_single_task(task_def):
            async with semaphore:
                return await self.execute_task(
                    task_type=task_def["task_type"],
                    task_config=task_def["task_config"],
                    context=task_def.get("context"),
                    timeout=task_def.get("timeout")
                )

        # 并发执行所有任务
        tasks = [execute_single_task(task_def) for task_def in task_definitions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 处理异常结果
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append(ExecutionResult(
                    status=ExecutionStatus.FAILED,
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    error_message=str(result)
                ))
            else:
                final_results.append(result)

        return final_results

    async def dry_run_task(
            self,
            task_type: str,
            task_config: Dict[str, Any],
            context: Optional[ExecutionContext] = None
    ) -> Dict[str, Any]:
        """
        任务试运行（验证模式）

        Args:
            task_type: 任务类型
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 试运行结果
        """
        try:
            # 获取执行器
            executor = self.registry.get_executor(task_type)
            if not executor:
                return {
                    "success": False,
                    "error": f"不支持的任务类型: {task_type}",
                    "supported_types": list(self.registry.list_executors().keys())
                }

            # 创建默认执行上下文
            if context is None:
                context = ExecutionContext(
                    task_id=task_config.get("task_id", "dry_run"),
                    dag_id=task_config.get("dag_id", "validation"),
                    execution_date=datetime.now().strftime("%Y-%m-%d")
                )

            # 执行试运行
            if hasattr(executor, 'dry_run'):
                result = await executor.dry_run(task_config, context)
            else:
                # 如果执行器没有实现干运行，只做配置验证
                config_valid = executor.validate_config(task_config)
                result = {
                    "success": config_valid,
                    "config_validation": config_valid,
                    "message": "配置验证完成" if config_valid else "配置验证失败"
                }

            result.update({
                "task_type": task_type,
                "executor_name": executor.executor_name,
                "dry_run_time": datetime.now().isoformat()
            })

            return result

        except Exception as e:
            logger.error(f"试运行失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "task_type": task_type,
                "dry_run_time": datetime.now().isoformat()
            }

    def get_execution_status(self, execution_key: str) -> Optional[Dict[str, Any]]:
        """
        获取任务执行状态

        Args:
            execution_key: 执行键

        Returns:
            Dict[str, Any]: 执行状态信息
        """
        # 检查是否在活跃执行中
        if execution_key in self.active_executions:
            task = self.active_executions[execution_key]
            return {
                "status": "running",
                "is_active": True,
                "is_done": task.done(),
                "start_time": datetime.now().isoformat()  # 这里应该记录真实开始时间
            }

        # 检查历史记录
        if execution_key in self.execution_history:
            result = self.execution_history[execution_key]
            return {
                "status": result.status.value,
                "is_active": False,
                "is_done": True,
                "start_time": result.start_time.isoformat(),
                "end_time": result.end_time.isoformat() if result.end_time else None,
                "duration_seconds": result.duration_seconds,
                "exit_code": result.exit_code,
                "error_message": result.error_message
            }

        return None

    def list_active_executions(self) -> List[Dict[str, Any]]:
        """
        列出所有活跃的执行任务

        Returns:
            List[Dict[str, Any]]: 活跃任务列表
        """
        active_tasks = []

        for execution_key, task in self.active_executions.items():
            parts = execution_key.split(".")
            active_tasks.append({
                "execution_key": execution_key,
                "dag_id": parts[0] if len(parts) > 0 else "unknown",
                "task_id": parts[1] if len(parts) > 1 else "unknown",
                "execution_date": parts[2] if len(parts) > 2 else "unknown",
                "retry_number": parts[3] if len(parts) > 3 else "0",
                "is_done": task.done(),
                "is_cancelled": task.cancelled() if task.done() else False
            })

        return active_tasks

    def get_execution_history(
            self,
            limit: int = 100,
            status_filter: Optional[ExecutionStatus] = None,
            start_date: Optional[datetime] = None,
            end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        获取执行历史记录

        Args:
            limit: 返回记录数限制
            status_filter: 状态过滤
            start_date: 开始时间过滤
            end_date: 结束时间过滤

        Returns:
            List[Dict[str, Any]]: 历史记录列表
        """
        history_list = []

        for execution_key, result in self.execution_history.items():
            # 应用过滤条件
            if status_filter and result.status != status_filter:
                continue

            if start_date and result.start_time < start_date:
                continue

            if end_date and result.start_time > end_date:
                continue

            parts = execution_key.split(".")
            history_list.append({
                "execution_key": execution_key,
                "dag_id": parts[0] if len(parts) > 0 else "unknown",
                "task_id": parts[1] if len(parts) > 1 else "unknown",
                "execution_date": parts[2] if len(parts) > 2 else "unknown",
                "retry_number": parts[3] if len(parts) > 3 else "0",
                "status": result.status.value,
                "start_time": result.start_time.isoformat(),
                "end_time": result.end_time.isoformat() if result.end_time else None,
                "duration_seconds": result.duration_seconds,
                "exit_code": result.exit_code,
                "error_message": result.error_message
            })

        # 按开始时间倒序排序
        history_list.sort(key=lambda x: x["start_time"], reverse=True)

        return history_list[:limit]

    async def cleanup_old_history(self):
        """清理过期的执行历史记录"""
        cutoff_date = datetime.now() - timedelta(days=self.history_retention_days)

        keys_to_remove = []
        for execution_key, result in self.execution_history.items():
            if result.start_time < cutoff_date:
                keys_to_remove.append(execution_key)

        for key in keys_to_remove:
            del self.execution_history[key]

        if keys_to_remove:
            logger.info(f"清理了 {len(keys_to_remove)} 条过期执行历史记录")

    async def health_check(self) -> Dict[str, Any]:
        """
        执行器服务健康检查

        Returns:
            Dict[str, Any]: 健康状态信息
        """
        try:
            # 检查所有注册的执行器
            executor_health = await self.registry.health_check_all()

            # 服务统计信息
            stats = {
                "service_healthy": True,
                "active_executions": len(self.active_executions),
                "history_records": len(self.execution_history),
                "registered_executors": len(self.registry.list_executors()),
                "max_concurrent_tasks": self.max_concurrent_tasks,
                "last_check": datetime.now().isoformat()
            }

            # 检查是否有不健康的执行器
            unhealthy_executors = []
            for task_type, health_info in executor_health.items():
                if not health_info.get("healthy", False):
                    unhealthy_executors.append({
                        "task_type": task_type,
                        "error": health_info.get("error", "未知错误")
                    })

            if unhealthy_executors:
                stats["service_healthy"] = False
                stats["unhealthy_executors"] = unhealthy_executors

            return {
                "executor_health": executor_health,
                "service_stats": stats
            }

        except Exception as e:
            return {
                "service_healthy": False,
                "error": str(e),
                "last_check": datetime.now().isoformat()
            }

    def get_supported_task_types(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有支持的任务类型信息

        Returns:
            Dict[str, Dict[str, Any]]: 任务类型信息
        """
        supported_types = {}

        for task_type, executor_name in self.registry.list_executors().items():
            executor = self.registry.get_executor(task_type)
            if executor:
                supported_types[task_type] = {
                    "executor_name": executor_name,
                    "supported_sub_types": executor.get_supported_task_types(),
                    "executor_info": executor.get_executor_info()
                }

        return supported_types

    def get_service_stats(self) -> Dict[str, Any]:
        """
        获取服务统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        # 按状态统计历史记录
        status_counts = {}
        for result in self.execution_history.values():
            status = result.status.value
            status_counts[status] = status_counts.get(status, 0) + 1

        # 计算成功率
        total_executions = len(self.execution_history)
        success_count = status_counts.get("success", 0)
        success_rate = (success_count / total_executions * 100) if total_executions > 0 else 0

        return {
            "total_executions": total_executions,
            "active_executions": len(self.active_executions),
            "success_rate_percent": round(success_rate, 2),
            "status_distribution": status_counts,
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "history_retention_days": self.history_retention_days,
            "supported_task_types": list(self.registry.list_executors().keys())
        }


# 创建全局执行器服务实例
executor_service = ExecutorService()