"""
监控告警系统集成模块
负责将监控服务与现有的调度系统和数据同步系统集成
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from loguru import logger

from app.services.monitoring_service import monitoring_service
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.executor_service import ExecutorService
from app.services.smart_sync_service import SmartSyncService
from app.services.dag_generator_service import dag_generator_service


class MonitoringIntegration:
    """监控系统集成类"""

    def __init__(self):
        self.monitoring_service = monitoring_service
        self.executor_service: Optional['ExecutorService'] = None
        self.sync_service: Optional[SmartSyncService] = None
        self.initialized = False

    async def initialize(self, executor_service: 'ExecutorService', sync_service: 'SmartSyncService' = None):
        """初始化监控集成"""
        try:
            self.executor_service = executor_service
            self.sync_service = sync_service

            # 初始化监控服务
            await self.monitoring_service.initialize(executor_service)

            # 集成任务执行器的监控回调
            await self._integrate_executor_monitoring()

            # 集成数据同步的监控回调
            if sync_service:
                await self._integrate_sync_monitoring()

            # 集成DAG生成器的监控
            await self._integrate_dag_monitoring()

            self.initialized = True
            logger.info("监控系统集成初始化完成")

        except Exception as e:
            logger.error(f"监控系统集成初始化失败: {e}")
            raise

    async def _integrate_executor_monitoring(self):
        """集成任务执行器监控"""
        if not self.executor_service:
            return

        # 为执行器服务添加监控回调
        original_execute_task = self.executor_service.execute_task

        async def monitored_execute_task(task_type: str, task_config: Dict[str, Any], context=None, timeout=None):
            """带监控的任务执行"""
            start_time = datetime.now()
            task_id = task_config.get("task_id", "unknown")
            dag_id = task_config.get("dag_id", "manual")

            try:
                # 执行原始任务
                result = await original_execute_task(task_type, task_config, context, timeout)

                # 构造监控上下文
                monitoring_context = {
                    "task_id": task_id,
                    "dag_id": dag_id,
                    "task_type": task_type,
                    "execution_date": datetime.now().strftime("%Y-%m-%d"),
                    "start_time": start_time.isoformat()
                }

                # 发送监控检查
                await self.monitoring_service.check_task_execution(result, monitoring_context)

                return result

            except Exception as e:
                logger.error(f"监控集成执行任务失败: {e}")
                # 即使监控失败，也要返回原始结果
                return await original_execute_task(task_type, task_config, context, timeout)

        # 替换执行器的execute_task方法
        self.executor_service.execute_task = monitored_execute_task
        logger.info("任务执行器监控集成完成")

    async def _integrate_sync_monitoring(self):
        """集成数据同步监控"""
        if not self.sync_service:
            return

        # 为同步服务添加监控回调
        original_execute_sync = self.sync_service.datax_service.execute_sync_task

        async def monitored_execute_sync(task_config: Dict[str, Any]):
            """带监控的同步任务执行"""
            sync_task_id = task_config.get("id", "unknown")
            source_info = task_config.get("source", {})
            target_info = task_config.get("target", {})

            try:
                # 执行原始同步任务
                result = await original_execute_sync(task_config)

                # 发送同步状态回调
                await self._send_sync_callback(
                    sync_task_id=sync_task_id,
                    source_name=source_info.get("name", "unknown"),
                    target_name=target_info.get("name", "unknown"),
                    table_name=source_info.get("table", "unknown"),
                    status="success" if result.get("success") else "failed",
                    error_message=result.get("error"),
                    duration_seconds=result.get("duration_seconds")
                )

                return result

            except Exception as e:
                logger.error(f"监控集成同步任务失败: {e}")

                # 发送失败回调
                await self._send_sync_callback(
                    sync_task_id=sync_task_id,
                    source_name=source_info.get("name", "unknown"),
                    target_name=target_info.get("name", "unknown"),
                    table_name=source_info.get("table", "unknown"),
                    status="failed",
                    error_message=str(e)
                )

                return await original_execute_sync(task_config)

        # 替换同步服务的执行方法
        self.sync_service.datax_service.execute_sync_task = monitored_execute_sync
        logger.info("数据同步监控集成完成")

    async def _integrate_dag_monitoring(self):
        """集成DAG生成器监控"""
        original_generate_dag = dag_generator_service.generate_dag

        async def monitored_generate_dag(task_config: Dict[str, Any], schedule_config=None):
            """带监控的DAG生成"""
            dag_id = task_config.get("dag_id", "unknown")

            try:
                # 执行原始DAG生成
                result = await original_generate_dag(task_config, schedule_config)

                # 如果DAG生成失败，触发告警
                if not result.get("success"):
                    await self.monitoring_service._trigger_alert(
                        self.monitoring_service.AlertType.SYSTEM_ERROR,
                        self.monitoring_service.AlertLevel.MEDIUM,
                        f"DAG生成失败: {dag_id}",
                        f"DAG ID: {dag_id}\n错误信息: {result.get('error')}",
                        {
                            "dag_id": dag_id,
                            "error": result.get("error"),
                            "task_config": task_config
                        }
                    )

                return result

            except Exception as e:
                logger.error(f"监控集成DAG生成失败: {e}")
                return await original_generate_dag(task_config, schedule_config)

        # 替换DAG生成器的方法
        dag_generator_service.generate_dag = monitored_generate_dag
        logger.info("DAG生成器监控集成完成")

    async def _send_sync_callback(self, sync_task_id: str, source_name: str, target_name: str,
                                  table_name: str, status: str, error_message: str = None,
                                  rows_synced: int = None, data_size_mb: float = None,
                                  duration_seconds: float = None):
        """发送同步状态回调到监控系统"""
        try:
            from app.services.monitoring_service import AlertType, AlertLevel

            # 如果同步失败，触发告警
            if status == "failed":
                await self.monitoring_service._trigger_alert(
                    AlertType.SYNC_FAILURE,
                    AlertLevel.HIGH,
                    f"数据同步失败: {table_name}",
                    f"同步任务: {sync_task_id}\n数据源: {source_name} -> {target_name}\n表: {table_name}\n错误: {error_message}",
                    {
                        "sync_task_id": sync_task_id,
                        "source_name": source_name,
                        "target_name": target_name,
                        "table_name": table_name,
                        "error_message": error_message
                    }
                )

            # 记录同步性能指标
            if status == "success" and duration_seconds:
                from app.services.monitoring_service import PerformanceMetrics

                sync_metrics = PerformanceMetrics(
                    task_id=sync_task_id,
                    dag_id="data_sync",
                    execution_date=datetime.now().strftime("%Y-%m-%d"),
                    duration_seconds=duration_seconds,
                    cpu_usage_percent=0,
                    memory_usage_mb=0,
                    disk_io_mb=data_size_mb or 0,
                    network_io_mb=data_size_mb or 0,
                    success_rate=1.0,
                    retry_count=0
                )

                self.monitoring_service.performance_history.append(sync_metrics)

        except Exception as e:
            logger.error(f"发送同步回调失败: {e}")

    def get_integration_status(self) -> Dict[str, Any]:
        """获取集成状态"""
        return {
            "initialized": self.initialized,
            "executor_integrated": self.executor_service is not None,
            "sync_integrated": self.sync_service is not None,
            "monitoring_active": (
                    self.monitoring_service._monitoring_task is not None and
                    not self.monitoring_service._monitoring_task.done()
            ),
            "performance_monitoring_active": (
                    self.monitoring_service._performance_task is not None and
                    not self.monitoring_service._performance_task.done()
            ),
            "integration_time": datetime.now().isoformat()
        }


# 全局集成实例
monitoring_integration = MonitoringIntegration()


# ==================== 启动时集成函数 ====================

async def setup_monitoring_integration(executor_service=None):
    """设置监控集成"""
    try:
        if executor_service is None:
            raise ValueError("executor_service 参数不能为空")

        # 尝试导入智能同步服务
        try:
            from app.services.smart_sync_service import SmartSyncService
            sync_service = SmartSyncService()
        except ImportError:
            sync_service = None
            logger.warning("智能同步服务导入失败，跳过同步监控集成")

        # 初始化监控集成
        await monitoring_integration.initialize(executor_service, sync_service)

        logger.info("监控系统集成设置完成")
        return True

    except Exception as e:
        logger.error(f"设置监控集成失败: {e}")
        return False


# ==================== FastAPI应用事件处理器 ====================

async def monitoring_startup_event(executor_service=None):
    """应用启动时的监控初始化事件处理器"""
    try:
        logger.info("开始初始化监控告警系统...")

        if executor_service is None:
            raise ValueError("executor_service 参数不能为空")

        # 直接使用传入的 executor_service
        await monitoring_integration.initialize(executor_service)
        logger.info("监控告警系统初始化成功")

    except Exception as e:
        logger.error(f"监控启动事件处理失败: {e}")


async def monitoring_shutdown_event():
    """
    应用关闭时的监控清理事件处理器
    在main.py中的shutdown事件中调用
    """
    try:
        logger.info("开始关闭监控告警系统...")

        # 停止监控任务
        await monitoring_service.stop_monitoring()

        # 关闭Redis连接
        if monitoring_service.redis_client:
            await monitoring_service.redis_client.close()

        logger.info("✅ 监控告警系统已关闭")

    except Exception as e:
        logger.error(f"监控关闭事件处理失败: {e}")


# ==================== 装饰器函数 ====================

def monitor_task(alert_on_failure: bool = True, alert_on_timeout: bool = True):
    """
    任务监控装饰器
    可以用于装饰任何需要监控的函数
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = datetime.now()

            try:
                result = await func(*args, **kwargs)

                # 记录成功执行
                if monitoring_integration.initialized:
                    execution_time = (datetime.now() - start_time).total_seconds()
                    logger.info(f"监控任务 {func.__name__} 执行成功，耗时 {execution_time:.2f}秒")

                return result

            except Exception as e:
                # 记录失败并可能触发告警
                if monitoring_integration.initialized and alert_on_failure:
                    await monitoring_service._trigger_alert(
                        monitoring_service.AlertType.TASK_FAILURE,
                        monitoring_service.AlertLevel.MEDIUM,
                        f"函数执行失败: {func.__name__}",
                        f"函数: {func.__name__}\n错误: {str(e)}",
                        {
                            "function_name": func.__name__,
                            "error_message": str(e),
                            "execution_time": (datetime.now() - start_time).total_seconds()
                        }
                    )

                raise e

        return wrapper

    return decorator


# ==================== 手动监控触发函数 ====================

async def manual_health_check():
    """手动触发系统健康检查"""
    if not monitoring_integration.initialized:
        logger.warning("监控系统未初始化")
        return {"status": "not_initialized"}

    return await monitoring_service.check_system_health()


async def trigger_test_alert(level: str = "low", message: str = "测试告警"):
    """手动触发测试告警"""
    if not monitoring_integration.initialized:
        logger.warning("监控系统未初始化")
        return {"status": "not_initialized"}

    from app.services.monitoring_service import AlertType, AlertLevel

    alert_level = AlertLevel(level) if level in [l.value for l in AlertLevel] else AlertLevel.LOW

    await monitoring_service._trigger_alert(
        AlertType.SYSTEM_ERROR,
        alert_level,
        "手动测试告警",
        message,
        {"trigger_type": "manual", "trigger_time": datetime.now().isoformat()}
    )

    return {"status": "alert_triggered", "level": alert_level.value}


# ==================== 配置验证函数 ====================

def validate_monitoring_config() -> Dict[str, Any]:
    """验证监控配置是否正确"""
    validation_result = {
        "valid": True,
        "errors": [],
        "warnings": [],
        "config_status": {}
    }

    # 检查邮件配置
    email_config = monitoring_service.notification_config.get("email", {})
    if not email_config.get("smtp_server"):
        validation_result["warnings"].append("邮件SMTP服务器未配置")
    if not email_config.get("from_email"):
        validation_result["warnings"].append("发件人邮箱未配置")

    validation_result["config_status"]["email"] = bool(
        email_config.get("smtp_server") and email_config.get("from_email")
    )

    # 检查钉钉配置
    dingtalk_config = monitoring_service.notification_config.get("dingtalk", {})
    validation_result["config_status"]["dingtalk"] = bool(dingtalk_config.get("webhook_url"))

    # 检查Webhook配置
    webhook_config = monitoring_service.notification_config.get("webhook", {})
    validation_result["config_status"]["webhook"] = bool(webhook_config.get("default_url"))

    # 检查是否有至少一个可用的通知渠道
    available_channels = sum(validation_result["config_status"].values())
    if available_channels == 0:
        validation_result["valid"] = False
        validation_result["errors"].append("没有可用的通知渠道配置")

    validation_result["available_channels"] = available_channels
    validation_result["total_channels"] = len(validation_result["config_status"])

    return validation_result


# ==================== 监控数据导出函数 ====================

async def export_monitoring_data(
        hours: int = 24,
        include_alerts: bool = True,
        include_performance: bool = True,
        include_rules: bool = False
) -> Dict[str, Any]:
    """导出监控数据"""
    if not monitoring_integration.initialized:
        return {"error": "监控系统未初始化"}

    export_data = {
        "export_time": datetime.now().isoformat(),
        "time_range_hours": hours,
        "data": {}
    }

    try:
        # 导出告警数据
        if include_alerts:
            export_data["data"]["alerts"] = {
                "active_alerts": monitoring_service.get_active_alerts(),
                "alert_history": monitoring_service.get_alert_history(limit=1000)
            }

        # 导出性能数据
        if include_performance:
            export_data["data"]["performance"] = monitoring_service.get_performance_metrics(hours=hours)

        # 导出规则配置
        if include_rules:
            export_data["data"]["alert_rules"] = [
                monitoring_service._serialize_alert_rule(rule)
                for rule in monitoring_service.alert_rules.values()
            ]

        # 导出统计信息
        export_data["data"]["stats"] = monitoring_service.get_monitoring_stats()

        return export_data

    except Exception as e:
        logger.error(f"导出监控数据失败: {e}")
        return {"error": str(e)}