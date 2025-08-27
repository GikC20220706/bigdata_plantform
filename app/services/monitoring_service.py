"""
监控告警服务
提供任务监控、性能监控、异常检测和告警通知功能
"""

import asyncio
import json
import smtplib
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Set
from enum import Enum
from dataclasses import dataclass, field
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from pathlib import Path
import aiohttp
import aioredis
import psutil
from loguru import logger

from app.executors.base_executor import ExecutionStatus, ExecutionResult
from app.services.executor_service import ExecutorService
from app.utils.airflow_client import airflow_client
from config.settings import settings


class AlertLevel(Enum):
    """告警级别"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    """告警类型"""
    TASK_FAILURE = "task_failure"
    TASK_TIMEOUT = "task_timeout"
    TASK_RETRY_EXHAUSTED = "task_retry_exhausted"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    SYSTEM_ERROR = "system_error"
    DATA_QUALITY_ISSUE = "data_quality_issue"
    SYNC_FAILURE = "sync_failure"


class NotificationChannel(Enum):
    """通知渠道"""
    EMAIL = "email"
    WEBHOOK = "webhook"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"
    SMS = "sms"


@dataclass
class AlertRule:
    """告警规则"""
    id: str
    name: str
    description: str
    alert_type: AlertType
    level: AlertLevel
    condition: str  # 条件表达式
    threshold: Optional[float] = None
    duration_minutes: int = 0  # 持续时间触发
    enabled: bool = True
    channels: List[NotificationChannel] = field(default_factory=list)
    recipients: List[str] = field(default_factory=list)
    silence_minutes: int = 60  # 静默时间，避免重复告警
    custom_message: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """告警实例"""
    id: str
    rule_id: str
    alert_type: AlertType
    level: AlertLevel
    title: str
    message: str
    source: str  # 告警源
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    context: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """性能指标"""
    task_id: str
    dag_id: str
    execution_date: str
    duration_seconds: float
    cpu_usage_percent: float
    memory_usage_mb: float
    disk_io_mb: float
    network_io_mb: float
    success_rate: float
    retry_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class MonitoringService:
    """监控告警服务"""

    def __init__(self):
        self.executor_service: ExecutorService = None
        self.redis_client: Optional[aioredis.Redis] = None

        # 告警规则和历史
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.silenced_alerts: Set[str] = set()

        # 性能监控
        self.performance_history: List[PerformanceMetrics] = []
        self.baseline_metrics: Dict[str, Dict[str, float]] = {}

        # 监控配置
        self.monitoring_interval = 30  # 监控间隔(秒)
        self.metric_retention_hours = 24 * 7  # 指标保留7天
        self.alert_history_retention_days = 30

        # 通知配置
        self.notification_config = {
            "email": {
                "smtp_server": getattr(settings, 'SMTP_SERVER', 'localhost'),
                "smtp_port": getattr(settings, 'SMTP_PORT', 587),
                "username": getattr(settings, 'SMTP_USERNAME', ''),
                "password": getattr(settings, 'SMTP_PASSWORD', ''),
                "from_email": getattr(settings, 'SMTP_FROM', 'bigdata-platform@company.com')
            },
            "webhook": {
                "default_url": getattr(settings, 'WEBHOOK_URL', ''),
                "timeout": 10
            },
            "dingtalk": {
                "webhook_url": getattr(settings, 'DINGTALK_WEBHOOK', ''),
                "secret": getattr(settings, 'DINGTALK_SECRET', '')
            }
        }

        # 启动监控任务
        self._monitoring_task = None
        self._performance_task = None

        logger.info("监控告警服务初始化完成")

    async def initialize(self, executor_service: ExecutorService):
        """初始化监控服务"""
        self.executor_service = executor_service

        # 初始化Redis连接
        try:
            self.redis_client = aioredis.from_url(
                getattr(settings, 'REDIS_URL', 'redis://localhost:6379'),
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("Redis连接成功")
        except Exception as e:
            logger.warning(f"Redis连接失败: {e}")

        # 加载默认告警规则
        await self._load_default_alert_rules()

        # 启动监控任务
        await self.start_monitoring()

    async def start_monitoring(self):
        """启动监控任务"""
        if self._monitoring_task is None or self._monitoring_task.done():
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            logger.info("监控任务已启动")

        if self._performance_task is None or self._performance_task.done():
            self._performance_task = asyncio.create_task(self._performance_monitoring_loop())
            logger.info("性能监控任务已启动")

    async def stop_monitoring(self):
        """停止监控任务"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            logger.info("监控任务已停止")

        if self._performance_task:
            self._performance_task.cancel()
            try:
                await self._performance_task
            except asyncio.CancelledError:
                pass
            logger.info("性能监控任务已停止")

    # ==================== 告警规则管理 ====================

    async def add_alert_rule(self, rule: AlertRule) -> Dict[str, Any]:
        """添加告警规则"""
        try:
            # 验证规则
            validation_result = self._validate_alert_rule(rule)
            if not validation_result["valid"]:
                return {
                    "success": False,
                    "error": validation_result["error"]
                }

            # 保存规则
            self.alert_rules[rule.id] = rule

            # 持久化到Redis
            if self.redis_client:
                await self.redis_client.hset(
                    "monitoring:alert_rules",
                    rule.id,
                    json.dumps(self._serialize_alert_rule(rule))
                )

            logger.info(f"告警规则已添加: {rule.name}")
            return {
                "success": True,
                "rule_id": rule.id,
                "message": f"告警规则 '{rule.name}' 添加成功"
            }

        except Exception as e:
            logger.error(f"添加告警规则失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def update_alert_rule(self, rule_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """更新告警规则"""
        try:
            if rule_id not in self.alert_rules:
                return {
                    "success": False,
                    "error": f"告警规则不存在: {rule_id}"
                }

            rule = self.alert_rules[rule_id]

            # 更新规则属性
            for key, value in updates.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)

            # 验证更新后的规则
            validation_result = self._validate_alert_rule(rule)
            if not validation_result["valid"]:
                return {
                    "success": False,
                    "error": validation_result["error"]
                }

            # 持久化更新
            if self.redis_client:
                await self.redis_client.hset(
                    "monitoring:alert_rules",
                    rule_id,
                    json.dumps(self._serialize_alert_rule(rule))
                )

            logger.info(f"告警规则已更新: {rule.name}")
            return {
                "success": True,
                "rule_id": rule_id,
                "message": f"告警规则 '{rule.name}' 更新成功"
            }

        except Exception as e:
            logger.error(f"更新告警规则失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def delete_alert_rule(self, rule_id: str) -> Dict[str, Any]:
        """删除告警规则"""
        try:
            if rule_id not in self.alert_rules:
                return {
                    "success": False,
                    "error": f"告警规则不存在: {rule_id}"
                }

            rule_name = self.alert_rules[rule_id].name
            del self.alert_rules[rule_id]

            # 从Redis删除
            if self.redis_client:
                await self.redis_client.hdel("monitoring:alert_rules", rule_id)

            logger.info(f"告警规则已删除: {rule_name}")
            return {
                "success": True,
                "message": f"告警规则 '{rule_name}' 删除成功"
            }

        except Exception as e:
            logger.error(f"删除告警规则失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    # ==================== 告警检测和触发 ====================

    async def check_task_execution(self, task_result: ExecutionResult, context: Dict[str, Any] = None):
        """检查任务执行结果并触发相应告警"""
        try:
            context = context or {}
            task_id = context.get("task_id", "unknown")
            dag_id = context.get("dag_id", "unknown")

            # 根据执行状态检查不同类型的告警
            if task_result.status == ExecutionStatus.FAILED:
                await self._check_task_failure_alert(task_result, context)
            elif task_result.status == ExecutionStatus.TIMEOUT:
                await self._check_task_timeout_alert(task_result, context)
            elif task_result.status == ExecutionStatus.SUCCESS:
                # 检查性能告警
                await self._check_performance_alert(task_result, context)

            # 记录性能指标
            await self._record_performance_metrics(task_result, context)

        except Exception as e:
            logger.error(f"检查任务执行告警失败: {e}")

    async def check_system_health(self) -> Dict[str, Any]:
        """系统健康检查"""
        try:
            health_status = {
                "overall_status": "healthy",
                "components": {},
                "alerts": [],
                "timestamp": datetime.now().isoformat()
            }

            # 检查系统资源
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            health_status["components"]["system"] = {
                "cpu_usage": cpu_percent,
                "memory_usage": memory.percent,
                "disk_usage": disk.percent,
                "status": "healthy"
            }

            # 检查资源告警
            if cpu_percent > 90:
                await self._trigger_alert(
                    AlertType.RESOURCE_EXHAUSTION,
                    AlertLevel.HIGH,
                    "CPU使用率过高",
                    f"CPU使用率: {cpu_percent}%",
                    {"cpu_usage": cpu_percent}
                )
                health_status["components"]["system"]["status"] = "warning"
                health_status["overall_status"] = "warning"

            if memory.percent > 90:
                await self._trigger_alert(
                    AlertType.RESOURCE_EXHAUSTION,
                    AlertLevel.HIGH,
                    "内存使用率过高",
                    f"内存使用率: {memory.percent}%",
                    {"memory_usage": memory.percent}
                )
                health_status["components"]["system"]["status"] = "warning"
                health_status["overall_status"] = "warning"

            # 检查Airflow连接
            try:
                airflow_health = await airflow_client.health_check()
                health_status["components"]["airflow"] = {
                    "status": "healthy" if airflow_health.get("status") == "healthy" else "unhealthy",
                    "version": airflow_health.get("version"),
                    "response_time": airflow_health.get("response_time")
                }

                if not airflow_health.get("status") == "healthy":
                    health_status["overall_status"] = "unhealthy"

            except Exception as e:
                health_status["components"]["airflow"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["overall_status"] = "unhealthy"

            # 检查执行器服务
            if self.executor_service:
                executor_stats = self.executor_service.get_service_stats()
                health_status["components"]["executor"] = {
                    "status": "healthy",
                    "active_tasks": executor_stats.get("active_executions", 0),
                    "success_rate": executor_stats.get("success_rate", 0)
                }

            # 检查Redis连接
            if self.redis_client:
                try:
                    await self.redis_client.ping()
                    health_status["components"]["redis"] = {"status": "healthy"}
                except Exception as e:
                    health_status["components"]["redis"] = {
                        "status": "unhealthy",
                        "error": str(e)
                    }
                    if health_status["overall_status"] == "healthy":
                        health_status["overall_status"] = "warning"

            return health_status

        except Exception as e:
            logger.error(f"系统健康检查失败: {e}")
            return {
                "overall_status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

    # ==================== 通知发送 ====================

    async def send_alert_notification(self, alert: Alert, channels: List[NotificationChannel] = None):
        """发送告警通知"""
        channels = channels or [NotificationChannel.EMAIL]

        for channel in channels:
            try:
                if channel == NotificationChannel.EMAIL:
                    await self._send_email_notification(alert)
                elif channel == NotificationChannel.WEBHOOK:
                    await self._send_webhook_notification(alert)
                elif channel == NotificationChannel.DINGTALK:
                    await self._send_dingtalk_notification(alert)
                else:
                    logger.warning(f"不支持的通知渠道: {channel}")

            except Exception as e:
                logger.error(f"发送{channel.value}通知失败: {e}")

    async def _send_email_notification(self, alert: Alert):
        """发送邮件通知"""
        try:
            config = self.notification_config["email"]

            # 创建邮件
            msg = MimeMultipart()
            msg['From'] = config["from_email"]
            msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"

            # 邮件内容
            body = self._format_email_body(alert)
            msg.attach(MimeText(body, 'html'))

            # 获取收件人
            recipients = self._get_alert_recipients(alert)
            if not recipients:
                logger.warning(f"告警 {alert.id} 没有配置收件人")
                return

            msg['To'] = ", ".join(recipients)

            # 发送邮件
            server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
            if config["username"]:
                server.starttls()
                server.login(config["username"], config["password"])

            server.send_message(msg)
            server.quit()

            logger.info(f"邮件通知已发送: {alert.title}")

        except Exception as e:
            logger.error(f"发送邮件通知失败: {e}")

    async def _send_webhook_notification(self, alert: Alert):
        """发送Webhook通知"""
        try:
            config = self.notification_config["webhook"]
            url = config.get("default_url")

            if not url:
                logger.warning("Webhook URL未配置")
                return

            payload = {
                "alert_id": alert.id,
                "alert_type": alert.alert_type.value,
                "level": alert.level.value,
                "title": alert.title,
                "message": alert.message,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "context": alert.context,
                "tags": alert.tags
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                        url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=config["timeout"])
                ) as response:
                    if response.status == 200:
                        logger.info(f"Webhook通知已发送: {alert.title}")
                    else:
                        logger.error(f"Webhook通知发送失败: HTTP {response.status}")

        except Exception as e:
            logger.error(f"发送Webhook通知失败: {e}")

    async def _send_dingtalk_notification(self, alert: Alert):
        """发送钉钉通知"""
        try:
            config = self.notification_config["dingtalk"]
            webhook_url = config.get("webhook_url")

            if not webhook_url:
                logger.warning("钉钉Webhook URL未配置")
                return

            # 构建钉钉消息
            color = self._get_alert_color(alert.level)
            message = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"告警通知 - {alert.title}",
                    "text": f"""## 🚨 告警通知

**告警标题:** {alert.title}

**告警级别:** <font color='{color}'>{alert.level.value.upper()}</font>

**告警类型:** {alert.alert_type.value}

**告警源:** {alert.source}

**告警时间:** {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

**告警详情:**
{alert.message}

---
*由大数据平台监控系统自动发送*
"""
                }
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=message) as response:
                    if response.status == 200:
                        logger.info(f"钉钉通知已发送: {alert.title}")
                    else:
                        logger.error(f"钉钉通知发送失败: HTTP {response.status}")

        except Exception as e:
            logger.error(f"发送钉钉通知失败: {e}")

    # ==================== 内部方法 ====================

    async def _monitoring_loop(self):
        """监控主循环"""
        while True:
            try:
                # 检查系统健康状态
                await self.check_system_health()

                # 检查活跃的执行任务
                if self.executor_service:
                    active_tasks = self.executor_service.list_active_executions()
                    for task_info in active_tasks:
                        await self._check_long_running_task(task_info)

                # 清理过期数据
                await self._cleanup_expired_data()

                # 等待下一次监控
                await asyncio.sleep(self.monitoring_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"监控循环异常: {e}")
                await asyncio.sleep(self.monitoring_interval)

    async def _performance_monitoring_loop(self):
        """性能监控循环"""
        while True:
            try:
                # 收集性能指标
                await self._collect_performance_metrics()

                # 分析性能趋势
                await self._analyze_performance_trends()

                # 等待下一次采集
                await asyncio.sleep(60)  # 每分钟采集一次

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"性能监控循环异常: {e}")
                await asyncio.sleep(60)

    async def _load_default_alert_rules(self):
        """加载默认告警规则"""
        default_rules = [
            AlertRule(
                id="task_failure",
                name="任务失败告警",
                description="任务执行失败时触发",
                alert_type=AlertType.TASK_FAILURE,
                level=AlertLevel.HIGH,
                condition="task_status == 'failed'",
                channels=[NotificationChannel.EMAIL, NotificationChannel.DINGTALK],
                silence_minutes=30
            ),
            AlertRule(
                id="task_timeout",
                name="任务超时告警",
                description="任务执行超时时触发",
                alert_type=AlertType.TASK_TIMEOUT,
                level=AlertLevel.MEDIUM,
                condition="task_status == 'timeout'",
                channels=[NotificationChannel.EMAIL],
                silence_minutes=60
            ),
            AlertRule(
                id="resource_high_usage",
                name="资源使用率高告警",
                description="系统资源使用率过高",
                alert_type=AlertType.RESOURCE_EXHAUSTION,
                level=AlertLevel.HIGH,
                condition="cpu_usage > 90 or memory_usage > 90",
                channels=[NotificationChannel.EMAIL, NotificationChannel.DINGTALK],
                silence_minutes=15
            ),
            AlertRule(
                id="sync_failure",
                name="数据同步失败告警",
                description="数据同步任务失败时触发",
                alert_type=AlertType.SYNC_FAILURE,
                level=AlertLevel.HIGH,
                condition="sync_status == 'failed'",
                channels=[NotificationChannel.EMAIL, NotificationChannel.DINGTALK],
                silence_minutes=30
            )
        ]

        for rule in default_rules:
            self.alert_rules[rule.id] = rule
            logger.info(f"默认告警规则已加载: {rule.name}")

    def _validate_alert_rule(self, rule: AlertRule) -> Dict[str, Any]:
        """验证告警规则"""
        try:
            # 基础字段验证
            if not rule.id or not rule.name:
                return {"valid": False, "error": "规则ID和名称不能为空"}

            # 条件表达式验证 - 简单的语法检查
            if rule.condition:
                # 这里可以添加更复杂的条件表达式验证逻辑
                forbidden_keywords = ['import', 'exec', 'eval', '__']
                for keyword in forbidden_keywords:
                    if keyword in rule.condition:
                        return {"valid": False, "error": f"条件表达式不能包含: {keyword}"}

            return {"valid": True}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    def _serialize_alert_rule(self, rule: AlertRule) -> Dict[str, Any]:
        """序列化告警规则"""
        return {
            "id": rule.id,
            "name": rule.name,
            "description": rule.description,
            "alert_type": rule.alert_type.value,
            "level": rule.level.value,
            "condition": rule.condition,
            "threshold": rule.threshold,
            "duration_minutes": rule.duration_minutes,
            "enabled": rule.enabled,
            "channels": [ch.value for ch in rule.channels],
            "recipients": rule.recipients,
            "silence_minutes": rule.silence_minutes,
            "custom_message": rule.custom_message,
            "tags": rule.tags
        }

    async def _trigger_alert(
            self,
            alert_type: AlertType,
            level: AlertLevel,
            title: str,
            message: str,
            context: Dict[str, Any] = None
    ):
        """触发告警"""
        try:
            alert_id = f"{alert_type.value}_{int(time.time())}"

            # 检查是否在静默期
            if alert_id in self.silenced_alerts:
                logger.debug(f"告警在静默期，跳过: {title}")
                return

            alert = Alert(
                id=alert_id,
                rule_id="",  # 系统触发的告警可以没有规则ID
                alert_type=alert_type,
                level=level,
                title=title,
                message=message,
                source="monitoring_service",
                timestamp=datetime.now(),
                context=context or {}
            )

            # 保存活跃告警
            self.active_alerts[alert_id] = alert

            # 添加到历史记录
            self.alert_history.append(alert)

            # 发送通知
            channels = [NotificationChannel.EMAIL]
            if level in [AlertLevel.HIGH, AlertLevel.CRITICAL]:
                channels.append(NotificationChannel.DINGTALK)

            await self.send_alert_notification(alert, channels)

            # 设置静默期
            self.silenced_alerts.add(alert_id)
            asyncio.create_task(self._remove_silence(alert_id, 60))  # 60分钟后解除静默

            logger.info(f"告警已触发: {title}")

        except Exception as e:
            logger.error(f"触发告警失败: {e}")

    async def _check_task_failure_alert(self, task_result: ExecutionResult, context: Dict[str, Any]):
        """检查任务失败告警"""
        task_id = context.get("task_id", "unknown")
        dag_id = context.get("dag_id", "unknown")

        await self._trigger_alert(
            AlertType.TASK_FAILURE,
            AlertLevel.HIGH,
            f"任务执行失败: {task_id}",
            f"DAG: {dag_id}\n任务: {task_id}\n错误信息: {task_result.error_message}",
            {
                "task_id": task_id,
                "dag_id": dag_id,
                "error_message": task_result.error_message,
                "duration": task_result.duration_seconds
            }
        )

    async def _check_task_timeout_alert(self, task_result: ExecutionResult, context: Dict[str, Any]):
        """检查任务超时告警"""
        task_id = context.get("task_id", "unknown")
        dag_id = context.get("dag_id", "unknown")

        await self._trigger_alert(
            AlertType.TASK_TIMEOUT,
            AlertLevel.MEDIUM,
            f"任务执行超时: {task_id}",
            f"DAG: {dag_id}\n任务: {task_id}\n超时时间: {task_result.duration_seconds}秒",
            {
                "task_id": task_id,
                "dag_id": dag_id,
                "duration": task_result.duration_seconds,
                "timeout_threshold": context.get("timeout", 3600)
            }
        )

    async def _check_performance_alert(self, task_result: ExecutionResult, context: Dict[str, Any]):
        """检查性能告警"""
        task_id = context.get("task_id", "unknown")
        dag_id = context.get("dag_id", "unknown")

        # 获取历史基线
        baseline_key = f"{dag_id}.{task_id}"
        baseline = self.baseline_metrics.get(baseline_key, {})

        if not baseline:
            # 首次执行，建立基线
            self.baseline_metrics[baseline_key] = {
                "avg_duration": task_result.duration_seconds or 0,
                "samples": 1
            }
            return

        # 检查性能下降
        current_duration = task_result.duration_seconds or 0
        baseline_duration = baseline.get("avg_duration", 0)

        if baseline_duration > 0 and current_duration > baseline_duration * 2:
            await self._trigger_alert(
                AlertType.PERFORMANCE_DEGRADATION,
                AlertLevel.MEDIUM,
                f"任务性能下降: {task_id}",
                f"DAG: {dag_id}\n任务: {task_id}\n当前执行时间: {current_duration:.2f}秒\n基线执行时间: {baseline_duration:.2f}秒\n性能下降: {((current_duration - baseline_duration) / baseline_duration * 100):.1f}%",
                {
                    "task_id": task_id,
                    "dag_id": dag_id,
                    "current_duration": current_duration,
                    "baseline_duration": baseline_duration,
                    "degradation_percent": (current_duration - baseline_duration) / baseline_duration * 100
                }
            )

        # 更新基线
        baseline["avg_duration"] = (baseline["avg_duration"] * baseline["samples"] + current_duration) / (
                    baseline["samples"] + 1)
        baseline["samples"] += 1

    async def _record_performance_metrics(self, task_result: ExecutionResult, context: Dict[str, Any]):
        """记录性能指标"""
        try:
            task_id = context.get("task_id", "unknown")
            dag_id = context.get("dag_id", "unknown")
            execution_date = context.get("execution_date", datetime.now().strftime("%Y-%m-%d"))

            metrics = PerformanceMetrics(
                task_id=task_id,
                dag_id=dag_id,
                execution_date=execution_date,
                duration_seconds=task_result.duration_seconds or 0,
                cpu_usage_percent=task_result.resource_usage.get("cpu_usage", 0) if task_result.resource_usage else 0,
                memory_usage_mb=task_result.resource_usage.get("memory_usage", 0) if task_result.resource_usage else 0,
                disk_io_mb=task_result.resource_usage.get("disk_io", 0) if task_result.resource_usage else 0,
                network_io_mb=task_result.resource_usage.get("network_io", 0) if task_result.resource_usage else 0,
                success_rate=1.0 if task_result.status == ExecutionStatus.SUCCESS else 0.0,
                retry_count=context.get("retry_number", 0)
            )

            self.performance_history.append(metrics)

            # 持久化到Redis
            if self.redis_client:
                metrics_data = {
                    "task_id": metrics.task_id,
                    "dag_id": metrics.dag_id,
                    "execution_date": metrics.execution_date,
                    "duration_seconds": metrics.duration_seconds,
                    "cpu_usage_percent": metrics.cpu_usage_percent,
                    "memory_usage_mb": metrics.memory_usage_mb,
                    "success_rate": metrics.success_rate,
                    "timestamp": metrics.timestamp.isoformat()
                }

                await self.redis_client.lpush(
                    f"monitoring:performance:{dag_id}:{task_id}",
                    json.dumps(metrics_data)
                )

                # 限制历史数据长度
                await self.redis_client.ltrim(
                    f"monitoring:performance:{dag_id}:{task_id}",
                    0, 1000
                )

            logger.debug(f"性能指标已记录: {task_id}")

        except Exception as e:
            logger.error(f"记录性能指标失败: {e}")

    async def _check_long_running_task(self, task_info: Dict[str, Any]):
        """检查长时间运行的任务"""
        try:
            execution_key = task_info.get("execution_key", "")
            start_time_str = task_info.get("start_time", "")

            if not start_time_str:
                return

            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            current_time = datetime.now()

            # 计算运行时间
            running_duration = (current_time - start_time).total_seconds()

            # 检查是否超过预警阈值（默认2小时）
            warning_threshold = 2 * 3600  # 2小时

            if running_duration > warning_threshold:
                parts = execution_key.split(".")
                dag_id = parts[0] if len(parts) > 0 else "unknown"
                task_id = parts[1] if len(parts) > 1 else "unknown"

                await self._trigger_alert(
                    AlertType.TASK_TIMEOUT,
                    AlertLevel.MEDIUM,
                    f"任务长时间运行: {task_id}",
                    f"DAG: {dag_id}\n任务: {task_id}\n已运行时间: {running_duration / 3600:.1f}小时\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
                    {
                        "task_id": task_id,
                        "dag_id": dag_id,
                        "running_duration": running_duration,
                        "start_time": start_time.isoformat()
                    }
                )

        except Exception as e:
            logger.error(f"检查长时间运行任务失败: {e}")

    async def _collect_performance_metrics(self):
        """收集系统性能指标"""
        try:
            # 系统资源指标
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # 网络IO
            network = psutil.net_io_counters()

            # 保存到Redis
            if self.redis_client:
                system_metrics = {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_gb": memory.available / (1024 ** 3),
                    "disk_percent": disk.percent,
                    "disk_free_gb": disk.free / (1024 ** 3),
                    "network_bytes_sent": network.bytes_sent,
                    "network_bytes_recv": network.bytes_recv,
                    "timestamp": datetime.now().isoformat()
                }

                await self.redis_client.lpush(
                    "monitoring:system_metrics",
                    json.dumps(system_metrics)
                )

                # 限制历史数据
                await self.redis_client.ltrim("monitoring:system_metrics", 0, 1440)  # 保留24小时数据（每分钟一条）

            logger.debug("系统性能指标已收集")

        except Exception as e:
            logger.error(f"收集系统性能指标失败: {e}")

    async def _analyze_performance_trends(self):
        """分析性能趋势"""
        try:
            # 获取最近的系统指标
            if not self.redis_client:
                return

            metrics_data = await self.redis_client.lrange("monitoring:system_metrics", 0, 59)  # 最近1小时

            if len(metrics_data) < 10:  # 数据不足
                return

            # 解析指标数据
            cpu_values = []
            memory_values = []

            for data in metrics_data:
                try:
                    metrics = json.loads(data)
                    cpu_values.append(metrics["cpu_percent"])
                    memory_values.append(metrics["memory_percent"])
                except:
                    continue

            if not cpu_values or not memory_values:
                return

            # 计算平均值和趋势
            avg_cpu = sum(cpu_values) / len(cpu_values)
            avg_memory = sum(memory_values) / len(memory_values)

            # 检查趋势告警
            if avg_cpu > 80:
                await self._trigger_alert(
                    AlertType.RESOURCE_EXHAUSTION,
                    AlertLevel.HIGH,
                    "CPU使用率持续过高",
                    f"过去1小时平均CPU使用率: {avg_cpu:.1f}%",
                    {"avg_cpu_1h": avg_cpu}
                )

            if avg_memory > 85:
                await self._trigger_alert(
                    AlertType.RESOURCE_EXHAUSTION,
                    AlertLevel.HIGH,
                    "内存使用率持续过高",
                    f"过去1小时平均内存使用率: {avg_memory:.1f}%",
                    {"avg_memory_1h": avg_memory}
                )

            logger.debug(f"性能趋势分析完成 - CPU: {avg_cpu:.1f}%, 内存: {avg_memory:.1f}%")

        except Exception as e:
            logger.error(f"分析性能趋势失败: {e}")

    async def _cleanup_expired_data(self):
        """清理过期数据"""
        try:
            current_time = datetime.now()

            # 清理过期的性能历史
            retention_threshold = current_time - timedelta(hours=self.metric_retention_hours)
            self.performance_history = [
                m for m in self.performance_history
                if m.timestamp > retention_threshold
            ]

            # 清理过期的告警历史
            alert_retention_threshold = current_time - timedelta(days=self.alert_history_retention_days)
            self.alert_history = [
                a for a in self.alert_history
                if a.timestamp > alert_retention_threshold
            ]

            # 清理已解决的告警
            resolved_alerts = [
                alert_id for alert_id, alert in self.active_alerts.items()
                if alert.resolved and alert.resolved_at and
                   (current_time - alert.resolved_at).total_seconds() > 3600
            ]

            for alert_id in resolved_alerts:
                del self.active_alerts[alert_id]

            logger.debug("过期数据清理完成")

        except Exception as e:
            logger.error(f"清理过期数据失败: {e}")

    async def _remove_silence(self, alert_id: str, delay_minutes: int):
        """移除告警静默"""
        await asyncio.sleep(delay_minutes * 60)
        self.silenced_alerts.discard(alert_id)

    def _get_alert_recipients(self, alert: Alert) -> List[str]:
        """获取告警接收人"""
        # 从告警规则获取接收人
        if alert.rule_id and alert.rule_id in self.alert_rules:
            rule = self.alert_rules[alert.rule_id]
            if rule.recipients:
                return rule.recipients

        # 默认接收人
        default_recipients = getattr(settings, 'DEFAULT_ALERT_RECIPIENTS', [
            'admin@company.com',
            'bigdata-team@company.com'
        ])

        return default_recipients

    def _format_email_body(self, alert: Alert) -> str:
        """格式化邮件内容"""
        color = self._get_alert_color(alert.level)

        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .header {{ background-color: {color}; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .footer {{ background-color: #f4f4f4; padding: 10px; font-size: 12px; color: #666; }}
                .context {{ background-color: #f9f9f9; padding: 15px; margin: 10px 0; border-left: 4px solid {color}; }}
                .tags {{ margin-top: 15px; }}
                .tag {{ display: inline-block; background-color: #e0e0e0; padding: 3px 8px; margin: 2px; border-radius: 3px; font-size: 11px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚨 告警通知</h1>
                <h2>{alert.title}</h2>
            </div>

            <div class="content">
                <p><strong>告警级别:</strong> <span style="color: {color}; font-weight: bold;">{alert.level.value.upper()}</span></p>
                <p><strong>告警类型:</strong> {alert.alert_type.value}</p>
                <p><strong>告警源:</strong> {alert.source}</p>
                <p><strong>告警时间:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>

                <h3>告警详情</h3>
                <div class="context">
                    {alert.message.replace(chr(10), '<br>')}
                </div>

                {self._format_context_html(alert.context) if alert.context else ''}

                {self._format_tags_html(alert.tags) if alert.tags else ''}
            </div>

            <div class="footer">
                <p>此邮件由大数据平台监控系统自动发送，请勿直接回复。</p>
                <p>告警ID: {alert.id}</p>
            </div>
        </body>
        </html>
        """

    def _format_context_html(self, context: Dict[str, Any]) -> str:
        """格式化上下文信息为HTML"""
        html = "<h3>上下文信息</h3><div class='context'><ul>"
        for key, value in context.items():
            html += f"<li><strong>{key}:</strong> {value}</li>"
        html += "</ul></div>"
        return html

    def _format_tags_html(self, tags: Dict[str, str]) -> str:
        """格式化标签为HTML"""
        html = "<div class='tags'><strong>标签:</strong> "
        for key, value in tags.items():
            html += f"<span class='tag'>{key}:{value}</span>"
        html += "</div>"
        return html

    def _get_alert_color(self, level: AlertLevel) -> str:
        """获取告警级别对应的颜色"""
        colors = {
            AlertLevel.LOW: "#28a745",
            AlertLevel.MEDIUM: "#ffc107",
            AlertLevel.HIGH: "#fd7e14",
            AlertLevel.CRITICAL: "#dc3545"
        }
        return colors.get(level, "#6c757d")

    # ==================== 公共查询接口 ====================

    def get_active_alerts(self, level_filter: AlertLevel = None) -> List[Dict[str, Any]]:
        """获取活跃告警"""
        alerts = list(self.active_alerts.values())

        if level_filter:
            alerts = [a for a in alerts if a.level == level_filter]

        return [self._serialize_alert(alert) for alert in alerts]

    def get_alert_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取告警历史"""
        history = sorted(self.alert_history, key=lambda x: x.timestamp, reverse=True)
        return [self._serialize_alert(alert) for alert in history[:limit]]

    def get_performance_metrics(self, task_id: str = None, hours: int = 24) -> List[Dict[str, Any]]:
        """获取性能指标"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        metrics = [
            m for m in self.performance_history
            if m.timestamp > cutoff_time and (not task_id or m.task_id == task_id)
        ]

        return [self._serialize_performance_metrics(m) for m in metrics]

    def get_monitoring_stats(self) -> Dict[str, Any]:
        """获取监控统计信息"""
        current_time = datetime.now()

        # 统计最近24小时的告警
        recent_alerts = [
            a for a in self.alert_history
            if (current_time - a.timestamp).total_seconds() < 86400
        ]

        return {
            "monitoring_status": "active" if self._monitoring_task and not self._monitoring_task.done() else "inactive",
            "active_alerts_count": len(self.active_alerts),
            "alert_rules_count": len(self.alert_rules),
            "alerts_last_24h": len(recent_alerts),
            "performance_metrics_count": len(self.performance_history),
            "system_health": "healthy",  # 这里可以添加实际的健康状态判断逻辑
            "last_check_time": current_time.isoformat(),
            "monitoring_interval_seconds": self.monitoring_interval,
            "alerts_by_level": {
                level.value: len([a for a in recent_alerts if a.level == level])
                for level in AlertLevel
            }
        }

    async def resolve_alert(self, alert_id: str) -> Dict[str, Any]:
        """解决告警"""
        try:
            if alert_id not in self.active_alerts:
                return {
                    "success": False,
                    "error": f"告警不存在: {alert_id}"
                }

            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.now()

            logger.info(f"告警已解决: {alert.title}")
            return {
                "success": True,
                "alert_id": alert_id,
                "resolved_at": alert.resolved_at.isoformat()
            }

        except Exception as e:
            logger.error(f"解决告警失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _serialize_alert(self, alert: Alert) -> Dict[str, Any]:
        """序列化告警对象"""
        return {
            "id": alert.id,
            "rule_id": alert.rule_id,
            "alert_type": alert.alert_type.value,
            "level": alert.level.value,
            "title": alert.title,
            "message": alert.message,
            "source": alert.source,
            "timestamp": alert.timestamp.isoformat(),
            "resolved": alert.resolved,
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
            "context": alert.context,
            "tags": alert.tags
        }

    def _serialize_performance_metrics(self, metrics: PerformanceMetrics) -> Dict[str, Any]:
        """序列化性能指标对象"""
        return {
            "task_id": metrics.task_id,
            "dag_id": metrics.dag_id,
            "execution_date": metrics.execution_date,
            "duration_seconds": metrics.duration_seconds,
            "cpu_usage_percent": metrics.cpu_usage_percent,
            "memory_usage_mb": metrics.memory_usage_mb,
            "disk_io_mb": metrics.disk_io_mb,
            "network_io_mb": metrics.network_io_mb,
            "success_rate": metrics.success_rate,
            "retry_count": metrics.retry_count,
            "timestamp": metrics.timestamp.isoformat()
        }


# 创建全局监控服务实例
monitoring_service = MonitoringService()