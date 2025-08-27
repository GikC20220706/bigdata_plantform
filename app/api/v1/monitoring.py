# app/api/v1/monitoring.py
"""
监控告警API端点
提供告警规则管理、告警查询、性能监控等接口
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks
from pydantic import BaseModel, Field
from loguru import logger

from app.services.monitoring_service import (
    monitoring_service, AlertRule, AlertLevel, AlertType,
    NotificationChannel, Alert, PerformanceMetrics
)
from app.utils.response import create_response

router = APIRouter()


# ==================== 请求模型 ====================

class AlertRuleRequest(BaseModel):
    """告警规则请求模型"""
    name: str = Field(..., description="规则名称")
    description: str = Field("", description="规则描述")
    alert_type: str = Field(..., description="告警类型")
    level: str = Field(..., description="告警级别")
    condition: str = Field(..., description="触发条件")
    threshold: Optional[float] = Field(None, description="阈值")
    duration_minutes: int = Field(0, description="持续时间(分钟)")
    enabled: bool = Field(True, description="是否启用")
    channels: List[str] = Field(default_factory=list, description="通知渠道")
    recipients: List[str] = Field(default_factory=list, description="接收人")
    silence_minutes: int = Field(60, description="静默时间(分钟)")
    custom_message: Optional[str] = Field(None, description="自定义消息")
    tags: Dict[str, str] = Field(default_factory=dict, description="标签")


class NotificationTestRequest(BaseModel):
    """通知测试请求模型"""
    channels: List[str] = Field(..., description="测试渠道")
    recipients: List[str] = Field(..., description="接收人")
    test_message: str = Field("这是一条测试告警", description="测试消息")


# ==================== 告警规则管理 ====================

@router.get("/rules", summary="获取告警规则列表")
async def get_alert_rules():
    """获取所有告警规则"""
    try:
        rules = []
        for rule_id, rule in monitoring_service.alert_rules.items():
            rules.append({
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
            })

        return create_response(
            data={
                "rules": rules,
                "total": len(rules),
                "enabled_count": len([r for r in monitoring_service.alert_rules.values() if r.enabled])
            },
            message=f"获取告警规则成功，共 {len(rules)} 条"
        )

    except Exception as e:
        logger.error(f"获取告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules", summary="创建告警规则")
async def create_alert_rule(rule_request: AlertRuleRequest):
    """创建新的告警规则"""
    try:
        # 生成规则ID
        rule_id = f"rule_{int(datetime.now().timestamp())}"

        # 验证枚举值
        try:
            alert_type = AlertType(rule_request.alert_type)
            level = AlertLevel(rule_request.level)
            channels = [NotificationChannel(ch) for ch in rule_request.channels]
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"无效的枚举值: {e}")

        # 创建告警规则对象
        alert_rule = AlertRule(
            id=rule_id,
            name=rule_request.name,
            description=rule_request.description,
            alert_type=alert_type,
            level=level,
            condition=rule_request.condition,
            threshold=rule_request.threshold,
            duration_minutes=rule_request.duration_minutes,
            enabled=rule_request.enabled,
            channels=channels,
            recipients=rule_request.recipients,
            silence_minutes=rule_request.silence_minutes,
            custom_message=rule_request.custom_message,
            tags=rule_request.tags
        )

        # 添加规则
        result = await monitoring_service.add_alert_rule(alert_rule)

        if result["success"]:
            return create_response(
                data={"rule_id": rule_id, "rule": alert_rule.__dict__},
                message=result["message"]
            )
        else:
            raise HTTPException(status_code=400, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启用告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules/{rule_id}/disable", summary="禁用告警规则")
async def disable_alert_rule(rule_id: str):
    """禁用告警规则"""
    try:
        result = await monitoring_service.update_alert_rule(rule_id, {"enabled": False})

        if result["success"]:
            return create_response(
                data={"rule_id": rule_id, "enabled": False},
                message="告警规则已禁用"
            )
        else:
            raise HTTPException(status_code=404, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"禁用告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 告警查询和管理 ====================

@router.get("/alerts", summary="获取告警列表")
async def get_alerts(
        status: str = Query("active", description="告警状态：active, resolved, all"),
        level: Optional[str] = Query(None, description="告警级别过滤"),
        alert_type: Optional[str] = Query(None, description="告警类型过滤"),
        limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
        start_time: Optional[str] = Query(None, description="开始时间(ISO格式)"),
        end_time: Optional[str] = Query(None, description="结束时间(ISO格式)")
):
    """获取告警列表"""
    try:
        # 验证告警级别
        level_filter = None
        if level:
            try:
                level_filter = AlertLevel(level)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的告警级别: {level}")

        # 验证告警类型
        type_filter = None
        if alert_type:
            try:
                type_filter = AlertType(alert_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的告警类型: {alert_type}")

        # 解析时间过滤
        start_datetime = None
        end_datetime = None

        if start_time:
            try:
                start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="开始时间格式错误")

        if end_time:
            try:
                end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="结束时间格式错误")

        # 获取告警数据
        if status == "active":
            alerts = monitoring_service.get_active_alerts(level_filter)
        elif status == "resolved" or status == "all":
            alerts = monitoring_service.get_alert_history(limit)
            if status == "resolved":
                alerts = [a for a in alerts if a.get("resolved", False)]
        else:
            raise HTTPException(status_code=400, detail="无效的状态参数")

        # 应用过滤条件
        filtered_alerts = []
        for alert in alerts:
            # 类型过滤
            if type_filter and alert["alert_type"] != type_filter.value:
                continue

            # 时间过滤
            alert_time = datetime.fromisoformat(alert["timestamp"].replace('Z', '+00:00'))
            if start_datetime and alert_time < start_datetime:
                continue
            if end_datetime and alert_time > end_datetime:
                continue

            filtered_alerts.append(alert)

        # 限制返回数量
        filtered_alerts = filtered_alerts[:limit]

        return create_response(
            data={
                "alerts": filtered_alerts,
                "total": len(filtered_alerts),
                "filters": {
                    "status": status,
                    "level": level,
                    "alert_type": alert_type,
                    "start_time": start_time,
                    "end_time": end_time
                }
            },
            message=f"获取告警列表成功，共 {len(filtered_alerts)} 条"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取告警列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts/{alert_id}", summary="获取告警详情")
async def get_alert_detail(alert_id: str):
    """获取指定告警的详细信息"""
    try:
        # 在活跃告警中查找
        alert = monitoring_service.active_alerts.get(alert_id)

        if not alert:
            # 在历史告警中查找
            for hist_alert in monitoring_service.alert_history:
                if hist_alert.id == alert_id:
                    alert = hist_alert
                    break

        if not alert:
            raise HTTPException(status_code=404, detail=f"告警不存在: {alert_id}")

        alert_data = monitoring_service._serialize_alert(alert)

        # 获取相关的告警规则信息
        if alert.rule_id and alert.rule_id in monitoring_service.alert_rules:
            rule = monitoring_service.alert_rules[alert.rule_id]
            alert_data["rule_info"] = {
                "rule_name": rule.name,
                "rule_description": rule.description,
                "condition": rule.condition
            }

        return create_response(
            data=alert_data,
            message="获取告警详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取告警详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/resolve", summary="解决告警")
async def resolve_alert(alert_id: str, comment: str = Body("", description="解决说明")):
    """手动解决告警"""
    try:
        result = await monitoring_service.resolve_alert(alert_id)

        if result["success"]:
            return create_response(
                data={
                    "alert_id": alert_id,
                    "resolved_at": result["resolved_at"],
                    "comment": comment
                },
                message="告警已解决"
            )
        else:
            raise HTTPException(status_code=404, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"解决告警失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts/stats", summary="获取告警统计信息")
async def get_alert_stats(
        hours: int = Query(24, ge=1, le=168, description="统计时间范围(小时)")
):
    """获取告警统计信息"""
    try:
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=hours)

        # 统计指定时间范围内的告警
        recent_alerts = [
            alert for alert in monitoring_service.alert_history
            if alert.timestamp > cutoff_time
        ]

        # 按级别统计
        stats_by_level = {}
        for level in AlertLevel:
            count = len([a for a in recent_alerts if a.level == level])
            stats_by_level[level.value] = count

        # 按类型统计
        stats_by_type = {}
        for alert_type in AlertType:
            count = len([a for a in recent_alerts if a.alert_type == alert_type])
            stats_by_type[alert_type.value] = count

        # 按小时统计趋势
        hourly_stats = {}
        for i in range(hours):
            hour_start = current_time - timedelta(hours=i + 1)
            hour_end = current_time - timedelta(hours=i)
            hour_alerts = [
                a for a in recent_alerts
                if hour_start <= a.timestamp < hour_end
            ]
            hour_key = hour_start.strftime("%Y-%m-%d %H:00")
            hourly_stats[hour_key] = len(hour_alerts)

        return create_response(
            data={
                "time_range_hours": hours,
                "total_alerts": len(recent_alerts),
                "active_alerts": len(monitoring_service.active_alerts),
                "resolved_alerts": len([a for a in recent_alerts if a.resolved]),
                "stats_by_level": stats_by_level,
                "stats_by_type": stats_by_type,
                "hourly_trend": dict(sorted(hourly_stats.items())),
                "most_frequent_type": max(stats_by_type, key=stats_by_type.get) if stats_by_type else None,
                "critical_alerts_count": stats_by_level.get("critical", 0)
            },
            message=f"获取 {hours} 小时告警统计成功"
        )

    except Exception as e:
        logger.error(f"获取告警统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 性能监控 ====================

@router.get("/performance", summary="获取性能监控数据")
async def get_performance_metrics(
        task_id: Optional[str] = Query(None, description="任务ID过滤"),
        dag_id: Optional[str] = Query(None, description="DAG ID过滤"),
        hours: int = Query(24, ge=1, le=168, description="时间范围(小时)"),
        metric_type: str = Query("all", description="指标类型：duration, cpu, memory, all")
):
    """获取性能监控指标"""
    try:
        metrics = monitoring_service.get_performance_metrics(task_id, hours)

        # DAG过滤
        if dag_id:
            metrics = [m for m in metrics if m.get("dag_id") == dag_id]

        # 计算统计信息
        if metrics:
            durations = [m["duration_seconds"] for m in metrics if m["duration_seconds"]]
            cpu_usages = [m["cpu_usage_percent"] for m in metrics if m["cpu_usage_percent"]]
            memory_usages = [m["memory_usage_mb"] for m in metrics if m["memory_usage_mb"]]
            success_rates = [m["success_rate"] for m in metrics]

            stats = {
                "total_executions": len(metrics),
                "avg_duration": sum(durations) / len(durations) if durations else 0,
                "max_duration": max(durations) if durations else 0,
                "min_duration": min(durations) if durations else 0,
                "avg_cpu_usage": sum(cpu_usages) / len(cpu_usages) if cpu_usages else 0,
                "avg_memory_usage": sum(memory_usages) / len(memory_usages) if memory_usages else 0,
                "overall_success_rate": sum(success_rates) / len(success_rates) if success_rates else 0
            }
        else:
            stats = {}

        # 按指标类型过滤
        if metric_type != "all":
            filtered_metrics = []
            for metric in metrics:
                filtered_metric = {
                    "task_id": metric["task_id"],
                    "dag_id": metric["dag_id"],
                    "execution_date": metric["execution_date"],
                    "timestamp": metric["timestamp"]
                }

                if metric_type == "duration":
                    filtered_metric["duration_seconds"] = metric["duration_seconds"]
                elif metric_type == "cpu":
                    filtered_metric["cpu_usage_percent"] = metric["cpu_usage_percent"]
                elif metric_type == "memory":
                    filtered_metric["memory_usage_mb"] = metric["memory_usage_mb"]

                filtered_metrics.append(filtered_metric)

            metrics = filtered_metrics

        return create_response(
            data={
                "metrics": metrics,
                "stats": stats,
                "filters": {
                    "task_id": task_id,
                    "dag_id": dag_id,
                    "hours": hours,
                    "metric_type": metric_type
                }
            },
            message=f"获取性能指标成功，共 {len(metrics)} 条记录"
        )

    except Exception as e:
        logger.error(f"获取性能指标失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/trends", summary="获取性能趋势分析")
async def get_performance_trends(
        task_id: Optional[str] = Query(None, description="任务ID"),
        dag_id: Optional[str] = Query(None, description="DAG ID"),
        days: int = Query(7, ge=1, le=30, description="分析天数")
):
    """获取性能趋势分析"""
    try:
        # 获取指定时间范围的性能数据
        hours = days * 24
        metrics = monitoring_service.get_performance_metrics(task_id, hours)

        if dag_id:
            metrics = [m for m in metrics if m.get("dag_id") == dag_id]

        if not metrics:
            return create_response(
                data={
                    "trends": {},
                    "recommendations": [],
                    "baseline": {}
                },
                message="没有找到性能数据"
            )

        # 按天分组统计
        daily_stats = {}
        for metric in metrics:
            date = metric["execution_date"]
            if date not in daily_stats:
                daily_stats[date] = {
                    "executions": 0,
                    "total_duration": 0,
                    "total_cpu": 0,
                    "total_memory": 0,
                    "success_count": 0
                }

            stats = daily_stats[date]
            stats["executions"] += 1
            stats["total_duration"] += metric["duration_seconds"]
            stats["total_cpu"] += metric["cpu_usage_percent"]
            stats["total_memory"] += metric["memory_usage_mb"]
            stats["success_count"] += metric["success_rate"]

        # 计算趋势
        trends = {}
        for date, stats in daily_stats.items():
            trends[date] = {
                "avg_duration": stats["total_duration"] / stats["executions"],
                "avg_cpu": stats["total_cpu"] / stats["executions"],
                "avg_memory": stats["total_memory"] / stats["executions"],
                "success_rate": stats["success_count"] / stats["executions"],
                "execution_count": stats["executions"]
            }

        # 生成性能建议
        recommendations = []

        # 计算整体平均值
        all_durations = [t["avg_duration"] for t in trends.values()]
        all_cpu = [t["avg_cpu"] for t in trends.values()]
        all_memory = [t["avg_memory"] for t in trends.values()]

        avg_duration = sum(all_durations) / len(all_durations)
        avg_cpu = sum(all_cpu) / len(all_cpu)
        avg_memory = sum(all_memory) / len(all_memory)

        # 性能建议逻辑
        if avg_duration > 1800:  # 30分钟
            recommendations.append({
                "type": "performance",
                "level": "medium",
                "message": f"平均执行时间较长({avg_duration / 60:.1f}分钟)，建议优化任务逻辑或增加资源"
            })

        if avg_cpu > 80:
            recommendations.append({
                "type": "resource",
                "level": "high",
                "message": f"CPU使用率较高({avg_cpu:.1f}%)，建议优化算法或增加CPU资源"
            })

        if avg_memory > 8192:  # 8GB
            recommendations.append({
                "type": "resource",
                "level": "medium",
                "message": f"内存使用较高({avg_memory / 1024:.1f}GB)，建议优化内存使用或增加内存"
            })

        # 建立基线
        baseline = {
            "avg_duration": avg_duration,
            "avg_cpu": avg_cpu,
            "avg_memory": avg_memory,
            "sample_days": len(daily_stats),
            "total_executions": sum(s["executions"] for s in daily_stats.values())
        }

        return create_response(
            data={
                "trends": dict(sorted(trends.items())),
                "recommendations": recommendations,
                "baseline": baseline,
                "analysis_period": {
                    "days": days,
                    "start_date": min(trends.keys()) if trends else None,
                    "end_date": max(trends.keys()) if trends else None
                }
            },
            message=f"性能趋势分析完成，分析了 {days} 天的数据"
        )

    except Exception as e:
        logger.error(f"获取性能趋势失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 系统健康检查 ====================

@router.get("/health", summary="系统健康检查")
async def system_health_check():
    """执行系统健康检查"""
    try:
        health_status = await monitoring_service.check_system_health()

        return create_response(
            data=health_status,
            message=f"系统健康检查完成，状态: {health_status['overall_status']}"
        )

    except Exception as e:
        logger.error(f"系统健康检查失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", summary="获取监控服务状态")
async def get_monitoring_status():
    """获取监控服务的运行状态和统计信息"""
    try:
        stats = monitoring_service.get_monitoring_stats()

        return create_response(
            data=stats,
            message="获取监控状态成功"
        )

    except Exception as e:
        logger.error(f"获取监控状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 通知管理 ====================

@router.post("/notifications/test", summary="测试通知发送")
async def test_notification(test_request: NotificationTestRequest):
    """测试通知渠道是否正常工作"""
    try:
        # 验证通知渠道
        channels = []
        for ch in test_request.channels:
            try:
                channels.append(NotificationChannel(ch))
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的通知渠道: {ch}")

        # 创建测试告警
        test_alert = Alert(
            id=f"test_{int(datetime.now().timestamp())}",
            rule_id="test",
            alert_type=AlertType.SYSTEM_ERROR,
            level=AlertLevel.LOW,
            title="测试通知",
            message=test_request.test_message,
            source="notification_test",
            timestamp=datetime.now()
        )

        # 发送测试通知
        await monitoring_service.send_alert_notification(test_alert, channels)

        return create_response(
            data={
                "test_alert_id": test_alert.id,
                "channels_tested": test_request.channels,
                "recipients": test_request.recipients,
                "test_time": datetime.now().isoformat()
            },
            message="测试通知已发送"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"测试通知发送失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notifications/config", summary="获取通知配置")
async def get_notification_config():
    """获取当前的通知配置信息"""
    try:
        config = monitoring_service.notification_config.copy()

        # 隐藏敏感信息
        if "email" in config and "password" in config["email"]:
            config["email"]["password"] = "***"

        if "dingtalk" in config and "secret" in config["dingtalk"]:
            config["dingtalk"]["secret"] = "***"

        return create_response(
            data={
                "notification_config": config,
                "supported_channels": [ch.value for ch in NotificationChannel],
                "alert_levels": [level.value for level in AlertLevel],
                "alert_types": [atype.value for atype in AlertType]
            },
            message="获取通知配置成功"
        )

    except Exception as e:
        logger.error(f"获取通知配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 监控控制 ====================

@router.post("/control/start", summary="启动监控服务")
async def start_monitoring():
    """启动监控任务"""
    try:
        await monitoring_service.start_monitoring()

        return create_response(
            data={
                "monitoring_status": "started",
                "start_time": datetime.now().isoformat()
            },
            message="监控服务已启动"
        )

    except Exception as e:
        logger.error(f"启动监控服务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/control/stop", summary="停止监控服务")
async def stop_monitoring():
    """停止监控任务"""
    try:
        await monitoring_service.stop_monitoring()

        return create_response(
            data={
                "monitoring_status": "stopped",
                "stop_time": datetime.now().isoformat()
            },
            message="监控服务已停止"
        )

    except Exception as e:
        logger.error(f"停止监控服务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/control/restart", summary="重启监控服务")
async def restart_monitoring():
    """重启监控任务"""
    try:
        await monitoring_service.stop_monitoring()
        await monitoring_service.start_monitoring()

        return create_response(
            data={
                "monitoring_status": "restarted",
                "restart_time": datetime.now().isoformat()
            },
            message="监控服务已重启"
        )

    except Exception as e:
        logger.error(f"重启监控服务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 与现有系统集成的回调接口 ====================

@router.post("/callbacks/task-status", summary="任务状态回调")
async def task_status_callback(
        dag_id: str = Body(..., description="DAG ID"),
        task_id: str = Body(..., description="任务ID"),
        status: str = Body(..., description="任务状态"),
        execution_date: str = Body(..., description="执行日期"),
        log_url: Optional[str] = Body(None, description="日志URL"),
        error_message: Optional[str] = Body(None, description="错误信息"),
        duration_seconds: Optional[float] = Body(None, description="执行时长"),
        resource_usage: Optional[Dict[str, Any]] = Body(None, description="资源使用情况")
):
    """
    接收来自Airflow或任务执行器的状态回调
    此接口被DAG模板中的notify_platform函数调用
    """
    try:
        # 构造执行结果对象
        from app.executors.base_executor import ExecutionResult, ExecutionStatus

        # 转换状态
        status_mapping = {
            "success": ExecutionStatus.SUCCESS,
            "failed": ExecutionStatus.FAILED,
            "timeout": ExecutionStatus.TIMEOUT,
            "retry": ExecutionStatus.RETRY,
            "running": ExecutionStatus.RUNNING
        }

        exec_status = status_mapping.get(status, ExecutionStatus.FAILED)

        # 创建执行结果
        task_result = ExecutionResult(
            status=exec_status,
            start_time=datetime.now() - timedelta(seconds=duration_seconds or 0),
            end_time=datetime.now(),
            duration_seconds=duration_seconds,
            error_message=error_message,
            resource_usage=resource_usage or {},
            logs=f"日志URL: {log_url}" if log_url else None
        )

        # 构造上下文
        context = {
            "task_id": task_id,
            "dag_id": dag_id,
            "execution_date": execution_date,
            "log_url": log_url
        }

        # 触发监控检查
        await monitoring_service.check_task_execution(task_result, context)

        return create_response(
            data={
                "dag_id": dag_id,
                "task_id": task_id,
                "status": status,
                "processed_at": datetime.now().isoformat()
            },
            message="任务状态回调处理成功"
        )

    except Exception as e:
        logger.error(f"处理任务状态回调失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/callbacks/sync-status", summary="数据同步状态回调")
async def sync_status_callback(
        sync_task_id: str = Body(..., description="同步任务ID"),
        source_name: str = Body(..., description="数据源名称"),
        target_name: str = Body(..., description="目标数据源名称"),
        table_name: str = Body(..., description="表名"),
        status: str = Body(..., description="同步状态"),
        rows_synced: Optional[int] = Body(None, description="同步行数"),
        data_size_mb: Optional[float] = Body(None, description="数据大小(MB)"),
        duration_seconds: Optional[float] = Body(None, description="同步时长"),
        error_message: Optional[str] = Body(None, description="错误信息")
):
    """
    接收来自智能数据同步服务的状态回调
    """
    try:
        # 如果同步失败，触发告警
        if status == "failed":
            await monitoring_service._trigger_alert(
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
            sync_metrics = PerformanceMetrics(
                task_id=sync_task_id,
                dag_id="data_sync",
                execution_date=datetime.now().strftime("%Y-%m-%d"),
                duration_seconds=duration_seconds,
                cpu_usage_percent=0,  # 同步任务暂不统计CPU
                memory_usage_mb=0,  # 同步任务暂不统计内存
                disk_io_mb=data_size_mb or 0,
                network_io_mb=data_size_mb or 0,
                success_rate=1.0,
                retry_count=0
            )

            monitoring_service.performance_history.append(sync_metrics)

        return create_response(
            data={
                "sync_task_id": sync_task_id,
                "table_name": table_name,
                "status": status,
                "processed_at": datetime.now().isoformat()
            },
            message="数据同步状态回调处理成功"
        )

    except Exception as e:
        logger.error(f"处理数据同步状态回调失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        raise
    except Exception as e:
        logger.error(f"创建告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/rules/{rule_id}", summary="更新告警规则")
async def update_alert_rule(rule_id: str, updates: Dict[str, Any] = Body(...)):
    """更新告警规则"""
    try:
        # 验证枚举值
        if "alert_type" in updates:
            try:
                AlertType(updates["alert_type"])
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的告警类型: {updates['alert_type']}")

        if "level" in updates:
            try:
                AlertLevel(updates["level"])
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的告警级别: {updates['level']}")

        if "channels" in updates:
            try:
                [NotificationChannel(ch) for ch in updates["channels"]]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"无效的通知渠道: {e}")

        result = await monitoring_service.update_alert_rule(rule_id, updates)

        if result["success"]:
            return create_response(
                data={"rule_id": rule_id, "updated_fields": list(updates.keys())},
                message=result["message"]
            )
        else:
            raise HTTPException(status_code=400, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/rules/{rule_id}", summary="删除告警规则")
async def delete_alert_rule(rule_id: str):
    """删除告警规则"""
    try:
        result = await monitoring_service.delete_alert_rule(rule_id)

        if result["success"]:
            return create_response(
                data={"rule_id": rule_id},
                message=result["message"]
            )
        else:
            raise HTTPException(status_code=404, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules/{rule_id}/enable", summary="启用告警规则")
async def enable_alert_rule(rule_id: str):
    """启用告警规则"""
    try:
        result = await monitoring_service.update_alert_rule(rule_id, {"enabled": True})

        if result["success"]:
            return create_response(
                data={"rule_id": rule_id, "enabled": True},
                message="告警规则已启用"
            )
        else:
            raise HTTPException(status_code=404, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启用告警规则失败:{e}")
        raise HTTPException(status_code=500,detail=str(e))