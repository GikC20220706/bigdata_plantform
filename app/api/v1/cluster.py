# app/api/v1/cluster.py - 更新后调用业务服务层
"""
集群管理API - 调用业务服务层版本
通过cluster_service进行业务逻辑处理，实现真正的分层架构
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.response import create_response
from app.utils.database import get_async_db
from app.services.cluster_service import cluster_service
from config.settings import settings

router = APIRouter()


# ==================== 集群API端点 ====================

@router.get("/", summary="获取所有集群概览")
async def get_clusters_overview(
        db: AsyncSession = Depends(get_async_db),
        force_refresh: bool = Query(False, description="是否强制刷新数据"),
        background_tasks: BackgroundTasks = None
):
    """获取所有集群的概览信息"""
    try:
        logger.info("开始获取集群概览信息")

        # 调用业务服务层
        overview_data = await cluster_service.get_all_clusters_overview(db, force_refresh)

        return create_response(
            data=overview_data,
            message="获取集群概览成功"
        )

    except Exception as e:
        logger.error(f"获取集群概览失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取集群概览失败: {str(e)}"
        )


@router.get("/{cluster_type}", summary="获取指定集群详情")
async def get_cluster_info(
        cluster_type: str,
        db: AsyncSession = Depends(get_async_db),
        include_nodes: bool = Query(True, description="是否包含节点详细信息"),
        include_history: bool = Query(False, description="是否包含历史数据"),
        history_hours: int = Query(24, ge=1, le=168, description="历史数据小时数")
):
    """获取指定类型集群的详细信息"""
    try:
        logger.info(f"获取 {cluster_type} 集群信息")

        # 调用业务服务层
        cluster_detail = await cluster_service.get_cluster_detail(
            db=db,
            cluster_type=cluster_type,
            include_nodes=include_nodes,
            include_history=include_history,
            history_hours=history_hours
        )

        if not cluster_detail:
            return create_response(
                data={
                    "cluster_type": cluster_type,
                    "status": "not_found",
                    "message": f"未找到 {cluster_type} 集群，可能需要初始化或该集群类型不被支持"
                },
                message=f"{cluster_type} 集群未找到"
            )

        return create_response(
            data=cluster_detail,
            message=f"获取{cluster_type}集群信息成功"
        )

    except ValueError as e:
        # 业务逻辑错误（如不支持的集群类型）
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"获取集群信息失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取集群信息失败: {str(e)}"
        )


@router.get("/{cluster_type}/nodes", summary="获取集群节点列表")
async def get_cluster_nodes(
        cluster_type: str,
        db: AsyncSession = Depends(get_async_db),
        status: Optional[str] = Query(None, description="节点状态过滤"),
        role: Optional[str] = Query(None, description="节点角色过滤")
):
    """获取指定集群的节点列表"""
    try:
        logger.info(f"获取 {cluster_type} 集群节点列表")

        # 获取集群详细信息（包含节点）
        cluster_detail = await cluster_service.get_cluster_detail(
            db=db,
            cluster_type=cluster_type,
            include_nodes=True
        )

        if not cluster_detail:
            raise HTTPException(
                status_code=404,
                detail=f"未找到 {cluster_type} 集群"
            )

        nodes = cluster_detail.get("nodes", [])

        # 应用过滤条件
        if status:
            status_map = {"正常": "online", "异常": "offline"}
            filter_status = status_map.get(status, status)
            nodes = [n for n in nodes if n.get('status') == filter_status]

        if role:
            nodes = [n for n in nodes if role.lower() in n.get('role', '').lower()]

        # 转换状态显示
        for node in nodes:
            node['status'] = "正常" if node.get('status') == 'online' else "异常"

        return create_response(
            data={
                "cluster_type": cluster_type,
                "total_nodes": cluster_detail.get('total_nodes', 0),
                "active_nodes": cluster_detail.get('active_nodes', 0),
                "filtered_nodes": len(nodes),
                "nodes": nodes,
                "last_update": cluster_detail.get('last_health_check'),
                "data_source": "database"
            },
            message=f"获取{cluster_type}集群节点信息成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取集群节点信息失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取集群节点信息失败: {str(e)}"
        )


@router.get("/{cluster_type}/resource-usage", summary="获取集群资源使用情况")
async def get_cluster_resource_usage(
        cluster_type: str,
        db: AsyncSession = Depends(get_async_db),
        hours: int = Query(24, ge=1, le=168, description="历史数据小时数")
):
    """获取集群资源使用情况"""
    try:
        logger.info(f"获取 {cluster_type} 集群资源使用情况")

        # 获取包含历史数据的集群信息
        cluster_detail = await cluster_service.get_cluster_detail(
            db=db,
            cluster_type=cluster_type,
            include_nodes=True,
            include_history=True,
            history_hours=hours
        )

        if not cluster_detail:
            raise HTTPException(
                status_code=404,
                detail=f"未找到 {cluster_type} 集群"
            )

        # 构建资源使用数据
        nodes_detail = cluster_detail.get("nodes", [])
        total_cpu_cores = sum(node.get('cpu_cores', 8) for node in nodes_detail)
        total_memory_gb = sum((node.get('memory_total', 16384) or 16384) / 1024 for node in nodes_detail)
        total_storage_tb = sum((node.get('disk_total', 2048000) or 2048000) / (1024 ** 2) for node in nodes_detail)

        resource_data = {
            "cluster_name": cluster_detail.get('name'),
            "cluster_type": cluster_type,
            "resource_config": {
                "cpu_cores": int(total_cpu_cores),
                "memory_gb": int(total_memory_gb),
                "storage_tb": int(total_storage_tb)
            },
            "current_usage": {
                "cpu_usage": cluster_detail.get('cpu_usage', 0.0),
                "memory_usage": cluster_detail.get('memory_usage', 0.0),
                "storage_usage": cluster_detail.get('disk_usage', 0.0)
            },
            "nodes_summary": {
                "total_nodes": cluster_detail.get('total_nodes', 0),
                "active_nodes": cluster_detail.get('active_nodes', 0),
                "nodes_detail": [
                    {
                        "hostname": node.get('hostname'),
                        "status": node.get('status'),
                        "cpu_usage": node.get('cpu_usage', 0.0),
                        "memory_usage": node.get('memory_usage', 0.0),
                        "disk_usage": node.get('disk_usage', 0.0)
                    }
                    for node in nodes_detail
                ]
            },
            "historical_data": cluster_detail.get('historical_data', []),
            "data_source": "database"
        }

        # 添加集群特定信息
        if cluster_type.lower() == "hadoop":
            hdfs_info = cluster_detail.get('hdfs_info', {})
            resource_data['hdfs_info'] = hdfs_info
        elif cluster_type.lower() == "flink":
            flink_info = cluster_detail.get('flink_info', {})
            resource_data['flink_info'] = flink_info
        elif cluster_type.lower() == "doris":
            doris_info = cluster_detail.get('doris_info', {})
            resource_data['doris_info'] = doris_info

        return create_response(
            data=resource_data,
            message=f"获取{cluster_type}集群资源使用情况成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取集群资源使用情况失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取集群资源使用情况失败: {str(e)}"
        )


@router.get("/alerts", summary="获取集群告警信息")
async def get_cluster_alerts(
        db: AsyncSession = Depends(get_async_db),
        cluster_type: Optional[str] = Query(None, description="集群类型过滤"),
        level: Optional[str] = Query(None, description="告警级别过滤")
):
    """获取集群告警信息"""
    try:
        logger.info("获取集群告警信息")

        # 调用业务服务层
        alerts = await cluster_service.get_cluster_alerts(db, cluster_type, level)

        # 生成统计信息
        alert_summary = {
            "total_alerts": len(alerts),
            "critical_alerts": len([a for a in alerts if a.get('level') == 'error']),
            "warning_alerts": len([a for a in alerts if a.get('level') == 'warning']),
            "info_alerts": len([a for a in alerts if a.get('level') == 'info']),
            "by_cluster": {}
        }

        # 按集群统计
        for alert in alerts:
            cluster_name = alert.get('cluster', 'unknown')
            if cluster_name not in alert_summary['by_cluster']:
                alert_summary['by_cluster'][cluster_name] = 0
            alert_summary['by_cluster'][cluster_name] += 1

        return create_response(
            data={
                "alerts": alerts,
                "summary": alert_summary,
                "available_levels": ["error", "warning", "info"],
                "data_source": "database",
                "last_update": datetime.now()
            },
            message="获取集群告警信息成功"
        )

    except Exception as e:
        logger.error(f"获取集群告警信息失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取集群告警信息失败: {str(e)}"
        )


@router.post("/refresh", summary="刷新集群数据")
async def refresh_cluster_data(
        db: AsyncSession = Depends(get_async_db),
        background_tasks: BackgroundTasks = None,
        cluster_types: Optional[List[str]] = Query(None, description="要刷新的集群类型列表")
):
    """手动刷新集群数据"""
    try:
        logger.info("开始手动刷新集群数据")

        # 调用业务服务层
        refresh_result = await cluster_service.refresh_cluster_data(db, cluster_types)

        return create_response(
            data=refresh_result,
            message=f"集群数据刷新完成，成功刷新 {refresh_result.get('successful_clusters', 0)} 个集群"
        )

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"刷新集群数据失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"刷新集群数据失败: {str(e)}"
        )


@router.get("/health/check", summary="集群健康检查")
async def health_check(db: AsyncSession = Depends(get_async_db)):
    """执行集群健康检查"""
    try:
        logger.info("开始集群健康检查")

        # 调用业务服务层
        health_report = await cluster_service.validate_cluster_health(db)

        return create_response(
            data=health_report,
            message="集群健康检查完成"
        )

    except Exception as e:
        logger.error(f"集群健康检查失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"集群健康检查失败: {str(e)}"
        )


# ==================== 管理端点 ====================

@router.post("/maintenance/cleanup", summary="清理历史数据")
async def cleanup_historical_data(
        db: AsyncSession = Depends(get_async_db),
        days_to_keep: int = Query(30, ge=7, le=365, description="保留天数")
):
    """清理历史指标数据"""
    try:
        logger.info(f"开始清理 {days_to_keep} 天前的历史数据")

        # 直接调用数据库服务进行清理
        from app.services.cluster_database_service import cluster_db_service
        deleted_count = await cluster_db_service.cleanup_old_metrics(db, days_to_keep)

        return create_response(
            data={
                "deleted_records": deleted_count,
                "days_kept": days_to_keep,
                "cleanup_time": datetime.now()
            },
            message=f"成功清理 {deleted_count} 条历史记录"
        )

    except Exception as e:
        logger.error(f"清理历史数据失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"清理历史数据失败: {str(e)}"
        )


@router.get("/supported-types", summary="获取支持的集群类型")
async def get_supported_cluster_types():
    """获取系统支持的集群类型列表"""
    try:
        return create_response(
            data={
                "supported_types": cluster_service.supported_clusters,
                "total_types": len(cluster_service.supported_clusters),
                "extensible": True
            },
            message="获取支持的集群类型成功"
        )

    except Exception as e:
        logger.error(f"获取支持的集群类型失败: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"获取支持的集群类型失败: {str(e)}"
        )