"""
调度管理API
提供DAG创建、管理、监控等功能
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Body
from loguru import logger

from app.utils.response import create_response
from app.services.dag_generator_service import (
    dag_generator_service,
    create_sql_dag,
    create_datax_dag,
    create_shell_dag
)
from app.utils.airflow_client import airflow_client, get_dag_status_summary
from app.schemas.scheduler import (
    CreateDAGRequest,
    UpdateDAGRequest,
    TaskConfigRequest,
    ScheduleConfigRequest,
    DAGListResponse,
    TaskStatusUpdate
)

router = APIRouter()


# ==================== DAG管理 ====================

@router.post("/dags", summary="创建DAG")
async def create_dag(request: CreateDAGRequest):
    """创建新的调度DAG"""
    try:
        logger.info(f"创建DAG请求: {request.dag_id}")

        # 本地文件检查：检查是否已经生成过同名的DAG文件
        dag_file_path = dag_generator_service.dag_folder / f"{request.dag_id}.py"
        if dag_file_path.exists():
            raise HTTPException(
                status_code=400,
                detail=f"DAG文件 '{request.dag_id}.py' 已存在，请使用不同的 DAG ID 或删除现有文件"
            )

        # 准备任务配置
        task_config = {
            "dag_id": request.dag_id,
            "display_name": request.display_name,
            "description": request.description,
            "task_type": request.task_type,
            "tasks": request.tasks,
            "dependencies": request.dependencies or [],
            "owner": request.owner
        }

        # 准备调度配置
        schedule_config = None
        if request.schedule:
            schedule_config = {
                "schedule_interval": request.schedule.schedule_interval,
                "start_date": request.schedule.start_date,
                "catchup": request.schedule.catchup,
                "max_active_runs": request.schedule.max_active_runs,
                "max_active_tasks": request.schedule.max_active_tasks,
                "retries": request.schedule.retries,
                "retry_delay_seconds": request.schedule.retry_delay_seconds
            }

        # 生成DAG
        result = await dag_generator_service.generate_dag(task_config, schedule_config)

        if result["success"]:
            logger.info(f"DAG '{request.dag_id}' 创建成功")

            return create_response(
                data={
                    **result,
                    "instructions": {
                        "next_steps": [
                            "DAG文件已生成到 Airflow DAG 目录",
                            "Airflow Scheduler 将在1-2分钟内自动扫描到新DAG",
                            "如需立即查看，可重启 Airflow Scheduler 或在WebUI中刷新",
                            f"DAG将出现在 Airflow WebUI: http://localhost:8080"
                        ],
                        "troubleshooting": {
                            "if_dag_not_visible": "docker-compose restart airflow-scheduler",
                            "manual_check": f"docker exec airflow-webserver airflow dags list | grep {request.dag_id}"
                        }
                    }
                },
                message=f"DAG '{request.dag_id}' 创建成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "DAG创建失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags", summary="获取DAG列表")
async def list_dags(
        limit: int = Query(50, ge=1, le=100, description="每页数量"),
        offset: int = Query(0, ge=0, description="偏移量"),
        dag_id_pattern: Optional[str] = Query(None, description="DAG ID过滤模式"),
        only_active: bool = Query(False, description="仅显示活跃的DAG")
):
    """获取DAG列表"""
    try:
        # 从Airflow获取DAG列表
        airflow_dags = await airflow_client.get_dags(limit=limit, offset=offset)

        # 获取生成的DAG列表
        generated_dags_result = await dag_generator_service.list_generated_dags()
        generated_dag_ids = {dag["dag_id"] for dag in generated_dags_result.get("dags", [])}

        # 处理DAG列表
        dags_data = []
        for dag in airflow_dags.get("dags", []):
            dag_id = dag["dag_id"]

            # 应用过滤条件
            if dag_id_pattern and dag_id_pattern.lower() not in dag_id.lower():
                continue
            if only_active and dag.get("is_paused", True):
                continue

            # 获取DAG状态摘要
            status_summary = await get_dag_status_summary(dag_id, days=7)

            dag_info = {
                "dag_id": dag_id,
                "description": dag.get("description", ""),
                "schedule_interval": dag.get("schedule_interval"),
                "is_paused": dag.get("is_paused", True),
                "is_active": dag.get("is_active", False),
                "last_parsed_time": dag.get("last_parsed_time"),
                "last_pickled": dag.get("last_pickled"),
                "max_active_tasks": dag.get("max_active_tasks", 16),
                "max_active_runs": dag.get("max_active_runs", 1),
                "has_task_concurrency_limits": dag.get("has_task_concurrency_limits", False),
                "has_import_errors": dag.get("has_import_errors", False),
                "next_dagrun": dag.get("next_dagrun"),
                "tags": dag.get("tags", []),
                "owners": dag.get("owners", []),
                "is_platform_generated": dag_id in generated_dag_ids,
                "status_summary": status_summary
            }

            dags_data.append(dag_info)

        return create_response(
            data={
                "dags": dags_data,
                "total_entries": airflow_dags.get("total_entries", 0),
                "limit": limit,
                "offset": offset
            },
            message="获取DAG列表成功"
        )

    except Exception as e:
        logger.error(f"获取DAG列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}", summary="获取DAG详情")
async def get_dag_detail(dag_id: str):
    """获取指定DAG的详细信息"""
    try:
        # 获取Airflow中的DAG信息
        dag_info = await airflow_client.get_dag(dag_id)

        # 获取DAG运行历史
        dag_runs = await airflow_client.get_dag_runs(dag_id, limit=10)

        # 获取状态摘要
        status_summary = await get_dag_status_summary(dag_id, days=30)

        return create_response(
            data={
                "dag_info": dag_info,
                "recent_runs": dag_runs.get("dag_runs", []),
                "status_summary": status_summary
            },
            message=f"获取DAG '{dag_id}' 详情成功"
        )

    except Exception as e:
        logger.error(f"获取DAG详情失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' 不存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/dags/{dag_id}", summary="更新DAG")
async def update_dag(dag_id: str, request: UpdateDAGRequest):
    """更新现有DAG"""
    try:
        logger.info(f"更新DAG请求: {dag_id}")

        # 准备更新配置
        task_config = {
            "dag_id": dag_id,
            "display_name": request.display_name,
            "description": request.description,
            "task_type": request.task_type,
            "tasks": request.tasks,
            "dependencies": request.dependencies or [],
            "owner": request.owner
        }

        schedule_config = None
        if request.schedule:
            schedule_config = {
                "schedule_interval": request.schedule.schedule_interval,
                "start_date": request.schedule.start_date,
                "catchup": request.schedule.catchup,
                "max_active_runs": request.schedule.max_active_runs,
                "max_active_tasks": request.schedule.max_active_tasks,
                "retries": request.schedule.retries,
                "retry_delay_seconds": request.schedule.retry_delay_seconds
            }

        # 更新DAG
        result = await dag_generator_service.update_dag(dag_id, task_config, schedule_config)

        if result["success"]:
            return create_response(
                data=result,
                message=f"DAG '{dag_id}' 更新成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "DAG更新失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/dags/{dag_id}", summary="删除DAG")
async def delete_dag(dag_id: str):
    """删除指定的DAG"""
    try:
        logger.info(f"删除DAG请求: {dag_id}")

        result = await dag_generator_service.delete_dag(dag_id)

        if result["success"]:
            return create_response(
                data=result,
                message=f"DAG '{dag_id}' 删除成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "DAG删除失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== DAG控制 ====================

@router.post("/dags/{dag_id}/trigger", summary="触发DAG运行")
async def trigger_dag(
        dag_id: str,
        conf: Optional[Dict[str, Any]] = Body(None, description="DAG配置参数"),
        execution_date: Optional[str] = Body(None, description="执行日期")
):
    """手动触发DAG运行"""
    try:
        logger.info(f"触发DAG运行: {dag_id}")

        result = await airflow_client.trigger_dag(
            dag_id=dag_id,
            conf=conf,
            execution_date=execution_date
        )

        return create_response(
            data=result,
            message=f"DAG '{dag_id}' 触发成功"
        )

    except Exception as e:
        logger.error(f"触发DAG运行失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' 不存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/{dag_id}/pause", summary="暂停DAG")
async def pause_dag(dag_id: str):
    """暂停指定的DAG"""
    try:
        result = await airflow_client.pause_dag(dag_id, is_paused=True)

        return create_response(
            data=result,
            message=f"DAG '{dag_id}' 已暂停"
        )

    except Exception as e:
        logger.error(f"暂停DAG失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' 不存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/{dag_id}/unpause", summary="启用DAG")
async def unpause_dag(dag_id: str):
    """启用指定的DAG"""
    try:
        result = await airflow_client.pause_dag(dag_id, is_paused=False)

        return create_response(
            data=result,
            message=f"DAG '{dag_id}' 已启用"
        )

    except Exception as e:
        logger.error(f"启用DAG失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' 不存在")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== DAG运行管理 ====================

@router.get("/dags/{dag_id}/runs", summary="获取DAG运行历史")
async def get_dag_runs(
        dag_id: str,
        limit: int = Query(25, ge=1, le=100, description="返回数量"),
        offset: int = Query(0, ge=0, description="偏移量"),
        state: Optional[List[str]] = Query(None, description="状态过滤"),
        start_date: Optional[str] = Query(None, description="开始日期"),
        end_date: Optional[str] = Query(None, description="结束日期")
):
    """获取DAG的运行历史"""
    try:
        result = await airflow_client.get_dag_runs(
            dag_id=dag_id,
            limit=limit,
            offset=offset,
            execution_date_gte=start_date,
            execution_date_lte=end_date,
            state=state
        )

        return create_response(
            data=result,
            message=f"获取DAG '{dag_id}' 运行历史成功"
        )

    except Exception as e:
        logger.error(f"获取DAG运行历史失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' 不存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}/runs/{dag_run_id}/tasks", summary="获取任务实例")
async def get_task_instances(dag_id: str, dag_run_id: str):
    """获取DAG运行中的任务实例"""
    try:
        result = await airflow_client.get_task_instances(
            dag_id=dag_id,
            dag_run_id=dag_run_id
        )

        return create_response(
            data=result,
            message=f"获取任务实例成功"
        )

    except Exception as e:
        logger.error(f"获取任务实例失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail="DAG运行不存在")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}/runs/{dag_run_id}/tasks/{task_id}/logs", summary="获取任务日志")
async def get_task_logs(
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: int = Query(1, description="任务尝试次数")
):
    """获取任务的执行日志"""
    try:
        result = await airflow_client.get_task_logs(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            task_try_number=task_try_number
        )

        return create_response(
            data=result,
            message="获取任务日志成功"
        )

    except Exception as e:
        logger.error(f"获取任务日志失败: {e}")
        if "404" in str(e):
            raise HTTPException(status_code=404, detail="任务不存在")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 便捷创建接口 ====================

@router.post("/dags/create-sql", summary="创建SQL任务DAG")
async def create_sql_dag_api(
        dag_id: str = Body(..., description="DAG ID"),
        sql_tasks: List[Dict[str, Any]] = Body(..., description="SQL任务列表"),
        schedule_interval: Optional[str] = Body(None, description="调度表达式"),
        display_name: Optional[str] = Body(None, description="显示名称"),
        description: Optional[str] = Body(None, description="描述")
):
    """创建SQL任务DAG的便捷接口"""
    try:
        result = await create_sql_dag(
            dag_id=dag_id,
            sql_tasks=sql_tasks,
            schedule_interval=schedule_interval,
            display_name=display_name or dag_id,
            description=description or f"SQL任务DAG - {dag_id}"
        )

        if result["success"]:
            return create_response(
                data=result,
                message=f"SQL DAG '{dag_id}' 创建成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "创建失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建SQL DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/create-datax", summary="创建DataX同步DAG")
async def create_datax_dag_api(
        dag_id: str = Body(..., description="DAG ID"),
        sync_tasks: List[Dict[str, Any]] = Body(..., description="同步任务列表"),
        schedule_interval: Optional[str] = Body(None, description="调度表达式"),
        display_name: Optional[str] = Body(None, description="显示名称"),
        description: Optional[str] = Body(None, description="描述")
):
    """创建DataX同步DAG的便捷接口"""
    try:
        result = await create_datax_dag(
            dag_id=dag_id,
            sync_tasks=sync_tasks,
            schedule_interval=schedule_interval,
            display_name=display_name or dag_id,
            description=description or f"DataX同步DAG - {dag_id}"
        )

        if result["success"]:
            return create_response(
                data=result,
                message=f"DataX DAG '{dag_id}' 创建成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "创建失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建DataX DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/create-shell", summary="创建Shell任务DAG")
async def create_shell_dag_api(
        dag_id: str = Body(..., description="DAG ID"),
        shell_tasks: List[Dict[str, Any]] = Body(..., description="Shell任务列表"),
        schedule_interval: Optional[str] = Body(None, description="调度表达式"),
        display_name: Optional[str] = Body(None, description="显示名称"),
        description: Optional[str] = Body(None, description="描述")
):
    """创建Shell任务DAG的便捷接口"""
    try:
        result = await create_shell_dag(
            dag_id=dag_id,
            shell_tasks=shell_tasks,
            schedule_interval=schedule_interval,
            display_name=display_name or dag_id,
            description=description or f"Shell任务DAG - {dag_id}"
        )

        if result["success"]:
            return create_response(
                data=result,
                message=f"Shell DAG '{dag_id}' 创建成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "创建失败")
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建Shell DAG失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 状态监控 ====================

@router.get("/overview", summary="调度系统概览")
async def get_scheduler_overview():
    """获取调度系统概览信息"""
    try:
        # 获取Airflow健康状态
        airflow_health = await airflow_client.health_check()

        # 获取DAG统计
        dags_result = await airflow_client.get_dags(limit=1000)
        total_dags = dags_result.get("total_entries", 0)

        active_dags = 0
        paused_dags = 0
        for dag in dags_result.get("dags", []):
            if dag.get("is_paused", True):
                paused_dags += 1
            else:
                active_dags += 1

        # 获取生成的DAG统计
        generated_result = await dag_generator_service.list_generated_dags()
        generated_dags = generated_result.get("total", 0)

        overview_data = {
            "airflow_status": {
                "healthy": airflow_health.get("metadatabase", {}).get("status") == "healthy",
                "scheduler_status": airflow_health.get("scheduler", {}).get("status"),
                "version": await airflow_client.get_version()
            },
            "dag_statistics": {
                "total_dags": total_dags,
                "active_dags": active_dags,
                "paused_dags": paused_dags,
                "platform_generated_dags": generated_dags
            },
            "system_info": {
                "dag_folder": str(dag_generator_service.dag_folder),
                "templates_folder": str(dag_generator_service.templates_folder),
                "last_update": datetime.now().isoformat()
            }
        }

        return create_response(
            data=overview_data,
            message="获取调度系统概览成功"
        )

    except Exception as e:
        logger.error(f"获取调度系统概览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/task-status", summary="任务状态回调")
async def task_status_callback(status_update: TaskStatusUpdate):
    """接收来自Airflow的任务状态更新"""
    try:
        logger.info(f"收到任务状态更新: {status_update.dag_id}.{status_update.task_id} -> {status_update.status}")

        # 这里可以添加状态更新的处理逻辑
        # 比如更新数据库、发送通知等

        return create_response(
            data={"received": True},
            message="任务状态更新已接收"
        )

    except Exception as e:
        logger.error(f"处理任务状态更新失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overview", summary="调度系统概览 - 增强版")
async def get_scheduler_overview_enhanced():
    """获取调度系统概览信息 - 包含统计数据"""
    try:
        # 获取Airflow健康状态
        airflow_health = await airflow_client.health_check()

        # 获取DAG统计
        dags_result = await airflow_client.get_dags(limit=1000)
        total_dags = dags_result.get("total_entries", 0)

        active_dags = 0
        paused_dags = 0
        for dag in dags_result.get("dags", []):
            if dag.get("is_paused", True):
                paused_dags += 1
            else:
                active_dags += 1

        # 获取今日任务执行统计
        today = datetime.now().strftime('%Y-%m-%d')
        today_stats = await get_daily_task_statistics(today)

        # 获取生成的DAG统计
        generated_result = await dag_generator_service.list_generated_dags()
        generated_dags = generated_result.get("total", 0)

        overview_data = {
            "airflow_status": {
                "healthy": airflow_health.get("metadatabase", {}).get("status") == "healthy",
                "scheduler_status": airflow_health.get("scheduler", {}).get("status"),
                "version": await airflow_client.get_version()
            },
            "dag_statistics": {
                "total_dags": total_dags,
                "active_dags": active_dags,
                "paused_dags": paused_dags,
                "platform_generated_dags": generated_dags
            },
            "task_statistics": today_stats,
            "system_info": {
                "dag_folder": str(dag_generator_service.dag_folder),
                "templates_folder": str(dag_generator_service.templates_folder),
                "last_update": datetime.now().isoformat()
            }
        }

        return create_response(
            data=overview_data,
            message="获取调度系统概览成功"
        )

    except Exception as e:
        logger.error(f"获取调度系统概览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/task-statistics", summary="任务执行统计")
async def get_task_statistics(
        date: str = Query(..., description="统计日期 YYYY-MM-DD"),
        group_by: str = Query("hour", description="分组方式: hour, status")
):
    """获取指定日期的任务执行统计"""
    try:
        if group_by == "hour":
            stats = await get_hourly_task_statistics(date)
        else:
            stats = await get_daily_task_statistics(date)

        return create_response(
            data=stats,
            message="获取任务统计成功"
        )

    except Exception as e:
        logger.error(f"获取任务统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent-task-instances", summary="最近的任务实例")
async def get_recent_task_instances(
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        search: Optional[str] = Query(None),
        date: Optional[str] = Query(None)
):
    """获取最近的任务实例列表"""
    try:
        # 获取所有活跃DAG
        dags_result = await airflow_client.get_dags(limit=1000)
        all_task_instances = []

        formatted_date = None
        if date:
            formatted_date = f"{date}T00:00:00Z"

        # 遍历每个DAG获取最近的运行实例
        for dag in dags_result.get("dags", []):
            dag_id = dag["dag_id"]

            # 应用搜索过滤
            if search and search.lower() not in dag_id.lower():
                continue

            try:
                # 获取最近的DAG运行
                dag_runs_result = await airflow_client.get_dag_runs(
                    dag_id=dag_id,
                    limit=5,
                    execution_date_gte=formatted_date if date else None
                )

                # 遍历每个DAG运行获取任务实例
                for dag_run in dag_runs_result.get("dag_runs", []):
                    dag_run_id = dag_run["dag_run_id"]

                    try:
                        tasks_result = await airflow_client.get_task_instances(
                            dag_id=dag_id,
                            dag_run_id=dag_run_id
                        )

                        for task in tasks_result.get("task_instances", []):
                            task_instance = {
                                "workflowInstanceId": f"{dag_id}_{dag_run_id}_{task['task_id']}",
                                "workflowName": f"{dag_id} - {task['task_id']}",
                                "duration": calculate_task_duration(task),
                                "startDateTime": task.get("start_date"),
                                "endDateTime": task.get("end_date"),
                                "status": map_airflow_task_status(task.get("state", "unknown")),
                                "lastModifiedBy": dag.get("owners", ["system"])[0] if dag.get("owners") else "system",
                                "dag_id": dag_id,
                                "task_id": task["task_id"],
                                "dag_run_id": dag_run_id
                            }
                            all_task_instances.append(task_instance)

                    except Exception as task_error:
                        logger.warning(f"获取 {dag_id}/{dag_run_id} 的任务实例失败: {task_error}")
                        continue

            except Exception as dag_error:
                logger.warning(f"获取 {dag_id} 的运行历史失败: {dag_error}")
                continue

        # 按开始时间排序
        all_task_instances.sort(
            key=lambda x: x["startDateTime"] or "1970-01-01",
            reverse=True
        )

        # 分页处理
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_tasks = all_task_instances[start_idx:end_idx]

        return create_response(
            data={
                "content": paginated_tasks,
                "totalElements": len(all_task_instances),
                "currentPage": page,
                "pageSize": page_size
            },
            message="获取任务实例成功"
        )

    except Exception as e:
        logger.error(f"获取任务实例失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 辅助函数
async def get_daily_task_statistics(date: str) -> Dict[str, Any]:
    """获取指定日期的任务统计"""
    try:
        dags_result = await airflow_client.get_dags(limit=1000)

        total_tasks = 0
        success_tasks = 0
        failed_tasks = 0
        running_tasks = 0
        date_start = f"{date}T00:00:00Z"
        date_end = f"{date}T23:59:59Z"

        for dag in dags_result.get("dags", []):
            dag_id = dag["dag_id"]

            try:
                dag_runs_result = await airflow_client.get_dag_runs(
                    dag_id=dag_id,
                    limit=10,
                    execution_date_gte=date_start,
                    execution_date_lte=date_end
                )

                for dag_run in dag_runs_result.get("dag_runs", []):
                    tasks_result = await airflow_client.get_task_instances(
                        dag_id=dag_id,
                        dag_run_id=dag_run["dag_run_id"]
                    )

                    for task in tasks_result.get("task_instances", []):
                        total_tasks += 1
                        state = task.get("state", "unknown")

                        if state == "success":
                            success_tasks += 1
                        elif state in ["failed", "up_for_failure"]:
                            failed_tasks += 1
                        elif state == "running":
                            running_tasks += 1

            except Exception:
                continue

        return {
            "total_tasks": total_tasks,
            "success_tasks": success_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "success_rate": round(success_tasks / total_tasks * 100, 1) if total_tasks > 0 else 0
        }

    except Exception as e:
        logger.error(f"获取日统计失败: {e}")
        return {
            "total_tasks": 0,
            "success_tasks": 0,
            "failed_tasks": 0,
            "running_tasks": 0,
            "success_rate": 0
        }


async def get_hourly_task_statistics(date: str) -> Dict[str, Any]:
    """获取指定日期按小时分组的任务统计"""
    try:
        hourly_stats = []

        # 初始化24小时数据
        for hour in range(24):
            hourly_stats.append({
                "hour": hour,
                "time_label": f"{hour:02d}:00",
                "success_count": 0,
                "failed_count": 0,
                "running_count": 0,
                "total_count": 0
            })

        # 获取所有DAG的任务数据并按小时统计
        dags_result = await airflow_client.get_dags(limit=1000)

        date_start = f"{date}T00:00:00Z"
        date_end = f"{date}T23:59:59Z"

        for dag in dags_result.get("dags", []):
            dag_id = dag["dag_id"]

            try:
                dag_runs_result = await airflow_client.get_dag_runs(
                    dag_id=dag_id,
                    limit=10,
                    execution_date_gte=date_start,
                    execution_date_lte=date_end
                )

                for dag_run in dag_runs_result.get("dag_runs", []):
                    tasks_result = await airflow_client.get_task_instances(
                        dag_id=dag_id,
                        dag_run_id=dag_run["dag_run_id"]
                    )

                    for task in tasks_result.get("task_instances", []):
                        start_date = task.get("start_date")
                        if start_date:
                            # 解析小时
                            task_hour = datetime.fromisoformat(start_date.replace('Z', '+00:00')).hour

                            if 0 <= task_hour <= 23:
                                state = task.get("state", "unknown")
                                hourly_stats[task_hour]["total_count"] += 1

                                if state == "success":
                                    hourly_stats[task_hour]["success_count"] += 1
                                elif state in ["failed", "up_for_failure"]:
                                    hourly_stats[task_hour]["failed_count"] += 1
                                elif state == "running":
                                    hourly_stats[task_hour]["running_count"] += 1

            except Exception:
                continue

        return {
            "instanceNumLine": hourly_stats,
            "hourly_statistics": hourly_stats,
            "date": date
        }

    except Exception as e:
        logger.error(f"获取小时统计失败: {e}")
        # 返回空数据
        return {
            "instanceNumLine": [
                {
                    "hour": hour,
                    "localTime": f"{hour:02d}:00",
                    "successNum": 0,
                    "failNum": 0,
                    "runningNum": 0
                } for hour in range(24)
            ]
        }


def calculate_task_duration(task: Dict[str, Any]) -> int:
    """计算任务执行时长（秒）"""
    start_date = task.get("start_date")
    end_date = task.get("end_date")

    if not start_date:
        return 0

    try:
        start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if end_date:
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        else:
            end = datetime.now()

        return int((end - start).total_seconds())
    except:
        return 0


def map_airflow_task_status(airflow_state: str) -> str:
    """映射Airflow任务状态到前端状态"""
    status_mapping = {
        "success": "SUCCESS",
        "running": "RUNNING",
        "failed": "FAIL",
        "up_for_failure": "FAIL",
        "up_for_retry": "RUNNING",
        "queued": "PENDING",
        "scheduled": "PENDING",
        "none": "PENDING",
        "skipped": "SUCCESS",
        "upstream_failed": "FAIL",
        "removed": "ABORT",
        "deferred": "PENDING"
    }
    return status_mapping.get(airflow_state, "PENDING")