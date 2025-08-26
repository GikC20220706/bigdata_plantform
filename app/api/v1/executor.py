"""
任务执行器API
提供任务执行、状态查询、历史记录等功能
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel, Field
from loguru import logger

from app.utils.response import create_response
from app.services.executor_service import executor_service
from app.executors.base_executor import ExecutionContext, ExecutionStatus

router = APIRouter()


# ==================== 请求模型 ====================

class ExecuteTaskRequest(BaseModel):
    """执行任务请求模型"""
    task_type: str = Field(..., description="任务类型")
    task_config: Dict[str, Any] = Field(..., description="任务配置")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文")
    timeout: Optional[int] = Field(None, description="超时时间(秒)")

    class Config:
        schema_extra = {
            "example": {
                "task_type": "sql",
                "task_config": {
                    "task_id": "test_sql_task",
                    "sql": "SELECT COUNT(*) FROM users",
                    "connection": {
                        "type": "mysql",
                        "host": "localhost",
                        "port": 3306,
                        "username": "root",
                        "password": "password",
                        "database": "test"
                    }
                },
                "context": {
                    "dag_id": "test_dag",
                    "execution_date": "2024-01-01"
                },
                "timeout": 300
            }
        }


class BatchExecuteRequest(BaseModel):
    """批量执行任务请求模型"""
    tasks: List[Dict[str, Any]] = Field(..., description="任务定义列表")
    max_concurrent: int = Field(5, description="最大并发数", ge=1, le=20)

    class Config:
        schema_extra = {
            "example": {
                "tasks": [
                    {
                        "task_type": "sql",
                        "task_config": {
                            "task_id": "sql_task_1",
                            "sql": "SELECT 1"
                        }
                    },
                    {
                        "task_type": "shell",
                        "task_config": {
                            "task_id": "shell_task_1",
                            "command": "echo 'Hello World'"
                        }
                    }
                ],
                "max_concurrent": 3
            }
        }


class DryRunRequest(BaseModel):
    """试运行请求模型"""
    task_type: str = Field(..., description="任务类型")
    task_config: Dict[str, Any] = Field(..., description="任务配置")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文")

    class Config:
        schema_extra = {
            "example": {
                "task_type": "datax",
                "task_config": {
                    "task_id": "test_datax_task",
                    "config_file": "/opt/datax/jobs/test_job.json"
                }
            }
        }


# ==================== API端点 ====================

@router.post("/execute", summary="执行任务")
async def execute_task(request: ExecuteTaskRequest):
    """执行单个任务"""
    try:
        logger.info(f"收到任务执行请求: {request.task_type} - {request.task_config.get('task_id', 'unknown')}")

        # 构建执行上下文
        context = None
        if request.context:
            context = ExecutionContext(
                task_id=request.context.get("task_id", request.task_config.get("task_id", "unknown")),
                dag_id=request.context.get("dag_id", "manual"),
                execution_date=request.context.get("execution_date", datetime.now().strftime("%Y-%m-%d")),
                retry_number=request.context.get("retry_number", 0),
                max_retries=request.context.get("max_retries", 3),
                timeout=request.timeout or request.context.get("timeout", 3600),
                working_directory=request.context.get("working_directory"),
                environment_variables=request.context.get("environment_variables", {}),
                log_level=request.context.get("log_level", "INFO")
            )

        # 执行任务
        result = await executor_service.execute_task(
            task_type=request.task_type,
            task_config=request.task_config,
            context=context,
            timeout=request.timeout
        )

        return create_response(
            data=result.to_dict(),
            message=f"任务执行{'成功' if result.status == ExecutionStatus.SUCCESS else '失败'}"
        )

    except Exception as e:
        logger.error(f"任务执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-execute", summary="批量执行任务")
async def batch_execute_tasks(request: BatchExecuteRequest):
    """批量执行多个任务"""
    try:
        logger.info(f"收到批量执行请求: {len(request.tasks)} 个任务")

        # 准备任务定义
        task_definitions = []
        for i, task in enumerate(request.tasks):
            task_def = {
                "task_type": task["task_type"],
                "task_config": task["task_config"],
                "context": task.get("context"),
                "timeout": task.get("timeout")
            }
            task_definitions.append(task_def)

        # 批量执行
        results = await executor_service.batch_execute_tasks(
            task_definitions=task_definitions,
            max_concurrent=request.max_concurrent
        )

        # 统计结果
        success_count = sum(1 for r in results if r.status == ExecutionStatus.SUCCESS)
        failed_count = len(results) - success_count

        return create_response(
            data={
                "total_tasks": len(results),
                "success_count": success_count,
                "failed_count": failed_count,
                "results": [r.to_dict() for r in results]
            },
            message=f"批量执行完成: {success_count} 成功, {failed_count} 失败"
        )

    except Exception as e:
        logger.error(f"批量执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dry-run", summary="任务试运行")
async def dry_run_task(request: DryRunRequest):
    """任务试运行（验证模式，不实际执行）"""
    try:
        logger.info(f"收到试运行请求: {request.task_type} - {request.task_config.get('task_id', 'unknown')}")

        # 构建执行上下文
        context = None
        if request.context:
            context = ExecutionContext(
                task_id=request.context.get("task_id", request.task_config.get("task_id", "dry_run")),
                dag_id=request.context.get("dag_id", "validation"),
                execution_date=request.context.get("execution_date", datetime.now().strftime("%Y-%m-%d")),
                working_directory=request.context.get("working_directory"),
                environment_variables=request.context.get("environment_variables", {})
            )

        # 执行试运行
        result = await executor_service.dry_run_task(
            task_type=request.task_type,
            task_config=request.task_config,
            context=context
        )

        return create_response(
            data=result,
            message="试运行完成"
        )

    except Exception as e:
        logger.error(f"试运行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{execution_key}", summary="获取任务执行状态")
async def get_execution_status(execution_key: str):
    """获取指定任务的执行状态"""
    try:
        status_info = executor_service.get_execution_status(execution_key)

        if status_info is None:
            raise HTTPException(status_code=404, detail="未找到指定的执行记录")

        return create_response(
            data=status_info,
            message="获取执行状态成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取执行状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cancel/{execution_key}", summary="取消任务执行")
async def cancel_task_execution(execution_key: str):
    """取消正在执行的任务"""
    try:
        success = await executor_service.cancel_task(execution_key)

        if success:
            return create_response(
                data={"cancelled": True, "execution_key": execution_key},
                message="任务取消成功"
            )
        else:
            raise HTTPException(status_code=404, detail="未找到活跃的执行任务或取消失败")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"取消任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/active", summary="获取活跃的执行任务")
async def list_active_executions():
    """获取所有正在执行的任务"""
    try:
        active_tasks = executor_service.list_active_executions()

        return create_response(
            data={
                "active_count": len(active_tasks),
                "tasks": active_tasks
            },
            message=f"获取活跃任务成功，共 {len(active_tasks)} 个"
        )

    except Exception as e:
        logger.error(f"获取活跃任务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", summary="获取执行历史")
async def get_execution_history(
        limit: int = Query(100, ge=1, le=1000, description="返回记录数限制"),
        status_filter: Optional[str] = Query(None, description="状态过滤"),
        start_date: Optional[str] = Query(None, description="开始日期(YYYY-MM-DD)"),
        end_date: Optional[str] = Query(None, description="结束日期(YYYY-MM-DD)")
):
    """获取任务执行历史记录"""
    try:
        # 处理状态过滤
        status_enum = None
        if status_filter:
            try:
                status_enum = ExecutionStatus(status_filter)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的状态值: {status_filter}")

        # 处理日期过滤
        start_datetime = None
        end_datetime = None

        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="开始日期格式错误，应为YYYY-MM-DD")

        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="结束日期格式错误，应为YYYY-MM-DD")

        # 获取历史记录
        history = executor_service.get_execution_history(
            limit=limit,
            status_filter=status_enum,
            start_date=start_datetime,
            end_date=end_datetime
        )

        return create_response(
            data={
                "total_returned": len(history),
                "history": history,
                "filters": {
                    "limit": limit,
                    "status_filter": status_filter,
                    "start_date": start_date,
                    "end_date": end_date
                }
            },
            message=f"获取执行历史成功，共 {len(history)} 条记录"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取执行历史失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supported-types", summary="获取支持的任务类型")
async def get_supported_task_types():
    """获取所有支持的任务类型及其信息"""
    try:
        supported_types = executor_service.get_supported_task_types()

        return create_response(
            data=supported_types,
            message=f"获取支持的任务类型成功，共 {len(supported_types)} 种"
        )

    except Exception as e:
        logger.error(f"获取支持的任务类型失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", summary="获取执行器服务统计信息")
async def get_service_stats():
    """获取执行器服务的统计信息"""
    try:
        stats = executor_service.get_service_stats()

        return create_response(
            data=stats,
            message="获取服务统计信息成功"
        )

    except Exception as e:
        logger.error(f"获取服务统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", summary="执行器服务健康检查")
async def health_check():
    """检查执行器服务和所有注册执行器的健康状态"""
    try:
        health_info = await executor_service.health_check()

        return create_response(
            data=health_info,
            message="健康检查完成"
        )

    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 特定任务类型的便捷接口 ====================

@router.post("/sql/execute", summary="执行SQL任务")
async def execute_sql_task(
        task_id: str = Body(..., description="任务ID"),
        sql: Optional[str] = Body(None, description="SQL语句"),
        sql_file: Optional[str] = Body(None, description="SQL文件路径"),
        connection: Optional[Dict[str, Any]] = Body(None, description="数据库连接配置"),
        data_source: Optional[str] = Body(None, description="数据源名称"),
        limit: int = Body(1000, description="查询结果限制", ge=1, le=10000),
        timeout: int = Body(300, description="超时时间(秒)", ge=1)
):
    """SQL任务的便捷执行接口"""
    try:
        # 构建任务配置
        task_config = {
            "task_id": task_id,
            "sql": sql,
            "sql_file": sql_file,
            "connection": connection,
            "data_source": data_source,
            "limit": limit,
            "fetch_data": True
        }

        # 移除空值
        task_config = {k: v for k, v in task_config.items() if v is not None}

        # 执行任务
        result = await executor_service.execute_task(
            task_type="sql",
            task_config=task_config,
            timeout=timeout
        )

        return create_response(
            data=result.to_dict(),
            message=f"SQL任务执行{'成功' if result.status == ExecutionStatus.SUCCESS else '失败'}"
        )

    except Exception as e:
        logger.error(f"SQL任务执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shell/execute", summary="执行Shell任务")
async def execute_shell_task(
        task_id: str = Body(..., description="任务ID"),
        command: Optional[str] = Body(None, description="Shell命令"),
        script_file: Optional[str] = Body(None, description="脚本文件路径"),
        shell: str = Body("/bin/bash", description="Shell类型"),
        cwd: Optional[str] = Body(None, description="工作目录"),
        environment: Optional[Dict[str, str]] = Body(None, description="环境变量"),
        timeout: int = Body(300, description="超时时间(秒)", ge=1)
):
    """Shell任务的便捷执行接口"""
    try:
        # 构建任务配置
        task_config = {
            "task_id": task_id,
            "command": command,
            "script_file": script_file,
            "shell": shell,
            "cwd": cwd,
            "environment": environment or {}
        }

        # 移除空值
        task_config = {k: v for k, v in task_config.items() if v is not None}

        # 执行任务
        result = await executor_service.execute_task(
            task_type="shell",
            task_config=task_config,
            timeout=timeout
        )

        return create_response(
            data=result.to_dict(),
            message=f"Shell任务执行{'成功' if result.status == ExecutionStatus.SUCCESS else '失败'}"
        )

    except Exception as e:
        logger.error(f"Shell任务执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datax/execute", summary="执行DataX任务")
async def execute_datax_task(
        task_id: str = Body(..., description="任务ID"),
        config: Optional[Dict[str, Any]] = Body(None, description="DataX配置"),
        config_file: Optional[str] = Body(None, description="DataX配置文件路径"),
        jvm_params: Optional[List[str]] = Body(None, description="JVM参数"),
        datax_params: Optional[Dict[str, Any]] = Body(None, description="DataX参数"),
        timeout: int = Body(1800, description="超时时间(秒)", ge=1)
):
    """DataX任务的便捷执行接口"""
    try:
        # 构建任务配置
        task_config = {
            "task_id": task_id,
            "config": config,
            "config_file": config_file,
            "jvm_params": jvm_params,
            "datax_params": datax_params or {}
        }

        # 移除空值
        task_config = {k: v for k, v in task_config.items() if v is not None}

        # 执行任务
        result = await executor_service.execute_task(
            task_type="datax",
            task_config=task_config,
            timeout=timeout
        )

        return create_response(
            data=result.to_dict(),
            message=f"DataX任务执行{'成功' if result.status == ExecutionStatus.SUCCESS else '失败'}"
        )

    except Exception as e:
        logger.error(f"DataX任务执行失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 实用工具接口 ====================

@router.post("/cleanup-history", summary="清理历史记录")
async def cleanup_execution_history():
    """清理过期的执行历史记录"""
    try:
        await executor_service.cleanup_old_history()

        return create_response(
            data={"cleaned": True},
            message="历史记录清理完成"
        )

    except Exception as e:
        logger.error(f"清理历史记录失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates/{task_type}", summary="获取任务模板")
async def get_task_template(task_type: str):
    """获取指定任务类型的配置模板"""
    try:
        templates = {
            "sql": {
                "task_id": "example_sql_task",
                "sql": "SELECT COUNT(*) as total FROM your_table WHERE date = '{{ ds }}'",
                "connection": {
                    "type": "mysql",
                    "host": "localhost",
                    "port": 3306,
                    "username": "user",
                    "password": "password",
                    "database": "your_db"
                },
                "limit": 1000
            },
            "shell": {
                "task_id": "example_shell_task",
                "command": "echo 'Hello World' && date",
                "shell": "/bin/bash",
                "environment": {
                    "CUSTOM_VAR": "value"
                }
            },
            "datax": {
                "task_id": "example_datax_task",
                "config": {
                    "job": {
                        "content": [{
                            "reader": {
                                "name": "mysqlreader",
                                "parameter": {
                                    "username": "source_user",
                                    "password": "source_pass",
                                    "connection": [{
                                        "jdbcUrl": ["jdbc:mysql://source:3306/source_db"],
                                        "table": ["source_table"]
                                    }],
                                    "column": ["*"]
                                }
                            },
                            "writer": {
                                "name": "mysqlwriter",
                                "parameter": {
                                    "username": "target_user",
                                    "password": "target_pass",
                                    "connection": [{
                                        "jdbcUrl": "jdbc:mysql://target:3306/target_db",
                                        "table": ["target_table"]
                                    }],
                                    "column": ["*"]
                                }
                            }
                        }],
                        "setting": {
                            "speed": {"channel": 3},
                            "errorLimit": {"record": 0, "percentage": 0.02}
                        }
                    }
                }
            }
        }

        if task_type not in templates:
            supported_types = list(templates.keys())
            raise HTTPException(
                status_code=404,
                detail=f"不支持的任务类型: {task_type}，支持的类型: {supported_types}"
            )

        return create_response(
            data={
                "task_type": task_type,
                "template": templates[task_type],
                "description": f"{task_type.upper()}任务配置模板"
            },
            message="获取任务模板成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))