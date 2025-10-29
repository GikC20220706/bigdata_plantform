"""
作业流和作业实例查询API
提供实例历史、状态查询、日志查看等功能
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.services.job_instance_service import job_instance_service

router = APIRouter(prefix="/job-instance", tags=["数据开发-作业实例"])


# ==================== 作业流实例查询 ====================

@router.get("/workflow/list", summary="查询作业流实例列表")
async def list_workflow_instances(
        workflowId: Optional[int] = Query(None, description="作业流ID"),
        status: Optional[str] = Query(None, description="状态"),
        triggerType: Optional[str] = Query(None, description="触发类型"),
        page: int = Query(0, ge=0, description="页码"),
        pageSize: int = Query(10, ge=1, le=100, description="每页大小"),
        db: AsyncSession = Depends(get_async_db)
):
    """分页查询作业流实例列表"""
    try:
        result = await job_instance_service.list_workflow_instances(
            db=db,
            workflow_id=workflowId,
            status=status,
            trigger_type=triggerType,
            page=page,
            page_size=pageSize
        )

        return create_response(
            data=result,
            message="查询成功"
        )
    except Exception as e:
        logger.error(f"查询作业流实例列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflow/{workflow_instance_id}", summary="获取作业流实例详情")
async def get_workflow_instance_detail(
        workflow_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业流实例的详细信息"""
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service

        result = await job_workflow_run_service.get_workflow_instance_status(
            db=db,
            workflow_instance_id=workflow_instance_id
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取作业流实例详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 作业实例查询 ====================

@router.get("/work/list", summary="查询作业实例列表")
async def list_work_instances(
        workflowInstanceId: Optional[str] = Query(None, description="作业流实例ID"),
        workId: Optional[int] = Query(None, description="作业ID"),
        status: Optional[str] = Query(None, description="状态"),
        page: int = Query(0, ge=0, description="页码"),
        pageSize: int = Query(10, ge=1, le=100, description="每页大小"),
        db: AsyncSession = Depends(get_async_db)
):
    """分页查询作业实例列表"""
    try:
        result = await job_instance_service.list_work_instances(
            db=db,
            workflow_instance_id=workflowInstanceId,
            work_id=workId,
            status=status,
            page=page,
            page_size=pageSize
        )

        return create_response(
            data=result,
            message="查询成功"
        )
    except Exception as e:
        logger.error(f"查询作业实例列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/work/{work_instance_id}", summary="获取作业实例详情")
async def get_work_instance_detail(
        work_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的详细信息"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.get_work_instance_status(
            db=db,
            work_instance_id=work_instance_id
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取作业实例详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/work/{work_instance_id}/log", summary="获取作业实例日志")
async def get_work_instance_log(
        work_instance_id: str,
        log_type: str = Query("all", regex="^(submit|running|all)$", description="日志类型"),
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的执行日志"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.get_work_instance_log(
            db=db,
            work_instance_id=work_instance_id,
            log_type=log_type
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取作业日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 统计查询 ====================

@router.get("/statistics/workflow/{workflow_id}", summary="获取作业流执行统计")
async def get_workflow_statistics(
        workflow_id: int,
        days: int = Query(7, ge=1, le=30, description="统计天数"),
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业流的执行统计信息"""
    try:
        result = await job_instance_service.get_workflow_statistics(
            db=db,
            workflow_id=workflow_id,
            days=days
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except Exception as e:
        logger.error(f"获取作业流统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/work/{work_id}", summary="获取作业执行统计")
async def get_work_statistics(
        work_id: int,
        days: int = Query(7, ge=1, le=30, description="统计天数"),
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业的执行统计信息"""
    try:
        result = await job_instance_service.get_work_statistics(
            db=db,
            work_id=work_id,
            days=days
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except Exception as e:
        logger.error(f"获取作业统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))