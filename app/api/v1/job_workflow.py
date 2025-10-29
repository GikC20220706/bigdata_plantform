"""
作业流API端点
提供作业流的增删改查、配置管理、运行控制等功能
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.job_workflow import (
    JobWorkflowCreate, JobWorkflowUpdate, JobWorkflowResponse,
    JobWorkflowPageQuery, JobWorkflowConfigSave, JobWorkflowSettingSave,
    JobWorkflowRun, JobWorkflowAbort
)
from app.services.job_workflow_service import job_workflow_service

router = APIRouter(prefix="/job-workflow", tags=["数据开发-作业流"])


# ==================== 作业流CRUD ====================

@router.post("/add", summary="创建作业流")
async def create_workflow(
    data: JobWorkflowCreate,
    db: AsyncSession = Depends(get_async_db),
    username: Optional[str] = None  # TODO: 从认证中获取
):
    """创建新的作业流"""
    try:
        workflow = await job_workflow_service.create_workflow(db, data, username)

        return create_response(
            data={
                "id": workflow.id,
                "name": workflow.name
            },
            message="创建作业流成功"
        )
    except Exception as e:
        logger.error(f"创建作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/page", summary="分页查询作业流列表")
async def page_workflows(
    page: int = Query(0, ge=0, description="页码（从0开始）"),
    pageSize: int = Query(10, ge=1, le=100, description="每页大小"),
    searchKeyWord: Optional[str] = Query(None, description="搜索关键词"),
    db: AsyncSession = Depends(get_async_db)
):
    """分页获取作业流列表"""
    try:
        query_params = JobWorkflowPageQuery(
            page=page,
            pageSize=pageSize,
            searchKeyWord=searchKeyWord
        )

        result = await job_workflow_service.page_workflows(db, query_params)

        # 转换为响应格式
        content = []
        for workflow in result["content"]:
            content.append({
                "id": workflow.id,
                "name": workflow.name,
                "remark": workflow.remark,
                "status": workflow.status.value,
                "nextDateTime": workflow.next_date_time.isoformat() if workflow.next_date_time else None,
                "createUsername": workflow.create_username,
                "createDateTime": workflow.created_at.isoformat() if workflow.created_at else None,
                "updateDateTime": workflow.updated_at.isoformat() if workflow.updated_at else None
            })

        return create_response(
            data={
                "content": content,
                "total": result["total"],
                "page": result["page"],
                "pageSize": result["pageSize"]
            },
            message="查询成功"
        )
    except Exception as e:
        logger.error(f"分页查询作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail/{workflow_id}", summary="获取作业流详情")
async def get_workflow_detail(
    workflow_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业流详细信息"""
    try:
        workflow = await job_workflow_service.get_workflow_by_id(db, workflow_id, load_works=True)

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={
                "id": workflow.id,
                "name": workflow.name,
                "remark": workflow.remark,
                "status": workflow.status.value,
                "webConfig": workflow.web_config,
                "cronConfig": workflow.cron_config,
                "alarmList": workflow.alarm_list,
                "otherConfig": workflow.other_config,
                "nextDateTime": workflow.next_date_time.isoformat() if workflow.next_date_time else None,
                "createUsername": workflow.create_username,
                "createDateTime": workflow.created_at.isoformat() if workflow.created_at else None,
                "updateDateTime": workflow.updated_at.isoformat() if workflow.updated_at else None,
                "works": [
                    {
                        "id": work.id,
                        "name": work.name,
                        "workType": work.work_type.value,
                        "status": work.status.value
                    }
                    for work in workflow.works
                ]
            },
            message="获取成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取作业流详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update", summary="更新作业流")
async def update_workflow(
    workflow_id: int = Body(..., embed=True),
    name: Optional[str] = Body(None),
    remark: Optional[str] = Body(None),
    db: AsyncSession = Depends(get_async_db)
):
    """更新作业流基本信息"""
    try:
        data = JobWorkflowUpdate(name=name, remark=remark)
        workflow = await job_workflow_service.update_workflow(db, workflow_id, data)

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"id": workflow.id},
            message="更新成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete/{workflow_id}", summary="删除作业流")
async def delete_workflow(
    workflow_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """删除作业流"""
    try:
        success = await job_workflow_service.delete_workflow(db, workflow_id)

        if not success:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"id": workflow_id},
            message="删除成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 流程图配置管理 ====================

@router.post("/saveConfig", summary="保存流程图配置")
async def save_workflow_config(
    data: JobWorkflowConfigSave,
    db: AsyncSession = Depends(get_async_db)
):
    """保存作业流的流程图配置"""
    try:
        workflow = await job_workflow_service.save_workflow_config(
            db, data.workflowId, data.webConfig
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"workflowId": workflow.id},
            message="保存成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"保存流程图配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/getConfig/{workflow_id}", summary="获取流程图配置")
async def get_workflow_config(
    workflow_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业流的流程图配置"""
    try:
        config = await job_workflow_service.get_workflow_config(db, workflow_id)

        if config is None:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(data=config, message="获取成功")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取流程图配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/saveSetting", summary="保存作业流设置")
async def save_workflow_setting(
    data: JobWorkflowSettingSave,
    db: AsyncSession = Depends(get_async_db)
):
    """保存作业流的定时、告警等设置"""
    try:
        workflow = await job_workflow_service.save_workflow_setting(
            db,
            data.workflowId,
            data.cronConfig,
            data.alarmList,
            data.otherConfig
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"workflowId": workflow.id},
            message="保存设置成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"保存作业流设置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 状态管理 ====================

@router.post("/publish/{workflow_id}", summary="发布作业流")
async def publish_workflow(
    workflow_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """发布作业流上线"""
    try:
        workflow = await job_workflow_service.publish_workflow(db, workflow_id)

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"id": workflow.id, "status": workflow.status.value},
            message="发布成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"发布作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/offline/{workflow_id}", summary="下线作业流")
async def offline_workflow(
    workflow_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """下线作业流"""
    try:
        workflow = await job_workflow_service.offline_workflow(db, workflow_id)

        if not workflow:
            raise HTTPException(status_code=404, detail="作业流不存在")

        return create_response(
            data={"id": workflow.id, "status": workflow.status.value},
            message="下线成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"下线作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 运行控制 ====================

@router.post("/run", summary="运行作业流")
async def run_workflow(
    data: JobWorkflowRun,
    db: AsyncSession = Depends(get_async_db),
    username: Optional[str] = None  # TODO: 从认证中获取
):
    """手动运行作业流"""
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service
        from app.models.job_instance import JobTriggerType

        result = await job_workflow_run_service.run_workflow(
            db=db,
            workflow_id=data.workflowId,
            trigger_type=JobTriggerType.MANUAL,
            trigger_user=username,
            context=data.context
        )

        return create_response(
            data=result,
            message="作业流已提交运行"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"运行作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/abort", summary="中止作业流")
async def abort_workflow(
    data: JobWorkflowAbort,
    db: AsyncSession = Depends(get_async_db)
):
    """中止正在运行的作业流"""
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service

        result = await job_workflow_run_service.abort_workflow(
            db=db,
            workflow_instance_id=data.workflowInstanceId
        )

        return create_response(
            data=result,
            message=result["message"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"中止作业流失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instance/{workflow_instance_id}", summary="获取作业流实例状态")
async def get_workflow_instance_status(
    workflow_instance_id: str,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业流实例的详细状态"""
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
        logger.error(f"获取作业流实例状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))