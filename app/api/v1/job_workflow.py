"""
作业流API端点
提供作业流的增删改查、配置管理、运行控制等功能
"""
import asyncio
import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.models import JobTriggerType, JobInstanceStatus
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


@router.post("/instance/{workflow_instance_id}/rerun", summary="重跑工作流实例")
async def rerun_workflow_instance(
        workflow_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """
    重跑已完成的工作流实例
    会创建新的实例，使用相同的配置重新执行
    """
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service
        from sqlalchemy import select
        from app.models import JobWorkflowInstance, JobWorkflow

        # 1. 获取原工作流实例
        result = await db.execute(
            select(JobWorkflowInstance).where(
                JobWorkflowInstance.workflow_instance_id == workflow_instance_id
            )
        )
        old_instance = result.scalar_one_or_none()

        if not old_instance:
            raise HTTPException(status_code=404, detail="工作流实例不存在")

        # 2. 检查原实例状态
        if old_instance.status not in [JobInstanceStatus.SUCCESS, JobInstanceStatus.FAIL, JobInstanceStatus.ABORT]:
            raise HTTPException(
                status_code=400,
                detail=f"工作流实例状态为 {old_instance.status.value}，只有已完成的实例才能重跑"
            )

        # 3. 获取工作流配置
        workflow_result = await db.execute(
            select(JobWorkflow).where(JobWorkflow.id == old_instance.workflow_id)
        )
        workflow = workflow_result.scalar_one_or_none()

        if not workflow:
            raise HTTPException(status_code=404, detail="工作流配置不存在")

        # 4. 重新运行工作流
        result = await job_workflow_run_service.run_workflow(
            db=db,
            workflow_id=workflow.id,
            trigger_type=JobTriggerType.MANUAL,
            trigger_user=old_instance.last_modified_by,
            context={}
        )

        logger.info(f"工作流重跑成功: 原实例={workflow_instance_id}, 新实例={result['workflowInstanceId']}")

        return create_response(
            data=result,
            message=f"工作流已重新运行"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重跑工作流失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{workflow_instance_id}/run-after-node", summary="重跑下游节点")
async def run_after_node(
        workflow_instance_id: str,
        work_id: int = Body(..., description="从哪个作业节点开始重跑"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    从指定节点开始重跑下游节点
    只执行该节点及其后续的所有节点
    """
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service
        from sqlalchemy import select
        from app.models import JobWorkflowInstance, JobWorkflow, JobWork

        # 1. 获取工作流实例
        result = await db.execute(
            select(JobWorkflowInstance).where(
                JobWorkflowInstance.workflow_instance_id == workflow_instance_id
            )
        )
        workflow_instance = result.scalar_one_or_none()

        if not workflow_instance:
            raise HTTPException(status_code=404, detail="工作流实例不存在")

        # 2. 获取工作流配置
        workflow_result = await db.execute(
            select(JobWorkflow).where(JobWorkflow.id == workflow_instance.workflow_id)
        )
        workflow = workflow_result.scalar_one_or_none()

        if not workflow:
            raise HTTPException(status_code=404, detail="工作流配置不存在")

        # 3. 解析工作流配置，获取执行顺序
        execution_plan = await job_workflow_run_service._parse_workflow_config(workflow)
        work_order = execution_plan.get('workOrder', [])

        # 4. 找到指定节点的位置
        if work_id not in work_order:
            raise HTTPException(status_code=400, detail="指定的作业不在工作流中")

        start_index = work_order.index(work_id)
        downstream_works = work_order[start_index:]  # 包含当前节点及下游

        logger.info(f"开始重跑下游节点: 从work_id={work_id}, 共{len(downstream_works)}个节点")

        # 5. 获取需要重跑的作业
        works_result = await db.execute(
            select(JobWork).where(JobWork.id.in_(downstream_works))
        )
        works_to_rerun = works_result.scalars().all()

        # 6. 创建新的工作流实例
        new_workflow_instance_id = f"WF_{uuid.uuid4().hex[:16].upper()}"
        new_instance = JobWorkflowInstance(
            workflow_instance_id=new_workflow_instance_id,
            workflow_id=workflow.id,
            workflow_name=f"{workflow.name}(重跑下游)",
            status=JobInstanceStatus.RUNNING,
            trigger_type=JobTriggerType.MANUAL,
            last_modified_by=workflow_instance.last_modified_by,
            start_datetime=datetime.now()
        )
        db.add(new_instance)
        await db.commit()

        # 7. 为下游节点创建作业实例
        work_instances = await job_workflow_run_service._create_work_instances(
            db, new_instance, works_to_rerun
        )

        # 8. 异步执行下游节点
        asyncio.create_task(
            job_workflow_run_service._execute_workflow_async(
                new_workflow_instance_id,
                {'workOrder': downstream_works},
                work_instances,
                {}
            )
        )

        return create_response(
            data={
                "workflowInstanceId": new_workflow_instance_id,
                "originalInstanceId": workflow_instance_id,
                "startWorkId": work_id,
                "downstreamCount": len(downstream_works)
            },
            message=f"已开始重跑下游{len(downstream_works)}个节点"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重跑下游节点失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{workflow_instance_id}/abort", summary="中断工作流")
async def abort_workflow_instance(
        workflow_instance_id: str,
        reason: str = Body(None, description="中断原因"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    中断正在运行的工作流实例
    会停止所有正在运行的作业
    """
    try:
        from app.services.job_workflow_run_service import job_workflow_run_service

        # 调用中止服务
        result = await job_workflow_run_service.abort_workflow(
            db=db,
            workflow_instance_id=workflow_instance_id
        )

        logger.info(f"工作流已中断: {workflow_instance_id}, 原因: {reason or '用户手动中断'}")

        return create_response(
            data=result,
            message="工作流已中断"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"中断工作流失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{workflow_instance_id}/rerun-current-node", summary="重跑当前节点")
async def rerun_current_node(
        workflow_instance_id: str,
        work_instance_id: str = Body(..., description="要重跑的作业实例ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    重跑指定的单个作业节点
    只重新执行这一个节点，不影响其他节点
    """
    try:
        from app.services.job_work_run_service import job_work_run_service
        from sqlalchemy import select
        from app.models import JobWorkInstance, JobWork

        # 1. 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(
                JobWorkInstance.instance_id == work_instance_id
            )
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        # 2. 验证作业实例属于指定的工作流实例
        if work_instance.workflow_instance_id != workflow_instance_id:
            raise HTTPException(status_code=400, detail="作业实例不属于指定的工作流实例")

        # 3. 获取作业配置
        work_result = await db.execute(
            select(JobWork).where(JobWork.id == work_instance.work_id)
        )
        work = work_result.scalar_one_or_none()

        if not work:
            raise HTTPException(status_code=404, detail="作业配置不存在")

        # 4. 创建新的作业实例用于重跑
        new_work_instance_id = f"WORK_{uuid.uuid4().hex[:16].upper()}"
        new_work_instance = JobWorkInstance(
            instance_id=new_work_instance_id,
            workflow_instance_id=workflow_instance_id,
            work_id=work.id,
            work_name=f"{work.name}(重跑)",
            work_type=work.work_type.value,
            status=JobInstanceStatus.PENDING
        )

        db.add(new_work_instance)
        await db.commit()
        await db.refresh(new_work_instance)

        # 5. 执行作业
        from app.services.executors import executor_manager

        # 更新状态为运行中
        new_work_instance.status = JobInstanceStatus.RUNNING
        new_work_instance.start_datetime = datetime.now()
        await db.commit()

        # 获取执行器
        executor = executor_manager.get_executor(work.executor)
        if not executor:
            raise ValueError(f"找不到执行器: {work.executor}")

        # 执行作业
        exec_result = await executor.execute(
            db, work.config or {}, new_work_instance.instance_id, {}
        )

        # 更新执行结果
        if exec_result.get('success'):
            new_work_instance.status = JobInstanceStatus.SUCCESS
            new_work_instance.result_data = exec_result.get('data')
        else:
            new_work_instance.status = JobInstanceStatus.FAIL
            new_work_instance.error_message = exec_result.get('error')

        new_work_instance.end_datetime = datetime.now()
        await db.commit()

        logger.info(
            f"单节点重跑完成: work_instance_id={new_work_instance_id}, "
            f"状态={new_work_instance.status.value}"
        )

        return create_response(
            data={
                "workInstanceId": new_work_instance_id,
                "originalWorkInstanceId": work_instance_id,
                "status": new_work_instance.status.value,
                "workName": work.name
            },
            message=f"节点 '{work.name}' 重跑完成"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重跑当前节点失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
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


@router.delete("/instance/{workflow_instance_id}", summary="删除工作流实例")
async def delete_workflow_instance(
        workflow_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除工作流实例
    注意：只能删除已完成的实例（SUCCESS、FAIL、ABORT）
    """
    try:
        from sqlalchemy import select, delete
        from app.models import JobWorkflowInstance, JobWorkInstance

        # 1. 获取工作流实例
        result = await db.execute(
            select(JobWorkflowInstance).where(
                JobWorkflowInstance.workflow_instance_id == workflow_instance_id
            )
        )
        workflow_instance = result.scalar_one_or_none()

        if not workflow_instance:
            raise HTTPException(status_code=404, detail="工作流实例不存在")

        # 2. 检查状态 - 只能删除已完成的实例
        if workflow_instance.status in [JobInstanceStatus.RUNNING, JobInstanceStatus.PENDING]:
            raise HTTPException(
                status_code=400,
                detail=f"工作流实例正在运行中，无法删除。请先中止工作流。"
            )

        # 3. 删除关联的作业实例
        await db.execute(
            delete(JobWorkInstance).where(
                JobWorkInstance.workflow_instance_id == workflow_instance_id
            )
        )

        # 4. 删除工作流实例
        await db.execute(
            delete(JobWorkflowInstance).where(
                JobWorkflowInstance.workflow_instance_id == workflow_instance_id
            )
        )

        await db.commit()

        logger.info(f"工作流实例删除成功: {workflow_instance_id}")

        return create_response(
            data={"workflowInstanceId": workflow_instance_id},
            message="工作流实例删除成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"删除工作流实例失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
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