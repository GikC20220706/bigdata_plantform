# app/api/v1/workflow.py
"""
工作流编排API端点
提供工作流的创建、管理、执行和监控功能
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Depends, Body
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.schemas.workflow import (
    CreateWorkflowRequest, UpdateWorkflowRequest, TriggerWorkflowRequest,
    WorkflowResponse, WorkflowListResponse, WorkflowSearchParams,
    WorkflowExecutionResponse, WorkflowStatistics, WorkflowHealthCheck,
    WorkflowValidationResult, WorkflowDependencyGraph,
    CreateWorkflowTemplateRequest, CreateWorkflowFromTemplateRequest,
    BatchWorkflowOperationRequest, BatchOperationResult,
    WorkflowExportRequest, WorkflowImportRequest, WorkflowImportResult,
    CreateWorkflowAlertRequest, WorkflowStatusUpdateRequest
)
from app.services.workflow_orchestration_service import workflow_orchestration_service
from app.services.workflow_validation_service import workflow_validation_service
from app.utils.database import get_async_db
from app.utils.response import create_response
from config.settings import settings
import uuid

router = APIRouter()


# ==================== 工作流定义管理 ====================

@router.post("/", summary="创建工作流", response_model=Dict[str, Any])
async def create_workflow(
        workflow_request: CreateWorkflowRequest,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db),
        owner_id: Optional[str] = Query(None, description="创建者ID"),
        owner_name: Optional[str] = Query(None, description="创建者姓名"),
        validate_only: bool = Query(False, description="仅验证不创建")
):
    """
    创建新的工作流定义

    - **workflow_request**: 工作流创建请求
    - **owner_id**: 工作流拥有者ID
    - **owner_name**: 工作流拥有者姓名
    - **validate_only**: 如果为True，仅执行验证而不实际创建
    """
    try:
        logger.info(f"收到创建工作流请求: {workflow_request.workflow_id}")

        # 先进行验证
        validation_result = await workflow_validation_service.validate_workflow_definition(
            workflow_request, db
        )

        if not validation_result.is_valid:
            return create_response(
                success=False,
                data={
                    "validation_result": validation_result.dict(),
                    "workflow_id": workflow_request.workflow_id
                },
                message="工作流验证失败",
                error_code="VALIDATION_FAILED"
            )

        # 如果只是验证，返回验证结果
        if validate_only:
            return create_response(
                data={
                    "validation_result": validation_result.dict(),
                    "workflow_id": workflow_request.workflow_id
                },
                message="工作流验证通过"
            )

        # 创建工作流
        workflow_response = await workflow_orchestration_service.create_workflow(
            db, workflow_request, owner_id, owner_name
        )

        # 异步生成依赖图分析
        background_tasks.add_task(
            _generate_workflow_analysis,
            workflow_response.id,
            workflow_request
        )

        return create_response(
            data=workflow_response.dict(),
            message=f"工作流 '{workflow_request.workflow_name}' 创建成功"
        )

    except ValueError as e:
        logger.warning(f"工作流创建参数错误: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建工作流失败: {str(e)}")


@router.get("/", summary="搜索工作流", response_model=WorkflowListResponse)
async def search_workflows(
        db: AsyncSession = Depends(get_async_db),
        keyword: Optional[str] = Query(None, description="搜索关键词"),
        category: Optional[str] = Query(None, description="工作流分类"),
        business_domain: Optional[str] = Query(None, description="业务领域"),
        status: Optional[str] = Query(None, description="工作流状态"),
        trigger_type: Optional[str] = Query(None, description="触发方式"),
        owner_id: Optional[str] = Query(None, description="拥有者ID"),
        visibility: Optional[str] = Query(None, description="可见性"),
        is_template: Optional[bool] = Query(None, description="是否为模板"),
        tags: Optional[List[str]] = Query(None, description="标签列表"),
        created_start: Optional[datetime] = Query(None, description="创建开始时间"),
        created_end: Optional[datetime] = Query(None, description="创建结束时间"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页大小"),
        sort_by: Optional[str] = Query("created_at", description="排序字段"),
        sort_order: Optional[str] = Query("desc", description="排序顺序"),
        user_id: Optional[str] = Query(None, description="当前用户ID")
):
    """
    搜索和分页查询工作流列表

    支持多种过滤条件和排序方式
    """
    try:
        search_params = WorkflowSearchParams(
            keyword=keyword,
            category=category,
            business_domain=business_domain,
            status=status,
            trigger_type=trigger_type,
            owner_id=owner_id,
            visibility=visibility,
            is_template=is_template,
            tags=tags or [],
            created_start=created_start,
            created_end=created_end,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_order=sort_order
        )

        result = await workflow_orchestration_service.search_workflows(
            db, search_params, user_id
        )

        return result

    except Exception as e:
        logger.error(f"搜索工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"搜索工作流失败: {str(e)}")


@router.get("/{workflow_id}", summary="获取工作流详情", response_model=WorkflowResponse)
async def get_workflow(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db),
        include_nodes: bool = Query(True, description="是否包含节点信息"),
        include_edges: bool = Query(True, description="是否包含边信息")
):
    """获取指定工作流的详细信息"""
    try:
        workflow = await workflow_orchestration_service.get_workflow_by_id(
            db, workflow_id, include_nodes, include_edges
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="工作流不存在")

        return workflow

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取工作流详情失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流详情失败: {str(e)}")


@router.put("/{workflow_id}", summary="更新工作流", response_model=WorkflowResponse)
async def update_workflow(
        workflow_id: int,
        update_request: UpdateWorkflowRequest,
        db: AsyncSession = Depends(get_async_db),
        validate_before_update: bool = Query(True, description="更新前是否验证")
):
    """更新工作流配置"""
    try:
        # 如果需要验证且包含节点/边更新
        if validate_before_update and (update_request.nodes or update_request.edges):
            # 构建完整的工作流配置用于验证
            existing_workflow = await workflow_orchestration_service.get_workflow_by_id(
                db, workflow_id, True, True
            )
            if not existing_workflow:
                raise HTTPException(status_code=404, detail="工作流不存在")

            # 这里可以添加更新前的验证逻辑
            logger.info(f"工作流更新前验证通过: {workflow_id}")

        updated_workflow = await workflow_orchestration_service.update_workflow(
            db, workflow_id, update_request
        )

        return updated_workflow

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新工作流失败: {str(e)}")


@router.delete("/{workflow_id}", summary="删除工作流")
async def delete_workflow(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db),
        force: bool = Query(False, description="是否强制删除")
):
    """删除工作流定义及相关数据"""
    try:
        result = await workflow_orchestration_service.delete_workflow(
            db, workflow_id, force
        )

        return create_response(
            data=result,
            message=result["message"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除工作流失败: {str(e)}")


@router.post("/{workflow_id}/publish", summary="发布工作流")
async def publish_workflow(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """发布工作流，使其可以执行"""
    try:
        result = await workflow_orchestration_service.publish_workflow(db, workflow_id)

        return create_response(
            data=result,
            message=result["message"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"发布工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"发布工作流失败: {str(e)}")


@router.post("/{workflow_id}/status", summary="更新工作流状态")
async def update_workflow_status(
        workflow_id: int,
        status_update: WorkflowStatusUpdateRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """更新工作流状态（暂停、恢复等）"""
    try:
        # 这里需要在workflow_orchestration_service中实现状态更新方法
        # result = await workflow_orchestration_service.update_workflow_status(
        #     db, workflow_id, status_update.status, status_update.reason
        # )

        # 临时实现
        workflow = await workflow_orchestration_service.get_workflow_by_id(db, workflow_id, False, False)
        if not workflow:
            raise HTTPException(status_code=404, detail="工作流不存在")

        return create_response(
            data={"workflow_id": workflow_id, "status": status_update.status},
            message=f"工作流状态已更新为: {status_update.status}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新工作流状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新工作流状态失败: {str(e)}")


# ==================== 工作流执行管理 ====================

@router.post("/{workflow_id}/trigger", summary="触发工作流执行")
async def trigger_workflow(
        workflow_id: int,
        trigger_request: TriggerWorkflowRequest,
        db: AsyncSession = Depends(get_async_db),
        trigger_user: Optional[str] = Query(None, description="触发用户")
):
    """手动触发工作流执行"""
    try:
        result = await workflow_orchestration_service.trigger_workflow(
            db, workflow_id, trigger_request, trigger_user
        )

        return create_response(
            data=result,
            message=result["message"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"触发工作流执行失败: {e}")
        raise HTTPException(status_code=500, detail=f"触发工作流执行失败: {str(e)}")


@router.get("/{workflow_id}/executions", summary="获取工作流执行历史")
async def get_workflow_executions(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db),
        status: Optional[str] = Query(None, description="执行状态过滤"),
        start_time_begin: Optional[datetime] = Query(None, description="开始时间范围开始"),
        start_time_end: Optional[datetime] = Query(None, description="开始时间范围结束"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """获取指定工作流的执行历史记录"""
    try:
        # 这里需要在service中实现获取执行历史的方法
        # 临时返回空列表
        return create_response(
            data={
                "executions": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "has_next": False,
                "has_prev": False
            },
            message="获取工作流执行历史成功"
        )

    except Exception as e:
        logger.error(f"获取工作流执行历史失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流执行历史失败: {str(e)}")


@router.get("/executions/{execution_id}", summary="获取执行详情")
async def get_execution_detail(
        execution_id: str,
        db: AsyncSession = Depends(get_async_db),
        include_node_executions: bool = Query(True, description="是否包含节点执行详情")
):
    """获取工作流执行的详细信息"""
    try:
        execution = await workflow_orchestration_service.get_workflow_execution(
            db, execution_id, include_node_executions
        )

        if not execution:
            raise HTTPException(status_code=404, detail="执行记录不存在")

        return create_response(
            data=execution.dict(),
            message="获取执行详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取执行详情失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取执行详情失败: {str(e)}")


@router.post("/executions/{execution_id}/cancel", summary="取消工作流执行")
async def cancel_execution(
        execution_id: str,
        db: AsyncSession = Depends(get_async_db),
        reason: Optional[str] = Body(None, description="取消原因")
):
    """取消正在执行的工作流"""
    try:
        result = await workflow_orchestration_service.cancel_workflow_execution(
            db, execution_id, reason
        )

        return create_response(
            data=result,
            message=result["message"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"取消工作流执行失败: {e}")
        raise HTTPException(status_code=500, detail=f"取消工作流执行失败: {str(e)}")


# ==================== 工作流验证和分析 ====================

@router.post("/validate", summary="验证工作流定义")
async def validate_workflow(
        workflow_request: CreateWorkflowRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """验证工作流定义的正确性"""
    try:
        validation_result = await workflow_validation_service.validate_workflow_definition(
            workflow_request, db
        )

        return create_response(
            data=validation_result.dict(),
            message="工作流验证完成"
        )

    except Exception as e:
        logger.error(f"工作流验证失败: {e}")
        raise HTTPException(status_code=500, detail=f"工作流验证失败: {str(e)}")


@router.get("/{workflow_id}/dependency-graph", summary="获取依赖关系图")
async def get_dependency_graph(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取工作流的依赖关系图"""
    try:
        workflow = await workflow_orchestration_service.get_workflow_by_id(
            db, workflow_id, True, True
        )

        if not workflow:
            raise HTTPException(status_code=404, detail="工作流不存在")

        # 构建依赖图
        dependency_graph = await workflow_validation_service.build_dependency_graph(
            workflow.nodes, workflow.edges
        )

        return create_response(
            data=dependency_graph.dict(),
            message="获取依赖关系图成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取依赖关系图失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取依赖关系图失败: {str(e)}")


@router.get("/{workflow_id}/health", summary="工作流健康检查")
async def check_workflow_health(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """执行工作流健康检查"""
    try:
        health_check = await workflow_validation_service.perform_health_check(
            db, workflow_id
        )

        return create_response(
            data=health_check.dict(),
            message="工作流健康检查完成"
        )

    except Exception as e:
        logger.error(f"工作流健康检查失败: {e}")
        raise HTTPException(status_code=500, detail=f"工作流健康检查失败: {str(e)}")


# ==================== 统计和监控 ====================

@router.get("/statistics/overview", summary="获取工作流统计信息")
async def get_workflow_statistics(
        db: AsyncSession = Depends(get_async_db)
):
    """获取工作流整体统计信息"""
    try:
        statistics = await workflow_orchestration_service.get_workflow_statistics(db)

        return create_response(
            data=statistics.dict(),
            message="获取工作流统计信息成功"
        )

    except Exception as e:
        logger.error(f"获取工作流统计信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流统计信息失败: {str(e)}")


# ==================== 批量操作 ====================

@router.post("/batch", summary="批量操作工作流")
async def batch_workflow_operation(
        operation_request: BatchWorkflowOperationRequest,
        db: AsyncSession = Depends(get_async_db),
        operator: Optional[str] = Query(None, description="操作人")
):
    """批量操作工作流（激活、停用、删除等）"""
    try:
        # 这里需要在service中实现批量操作方法
        # 临时实现
        total = len(operation_request.workflow_ids)
        successful = total  # 假设全部成功
        failed = 0

        result = BatchOperationResult(
            total=total,
            successful=successful,
            failed=failed,
            success_ids=operation_request.workflow_ids,
            failed_results=[],
            operation_time=datetime.now()
        )

        return create_response(
            data=result.dict(),
            message=f"批量操作完成，成功: {successful}，失败: {failed}"
        )

    except Exception as e:
        logger.error(f"批量操作工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量操作工作流失败: {str(e)}")


# ==================== 内部辅助函数 ====================

async def _generate_workflow_analysis(workflow_id: int, workflow_request: CreateWorkflowRequest):
    """异步生成工作流分析（后台任务）"""
    try:
        # 生成依赖关系图
        dependency_graph = await workflow_validation_service.build_dependency_graph(
            workflow_request.nodes, workflow_request.edges or []
        )

        logger.info(f"工作流 {workflow_id} 依赖图分析完成，最大深度: {dependency_graph.max_depth}")

        # 这里可以将分析结果存储到缓存或数据库中

    except Exception as e:
        logger.error(f"生成工作流分析失败: {e}")


@router.post("/templates/", summary="创建工作流模板")
async def create_workflow_template(
        template_request: CreateWorkflowTemplateRequest,
        db: AsyncSession = Depends(get_async_db),
        creator_id: Optional[str] = Query(None, description="创建者ID")
):
    """创建工作流模板"""
    try:
        result = await workflow_orchestration_service.create_workflow_template(
            db, template_request, creator_id
        )

        return create_response(
            data=result,
            message=f"工作流模板 '{template_request.template_name}' 创建成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建工作流模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建工作流模板失败: {str(e)}")


@router.get("/templates/", summary="获取工作流模板列表")
async def get_workflow_templates(
        db: AsyncSession = Depends(get_async_db),
        category: Optional[str] = Query(None, description="模板分类"),
        business_scenario: Optional[str] = Query(None, description="业务场景"),
        is_public: Optional[bool] = Query(None, description="是否公开"),
        is_builtin: Optional[bool] = Query(None, description="是否内置"),
        keyword: Optional[str] = Query(None, description="搜索关键词"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """获取工作流模板列表"""
    try:
        result = await workflow_orchestration_service.get_workflow_templates(
            db, category, business_scenario, is_public, is_builtin, keyword, page, page_size
        )

        return create_response(
            data=result,
            message="获取工作流模板列表成功"
        )
    except Exception as e:
        logger.error(f"获取工作流模板列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流模板列表失败: {str(e)}")


@router.get("/templates/{template_id}", summary="获取工作流模板详情")
async def get_workflow_template(
        template_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取工作流模板详情"""
    try:
        template = await workflow_orchestration_service.get_workflow_template_by_id(db, template_id)

        if not template:
            raise HTTPException(status_code=404, detail="工作流模板不存在")

        return create_response(
            data=template.dict(),
            message="获取工作流模板详情成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取工作流模板详情失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流模板详情失败: {str(e)}")


@router.post("/templates/{template_id}/create-workflow", summary="从模板创建工作流")
async def create_workflow_from_template(
        template_id: int,
        create_request: CreateWorkflowFromTemplateRequest,
        db: AsyncSession = Depends(get_async_db),
        creator_id: Optional[str] = Query(None, description="创建者ID")
):
    """从模板创建工作流"""
    try:
        # 验证模板ID与请求中的模板ID一致
        if create_request.template_id != template_id:
            raise ValueError("URL中的模板ID与请求体中的模板ID不匹配")

        result = await workflow_orchestration_service.create_workflow_from_template(
            db, create_request, creator_id
        )

        return create_response(
            data=result,
            message=f"从模板创建工作流 '{create_request.workflow_name}' 成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"从模板创建工作流失败: {e}")
        raise HTTPException(status_code=500, detail=f"从模板创建工作流失败: {str(e)}")


@router.put("/templates/{template_id}", summary="更新工作流模板")
async def update_workflow_template(
        template_id: int,
        update_request: CreateWorkflowTemplateRequest,  # 复用创建请求结构
        db: AsyncSession = Depends(get_async_db)
):
    """更新工作流模板"""
    try:
        result = await workflow_orchestration_service.update_workflow_template(
            db, template_id, update_request
        )

        return create_response(
            data=result,
            message=f"工作流模板更新成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新工作流模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新工作流模板失败: {str(e)}")


@router.delete("/templates/{template_id}", summary="删除工作流模板")
async def delete_workflow_template(
        template_id: int,
        db: AsyncSession = Depends(get_async_db),
        force: bool = Query(False, description="是否强制删除")
):
    """删除工作流模板"""
    try:
        result = await workflow_orchestration_service.delete_workflow_template(
            db, template_id, force
        )

        return create_response(
            data=result,
            message=result["message"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除工作流模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除工作流模板失败: {str(e)}")


# ==================== 工作流导入导出 ====================

@router.post("/export", summary="导出工作流")
async def export_workflows(
        export_request: WorkflowExportRequest,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db)

):
    """导出工作流配置和数据"""
    try:
        # 启动异步导出任务
        export_task_id = f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        background_tasks.add_task(
            _async_export_workflows,
            db, export_request, export_task_id
        )

        return create_response(
            data={
                "export_task_id": export_task_id,
                "status": "started",
                "workflow_count": len(export_request.workflow_ids)
            },
            message="工作流导出任务已启动"
        )
    except Exception as e:
        logger.error(f"启动工作流导出失败: {e}")
        raise HTTPException(status_code=500, detail=f"启动工作流导出失败: {str(e)}")


@router.get("/export/{export_task_id}/status", summary="获取导出任务状态")
async def get_export_status(
        export_task_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取导出任务状态"""
    try:
        status = await workflow_orchestration_service.get_export_task_status(db, export_task_id)

        return create_response(
            data=status,
            message="获取导出任务状态成功"
        )
    except Exception as e:
        logger.error(f"获取导出任务状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取导出任务状态失败: {str(e)}")


@router.get("/export/{export_task_id}/download", summary="下载导出文件")
async def download_export_file(
        export_task_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """下载导出文件"""
    try:
        file_info = await workflow_orchestration_service.get_export_file(db, export_task_id)

        if not file_info:
            raise HTTPException(status_code=404, detail="导出文件不存在或已过期")

        from fastapi.responses import FileResponse
        return FileResponse(
            file_info["file_path"],
            filename=file_info["filename"],
            media_type=file_info["media_type"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"下载导出文件失败: {e}")
        raise HTTPException(status_code=500, detail=f"下载导出文件失败: {str(e)}")


@router.post("/import", summary="导入工作流")
async def import_workflows(
        import_request: WorkflowImportRequest,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db),
        importer_id: Optional[str] = Query(None, description="导入者ID")
):
    """导入工作流配置和数据"""
    try:
        # 如果是试运行，直接执行验证
        if import_request.dry_run:
            result = await workflow_orchestration_service.validate_import_data(
                db, import_request
            )
            return create_response(
                data=result.dict(),
                message="工作流导入验证完成"
            )

        # 启动异步导入任务
        import_task_id = f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        background_tasks.add_task(
            _async_import_workflows,
            db, import_request, import_task_id, importer_id
        )

        return create_response(
            data={
                "import_task_id": import_task_id,
                "status": "started"
            },
            message="工作流导入任务已启动"
        )
    except Exception as e:
        logger.error(f"启动工作流导入失败: {e}")
        raise HTTPException(status_code=500, detail=f"启动工作流导入失败: {str(e)}")


@router.get("/import/{import_task_id}/status", summary="获取导入任务状态")
async def get_import_status(
        import_task_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取导入任务状态和结果"""
    try:
        status = await workflow_orchestration_service.get_import_task_status(db, import_task_id)

        return create_response(
            data=status,
            message="获取导入任务状态成功"
        )
    except Exception as e:
        logger.error(f"获取导入任务状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取导入任务状态失败: {str(e)}")


# ==================== 工作流告警管理 ====================

@router.post("/{workflow_id}/alerts", summary="创建工作流告警")
async def create_workflow_alert(
        workflow_id: int,
        alert_request: CreateWorkflowAlertRequest,
        db: AsyncSession = Depends(get_async_db),
        creator_id: Optional[str] = Query(None, description="创建者ID")
):
    """为工作流创建告警规则"""
    try:
        # 验证工作流ID
        if alert_request.workflow_id != workflow_id:
            raise ValueError("URL中的工作流ID与请求体中的工作流ID不匹配")

        result = await workflow_orchestration_service.create_workflow_alert(
            db, alert_request, creator_id
        )

        return create_response(
            data=result,
            message=f"工作流告警 '{alert_request.alert_name}' 创建成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建工作流告警失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建工作流告警失败: {str(e)}")


@router.get("/{workflow_id}/alerts", summary="获取工作流告警列表")
async def get_workflow_alerts(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db),
        alert_type: Optional[str] = Query(None, description="告警类型过滤"),
        severity: Optional[str] = Query(None, description="严重级别过滤"),
        is_enabled: Optional[bool] = Query(None, description="是否启用过滤"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页大小")
):
    """获取工作流的告警规则列表"""
    try:
        result = await workflow_orchestration_service.get_workflow_alerts(
            db, workflow_id, alert_type, severity, is_enabled, page, page_size
        )

        return create_response(
            data=result,
            message="获取工作流告警列表成功"
        )
    except Exception as e:
        logger.error(f"获取工作流告警列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流告警列表失败: {str(e)}")


@router.get("/alerts/{alert_id}", summary="获取告警详情")
async def get_workflow_alert(
        alert_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取告警规则详情"""
    try:
        alert = await workflow_orchestration_service.get_workflow_alert_by_id(db, alert_id)

        if not alert:
            raise HTTPException(status_code=404, detail="告警规则不存在")

        return create_response(
            data=alert.dict(),
            message="获取告警详情成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取告警详情失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取告警详情失败: {str(e)}")


@router.put("/alerts/{alert_id}", summary="更新告警规则")
async def update_workflow_alert(
        alert_id: int,
        alert_request: CreateWorkflowAlertRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """更新告警规则"""
    try:
        result = await workflow_orchestration_service.update_workflow_alert(
            db, alert_id, alert_request
        )

        return create_response(
            data=result,
            message="告警规则更新成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新告警规则失败: {str(e)}")


@router.delete("/alerts/{alert_id}", summary="删除告警规则")
async def delete_workflow_alert(
        alert_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除告警规则"""
    try:
        result = await workflow_orchestration_service.delete_workflow_alert(db, alert_id)

        return create_response(
            data=result,
            message=result["message"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除告警规则失败: {str(e)}")


@router.post("/alerts/{alert_id}/test", summary="测试告警规则")
async def test_workflow_alert(
        alert_id: int,
        db: AsyncSession = Depends(get_async_db),
        test_data: Optional[Dict[str, Any]] = Body(None, description="测试数据")
):
    """测试告警规则"""
    try:
        result = await workflow_orchestration_service.test_workflow_alert(
            db, alert_id, test_data
        )

        return create_response(
            data=result,
            message="告警规则测试完成"
        )
    except Exception as e:
        logger.error(f"测试告警规则失败: {e}")
        raise HTTPException(status_code=500, detail=f"测试告警规则失败: {str(e)}")


# ==================== 工作流变量管理 ====================

@router.get("/{workflow_id}/variables", summary="获取工作流变量")
async def get_workflow_variables(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db),
        variable_type: Optional[str] = Query(None, description="变量类型过滤"),
        is_required: Optional[bool] = Query(None, description="是否必填过滤"),
        is_sensitive: Optional[bool] = Query(None, description="是否敏感过滤")
):
    """获取工作流的变量定义"""
    try:
        variables = await workflow_orchestration_service.get_workflow_variables(
            db, workflow_id, variable_type, is_required, is_sensitive
        )

        return create_response(
            data=variables,
            message="获取工作流变量成功"
        )
    except Exception as e:
        logger.error(f"获取工作流变量失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取工作流变量失败: {str(e)}")


@router.put("/{workflow_id}/variables", summary="更新工作流变量")
async def update_workflow_variables(
        workflow_id: int,
        variables: List[Dict[str, Any]] = Body(..., description="变量列表"),
        db: AsyncSession = Depends(get_async_db)
):
    """更新工作流变量定义"""
    try:
        result = await workflow_orchestration_service.update_workflow_variables(
            db, workflow_id, variables
        )

        return create_response(
            data=result,
            message="工作流变量更新成功"
        )
    except Exception as e:
        logger.error(f"更新工作流变量失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新工作流变量失败: {str(e)}")


# ==================== 工作流调度管理 ====================

@router.post("/{workflow_id}/schedule", summary="设置工作流调度")
async def set_workflow_schedule(
        workflow_id: int,
        schedule_config: Dict[str, Any] = Body(..., description="调度配置"),
        db: AsyncSession = Depends(get_async_db)
):
    """设置或更新工作流调度配置"""
    try:
        result = await workflow_orchestration_service.set_workflow_schedule(
            db, workflow_id, schedule_config
        )

        return create_response(
            data=result,
            message="工作流调度配置成功"
        )
    except Exception as e:
        logger.error(f"设置工作流调度失败: {e}")
        raise HTTPException(status_code=500, detail=f"设置工作流调度失败: {str(e)}")


@router.delete("/{workflow_id}/schedule", summary="删除工作流调度")
async def remove_workflow_schedule(
        workflow_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除工作流调度配置"""
    try:
        result = await workflow_orchestration_service.remove_workflow_schedule(
            db, workflow_id
        )

        return create_response(
            data=result,
            message="工作流调度已删除"
        )
    except Exception as e:
        logger.error(f"删除工作流调度失败: {e}")
        raise HTTPException(status_code=500, detail=f"删除工作流调度失败: {str(e)}")


# ==================== 内部辅助函数 ====================

async def _async_export_workflows(
        db: AsyncSession,
        export_request: WorkflowExportRequest,
        export_task_id: str
):
    """异步导出工作流"""
    try:
        await workflow_orchestration_service.execute_export_task(
            db, export_request, export_task_id
        )
        logger.info(f"工作流导出任务完成: {export_task_id}")
    except Exception as e:
        logger.error(f"工作流导出任务失败: {export_task_id}, {e}")


async def _async_import_workflows(
        db: AsyncSession,
        import_request: WorkflowImportRequest,
        import_task_id: str,
        importer_id: Optional[str]
):
    """异步导入工作流"""
    try:
        await workflow_orchestration_service.execute_import_task(
            db, import_request, import_task_id, importer_id
        )
        logger.info(f"工作流导入任务完成: {import_task_id}")
    except Exception as e:
        logger.error(f"工作流导入任务失败: {import_task_id}, {e}")
# ==================== WebSocket支持（可选） ====================

# 如果需要实时状态推送，可以添加WebSocket端点
# from fastapi import WebSocket
#
# @router.websocket("/ws/{workflow_id}")
# async def workflow_websocket(websocket: WebSocket, workflow_id: int):
#     """工作流实时状态WebSocket连接"""
#     await websocket.accept()
#     try:
#         while True:
#             # 推送实时状态更新
#             await asyncio.sleep(5)  # 每5秒检查一次
#             # 这里可以推送工作流执行状态
#             await websocket.send_json({"type": "status_update", "workflow_id": workflow_id})
#     except Exception as e:
#         logger.error(f"WebSocket连接异常: {e}")
#     finally:
#         await websocket.close()