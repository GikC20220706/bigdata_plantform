"""
业务系统管理API端点
提供业务系统的完整CRUD操作和管理功能
"""

import asyncio
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, BackgroundTasks
from sqlalchemy import or_, and_
#from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.business_system import BusinessSystem
from app.schemas.business_system import (
    BusinessSystemCreate,
    BusinessSystemUpdate,
    BusinessSystemResponse,
    BusinessSystemSummary,
    BusinessSystemListResponse,
    BusinessSystemSearchParams,
    BusinessSystemStatistics,
    BusinessSystemHealth,
    BusinessSystemStatusUpdate,
    DataSourceAssociationCreate,
    DataSourceAssociationResponse,
    BusinessSystemBatchImport,
    BatchOperationResult,
    SystemStatus
)
from app.services.business_system_service import business_system_service
from app.utils.database import get_async_db
from app.utils.response import create_response
from loguru import logger

router = APIRouter()


# === 基础CRUD操作 ===

@router.post("/", response_model=BusinessSystemResponse, summary="创建业务系统")
async def create_business_system(
        system_data: BusinessSystemCreate,
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的业务系统

    - **system_name**: 业务系统名称（唯一）
    - **display_name**: 显示名称
    - **description**: 系统描述
    - **contact_person**: 联系人
    - **business_domain**: 业务领域
    - **criticality_level**: 重要程度（high/medium/low）
    """
    try:
        business_system = await business_system_service.create_business_system(db, system_data)
        return BusinessSystemResponse.from_orm(business_system)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建业务系统失败: {e}")
        raise HTTPException(status_code=500, detail="创建业务系统失败")


@router.get("/", response_model=BusinessSystemListResponse, summary="获取业务系统列表")
async def get_business_systems(
        keyword: Optional[str] = Query(None, description="搜索关键词"),
        status: Optional[SystemStatus] = Query(None, description="状态过滤"),
        system_type: Optional[str] = Query(None, description="系统类型过滤"),
        business_domain: Optional[str] = Query(None, description="业务领域过滤"),
        criticality_level: Optional[str] = Query(None, description="重要程度过滤"),
        tags: Optional[List[str]] = Query(None, description="标签过滤"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页大小"),
        order_by: str = Query("created_at", description="排序字段"),
        order_desc: bool = Query(True, description="是否降序"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取业务系统列表，支持搜索、过滤和分页

    - 支持按关键词搜索系统名称、显示名称、描述
    - 支持按状态、类型、业务领域等过滤
    - 支持标签过滤
    - 支持分页和排序
    """
    try:
        search_params = BusinessSystemSearchParams(
            keyword=keyword,
            status=status,
            system_type=system_type,
            business_domain=business_domain,
            criticality_level=criticality_level,
            tags=tags or [],
            page=page,
            page_size=page_size,
            order_by=order_by,
            order_desc=order_desc
        )

        systems, total = await business_system_service.get_business_systems_list(db, search_params)

        # 转换为摘要格式
        system_summaries = [BusinessSystemSummary.from_orm(system) for system in systems]

        total_pages = (total + page_size - 1) // page_size

        return BusinessSystemListResponse(
            items=system_summaries,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )

    except Exception as e:
        logger.error(f"获取业务系统列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取业务系统列表失败")


@router.get("/{system_id}", response_model=BusinessSystemResponse, summary="获取业务系统详情")
async def get_business_system(
        system_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """根据ID获取业务系统详细信息"""
    try:
        business_system = await business_system_service.get_business_system_by_id(db, system_id)
        if not business_system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return BusinessSystemResponse.from_orm(business_system)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取业务系统详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取业务系统详情失败")


@router.put("/{system_id}", response_model=BusinessSystemResponse, summary="更新业务系统")
async def update_business_system(
        system_id: int,
        update_data: BusinessSystemUpdate,
        db: AsyncSession = Depends(get_async_db)
):
    """更新业务系统信息"""
    try:
        business_system = await business_system_service.update_business_system(db, system_id, update_data)
        if not business_system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return BusinessSystemResponse.from_orm(business_system)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新业务系统失败: {e}")
        raise HTTPException(status_code=500, detail="更新业务系统失败")


@router.delete("/{system_id}", summary="删除业务系统")
async def delete_business_system(
        system_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除业务系统（软删除）"""
    try:
        success = await business_system_service.delete_business_system(db, system_id)
        if not success:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return create_response(message="业务系统删除成功")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除业务系统失败: {e}")
        raise HTTPException(status_code=500, detail="删除业务系统失败")


# === 状态管理 ===

@router.post("/{system_id}/status", response_model=BusinessSystemResponse, summary="更新业务系统状态")
async def update_system_status(
        system_id: int,
        status_update: BusinessSystemStatusUpdate,
        db: AsyncSession = Depends(get_async_db)
):
    """更新业务系统状态"""
    try:
        business_system = await business_system_service.update_system_status(
            db, system_id, status_update.status, status_update.reason
        )
        if not business_system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return BusinessSystemResponse.from_orm(business_system)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新系统状态失败: {e}")
        raise HTTPException(status_code=500, detail="更新系统状态失败")


@router.post("/{system_id}/activate", response_model=BusinessSystemResponse, summary="激活业务系统")
async def activate_business_system(
        system_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """激活业务系统"""
    try:
        business_system = await business_system_service.update_system_status(
            db, system_id, SystemStatus.ACTIVE, "手动激活"
        )
        if not business_system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return BusinessSystemResponse.from_orm(business_system)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"激活业务系统失败: {e}")
        raise HTTPException(status_code=500, detail="激活业务系统失败")


@router.post("/{system_id}/deactivate", response_model=BusinessSystemResponse, summary="停用业务系统")
async def deactivate_business_system(
        system_id: int,
        reason: Optional[str] = Query(None, description="停用原因"),
        db: AsyncSession = Depends(get_async_db)
):
    """停用业务系统"""
    try:
        business_system = await business_system_service.update_system_status(
            db, system_id, SystemStatus.INACTIVE, reason or "手动停用"
        )
        if not business_system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return BusinessSystemResponse.from_orm(business_system)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"停用业务系统失败: {e}")
        raise HTTPException(status_code=500, detail="停用业务系统失败")


# === 统计和监控 ===

@router.get("/statistics/overview", response_model=BusinessSystemStatistics, summary="获取业务系统统计信息")
async def get_business_systems_statistics(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取业务系统统计信息

    包括：
    - 系统总数和活跃数量
    - 按类型、业务领域、重要程度分组统计
    - 总表数量和平均数据质量分数
    """
    try:
        statistics = await business_system_service.get_business_systems_statistics(db)
        return statistics

    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取统计信息失败")


@router.get("/count", summary="获取活跃业务系统数量")
async def get_active_systems_count(
        db: AsyncSession = Depends(get_async_db)
):
    """获取活跃业务系统数量（用于总览页面）"""
    try:
        count = await business_system_service.get_business_systems_count(db)
        return create_response(
            data={"count": count},
            message="获取业务系统数量成功"
        )

    except Exception as e:
        logger.error(f"获取业务系统数量失败: {e}")
        raise HTTPException(status_code=500, detail="获取业务系统数量失败")


@router.get("/{system_id}/health", response_model=BusinessSystemHealth, summary="获取业务系统健康状态")
async def get_business_system_health(
        system_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取业务系统健康状态"""
    try:
        health = await business_system_service.get_business_system_health(db, system_id)
        if not health:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        return health

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取系统健康状态失败: {e}")
        raise HTTPException(status_code=500, detail="获取系统健康状态失败")


# === 数据源关联管理 ===

@router.get("/{system_id}/data-sources", response_model=List[DataSourceAssociationResponse], summary="获取关联的数据源")
async def get_system_data_sources(
        system_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取业务系统关联的数据源列表"""
    try:
        # 检查系统是否存在
        system = await business_system_service.get_business_system_by_id(db, system_id)
        if not system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        # 获取关联的数据源
        associations = await business_system_service.get_system_data_sources(db, system_id)

        return [DataSourceAssociationResponse.from_orm(assoc) for assoc in associations]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取数据源关联失败: {e}")
        raise HTTPException(status_code=500, detail="获取数据源关联失败")


@router.post("/{system_id}/data-sources", response_model=DataSourceAssociationResponse, summary="添加数据源关联")
async def add_data_source_association(
        system_id: int,
        association_data: DataSourceAssociationCreate,
        db: AsyncSession = Depends(get_async_db)
):
    """为业务系统添加数据源关联"""
    try:
        association = await business_system_service.add_data_source_association(
            db, system_id, association_data
        )
        return DataSourceAssociationResponse.from_orm(association)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"添加数据源关联失败: {e}")
        raise HTTPException(status_code=500, detail="添加数据源关联失败")


@router.delete("/{system_id}/data-sources/{association_id}", summary="删除数据源关联")
async def remove_data_source_association(
        system_id: int,
        association_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    try:
        from app.models.business_system import BusinessSystemDataSource
        from sqlalchemy import select

        # 改为异步查询
        result = await db.execute(
            select(BusinessSystemDataSource).where(
                and_(
                    BusinessSystemDataSource.id == association_id,
                    BusinessSystemDataSource.business_system_id == system_id
                )
            )
        )
        association = result.scalar_one_or_none()

        if not association:
            raise HTTPException(status_code=404, detail="数据源关联不存在")

        await db.delete(association)
        await db.commit()  # 加 await

        return create_response(message="数据源关联删除成功")

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()  # 加 await
        logger.error(f"删除数据源关联失败: {e}")
        raise HTTPException(status_code=500, detail="删除数据源关联失败")


# === 批量操作 ===

@router.post("/batch/import", response_model=BatchOperationResult, summary="批量导入业务系统")
async def batch_import_business_systems(
        import_data: BusinessSystemBatchImport,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db)
):
    """
    批量导入业务系统

    - 支持覆盖已存在的系统
    - 提供详细的成功/失败统计
    - 大批量数据使用后台任务处理
    """
    try:
        # 如果数据量较大，使用后台任务
        if len(import_data.systems) > 50:
            # 启动后台任务
            background_tasks.add_task(
                _batch_import_background,
                import_data.systems,
                import_data.overwrite_existing
            )

            return BatchOperationResult(
                total_count=len(import_data.systems),
                success_count=0,
                failed_count=0,
                failed_items=[],
                operation_time=datetime.now()
            )

        # 小批量数据直接处理
        result = await business_system_service.batch_import_systems(
            db, import_data.systems, import_data.overwrite_existing
        )
        return result

    except Exception as e:
        logger.error(f"批量导入失败: {e}")
        raise HTTPException(status_code=500, detail="批量导入失败")


async def _batch_import_background(systems: List[BusinessSystemCreate], overwrite: bool):
    """后台批量导入任务"""
    try:
        db = next(get_async_db())
        result = await business_system_service.batch_import_systems(db, systems, overwrite)
        logger.info(f"后台批量导入完成: 成功 {result.success_count}/{result.total_count}")
    except Exception as e:
        logger.error(f"后台批量导入失败: {e}")
    finally:
        db.close()


@router.post("/batch/update-status", summary="批量更新系统状态")
async def batch_update_system_status(
        system_ids: List[int],
        status: SystemStatus,
        reason: Optional[str] = None,
        db: AsyncSession = Depends(get_async_db)
):
    """批量更新业务系统状态"""
    try:
        success_count = 0
        failed_count = 0
        failed_items = []

        for system_id in system_ids:
            try:
                result = await business_system_service.update_system_status(
                    db, system_id, status, reason
                )
                if result:
                    success_count += 1
                else:
                    failed_items.append({"system_id": system_id, "error": "系统不存在"})
                    failed_count += 1
            except Exception as e:
                failed_items.append({"system_id": system_id, "error": str(e)})
                failed_count += 1

        return create_response(
            data={
                "total_count": len(system_ids),
                "success_count": success_count,
                "failed_count": failed_count,
                "failed_items": failed_items
            },
            message="批量状态更新完成"
        )

    except Exception as e:
        logger.error(f"批量更新状态失败: {e}")
        raise HTTPException(status_code=500, detail="批量更新状态失败")


# === 数据同步和维护 ===

@router.post("/{system_id}/sync-data", summary="手动触发数据同步")
async def trigger_data_sync(
        system_id: int,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db)
):
    """手动触发业务系统数据同步"""
    try:
        system = await business_system_service.get_business_system_by_id(db, system_id)
        if not system:
            raise HTTPException(status_code=404, detail="业务系统不存在")

        # 启动后台同步任务
        background_tasks.add_task(_sync_system_data_background, system_id)

        return create_response(
            data={"system_id": system_id, "sync_started": True},
            message="数据同步任务已启动"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"触发数据同步失败: {e}")
        raise HTTPException(status_code=500, detail="触发数据同步失败")


async def _sync_system_data_background(system_id: int):
    """后台数据同步任务"""
    try:
        db = next(get_async_db())
        system = await business_system_service.get_business_system_by_id(db, system_id)

        if system:
            # 这里实现实际的数据同步逻辑
            # 例如：连接数据源、统计表数量、更新质量分数等

            # 模拟同步过程
            await asyncio.sleep(5)

            # 更新同步时间
            system.last_data_sync = datetime.now()
            system.table_count = 100  # 模拟统计结果
            system.data_quality_score = 85  # 模拟质量评分

            db.commit()
            logger.info(f"系统数据同步完成: {system.system_name}")

    except Exception as e:
        logger.error(f"后台数据同步失败: {e}")
    finally:
        db.close()


# === 搜索和建议 ===

@router.get("/search/suggestions", summary="获取搜索建议")
async def get_search_suggestions(
        q: str = Query(..., min_length=1, description="搜索关键词"),
        limit: int = Query(10, ge=1, le=20, description="建议数量限制"),
        db: AsyncSession = Depends(get_async_db)
):
    """根据输入提供搜索建议"""
    try:
        # 搜索系统名称和显示名称
        suggestions = db.query(BusinessSystem.system_name, BusinessSystem.display_name).filter(
            or_(
                BusinessSystem.system_name.ilike(f"%{q}%"),
                BusinessSystem.display_name.ilike(f"%{q}%")
            )
        ).limit(limit).all()

        result = []
        for system_name, display_name in suggestions:
            result.append({
                "system_name": system_name,
                "display_name": display_name,
                "label": f"{display_name} ({system_name})"
            })

        return create_response(
            data={"suggestions": result},
            message="获取搜索建议成功"
        )

    except Exception as e:
        logger.error(f"获取搜索建议失败: {e}")
        raise HTTPException(status_code=500, detail="获取搜索建议失败")