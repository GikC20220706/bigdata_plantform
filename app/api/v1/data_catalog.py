# app/api/v1/data_catalog.py
"""
数据资源目录API端点
提供目录的增删改查和树形结构查询
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.data_catalog import (
    DataCatalogCreate, DataCatalogUpdate,
    DataCatalogResponse, DataCatalogTreeNode,
    DataCatalogListResponse
)
from app.services.data_catalog_service import data_catalog_service

router = APIRouter(prefix="/data-catalog", tags=["数据资源目录"])


@router.get("/tree", summary="获取目录树")
async def get_catalog_tree(
        parent_id: Optional[int] = Query(None, description="父级目录ID，不传则从根节点开始"),
        include_inactive: bool = Query(False, description="是否包含未启用的目录"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取目录树形结构

    - **parent_id**: 父级ID，None表示从根节点开始
    - **include_inactive**: 是否包含未启用的目录

    返回递归的树形结构，每个节点包含children字段
    """
    try:
        tree_nodes = await data_catalog_service.get_catalog_tree(
            db,
            parent_id=parent_id,
            include_inactive=include_inactive
        )

        return create_response(
            data=tree_nodes,
            message=f"获取目录树成功，共 {len(tree_nodes)} 个根节点"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取目录树失败: {e}")
        raise HTTPException(status_code=500, detail="获取目录树失败")


@router.get("/list", summary="获取目录列表（分页）")
async def get_catalog_list(
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页数量"),
        keyword: Optional[str] = Query(None, description="搜索关键词（名称、编码、描述）"),
        catalog_type: Optional[str] = Query(None, description="目录类型: domain/subject/dataset"),
        parent_id: Optional[int] = Query(None, description="父级目录ID"),
        is_active: Optional[bool] = Query(None, description="是否启用"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    分页查询目录列表

    支持多条件组合搜索：
    - 关键词模糊匹配
    - 目录类型筛选
    - 父级目录筛选
    - 启用状态筛选
    """
    try:
        catalogs, total = await data_catalog_service.search_catalogs(
            db,
            page=page,
            page_size=page_size,
            keyword=keyword,
            catalog_type=catalog_type,
            parent_id=parent_id,
            is_active=is_active
        )

        # 构建响应
        response = DataCatalogListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=[DataCatalogResponse.model_validate(c) for c in catalogs]
        )

        return create_response(
            data=response.model_dump(),
            message=f"查询成功，共 {total} 条记录"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"查询目录列表失败: {e}")
        raise HTTPException(status_code=500, detail="查询目录列表失败")


@router.get("/{catalog_id}", summary="获取目录详情")
async def get_catalog_detail(
        catalog_id: int = Path(..., description="目录ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取单个目录的详细信息

    - **catalog_id**: 目录ID
    """
    try:
        catalog = await data_catalog_service.get_catalog_by_id(db, catalog_id)

        if not catalog:
            raise HTTPException(status_code=404, detail=f"目录 ID {catalog_id} 不存在")

        response = DataCatalogResponse.model_validate(catalog)

        return create_response(
            data=response.model_dump(),
            message="获取目录详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取目录详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取目录详情失败")


@router.get("/{catalog_id}/path", summary="获取目录路径")
async def get_catalog_path(
        catalog_id: int = Path(..., description="目录ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取目录的完整路径（从根到当前节点）

    返回路径上所有目录的列表
    """
    try:
        path_list = await data_catalog_service.get_catalog_path_list(db, catalog_id)

        if not path_list:
            raise HTTPException(status_code=404, detail=f"目录 ID {catalog_id} 不存在")

        path_data = [DataCatalogResponse.model_validate(c) for c in path_list]

        return create_response(
            data=path_data,
            message=f"获取目录路径成功，深度 {len(path_list)}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取目录路径失败: {e}")
        raise HTTPException(status_code=500, detail="获取目录路径失败")


@router.post("/", summary="创建目录")
async def create_catalog(
        catalog_data: DataCatalogCreate,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的数据目录

    - **catalog_name**: 目录名称
    - **catalog_code**: 目录编码（唯一）
    - **catalog_type**: 目录类型（domain/subject/dataset）
    - **parent_id**: 父级目录ID，不传则创建顶级目录
    - **level**: 层级（1-业务域 2-主题域 3-数据集）

    注意：
    - 编码必须唯一
    - 父目录必须存在
    - 层级关系必须正确（父目录层级 = 当前层级 - 1）
    """
    try:
        catalog = await data_catalog_service.create_catalog(
            db,
            catalog_data,
            creator
        )

        response = DataCatalogResponse.model_validate(catalog)

        return create_response(
            data=response.model_dump(),
            message=f"创建目录成功: {catalog.catalog_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建目录失败: {e}")
        raise HTTPException(status_code=500, detail="创建目录失败")


@router.put("/{catalog_id}", summary="更新目录")
async def update_catalog(
        catalog_id: int = Path(..., description="目录ID"),
        catalog_data: DataCatalogUpdate = None,
        updater: Optional[str] = Query(None, description="更新人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新目录信息

    支持部分字段更新，只传需要修改的字段即可

    - **catalog_name**: 目录名称
    - **description**: 描述
    - **sort_order**: 排序
    - **icon**: 图标
    - **tags**: 标签
    - **is_active**: 是否启用

    注意：
    - 名称变更会自动更新路径
    - 会递归更新子目录的路径
    """
    try:
        catalog = await data_catalog_service.update_catalog(
            db,
            catalog_id,
            catalog_data,
            updater
        )

        response = DataCatalogResponse.model_validate(catalog)

        return create_response(
            data=response.model_dump(),
            message=f"更新目录成功: {catalog.catalog_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新目录失败: {e}")
        raise HTTPException(status_code=500, detail="更新目录失败")


@router.delete("/{catalog_id}", summary="删除目录")
async def delete_catalog(
        catalog_id: int = Path(..., description="目录ID"),
        force: bool = Query(False, description="是否强制删除（忽略子节点和资产检查）"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除目录

    - **catalog_id**: 目录ID
    - **force**: 是否强制删除

    普通删除（force=False）：
    - 检查是否有子目录，有则不允许删除
    - 检查是否有数据资产，有则不允许删除

    强制删除（force=True）：
    - 忽略所有检查，直接删除
    - 会级联删除关联的数据
    """
    try:
        success = await data_catalog_service.delete_catalog(
            db,
            catalog_id,
            force
        )

        if success:
            return create_response(
                data={"catalog_id": catalog_id, "deleted": True},
                message="删除目录成功"
            )
        else:
            raise HTTPException(status_code=500, detail="删除目录失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除目录失败: {e}")
        raise HTTPException(status_code=500, detail="删除目录失败")


@router.get("/statistics/summary", summary="获取目录统计摘要")
async def get_catalog_statistics(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取所有目录的统计摘要

    返回：
    - 各层级目录数量
    - 总资产数量
    - 各类型目录数量
    """
    try:
        from sqlalchemy import select, func
        from app.models.data_catalog import DataCatalog

        # 按层级统计
        level_stats_result = await db.execute(
            select(
                DataCatalog.level,
                func.count(DataCatalog.id).label('count')
            )
            .where(DataCatalog.is_active == True)
            .group_by(DataCatalog.level)
        )
        level_stats = {row[0]: row[1] for row in level_stats_result.all()}

        # 按类型统计
        type_stats_result = await db.execute(
            select(
                DataCatalog.catalog_type,
                func.count(DataCatalog.id).label('count')
            )
            .where(DataCatalog.is_active == True)
            .group_by(DataCatalog.catalog_type)
        )
        type_stats = {row[0]: row[1] for row in type_stats_result.all()}

        # 总资产数
        total_assets_result = await db.execute(
            select(func.sum(DataCatalog.asset_count))
            .where(DataCatalog.is_active == True)
        )
        total_assets = total_assets_result.scalar_one_or_none() or 0

        # 总目录数
        total_catalogs_result = await db.execute(
            select(func.count(DataCatalog.id))
            .where(DataCatalog.is_active == True)
        )
        total_catalogs = total_catalogs_result.scalar_one()

        statistics = {
            'total_catalogs': total_catalogs,
            'total_assets': total_assets,
            'level_statistics': {
                'level_1_domain': level_stats.get(1, 0),
                'level_2_subject': level_stats.get(2, 0),
                'level_3_dataset': level_stats.get(3, 0)
            },
            'type_statistics': type_stats
        }

        return create_response(
            data=statistics,
            message="获取统计信息成功"
        )

    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取统计信息失败")