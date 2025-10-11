# app/api/v1/field_standard.py
"""
字段标准API端点
提供字段标准的管理功能
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.data_catalog import (
    FieldStandardCreate, FieldStandardUpdate,
    FieldStandardResponse, FieldStandardListResponse
)
from app.services.field_standard_service import field_standard_service

router = APIRouter(prefix="/field-standard", tags=["字段标准"])


@router.get("/", summary="获取字段标准列表")
async def get_standard_list(
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页数量"),
        keyword: Optional[str] = Query(None, description="搜索关键词（名称、编码、定义）"),
        category: Optional[str] = Query(None, description="分类"),
        data_type: Optional[str] = Query(None, description="数据类型"),
        is_active: Optional[bool] = Query(None, description="是否启用"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    分页查询字段标准列表

    支持多条件筛选：
    - 关键词模糊匹配
    - 分类筛选
    - 数据类型筛选
    - 启用状态筛选
    """
    try:
        standards, total = await field_standard_service.search_standards(
            db,
            page=page,
            page_size=page_size,
            keyword=keyword,
            category=category,
            data_type=data_type,
            is_active=is_active
        )

        # 构建响应
        response = FieldStandardListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=[FieldStandardResponse.model_validate(s) for s in standards]
        )

        return create_response(
            data=response.model_dump(),
            message=f"查询成功，共 {total} 条记录"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"查询字段标准列表失败: {e}")
        raise HTTPException(status_code=500, detail="查询字段标准列表失败")


@router.get("/{standard_id}", summary="获取字段标准详情")
async def get_standard_detail(
        standard_id: int = Path(..., description="标准ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取单个字段标准的详细信息

    - **standard_id**: 标准ID
    """
    try:
        standard = await field_standard_service.get_standard_by_id(db, standard_id)

        if not standard:
            raise HTTPException(status_code=404, detail=f"字段标准 ID {standard_id} 不存在")

        response = FieldStandardResponse.model_validate(standard)

        return create_response(
            data=response.model_dump(),
            message="获取字段标准详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取字段标准详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取字段标准详情失败")


@router.post("/", summary="创建字段标准")
async def create_standard(
        standard_data: FieldStandardCreate,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的字段标准

    - **standard_code**: 标准编码（唯一）
    - **standard_name**: 标准名称
    - **data_type**: 标准数据类型
    - **business_definition**: 业务定义
    - **category**: 分类

    注意：
    - 编码必须唯一
    - 建议按业务分类管理标准
    """
    try:
        standard = await field_standard_service.create_standard(
            db,
            standard_data,
            creator
        )

        response = FieldStandardResponse.model_validate(standard)

        return create_response(
            data=response.model_dump(),
            message=f"创建字段标准成功: {standard.standard_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建字段标准失败: {e}")
        raise HTTPException(status_code=500, detail="创建字段标准失败")


@router.put("/{standard_id}", summary="更新字段标准")
async def update_standard(
        standard_id: int = Path(..., description="标准ID"),
        standard_data: FieldStandardUpdate = None,
        updater: Optional[str] = Query(None, description="更新人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新字段标准信息

    支持部分字段更新

    可更新字段：
    - **standard_name**: 标准名称
    - **business_definition**: 业务定义
    - **usage_guidelines**: 使用指南
    - **is_active**: 是否启用
    - **tags**: 标签
    """
    try:
        standard = await field_standard_service.update_standard(
            db,
            standard_id,
            standard_data,
            updater
        )

        response = FieldStandardResponse.model_validate(standard)

        return create_response(
            data=response.model_dump(),
            message=f"更新字段标准成功: {standard.standard_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新字段标准失败: {e}")
        raise HTTPException(status_code=500, detail="更新字段标准失败")


@router.delete("/{standard_id}", summary="删除字段标准")
async def delete_standard(
        standard_id: int = Path(..., description="标准ID"),
        force: bool = Query(False, description="是否强制删除"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除字段标准

    - **standard_id**: 标准ID
    - **force**: 是否强制删除

    普通删除（force=False）：
    - 检查是否正在使用，如正在使用则不允许删除
    - 软删除（标记为不启用）

    强制删除（force=True）：
    - 物理删除，从数据库中移除
    """
    try:
        success = await field_standard_service.delete_standard(
            db,
            standard_id,
            force
        )

        if success:
            return create_response(
                data={"standard_id": standard_id, "deleted": True, "force": force},
                message="删除字段标准成功"
            )
        else:
            raise HTTPException(status_code=500, detail="删除字段标准失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除字段标准失败: {e}")
        raise HTTPException(status_code=500, detail="删除字段标准失败")


@router.get("/categories/list", summary="获取所有分类")
async def get_categories(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取所有字段标准分类

    返回：
    - 分类列表（去重、排序）

    用于下拉选择等场景
    """
    try:
        categories = await field_standard_service.get_all_categories(db)

        return create_response(
            data={"categories": categories, "count": len(categories)},
            message=f"获取分类列表成功，共 {len(categories)} 个分类"
        )

    except Exception as e:
        logger.error(f"获取分类列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取分类列表失败")


@router.get("/category/{category}", summary="根据分类获取标准")
async def get_standards_by_category(
        category: str = Path(..., description="分类名称"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    根据分类获取所有启用的字段标准

    - **category**: 分类名称

    返回该分类下所有启用的字段标准
    """
    try:
        standards = await field_standard_service.get_standards_by_category(db, category)

        standard_list = [FieldStandardResponse.model_validate(s) for s in standards]

        return create_response(
            data={"category": category, "standards": standard_list, "count": len(standards)},
            message=f"获取分类标准成功，共 {len(standards)} 条"
        )

    except Exception as e:
        logger.error(f"获取分类标准失败: {e}")
        raise HTTPException(status_code=500, detail="获取分类标准失败")