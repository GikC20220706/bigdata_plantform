# app/api/v1/indicator_system.py
"""
指标体系建设API端点
提供指标的增删改查、Excel导入导出、资产关联等功能
"""
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.indicator_system import (
    IndicatorSystemCreate, IndicatorSystemUpdate,
    IndicatorSystemResponse, IndicatorSystemListResponse,
    LinkAssetsRequest, IndicatorAssetRelationResponse,
    BatchCreateIndicatorsRequest, BatchOperationResult,
    ExcelImportResult, IndicatorStatistics
)
from app.services.indicator_system_service import indicator_system_service

router = APIRouter(prefix="/indicator-system", tags=["指标体系建设"])


# ==================== 基础CRUD ====================

@router.get("/", summary="获取指标列表")
async def get_indicator_list(
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页数量"),
        keyword: Optional[str] = Query(None, description="搜索关键词（指标名称、指标说明、来源系统）"),
        business_domain: Optional[str] = Query(None, description="业务领域"),
        business_theme: Optional[str] = Query(None, description="业务主题"),
        indicator_category: Optional[str] = Query(None, description="指标类别"),
        indicator_type: Optional[str] = Query(None, description="指标类型"),
        responsible_dept: Optional[str] = Query(None, description="权责部门"),
        collection_frequency: Optional[str] = Query(None, description="采集频率"),
        is_active: Optional[bool] = Query(None, description="是否启用"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    分页查询指标列表

    支持多条件筛选：
    - 关键词模糊搜索
    - 业务领域筛选
    - 业务主题筛选
    - 指标类别筛选
    - 指标类型筛选
    - 权责部门筛选
    - 采集频率筛选
    - 启用状态筛选
    """
    try:
        indicators, total = await indicator_system_service.search_indicators(
            db,
            page=page,
            page_size=page_size,
            keyword=keyword,
            business_domain=business_domain,
            business_theme=business_theme,
            indicator_category=indicator_category,
            indicator_type=indicator_type,
            responsible_dept=responsible_dept,
            collection_frequency=collection_frequency,
            is_active=is_active
        )

        # 构建响应
        items = []
        for indicator in indicators:
            indicator_dict = IndicatorSystemResponse.model_validate(indicator).model_dump()
            # 补充关联资产数量
            indicator_dict['asset_count'] = len(indicator.asset_relations) if indicator.asset_relations else 0
            items.append(indicator_dict)

        response = IndicatorSystemListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=items
        )

        return create_response(
            data=response.model_dump(),
            message=f"查询成功，共 {total} 条记录"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"查询指标列表失败: {e}")
        raise HTTPException(status_code=500, detail="查询指标列表失败")


@router.get("/{indicator_id}", summary="获取指标详情")
async def get_indicator_detail(
        indicator_id: int = Path(..., description="指标ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取单个指标的详细信息

    - **indicator_id**: 指标ID

    返回完整的指标信息，包括关联的资产列表
    """
    try:
        indicator = await indicator_system_service.get_indicator_by_id(
            db,
            indicator_id,
            load_relations=True
        )

        if not indicator:
            raise HTTPException(status_code=404, detail=f"指标 ID {indicator_id} 不存在")

        # 构建响应
        indicator_dict = IndicatorSystemResponse.model_validate(indicator).model_dump()
        indicator_dict['asset_count'] = len(indicator.asset_relations) if indicator.asset_relations else 0

        # 获取关联的资产列表
        linked_assets = await indicator_system_service.get_linked_assets(db, indicator_id)
        indicator_dict['linked_assets'] = linked_assets

        return create_response(
            data=indicator_dict,
            message="获取指标详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取指标详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取指标详情失败")


@router.post("/", summary="创建指标")
async def create_indicator(
        indicator_data: IndicatorSystemCreate,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的指标

    - **indicator_name**: 指标名称（必填）
    - 其他字段均为可选

    创建流程：
    1. 验证必填字段
    2. 创建指标记录
    3. 返回创建的指标信息
    """
    try:
        indicator = await indicator_system_service.create_indicator(
            db,
            indicator_data,
            creator
        )

        response = IndicatorSystemResponse.model_validate(indicator)

        return create_response(
            data=response.model_dump(),
            message=f"创建指标成功: {indicator.indicator_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建指标失败: {e}")
        raise HTTPException(status_code=500, detail="创建指标失败")


@router.put("/{indicator_id}", summary="更新指标")
async def update_indicator(
        indicator_id: int = Path(..., description="指标ID"),
        indicator_data: IndicatorSystemUpdate = None,
        updater: Optional[str] = Query(None, description="更新人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新指标信息

    - **indicator_id**: 指标ID
    - 只更新传入的字段，未传入的字段保持不变

    注意：
    - 所有字段均可选
    - 只更新提供的字段
    """
    try:
        indicator = await indicator_system_service.update_indicator(
            db,
            indicator_id,
            indicator_data,
            updater
        )

        response = IndicatorSystemResponse.model_validate(indicator)

        return create_response(
            data=response.model_dump(),
            message=f"更新指标成功: {indicator.indicator_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新指标失败: {e}")
        raise HTTPException(status_code=500, detail="更新指标失败")


@router.delete("/{indicator_id}", summary="删除指标")
async def delete_indicator(
        indicator_id: int = Path(..., description="指标ID"),
        force: bool = Query(False, description="是否强制删除（物理删除）"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除指标

    - **indicator_id**: 指标ID
    - **force**: 是否强制删除
      - False: 软删除（修改is_active为False）
      - True: 物理删除（从数据库删除记录）

    注意：
    - 物理删除会级联删除所有关联关系
    - 建议使用软删除
    """
    try:
        success = await indicator_system_service.delete_indicator(db, indicator_id, force)

        if success:
            return create_response(
                data={"indicator_id": indicator_id, "deleted": True, "force": force},
                message="删除指标成功"
            )
        else:
            raise HTTPException(status_code=500, detail="删除指标失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除指标失败: {e}")
        raise HTTPException(status_code=500, detail="删除指标失败")


# ==================== 资产关联管理 ====================

@router.post("/{indicator_id}/link-assets", summary="关联数据资产")
async def link_assets(
        indicator_id: int = Path(..., description="指标ID"),
        request: LinkAssetsRequest = None,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    将数据资产关联到指标

    - **indicator_id**: 指标ID
    - **asset_ids**: 数据资产ID列表
    - **relation_type**: 关联类型（source/reference/derived）
    - **relation_description**: 关联说明

    用途：
    - 标识指标的数据来源
    - 建立指标与数据资产的追溯关系
    """
    try:
        relations = await indicator_system_service.link_assets(
            db,
            indicator_id,
            request.asset_ids,
            request.relation_type.value,
            request.relation_description,
            creator
        )

        return create_response(
            data={"indicator_id": indicator_id, "linked_count": len(relations)},
            message=f"成功关联 {len(relations)} 个数据资产"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"关联数据资产失败: {e}")
        raise HTTPException(status_code=500, detail="关联数据资产失败")


@router.delete("/{indicator_id}/unlink-asset/{asset_id}", summary="解除资产关联")
async def unlink_asset(
        indicator_id: int = Path(..., description="指标ID"),
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    解除指标与数据资产的关联

    - **indicator_id**: 指标ID
    - **asset_id**: 资产ID
    """
    try:
        success = await indicator_system_service.unlink_asset(db, indicator_id, asset_id)

        if success:
            return create_response(
                data={"indicator_id": indicator_id, "asset_id": asset_id, "unlinked": True},
                message="解除关联成功"
            )
        else:
            raise HTTPException(status_code=500, detail="解除关联失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"解除关联失败: {e}")
        raise HTTPException(status_code=500, detail="解除关联失败")


@router.get("/{indicator_id}/linked-assets", summary="获取关联的资产")
async def get_linked_assets(
        indicator_id: int = Path(..., description="指标ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取指标关联的所有数据资产

    - **indicator_id**: 指标ID

    返回：
    - 关联的资产列表
    - 包含资产基本信息和关联关系信息
    """
    try:
        assets = await indicator_system_service.get_linked_assets(db, indicator_id)

        return create_response(
            data={"indicator_id": indicator_id, "assets": assets, "count": len(assets)},
            message=f"获取关联资产成功，共 {len(assets)} 个"
        )

    except Exception as e:
        logger.error(f"获取关联资产失败: {e}")
        raise HTTPException(status_code=500, detail="获取关联资产失败")


# ==================== 批量操作 ====================

@router.post("/batch/create", summary="批量创建指标")
async def batch_create_indicators(
        request: BatchCreateIndicatorsRequest,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    批量创建指标

    - **indicators**: 指标创建数据列表

    适用场景：
    - 从Excel导入
    - 批量迁移指标

    返回结果包含：
    - 成功创建的指标列表
    - 失败的项目列表（包含失败原因）

    注意：
    - 部分失败不影响其他项目的创建
    """
    try:
        success_indicators, failed_items = await indicator_system_service.batch_create_indicators(
            db,
            request.indicators,
            creator
        )

        result = BatchOperationResult(
            total=len(request.indicators),
            success_count=len(success_indicators),
            failed_count=len(failed_items),
            success_ids=[indicator.id for indicator in success_indicators],
            failed_items=failed_items
        )

        return create_response(
            data=result.model_dump(),
            message=f"批量创建完成：成功 {result.success_count}，失败 {result.failed_count}"
        )

    except Exception as e:
        logger.error(f"批量创建指标失败: {e}")
        raise HTTPException(status_code=500, detail="批量创建指标失败")


# ==================== Excel导入导出 ====================

@router.get("/template/download", summary="下载Excel模板")
async def download_template():
    """
    下载指标导入模板

    返回：
    - Excel文件（.xlsx格式）
    - 包含18个字段的表头
    - 包含示例数据

    模板格式：
    - 第一行：中文表头
    - 第二行：英文字段名
    - 第三行：示例数据
    """
    try:
        template_file = indicator_system_service.generate_excel_template()

        return StreamingResponse(
            template_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=indicator_system_template.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"下载模板失败: {e}")
        raise HTTPException(status_code=500, detail="下载模板失败")


@router.post("/import/excel", summary="从Excel导入指标")
async def import_from_excel(
        file: UploadFile = File(..., description="Excel文件"),
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    从Excel批量导入指标

    - **file**: Excel文件（.xlsx或.xls格式）

    文件格式要求：
    - 第一行：中文表头
    - 第二行：英文字段名
    - 第三行起：数据行

    导入规则：
    - 只有指标名称是必填字段
    - 其他字段均为可选
    - 跳过空行
    - 逐行导入，失败不影响其他行

    返回：
    - 成功导入数量
    - 失败行数及原因
    """
    try:
        # 验证文件类型
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="仅支持Excel文件（.xlsx或.xls）")

        # 读取文件内容
        file_content = await file.read()

        # 导入数据
        success_indicators, failed_rows = await indicator_system_service.import_from_excel(
            db,
            file_content,
            creator
        )

        result = ExcelImportResult(
            total_rows=len(success_indicators) + len(failed_rows),
            success_count=len(success_indicators),
            failed_count=len(failed_rows),
            failed_rows=failed_rows,
            message=f"导入完成：成功 {len(success_indicators)} 条，失败 {len(failed_rows)} 条"
        )

        return create_response(
            data=result.model_dump(),
            message=result.message
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"从Excel导入失败: {e}")
        raise HTTPException(status_code=500, detail=f"从Excel导入失败: {str(e)}")


@router.get("/export/excel", summary="导出指标数据")
async def export_indicators(
        keyword: Optional[str] = Query(None, description="搜索关键词"),
        business_domain: Optional[str] = Query(None, description="业务领域"),
        business_theme: Optional[str] = Query(None, description="业务主题"),
        indicator_category: Optional[str] = Query(None, description="指标类别"),
        is_active: Optional[bool] = Query(None, description="是否启用"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    导出指标数据为Excel

    支持按筛选条件导出
    """
    try:
        # 查询所有符合条件的数据（不分页）
        indicators, total = await indicator_system_service.search_indicators(
            db,
            page=1,
            page_size=10000,  # 最多导出10000条
            keyword=keyword,
            business_domain=business_domain,
            business_theme=business_theme,
            indicator_category=indicator_category,
            is_active=is_active
        )

        # 生成Excel文件
        excel_file = indicator_system_service.export_to_excel(indicators)

        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=indicators_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            }
        )

    except Exception as e:
        logger.error(f"导出指标数据失败: {e}")
        raise HTTPException(status_code=500, detail="导出数据失败")
# ==================== 统计分析 ====================

@router.get("/statistics/overview", summary="获取统计信息")
async def get_statistics(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取指标统计信息

    返回：
    - 总指标数
    - 启用/未启用数量
    - 按业务领域统计
    - 按指标类别统计
    - 按采集频率统计

    用于仪表盘展示
    """
    try:
        statistics = await indicator_system_service.get_statistics(db)

        return create_response(
            data=statistics.model_dump(),
            message="获取统计信息成功"
        )

    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取统计信息失败")


@router.get("/options/business-domains", summary="获取所有业务领域")
async def get_business_domains(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取所有业务领域（去重）

    用于下拉选择框
    """
    try:
        domains = await indicator_system_service.get_all_business_domains(db)

        return create_response(
            data={"domains": domains, "count": len(domains)},
            message=f"获取业务领域成功，共 {len(domains)} 个"
        )

    except Exception as e:
        logger.error(f"获取业务领域失败: {e}")
        raise HTTPException(status_code=500, detail="获取业务领域失败")


@router.get("/options/indicator-categories", summary="获取所有指标类别")
async def get_indicator_categories(
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取所有指标类别（去重）

    用于下拉选择框
    """
    try:
        categories = await indicator_system_service.get_all_indicator_categories(db)

        return create_response(
            data={"categories": categories, "count": len(categories)},
            message=f"获取指标类别成功，共 {len(categories)} 个"
        )

    except Exception as e:
        logger.error(f"获取指标类别失败: {e}")
        raise HTTPException(status_code=500, detail="获取指标类别失败")