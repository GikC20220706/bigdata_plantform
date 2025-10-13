# app/api/v1/data_asset.py
"""
数据资产API端点
提供资产的增删改查、字段管理等功能
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.data_catalog import (
    DataAssetCreate, DataAssetUpdate,
    DataAssetResponse, DataAssetListResponse,
    AssetColumnsResponse, AssetColumnDetail,
    BatchCreateAssetsRequest, BatchOperationResult,
    ImportTablesRequest
)
from app.services.data_asset_service import data_asset_service

router = APIRouter(prefix="/data-asset", tags=["数据资产"])


# ==================== 基础CRUD ====================

@router.get("/", summary="获取资产列表")
async def get_asset_list(
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页数量"),
        keyword: Optional[str] = Query(None, description="搜索关键词（名称、编码、表名、描述）"),
        catalog_id: Optional[int] = Query(None, description="目录ID"),
        data_source_id: Optional[int] = Query(None, description="数据源ID"),
        asset_type: Optional[str] = Query(None, description="资产类型: table/view/external/temp"),
        status: Optional[str] = Query(None, description="状态: normal/offline/maintenance/deprecated"),
        quality_level: Optional[str] = Query(None, description="质量等级: A/B/C/D"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    分页查询资产列表

    支持多条件筛选：
    - 关键词搜索（资产名称、编码、表名、描述）
    - 目录筛选
    - 数据源筛选
    - 类型筛选
    - 状态筛选
    - 质量等级筛选

    返回结果包含关联的目录名称和数据源名称
    """
    try:
        assets, total = await data_asset_service.search_assets(
            db,
            page=page,
            page_size=page_size,
            keyword=keyword,
            catalog_id=catalog_id,
            data_source_id=data_source_id,
            asset_type=asset_type,
            status=status,
            quality_level=quality_level
        )

        # 构建响应，补充关联信息
        items = []
        for asset in assets:
            asset_dict = DataAssetResponse.model_validate(asset).model_dump()
            # 补充目录名称
            if asset.catalog:
                asset_dict['catalog_name'] = asset.catalog.catalog_name
            # 补充数据源名称
            if asset.data_source:
                asset_dict['data_source_name'] = asset.data_source.name
            items.append(asset_dict)

        response = DataAssetListResponse(
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
        logger.error(f"查询资产列表失败: {e}")
        raise HTTPException(status_code=500, detail="查询资产列表失败")


@router.get("/{asset_id}", summary="获取资产详情")
async def get_asset_detail(
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取单个资产的详细信息

    - **asset_id**: 资产ID

    返回完整的资产信息，包括关联的目录和数据源
    """
    try:
        asset = await data_asset_service.get_asset_by_id(
            db,
            asset_id,
            load_relationships=True
        )

        if not asset:
            raise HTTPException(status_code=404, detail=f"资产 ID {asset_id} 不存在")

        # 构建响应
        asset_dict = DataAssetResponse.model_validate(asset).model_dump()
        if asset.catalog:
            asset_dict['catalog_name'] = asset.catalog.catalog_name
        if asset.data_source:
            asset_dict['data_source_name'] = asset.data_source.name

        return create_response(
            data=asset_dict,
            message="获取资产详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取资产详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取资产详情失败")


@router.post("/", summary="创建资产")
async def create_asset(
        asset_data: DataAssetCreate,
        creator: Optional[str] = Query(None, description="创建人"),
        auto_fetch_metadata: bool = Query(True, description="是否自动获取表元数据"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的数据资产

    - **asset_name**: 资产名称
    - **asset_code**: 资产编码（唯一）
    - **catalog_id**: 所属目录ID
    - **data_source_id**: 数据源ID
    - **table_name**: 表名
    - **auto_fetch_metadata**: 是否自动获取表结构和统计信息

    创建流程：
    1. 验证编码唯一性
    2. 验证目录和数据源存在性
    3. 如果开启auto_fetch_metadata，自动获取：
       - 表字段信息
       - 表行数
       - 数据大小
    4. 更新目录的资产计数

    注意：
    - 编码必须唯一
    - 目录和数据源必须存在
    - 自动获取元数据可能需要较长时间
    """
    try:
        asset = await data_asset_service.create_asset(
            db,
            asset_data,
            creator,
            auto_fetch_metadata
        )

        response = DataAssetResponse.model_validate(asset)

        return create_response(
            data=response.model_dump(),
            message=f"创建资产成功: {asset.asset_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建资产失败: {e}")
        raise HTTPException(status_code=500, detail="创建资产失败")


@router.put("/{asset_id}", summary="更新资产")
async def update_asset(
        asset_id: int = Path(..., description="资产ID"),
        asset_data: DataAssetUpdate = None,
        updater: Optional[str] = Query(None, description="更新人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新资产信息

    支持部分字段更新，只传需要修改的字段即可

    可更新字段：
    - **asset_name**: 资产名称
    - **catalog_id**: 所属目录
    - **business_description**: 业务描述
    - **technical_description**: 技术说明
    - **quality_level**: 质量等级
    - **usage_frequency**: 使用频率
    - **importance_level**: 重要程度
    - **status**: 状态
    - **business_owner**: 业务负责人
    - **technical_owner**: 技术负责人
    - **tags**: 标签
    - **allow_download**: 是否允许下载
    - **max_download_rows**: 最大下载行数

    注意：
    - 目录变更会自动更新目录的资产计数
    """
    try:
        asset = await data_asset_service.update_asset(
            db,
            asset_id,
            asset_data,
            updater
        )

        response = DataAssetResponse.model_validate(asset)

        return create_response(
            data=response.model_dump(),
            message=f"更新资产成功: {asset.asset_name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新资产失败: {e}")
        raise HTTPException(status_code=500, detail="更新资产失败")


@router.delete("/{asset_id}", summary="删除资产")
async def delete_asset(
        asset_id: int = Path(..., description="资产ID"),
        force: bool = Query(False, description="是否强制删除（物理删除）"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除资产

    - **asset_id**: 资产ID
    - **force**: 是否强制删除

    普通删除（force=False）：
    - 软删除，将状态改为deprecated
    - 设置is_public为False
    - 检查是否已发布API，如已发布则不允许删除

    强制删除（force=True）：
    - 物理删除，从数据库中移除记录
    - 级联删除字段信息和访问日志
    - 自动更新目录的资产计数
    """
    try:
        success = await data_asset_service.delete_asset(
            db,
            asset_id,
            force
        )

        if success:
            return create_response(
                data={"asset_id": asset_id, "deleted": True, "force": force},
                message="删除资产成功"
            )
        else:
            raise HTTPException(status_code=500, detail="删除资产失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"删除资产失败: {e}")
        raise HTTPException(status_code=500, detail="删除资产失败")


# ==================== 字段管理 ====================

@router.get("/{asset_id}/columns", summary="获取资产字段")
async def get_asset_columns(
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资产的所有字段信息

    - **asset_id**: 资产ID

    返回完整的字段列表，包括：
    - 字段基本信息（名称、类型、长度等）
    - 约束信息（主键、可空、唯一等）
    - 业务信息（中文名、描述等）
    - 统计信息（唯一值数、空值数等）
    - 关联的字段标准
    """
    try:
        # 验证资产是否存在
        asset = await data_asset_service.get_asset_by_id(db, asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail=f"资产 ID {asset_id} 不存在")

        # 获取字段列表
        columns = await data_asset_service.get_asset_columns(db, asset_id)

        # 构建响应
        column_details = [AssetColumnDetail.model_validate(col) for col in columns]

        response = AssetColumnsResponse(
            asset_id=asset.id,
            asset_name=asset.asset_name,
            total_columns=len(columns),
            columns=column_details
        )

        return create_response(
            data=response.model_dump(),
            message=f"获取字段信息成功，共 {len(columns)} 个字段"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取资产字段失败: {e}")
        raise HTTPException(status_code=500, detail="获取资产字段失败")


@router.post("/{asset_id}/columns/refresh", summary="刷新字段信息")
async def refresh_asset_columns(
        asset_id: int = Path(..., description="资产ID"),
        background_tasks: BackgroundTasks = None,
        db: AsyncSession = Depends(get_async_db)
):
    """
    从数据源重新获取字段信息

    - **asset_id**: 资产ID

    使用场景：
    - 表结构发生变更后同步
    - 更新字段统计信息

    操作流程：
    1. 删除旧的字段信息
    2. 从数据源重新获取表结构
    3. 保存新的字段信息
    4. 更新资产的字段数量和最后更新时间

    注意：
    - 此操作会删除旧的字段信息，包括手动添加的描述等
    - 建议在表结构确实发生变化时才执行
    """
    try:
        asset = await data_asset_service.refresh_asset_metadata(db, asset_id)

        response = DataAssetResponse.model_validate(asset)

        return create_response(
            data=response.model_dump(),
            message=f"刷新字段信息成功，共 {asset.column_count} 个字段"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"刷新字段信息失败: {e}")
        raise HTTPException(status_code=500, detail="刷新字段信息失败")


# ==================== 批量操作 ====================

@router.post("/batch/create", summary="批量创建资产")
async def batch_create_assets(
        request: BatchCreateAssetsRequest,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    批量创建资产

    - **assets**: 资产创建数据列表

    适用场景：
    - 从Excel/CSV导入
    - 批量迁移资产
    - 从数据源批量导入表

    返回结果包含：
    - 成功创建的资产列表
    - 失败的项目列表（包含失败原因）

    注意：
    - 批量创建时不会自动获取元数据，以提高速度
    - 如需元数据，可在创建后调用刷新接口
    - 部分失败不影响其他项目的创建
    """
    try:
        success_assets, failed_items = await data_asset_service.batch_create_assets(
            db,
            request.assets,
            creator
        )

        result = BatchOperationResult(
            total=len(request.assets),
            success_count=len(success_assets),
            failed_count=len(failed_items),
            success_ids=[asset.id for asset in success_assets],
            failed_items=failed_items
        )

        return create_response(
            data=result.model_dump(),
            message=f"批量创建完成：成功 {result.success_count}，失败 {result.failed_count}"
        )

    except Exception as e:
        logger.error(f"批量创建资产失败: {e}")
        raise HTTPException(status_code=500, detail="批量创建资产失败")


@router.post("/batch/update-catalog", summary="批量移动到目录")
async def batch_move_to_catalog(
        asset_ids: List[int] = Query(..., description="资产ID列表"),
        target_catalog_id: int = Query(..., description="目标目录ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    批量将资产移动到指定目录

    - **asset_ids**: 资产ID列表
    - **target_catalog_id**: 目标目录ID

    操作：
    - 更新所有指定资产的catalog_id
    - 自动更新原目录和目标目录的资产计数
    """
    try:
        from app.schemas.data_catalog import DataAssetUpdate

        success_count = 0
        failed_items = []

        for asset_id in asset_ids:
            try:
                update_data = DataAssetUpdate(catalog_id=target_catalog_id)
                await data_asset_service.update_asset(db, asset_id, update_data)
                success_count += 1
            except Exception as e:
                failed_items.append({
                    "asset_id": asset_id,
                    "error": str(e)
                })

        result = BatchOperationResult(
            total=len(asset_ids),
            success_count=success_count,
            failed_count=len(failed_items),
            success_ids=[],
            failed_items=failed_items
        )

        return create_response(
            data=result.model_dump(),
            message=f"批量移动完成：成功 {success_count}，失败 {len(failed_items)}"
        )

    except Exception as e:
        logger.error(f"批量移动资产失败: {e}")
        raise HTTPException(status_code=500, detail="批量移动资产失败")


@router.post("/batch/update-status", summary="批量更新状态")
async def batch_update_status(
        asset_ids: List[int] = Query(..., description="资产ID列表"),
        status: str = Query(..., description="新状态: normal/offline/maintenance/deprecated"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    批量更新资产状态

    - **asset_ids**: 资产ID列表
    - **status**: 新状态

    适用场景：
    - 批量下线资产
    - 批量启用资产
    - 批量标记维护中
    """
    try:
        from app.schemas.data_catalog import DataAssetUpdate, AssetStatus

        success_count = 0
        failed_items = []

        for asset_id in asset_ids:
            try:
                update_data = DataAssetUpdate(status=AssetStatus(status))
                await data_asset_service.update_asset(db, asset_id, update_data)
                success_count += 1
            except Exception as e:
                failed_items.append({
                    "asset_id": asset_id,
                    "error": str(e)
                })

        result = BatchOperationResult(
            total=len(asset_ids),
            success_count=success_count,
            failed_count=len(failed_items),
            success_ids=[],
            failed_items=failed_items
        )

        return create_response(
            data=result.model_dump(),
            message=f"批量更新状态完成：成功 {success_count}，失败 {len(failed_items)}"
        )

    except Exception as e:
        logger.error(f"批量更新状态失败: {e}")
        raise HTTPException(status_code=500, detail="批量更新状态失败")


# ==================== 数据源集成 ====================

@router.post("/import-from-datasource", summary="从数据源导入表")
async def import_from_datasource(
        data_source_name: str = Query(..., description="数据源名称"),  # ← 改这里
        catalog_id: int = Query(..., description="目标目录ID"),
        database_name: Optional[str] = Query(None, description="数据库名（可选）"),
        table_patterns: Optional[List[str]] = Query(None, description="表名匹配模式，如: cus_%, order_%"),
        include_columns: bool = Query(True, description="是否导入字段信息"),
        background_tasks: BackgroundTasks = None,
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    从数据源批量导入表作为资产

    - **data_source_name**: 数据源名称（如：cwjg, test-mysql）  # ← 改文档
    - **catalog_id**: 目标目录ID
    - **database_name**: 数据库名（可选，某些数据源需要）
    - **table_patterns**: 表名匹配模式（可选）
      - 使用SQL LIKE语法，如: ['cus_%', 'order_%']
      - 不传则导入所有表
    - **include_columns**: 是否导入字段信息（默认true）

    # ... 其他文档保持不变
    """
    try:
        logger.info(
            f"开始批量导入: 数据源={data_source_name}, "  # ← 改这里
            f"目录={catalog_id}, 数据库={database_name}, "
            f"模式={table_patterns}"
        )

        success_assets, failed_items = await data_asset_service.import_tables_from_datasource(
            db,
            data_source_name=data_source_name,  # ← 改这里：传 name 而不是 id
            catalog_id=catalog_id,
            database_name=database_name,
            table_patterns=table_patterns,
            include_columns=include_columns,
            creator=creator
        )

        result = BatchOperationResult(
            total=len(success_assets) + len(failed_items),
            success_count=len(success_assets),
            failed_count=len(failed_items),
            success_ids=[asset.id for asset in success_assets],
            failed_items=failed_items
        )

        return create_response(
            data=result.model_dump(),
            message=f"批量导入完成：成功 {result.success_count}，失败 {result.failed_count}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"从数据源导入表失败: {e}")
        raise HTTPException(status_code=500, detail=f"从数据源导入表失败: {str(e)}")


# ==================== 数据预览 ====================

@router.post("/{asset_id}/preview", summary="预览数据")
async def preview_data(
        asset_id: int = Path(..., description="资产ID"),
        limit: int = Query(100, ge=1, le=1000, description="预览行数"),
        offset: int = Query(0, ge=0, description="偏移量"),
        columns: Optional[List[str]] = Query(None, description="指定列，不传则返回全部"),
        user: Optional[str] = Query(None, description="当前用户"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    预览资产数据

    - **asset_id**: 资产ID
    - **limit**: 预览行数（最多1000行）
    - **offset**: 偏移量（用于分页）
    - **columns**: 指定要查询的列，不传则查询全部列

    功能：
    - 支持分页预览
    - 支持指定列查询
    - 自动记录访问日志
    - 更新预览次数

    返回：
    - 列信息（名称、类型）
    - 数据行
    - 是否有更多数据

    注意：
    - 最多预览1000行
    - 不支持复杂查询条件
    - 预览操作会被记录
    """
    try:
        from fastapi import Request

        # 获取客户端IP（需要在函数参数中添加request）
        # 这里简化处理
        user_ip = "unknown"

        result = await data_asset_service.preview_asset_data(
            db,
            asset_id=asset_id,
            limit=limit,
            offset=offset,
            columns=columns,
            user=user,
            user_ip=user_ip
        )

        return create_response(
            data=result,
            message=f"预览成功，返回 {result['preview_rows']} 行数据"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"预览数据失败: {e}")
        raise HTTPException(status_code=500, detail="预览数据失败")


# ==================== 数据下载 ====================

@router.post("/{asset_id}/download", summary="下载数据")
async def download_data(
        asset_id: int = Path(..., description="资产ID"),
        format: str = Query("excel", description="下载格式: excel/csv/json"),
        columns: Optional[List[str]] = Query(None, description="指定列"),
        max_rows: Optional[int] = Query(None, description="最大行数"),
        filename: Optional[str] = Query(None, description="文件名"),
        user: Optional[str] = Query(None, description="当前用户"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    下载资产数据

    - **asset_id**: 资产ID
    - **format**: 下载格式（excel/csv/json）
    - **columns**: 指定列，不传则下载全部
    - **max_rows**: 最大行数，不传则使用资产配置的限制
    - **filename**: 自定义文件名

    支持格式：
    - **Excel** (.xlsx): 适合一般数据查看
    - **CSV** (.csv): 适合数据导入
    - **JSON** (.json): 适合程序处理

    流程：
    1. 检查是否允许下载
    2. 验证行数限制
    3. 查询数据
    4. 生成文件
    5. 保存到临时目录
    6. 返回下载信息
    7. 记录访问日志

    返回：
    - download_id: 下载任务ID
    - filename: 文件名
    - file_size: 文件大小
    - download_url: 下载链接
    - expires_at: 过期时间

    注意：
    - 文件保存在临时目录，当天过期
    - 大文件下载可能需要较长时间
    - 下载次数会被统计
    """
    try:
        user_ip = "unknown"

        result = await data_asset_service.download_asset_data(
            db,
            asset_id=asset_id,
            format=format,
            columns=columns,
            max_rows=max_rows,
            filename=filename,
            user=user,
            user_ip=user_ip
        )

        return create_response(
            data=result,
            message=f"下载任务创建成功，共 {result['row_count']} 行数据"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"下载数据失败: {e}")
        raise HTTPException(status_code=500, detail="下载数据失败")


@router.get("/{asset_id}/download/{download_id}", summary="获取下载文件")
async def get_download_file(
        asset_id: int = Path(..., description="资产ID"),
        download_id: str = Path(..., description="下载任务ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取下载的文件

    - **asset_id**: 资产ID
    - **download_id**: 下载任务ID（从下载接口返回）

    返回文件流，浏览器会自动下载

    注意：
    - 文件仅在当天有效
    - 下载后文件仍保留在服务器
    """
    try:
        from fastapi.responses import FileResponse
        from pathlib import Path as PathLib

        # 查找文件
        download_dir = PathLib("downloads/temp")

        # 简单实现：遍历文件找到匹配的
        found_file = None
        for file_path in download_dir.glob(f"*{download_id[:8]}*"):
            found_file = file_path
            break

        if not found_file or not found_file.exists():
            raise HTTPException(status_code=404, detail="文件不存在或已过期")

        # 返回文件
        return FileResponse(
            path=str(found_file),
            filename=found_file.name,
            media_type="application/octet-stream"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取下载文件失败: {e}")
        raise HTTPException(status_code=500, detail="获取下载文件失败")


# ==================== API发布 ====================

@router.post("/{asset_id}/publish-api", summary="发布为API")
async def publish_as_api(
        asset_id: int = Path(..., description="资产ID"),
        api_name: str = Query(..., description="API名称"),
        api_path: str = Query(..., description="API路径，如: /customer_info"),
        description: Optional[str] = Query(None, description="API描述"),
        http_method: str = Query("GET", description="HTTP方法: GET/POST"),
        response_format: str = Query("json", description="响应格式: json/csv/excel"),
        select_columns: Optional[List[str]] = Query(None, description="SELECT的字段"),
        where_conditions: Optional[str] = Query(None, description="WHERE条件"),
        order_by: Optional[str] = Query(None, description="排序字段"),
        limit: int = Query(1000, ge=1, le=10000, description="默认返回行数"),
        is_public: bool = Query(True, description="是否公开"),
        rate_limit: int = Query(100, ge=1, le=1000, description="频率限制（次/分钟）"),
        cache_ttl: int = Query(300, ge=0, le=3600, description="缓存时间（秒）"),
        creator: Optional[str] = Query(None, description="创建人"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    将资产发布为API接口

    - **asset_id**: 资产ID
    - **api_name**: API名称
    - **api_path**: API路径（会自动加上 /api/custom/ 前缀）
    - **http_method**: HTTP方法（GET/POST）
    - **response_format**: 响应格式（json/csv/excel）

    查询配置：
    - **select_columns**: SELECT的字段，不传则查询全部
    - **where_conditions**: WHERE条件，如: status='active'
    - **order_by**: 排序字段，如: created_at DESC
    - **limit**: 默认返回行数

    访问控制：
    - **is_public**: 是否公开访问
    - **rate_limit**: 频率限制（次/分钟）
    - **cache_ttl**: 缓存时间（秒）

    功能：
    1. 自动生成SQL查询模板
    2. 创建CustomAPI记录
    3. 注册到动态路由
    4. 关联到资产

    生成的API支持：
    - 参数化查询
    - 多种响应格式
    - 缓存机制
    - 频率限制

    返回：
    - api_id: API ID
    - api_url: 完整访问URL
    - test_url: 测试URL

    注意：
    - 资产已发布API则不能重复发布
    - API路径必须唯一
    - 发布后可以立即访问
    """
    try:
        # 构建API配置
        api_config = {
            'api_name': api_name,
            'api_path': api_path,
            'description': description,
            'http_method': http_method,
            'response_format': response_format,
            'select_columns': select_columns,
            'where_conditions': where_conditions,
            'order_by': order_by,
            'limit': limit,
            'is_public': is_public,
            'rate_limit': rate_limit,
            'cache_ttl': cache_ttl
        }

        result = await data_asset_service.publish_asset_as_api(
            db,
            asset_id,
            api_config,
            creator
        )

        return create_response(
            data=result,
            message=f"发布API成功: {result['api_name']}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"发布API失败: {e}")
        raise HTTPException(status_code=500, detail="发布API失败")


@router.get("/{asset_id}/api-info", summary="获取已发布的API信息")
async def get_published_api_info(
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资产已发布的API信息

    - **asset_id**: 资产ID

    返回：
    - API基本信息
    - 访问URL
    - 调用统计
    - 配置信息

    如果资产未发布API，返回404
    """
    try:
        # 获取资产
        asset = await data_asset_service.get_asset_by_id(db, asset_id, load_relationships=True)

        if not asset:
            raise HTTPException(status_code=404, detail=f"资产 ID {asset_id} 不存在")

        if not asset.is_api_published or not asset.published_api_id:
            raise HTTPException(status_code=404, detail="资产未发布API")

        # 获取API信息
        from app.services.custom_api_service import custom_api_service
        api = await custom_api_service.get_api(db, asset.published_api_id)

        if not api:
            raise HTTPException(status_code=404, detail="关联的API不存在")

        # 构建响应
        api_info = {
            'api_id': api.id,
            'api_name': api.api_name,
            'api_path': api.api_path,
            'api_url': f"http://localhost:8000{api.api_path}",  # TODO: 使用配置的域名
            'http_method': api.http_method.value,
            'response_format': api.response_format.value,
            'is_active': api.is_active,
            'is_public': api.is_public,
            'rate_limit': api.rate_limit,
            'cache_ttl': api.cache_ttl,
            'total_calls': api.total_calls,
            'success_calls': api.success_calls,
            'published_at': asset.api_publish_time,
            'test_url': f"http://localhost:8000{api.api_path}?limit=10"
        }

        return create_response(
            data=api_info,
            message="获取API信息成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取API信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取API信息失败")


@router.delete("/{asset_id}/unpublish-api", summary="取消发布API")
async def unpublish_api(
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    取消发布的API

    - **asset_id**: 资产ID

    操作：
    1. 注销动态路由
    2. 删除CustomAPI记录
    3. 清除资产的API关联信息

    注意：
    - 取消后API立即不可访问
    - 历史调用记录会被保留
    """
    try:
        success = await data_asset_service.unpublish_asset_api(db, asset_id)

        if success:
            return create_response(
                data={"asset_id": asset_id, "unpublished": True},
                message="取消API发布成功"
            )
        else:
            raise HTTPException(status_code=500, detail="取消API发布失败")

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"取消API发布失败: {e}")
        raise HTTPException(status_code=500, detail="取消API发布失败")


# ==================== 统计信息 ====================

@router.get("/{asset_id}/statistics", summary="获取资产统计")
async def get_asset_statistics(
        asset_id: int = Path(..., description="资产ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资产的统计信息

    - **asset_id**: 资产ID

    包括：
    - 基础统计（行数、大小、字段数）
    - 访问统计（预览、下载、API调用次数）
    - 最近访问时间
    - 热度评分（0-100）

    热度评分算法：
    - 预览 × 1 + 下载 × 5 + API调用 × 3
    - 对数归一化到0-100
    """
    try:
        statistics = await data_asset_service.get_asset_statistics(db, asset_id)

        return create_response(
            data=statistics,
            message="获取统计信息成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取统计信息失败")


@router.post("/{asset_id}/refresh-statistics", summary="刷新统计信息")
async def refresh_statistics(
    asset_id: int = Path(..., description="资产ID"),
    db: AsyncSession = Depends(get_async_db),
    background_tasks: BackgroundTasks = None  # ✅ 添加默认值或调整顺序
):
    """
    刷新资产统计信息

    - **asset_id**: 资产ID

    从数据源重新获取：
    - 表行数
    - 数据大小
    - 字段统计信息

    注意：
    - 此操作可能耗时较长
    - 建议使用后台任务异步处理
    """
    try:
        # 简化实现：直接调用刷新元数据
        asset = await data_asset_service.refresh_asset_metadata(db, asset_id)

        return create_response(
            data={
                "asset_id": asset.id,
                "row_count": asset.row_count,
                "data_size": asset.data_size,
                "column_count": asset.column_count
            },
            message="刷新统计信息成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"刷新统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="刷新统计信息失败")


# ==================== 访问日志 ====================

@router.get("/{asset_id}/access-logs", summary="获取访问日志")
async def get_access_logs(
        asset_id: int = Path(..., description="资产ID"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(20, ge=1, le=100, description="每页数量"),
        access_type: Optional[str] = Query(None, description="访问类型: preview/download/api"),
        start_time: Optional[str] = Query(None, description="开始时间，格式: 2025-01-10T00:00:00"),
        end_time: Optional[str] = Query(None, description="结束时间，格式: 2025-01-10T23:59:59"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资产的访问日志

    - **asset_id**: 资产ID
    - **page**: 页码
    - **page_size**: 每页数量
    - **access_type**: 访问类型筛选
    - **start_time**: 开始时间
    - **end_time**: 结束时间

    支持筛选：
    - 访问类型（预览/下载/API）
    - 时间范围

    返回信息包括：
    - 访问时间
    - 访问用户
    - 访问IP
    - 返回记录数
    - 响应时间
    - 状态（成功/失败）
    """
    try:
        from datetime import datetime as dt
        from app.schemas.data_catalog import AssetAccessLogListResponse, AssetAccessLogResponse

        # 解析时间
        start_dt = dt.fromisoformat(start_time) if start_time else None
        end_dt = dt.fromisoformat(end_time) if end_time else None

        logs, total = await data_asset_service.get_asset_access_logs(
            db,
            asset_id=asset_id,
            page=page,
            page_size=page_size,
            access_type=access_type,
            start_time=start_dt,
            end_time=end_dt
        )

        # 构建响应
        log_items = [AssetAccessLogResponse.model_validate(log) for log in logs]

        response = AssetAccessLogListResponse(
            total=total,
            page=page,
            page_size=page_size,
            items=log_items
        )

        return create_response(
            data=response.model_dump(),
            message=f"查询成功，共 {total} 条记录"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取访问日志失败: {e}")
        raise HTTPException(status_code=500, detail="获取访问日志失败")