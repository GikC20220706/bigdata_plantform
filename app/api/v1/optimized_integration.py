import json

from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks, UploadFile, File, Form
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.utils.response import create_response
from app.services.optimized_data_integration_service import get_optimized_data_integration_service
from app.utils.integration_cache import integration_cache
from loguru import logger

router = APIRouter()

# 1. 数据集成概览
@router.get("/", summary="获取数据集成概览")
async def get_integration_overview():
    try:
        service = get_optimized_data_integration_service()
        overview = await service.get_data_sources_overview()
        return create_response(data=overview, message="获取数据集成概览成功")
    except Exception as e:
        logger.error(f"获取数据集成概览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 2. 数据源管理
@router.get("/sources", summary="获取数据源列表")
async def get_data_sources(
        page: int = Query(1, ge=1, description="页码，从1开始"),
        page_size: int = Query(10, ge=1, le=100, description="每页数量，最大100"),
        include_table_count: bool = Query(False, description="是否包含表数量统计"),
        table_count_limit: int = Query(1000, ge=1, le=10000, description="表数量统计的最大查询数量"),
        fast_mode: bool = Query(True, description="是否使用快速模式（避免耗时查询）")
):
    try:
        service = get_optimized_data_integration_service()

        if include_table_count:
            # 获取所有数据源的详细信息
            all_sources = await service.get_data_sources_list_with_limited_stats(
                table_limit=table_count_limit,
                fast_mode=fast_mode
            )
        else:
            # 获取所有数据源的基础信息
            all_sources = await service.get_data_sources_list_basic()

        # 应用分页
        total = len(all_sources)
        offset = (page - 1) * page_size
        sources = all_sources[offset:offset + page_size]

        connected_count = len([s for s in all_sources if s.get('status') == 'connected'])
        disconnected_count = total - connected_count

        return create_response(
            data={
                "sources": sources,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": (total + page_size - 1) // page_size,
                "connected": connected_count,
                "disconnected": disconnected_count,
                "query_params": {
                    "include_table_count": include_table_count,
                    "table_count_limit": table_count_limit if include_table_count else None,
                    "fast_mode": fast_mode
                }
            },
            message="获取数据源列表成功"
        )
    except Exception as e:
        logger.error(f"获取数据源列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sources", summary="创建数据源")
async def create_data_source(request: dict):
    try:
        logger.info(f"收到创建数据源请求: {request}")

        name = request.get('name')
        db_type = request.get('type')
        config = request.get('config')
        description = request.get('description', '')

        if not all([name, db_type, config]):
            raise HTTPException(status_code=400, detail="缺少必要字段: name, type, config")

        logger.info(f"开始创建数据源: {name}")
        service = get_optimized_data_integration_service()
        result = await service.add_data_source(
            name=name,
            db_type=db_type,
            config=config,
            description=description
        )

        logger.info(f"数据源创建结果: {result}")

        if result.get('success'):
            return create_response(data=result, message=f"数据源 {name} 创建成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '创建数据源失败'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建数据源失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/sources/{source_name}", summary="删除数据源")
async def delete_data_source(source_name: str):
    try:
        service = get_optimized_data_integration_service()
        result = await service.remove_data_source(source_name)
        if result.get('success'):
            return create_response(data=result, message=f"数据源 {source_name} 删除成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '数据源不存在'))
    except Exception as e:
        logger.error(f"删除数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sources/{source_name}/test", summary="测试数据源连接")
async def test_data_source(source_name: str):
    try:
        service = get_optimized_data_integration_service()
        result = await service.test_data_source(source_name)
        return create_response(data=result, message="连接测试完成")
    except Exception as e:
        logger.error(f"测试数据源连接失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch/test", summary="批量测试数据源连接")
async def batch_test_connections(source_names: List[str] = Body(..., description="数据源名称列表")):
    try:
        if len(source_names) > 50:
            raise HTTPException(status_code=400, detail="批量测试数量不能超过50个")
        service = get_optimized_data_integration_service()
        summary = await service.batch_test_connections(source_names)
        return create_response(data=summary, message=f"批量连接测试完成，成功 {summary['successful']}/{summary['total_tested']}")
    except Exception as e:
        logger.error(f"批量测试连接失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 3. 数据库/表操作
@router.get("/sources/{source_name}/databases", summary="获取数据库列表")
async def get_databases(source_name: str):
    try:
        service = get_optimized_data_integration_service()
        result = await service.get_databases(source_name)
        if result.get('success'):
            return create_response(data=result, message=f"获取 {source_name} 数据库列表成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取数据库列表失败'))
    except Exception as e:
        logger.error(f"获取数据库列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/tables", summary="获取表列表")
async def get_tables(
    source_name: str,
    database: Optional[str] = Query(None, description="数据库名称"),
    schema: Optional[str] = Query(None, description="Schema名称"),
    limit: int = Query(100, ge=1, le=1000, description="每页数量"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    try:
        service = get_optimized_data_integration_service()
        result = await service.get_tables(
            source_name, database, schema, limit, offset
        )
        if result.get('success'):
            return create_response(data=result, message=f"获取 {source_name} 表列表成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取表列表失败'))
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources/{source_name}/tables/{table_name}/schema", summary="获取表结构")
async def get_table_schema(source_name: str, table_name: str,
                           database: Optional[str] = Query(None, description="数据库名称"),
                           schema: Optional[str] = Query(None, description="Schema名称")):
    try:
        logger.info(f"API层接收到请求: source={source_name}, table={table_name}, db={database}, schema={schema}")
        service = get_optimized_data_integration_service()
        result = await service.get_table_schema(source_name, table_name, database, schema)

        logger.info(f"Service返回结果: success={result.get('success')}, error={result.get('error')}")

        if result.get('success'):
            return create_response(data=result, message=f"获取表 {table_name} 结构成功")
        else:
            error_msg = result.get('error', '获取表结构失败')
            logger.error(f"Service层返回失败: {error_msg}")
            if result.get('error_trace'):
                logger.error(f"Service层错误堆栈:\n{result.get('error_trace')}")
            raise HTTPException(status_code=404, detail=error_msg)
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"API层异常: {e}")
        logger.error(f"API层错误堆栈:\n{error_detail}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/tables/{table_name}/metadata", summary="获取表元数据")
async def get_table_metadata(source_name: str, table_name: str, database: Optional[str] = Query(None, description="数据库名称"),
    schema: Optional[str] = Query(None, description="Schema名称")):
    try:
        service = get_optimized_data_integration_service()
        result = await service.get_table_metadata(source_name, table_name, database,schema)
        if result.get('success'):
            return create_response(data=result, message=f"获取表 {table_name} 元数据成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取表元数据失败'))
    except Exception as e:
        logger.error(f"获取表元数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tables/search", summary="搜索表")
async def search_tables(keyword: Optional[str] = Query(None), source_name: Optional[str] = Query(None), table_type: Optional[str] = Query(None)):
    try:
        service = get_optimized_data_integration_service()
        result = await service.search_tables(keyword=keyword, source_name=source_name, table_type=table_type)
        if result.get('success'):
            return create_response(data=result, message="表搜索成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '表搜索失败'))
    except Exception as e:
        logger.error(f"搜索表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 4. 查询与预览
@router.post("/sources/{source_name}/query", summary="执行查询")
async def execute_query(source_name: str, request: dict):
    try:
        query = request.get('query')
        database = request.get('database')
        schema = request.get('schema')
        limit = request.get('limit', 100)
        if not query:
            raise HTTPException(status_code=400, detail="查询语句不能为空")
        service = get_optimized_data_integration_service()
        result = await service.execute_query(source_name=source_name, query=query, database=database, schema=schema, limit=limit)
        if result.get('success'):
            return create_response(data=result, message="查询执行成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '查询执行失败'))
    except Exception as e:
        logger.error(f"执行查询失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/preview", summary="预览数据源数据")
async def preview_data_source(source_name: str, table_name: Optional[str] = Query(None), database: Optional[str] = Query(None), limit: int = Query(10, ge=1, le=100)):
    try:
        service = get_optimized_data_integration_service()
        result = await service.preview_data_source(source_name=source_name, table_name=table_name, database=database, limit=limit)
        if result.get('success'):
            return create_response(data=result, message="数据预览成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '数据预览失败'))
    except Exception as e:
        logger.error(f"数据预览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 5. 健康检查
@router.get("/health", summary="获取数据集成模块健康状态")
async def get_integration_health():
    try:
        service = get_optimized_data_integration_service()
        health_data = await service.get_health_status()
        return create_response(data=health_data, message="获取数据集成模块健康状态成功")
    except Exception as e:
        logger.error(f"获取健康状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 6. 缓存管理
@router.get("/cache/status", summary="获取缓存状态")
async def get_cache_status():
    try:
        cache_stats = integration_cache.get_cache_stats()
        return create_response(
            data={
                "cache_statistics": cache_stats,
                "cache_levels": {
                    "memory_cache": "60s TTL - 最快访问",
                    "redis_cache": "300s TTL - 跨进程共享"
                },
                "last_check": datetime.now()
            },
            message="获取缓存状态成功"
        )
    except Exception as e:
        logger.error(f"获取缓存状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/cache/clear", summary="清除缓存")
async def clear_cache(pattern: Optional[str] = Query(None, description="清除模式(可选)")):
    try:
        await integration_cache.invalidate_cache(pattern)
        return create_response(
            data={
                "cleared_pattern": pattern or "all",
                "cleared_at": datetime.now(),
                "message": "缓存已清除，下次请求将重新收集数据"
            },
            message="缓存清除成功"
        )
    except Exception as e:
        logger.error(f"清除缓存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 7. Excel相关
@router.post("/sources/excel/upload", summary="上传Excel文件创建数据源")
async def upload_excel_source(name: str = Form(...), file: UploadFile = File(...), description: Optional[str] = Form(None)):
    try:
        service = get_optimized_data_integration_service()
        result = await service.upload_excel_source(name, file, description)
        if result.get('success'):
            return create_response(data=result, message=f"Excel数据源 {name} 创建成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '上传Excel文件失败'))
    except Exception as e:
        logger.error(f"上传Excel文件失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/excel/files", summary="获取已上传的Excel文件列表")
async def list_excel_files():
    try:
        service = get_optimized_data_integration_service()
        files = await service.list_excel_files()
        return create_response(data={"files": files, "total_count": len(files)}, message="获取Excel文件列表成功")
    except Exception as e:
        logger.error(f"获取Excel文件列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/sheets", summary="获取Excel工作表列表")
async def get_excel_sheets(source_name: str):
    try:
        service = get_optimized_data_integration_service()
        result = await service.get_excel_sheets(source_name)
        if result.get('success'):
            return create_response(data=result, message="获取Excel工作表列表成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '获取Excel工作表失败'))
    except Exception as e:
        logger.error(f"获取Excel工作表列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/sheets/{sheet_name}/preview", summary="预览Excel工作表数据")
async def preview_excel_sheet(source_name: str, sheet_name: str, limit: int = Query(10, ge=1, le=100)):
    try:
        service = get_optimized_data_integration_service()
        result = await service.preview_excel_sheet(source_name, sheet_name, limit)
        if result.get('success'):
            return create_response(data=result, message="预览Excel工作表数据成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '预览Excel工作表失败'))
    except Exception as e:
        logger.error(f"预览Excel工作表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/sources/{source_name}/excel", summary="删除Excel数据源和文件")
async def delete_excel_source(source_name: str):
    try:
        service = get_optimized_data_integration_service()
        result = await service.delete_excel_source(source_name)
        if result.get('success'):
            return create_response(data=result, message="Excel数据源及文件删除成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '删除Excel数据源失败'))
    except Exception as e:
        logger.error(f"删除Excel数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sources/{source_name}/tables/refresh", summary="刷新数据源表信息")
async def refresh_source_tables(
        source_name: str,
        database: Optional[str] = Query(None, description="数据库名称"),
        schema: Optional[str] = Query(None, description="Schema名称")
):
    """手动刷新数据源的表信息到本地数据库"""
    try:
        service = get_optimized_data_integration_service()
        result = await service._refresh_tables_to_db(source_name, database, schema)

        if result.get('success'):
            return create_response(data=result, message=f"数据源 {source_name} 表信息刷新成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '刷新失败'))

    except Exception as e:
        logger.error(f"刷新表信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/sources/{source_name}", summary="更新数据源")
async def update_data_source(
        source_name: str,
        request: dict = Body(..., description="更新请求")
):
    """
    更新数据源配置

    请求格式:
    {
        "type": "mysql",  # 可选，数据库类型
        "config": {       # 可选，连接配置
            "host": "new-host",
            "port": 3306,
            "username": "user",
            "password": "pass",
            "database": "db"
        },
        "description": "更新的描述"  # 可选
    }
    """
    try:
        logger.info(f"收到更新数据源请求: {source_name} -> {request}")

        db_type = request.get('type')
        config = request.get('config')
        description = request.get('description')

        if not any([db_type, config, description]):
            raise HTTPException(
                status_code=400,
                detail="至少需要提供 type、config 或 description 中的一个字段"
            )

        service = get_optimized_data_integration_service()
        result = await service.update_data_source(
            name=source_name,
            db_type=db_type,
            config=config,
            description=description
        )

        logger.info(f"数据源更新结果: {result}")

        if result.get('success'):
            return create_response(
                data=result,
                message=f"数据源 {source_name} 更新成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get('error', '更新数据源失败')
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新数据源失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources/{source_name}/info", summary="获取数据源详细信息")
async def get_data_source_info(source_name: str):
    """获取数据源的详细信息，包括配置、状态、统计信息等"""
    try:
        service = get_optimized_data_integration_service()
        result = await service.get_data_source_info(source_name)

        if result.get('success'):
            return create_response(
                data=result,
                message=f"获取数据源 {source_name} 信息成功"
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=result.get('error', '数据源不存在')
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取数据源信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources/{source_name}/health", summary="检测数据源健康状态")
async def detect_data_source_health(source_name: str):
    """
    检测指定数据源的健康状态
    包括连接测试、数据库访问、表结构检查等
    """
    try:
        service = get_optimized_data_integration_service()
        result = await service.detect_data_source_health(source_name)

        if result.get('success'):
            return create_response(
                data=result,
                message=f"数据源 {source_name} 健康检测完成"
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=result.get('error', '数据源不存在')
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"数据源健康检测失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources/health", summary="批量检测所有数据源健康状态")
async def detect_all_data_sources_health():
    """
    检测所有数据源的健康状态
    返回整体健康统计和详细检测结果
    """
    try:
        service = get_optimized_data_integration_service()
        result = await service.detect_data_source_health()

        return create_response(
            data=result,
            message=f"所有数据源健康检测完成"
        )

    except Exception as e:
        logger.error(f"批量数据源健康检测失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sources/{source_name}/validate", summary="验证数据源配置")
async def validate_data_source_config(
        source_name: str,
        config: dict = Body(..., description="要验证的配置")
):
    """
    验证数据源配置是否有效
    不会实际更新数据源，只进行配置验证和连接测试
    """
    try:
        db_type = config.get('type')
        connection_config = config.get('config', {})

        if not db_type or not connection_config:
            raise HTTPException(
                status_code=400,
                detail="需要提供 type 和 config 字段"
            )

        # 创建临时客户端进行验证
        from app.utils.data_integration_clients import DatabaseClientFactory

        try:
            temp_client = DatabaseClientFactory.create_client(db_type, connection_config)
            test_result = await temp_client.test_connection()

            return create_response(
                data={
                    "valid": test_result.get('success', False),
                    "test_result": test_result,
                    "validated_at": datetime.now()
                },
                message="配置验证完成"
            )

        except Exception as client_error:
            return create_response(
                data={
                    "valid": False,
                    "error": str(client_error),
                    "validated_at": datetime.now()
                },
                message="配置验证失败"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"验证数据源配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources/{source_name}/health/history", summary="获取数据源健康检测历史")
async def get_data_source_health_history(
        source_name: str,
        limit: int = Query(50, ge=1, le=200, description="记录数量限制"),
        days: int = Query(7, ge=1, le=90, description="查询天数")
):
    """获取指定数据源的健康检测历史记录"""
    try:
        from app.utils.database import get_sync_db_session
        from sqlalchemy import text

        db = get_sync_db_session()

        try:
            # 检查表是否存在
            result = db.execute(text("SHOW TABLES LIKE 'data_source_health_checks'"))
            if not result.fetchone():
                return create_response(
                    data={
                        "source_name": source_name,
                        "history": [],
                        "total_count": 0,
                        "message": "健康检测历史表不存在"
                    },
                    message="健康检测历史查询完成"
                )

            # 查询健康检测历史
            history_sql = """
                SELECT hc.check_timestamp, hc.is_healthy, hc.health_score,
                       hc.connection_status, hc.connection_time_ms, 
                       hc.databases_count, hc.tables_count,
                       hc.connection_error, hc.general_error, hc.checks_performed
                FROM data_source_health_checks hc
                INNER JOIN data_sources ds ON hc.data_source_id = ds.id
                WHERE ds.name = :source_name 
                  AND hc.check_timestamp >= DATE_SUB(NOW(), INTERVAL :days DAY)
                ORDER BY hc.check_timestamp DESC
                LIMIT :limit
            """

            result = db.execute(text(history_sql), {
                "source_name": source_name,
                "days": days,
                "limit": limit
            })

            history_records = []
            for row in result.fetchall():
                history_records.append({
                    "check_timestamp": row[0],
                    "is_healthy": bool(row[1]),
                    "health_score": row[2],
                    "connection_status": row[3],
                    "connection_time_ms": row[4],
                    "databases_count": row[5],
                    "tables_count": row[6],
                    "connection_error": row[7],
                    "general_error": row[8],
                    "checks_performed": json.loads(row[9]) if row[9] else []
                })

            return create_response(
                data={
                    "source_name": source_name,
                    "history": history_records,
                    "total_count": len(history_records),
                    "query_days": days,
                    "limit": limit
                },
                message=f"获取 {source_name} 健康检测历史成功"
            )

        finally:
            db.close()

    except Exception as e:
        logger.error(f"获取健康检测历史失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))