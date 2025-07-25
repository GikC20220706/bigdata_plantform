from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks, UploadFile, File, Form
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.utils.response import create_response
from app.services.optimized_data_integration_service import optimized_data_integration_service
from app.utils.integration_cache import integration_cache
from loguru import logger

router = APIRouter()

# 1. 数据集成概览
@router.get("/", summary="获取数据集成概览")
async def get_integration_overview():
    try:
        overview = await optimized_data_integration_service.get_data_sources_overview()
        return create_response(data=overview, message="获取数据集成概览成功")
    except Exception as e:
        logger.error(f"获取数据集成概览失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 2. 数据源管理
@router.get("/sources", summary="获取数据源列表")
async def get_data_sources():
    try:
        sources = await optimized_data_integration_service.get_data_sources_list()
        return create_response(
            data={
                "sources": sources,
                "total": len(sources),
                "connected": len([s for s in sources if s.get('status') == 'connected']),
                "disconnected": len([s for s in sources if s.get('status') == 'disconnected'])
            },
            message="获取数据源列表成功"
        )
    except Exception as e:
        logger.error(f"获取数据源列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sources", summary="创建数据源")
async def create_data_source(request: dict):
    try:
        name = request.get('name')
        db_type = request.get('type')
        config = request.get('config')
        if not all([name, db_type, config]):
            raise HTTPException(status_code=400, detail="缺少必要字段: name, type, config")
        result = await optimized_data_integration_service.add_data_source(name=name, db_type=db_type, config=config)
        if result.get('success'):
            return create_response(data=result, message=f"数据源 {name} 创建成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '创建数据源失败'))
    except Exception as e:
        logger.error(f"创建数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/sources/{source_name}", summary="删除数据源")
async def delete_data_source(source_name: str):
    try:
        result = await optimized_data_integration_service.remove_data_source(source_name)
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
        result = await optimized_data_integration_service.test_data_source(source_name)
        return create_response(data=result, message="连接测试完成")
    except Exception as e:
        logger.error(f"测试数据源连接失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/batch/test", summary="批量测试数据源连接")
async def batch_test_connections(source_names: List[str] = Body(..., description="数据源名称列表")):
    try:
        if len(source_names) > 50:
            raise HTTPException(status_code=400, detail="批量测试数量不能超过50个")
        summary = await optimized_data_integration_service.batch_test_connections(source_names)
        return create_response(data=summary, message=f"批量连接测试完成，成功 {summary['successful']}/{summary['total_tested']}")
    except Exception as e:
        logger.error(f"批量测试连接失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 3. 数据库/表操作
@router.get("/sources/{source_name}/databases", summary="获取数据库列表")
async def get_databases(source_name: str):
    try:
        result = await optimized_data_integration_service.get_databases(source_name)
        if result.get('success'):
            return create_response(data=result, message=f"获取 {source_name} 数据库列表成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取数据库列表失败'))
    except Exception as e:
        logger.error(f"获取数据库列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/tables", summary="获取表列表")
async def get_tables(source_name: str, database: Optional[str] = Query(None, description="数据库名称")):
    try:
        result = await optimized_data_integration_service.get_tables(source_name, database)
        if result.get('success'):
            return create_response(data=result, message=f"获取 {source_name} 表列表成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取表列表失败'))
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/tables/{table_name}/schema", summary="获取表结构")
async def get_table_schema(source_name: str, table_name: str, database: Optional[str] = Query(None, description="数据库名称")):
    try:
        result = await optimized_data_integration_service.get_table_schema(source_name, table_name, database)
        if result.get('success'):
            return create_response(data=result, message=f"获取表 {table_name} 结构成功")
        else:
            raise HTTPException(status_code=404, detail=result.get('error', '获取表结构失败'))
    except Exception as e:
        logger.error(f"获取表结构失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/tables/{table_name}/metadata", summary="获取表元数据")
async def get_table_metadata(source_name: str, table_name: str, database: Optional[str] = Query(None, description="数据库名称")):
    try:
        result = await optimized_data_integration_service.get_table_metadata(source_name, table_name, database)
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
        result = await optimized_data_integration_service.search_tables(keyword=keyword, source_name=source_name, table_type=table_type)
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
        result = await optimized_data_integration_service.execute_query(source_name=source_name, query=query, database=database, schema=schema, limit=limit)
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
        result = await optimized_data_integration_service.preview_data_source(source_name=source_name, table_name=table_name, database=database, limit=limit)
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
        health_data = await optimized_data_integration_service.get_health_status()
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
        result = await optimized_data_integration_service.upload_excel_source(name, file, description)
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
        files = await optimized_data_integration_service.list_excel_files()
        return create_response(data={"files": files, "total_count": len(files)}, message="获取Excel文件列表成功")
    except Exception as e:
        logger.error(f"获取Excel文件列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sources/{source_name}/sheets", summary="获取Excel工作表列表")
async def get_excel_sheets(source_name: str):
    try:
        result = await optimized_data_integration_service.get_excel_sheets(source_name)
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
        result = await optimized_data_integration_service.preview_excel_sheet(source_name, sheet_name, limit)
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
        result = await optimized_data_integration_service.delete_excel_source(source_name)
        if result.get('success'):
            return create_response(data=result, message="Excel数据源及文件删除成功")
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '删除Excel数据源失败'))
    except Exception as e:
        logger.error(f"删除Excel数据源失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))