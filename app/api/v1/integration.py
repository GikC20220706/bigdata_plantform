# """
# Data Integration API Endpoints
# 数据集成模块的API端点
# """
#
# from datetime import datetime
# from typing import Dict, List, Optional, Any
# from fastapi import APIRouter, HTTPException, Query, Body
# from pydantic import BaseModel, Field
#
# from app.utils.response import create_response
# from app.services.data_integration_service import data_integration_service
# from loguru import logger
# from fastapi import UploadFile, File, Form
#
#
# router = APIRouter()
#
#
# # === Pydantic 模型定义 ===
#
# class DataSourceConfig(BaseModel):
#     """数据源配置模型"""
#     host: str = Field(..., description="数据库主机地址")
#     port: int = Field(..., description="数据库端口")
#     username: str = Field(..., description="用户名")
#     password: Optional[str] = Field(None, description="密码")
#     database: Optional[str] = Field(None, description="数据库名称")
#     extra_params: Optional[Dict[str, Any]] = Field(None, description="额外参数")
#
#
# class CreateDataSourceRequest(BaseModel):
#     """创建数据源请求模型"""
#     name: str = Field(..., description="数据源名称")
#     type: str = Field(..., description="数据库类型")
#     config: DataSourceConfig = Field(..., description="连接配置")
#     description: Optional[str] = Field(None, description="描述信息")
#
#
# class QueryRequest(BaseModel):
#     """查询请求模型"""
#     query: str = Field(..., description="SQL查询语句")
#     database: Optional[str] = Field(None, description="数据库名称")
#     schema: Optional[str] = Field(None, description="模式名（schema）")
#     limit: int = Field(100, ge=1, le=1000, description="结果限制条数")
#
#
# # ===数据接入 API 端点 ===
#
# @router.get("/", summary="获取数据集成概览")
# async def get_integration_overview():
#     """获取数据集成模块概览信息"""
#     try:
#         overview = await data_integration_service.get_data_sources_overview()
#         return create_response(
#             data=overview,
#             message="获取数据集成概览成功"
#         )
#     except Exception as e:
#         logger.error(f"获取数据集成概览失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取数据集成概览失败: {str(e)}"
#         )
#
#
# @router.get("/sources", summary="获取数据源列表")
# async def get_data_sources():
#     """获取所有数据源列表"""
#     try:
#         sources = await data_integration_service.get_data_sources_list()
#         return create_response(
#             data={
#                 "sources": sources,
#                 "total": len(sources),
#                 "connected": len([s for s in sources if s.get('status') == 'connected']),
#                 "disconnected": len([s for s in sources if s.get('status') == 'disconnected'])
#             },
#             message="获取数据源列表成功"
#         )
#     except Exception as e:
#         logger.error(f"获取数据源列表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取数据源列表失败: {str(e)}"
#         )
#
#
# @router.post("/sources", summary="创建数据源")
# async def create_data_source(request: CreateDataSourceRequest):
#     """创建新的数据源连接"""
#     try:
#         config_dict = request.config.dict()
#         if request.config.extra_params:
#             config_dict.update(request.config.extra_params)
#
#         result = await data_integration_service.add_data_source(
#             name=request.name,
#             db_type=request.type,
#             config=config_dict
#         )
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"数据源 {request.name} 创建成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '创建数据源失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"创建数据源失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"创建数据源失败: {str(e)}"
#         )
#
#
# @router.delete("/sources/{source_name}", summary="删除数据源")
# async def delete_data_source(source_name: str):
#     """删除指定的数据源"""
#     try:
#         result = await data_integration_service.remove_data_source(source_name)
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"数据源 {source_name} 删除成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '数据源不存在')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"删除数据源失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"删除数据源失败: {str(e)}"
#         )
#
#
# @router.post("/sources/{source_name}/test", summary="测试数据源连接")
# async def test_data_source(source_name: str):
#     """测试指定数据源的连接状态"""
#     try:
#         result = await data_integration_service.test_data_source(source_name)
#
#         return create_response(
#             data=result,
#             message="连接测试完成"
#         )
#     except Exception as e:
#         logger.error(f"测试数据源连接失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"测试数据源连接失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/databases", summary="获取数据库列表")
# async def get_databases(source_name: str):
#     """获取指定数据源的数据库列表"""
#     try:
#         result = await data_integration_service.get_databases(source_name)
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"获取 {source_name} 数据库列表成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '获取数据库列表失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取数据库列表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取数据库列表失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/tables", summary="获取表列表")
# async def get_tables(
#     source_name: str,
#     database: Optional[str] = Query(None, description="数据库名称")
# ):
#     """获取指定数据源的表列表"""
#     try:
#         result = await data_integration_service.get_tables(source_name, database)
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"获取 {source_name} 表列表成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '获取表列表失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取表列表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取表列表失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/tables/{table_name}/schema", summary="获取表结构")
# async def get_table_schema(
#     source_name: str,
#     table_name: str,
#     database: Optional[str] = Query(None, description="数据库名称")
# ):
#     """获取指定表的结构信息"""
#     try:
#         result = await data_integration_service.get_table_schema(
#             source_name, table_name, database
#         )
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"获取表 {table_name} 结构成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '获取表结构失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取表结构失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取表结构失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/tables/{table_name}/metadata", summary="获取表元数据")
# async def get_table_metadata(
#     source_name: str,
#     table_name: str,
#     database: Optional[str] = Query(None, description="数据库名称")
# ):
#     """获取指定表的完整元数据信息"""
#     try:
#         result = await data_integration_service.get_table_metadata(
#             source_name, table_name, database
#         )
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"获取表 {table_name} 元数据成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '获取表元数据失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取表元数据失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取表元数据失败: {str(e)}"
#         )
#
#
# @router.post("/sources/{source_name}/query", summary="执行查询")
# async def execute_query(source_name: str, request: QueryRequest):
#     """在指定数据源上执行查询"""
#     try:
#         result = await data_integration_service.execute_query(
#             source_name=source_name,
#             query=request.query,
#             database=request.database,
#             schema=request.schema,
#             limit=request.limit
#         )
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message="查询执行成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '查询执行失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"执行查询失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"执行查询失败: {str(e)}"
#         )
#
#
# @router.get("/tables/search", summary="搜索表")
# async def search_tables(
#     keyword: Optional[str] = Query(None, description="搜索关键词"),
#     source_name: Optional[str] = Query(None, description="数据源名称"),
#     table_type: Optional[str] = Query(None, description="表类型")
# ):
#     """跨数据源搜索表"""
#     try:
#         result = await data_integration_service.search_tables(
#             keyword=keyword,
#             source_name=source_name,
#             table_type=table_type
#         )
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message="表搜索成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '表搜索失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"搜索表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"搜索表失败: {str(e)}"
#         )
#
#
# @router.get("/types", summary="获取支持的数据库类型")
# async def get_supported_types():
#     """获取支持的数据库类型列表"""
#     try:
#         types = await data_integration_service.get_supported_database_types()
#         return create_response(
#             data={
#                 "supported_types": types,
#                 "total_count": len(types)
#             },
#             message="获取支持的数据库类型成功"
#         )
#     except Exception as e:
#         logger.error(f"获取支持的数据库类型失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取支持的数据库类型失败: {str(e)}"
#         )
#
#
# @router.get("/health", summary="获取数据集成模块健康状态")
# async def get_integration_health():
#     """获取数据集成模块的健康状态"""
#     try:
#         overview = await data_integration_service.get_data_sources_overview()
#
#         total_sources = overview.get('total_sources', 0)
#         active_connections = overview.get('active_connections', 0)
#         failed_connections = overview.get('failed_connections', 0)
#
#         # 计算健康分数
#         if total_sources == 0:
#             health_score = 100
#             status = "healthy"
#         else:
#             health_score = (active_connections / total_sources) * 100
#             if health_score >= 90:
#                 status = "healthy"
#             elif health_score >= 70:
#                 status = "warning"
#             else:
#                 status = "critical"
#
#         health_data = {
#             "status": status,
#             "health_score": round(health_score, 1),
#             "total_sources": total_sources,
#             "active_connections": active_connections,
#             "failed_connections": failed_connections,
#             "uptime_percentage": health_score,
#             "last_check": datetime.now(),
#             "issues": []
#         }
#
#         # 添加问题描述
#         if failed_connections > 0:
#             health_data["issues"].append(f"{failed_connections} 个数据源连接失败")
#
#         if total_sources == 0:
#             health_data["issues"].append("未配置任何数据源")
#
#         return create_response(
#             data=health_data,
#             message="获取数据集成模块健康状态成功"
#         )
#     except Exception as e:
#         logger.error(f"获取健康状态失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取健康状态失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/preview", summary="预览数据源数据")
# async def preview_data_source(
#         source_name: str,
#         table_name: Optional[str] = Query(None, description="表名称"),
#         database: Optional[str] = Query(None, description="数据库名称"),
#         limit: int = Query(10, ge=1, le=100, description="预览行数")
# ):
#     """预览数据源中的数据"""
#     try:
#         if not table_name:
#             # 如果没有指定表名，返回数据源的基本信息
#             result = await data_integration_service.get_tables(source_name, database)
#             if result.get('success') and result.get('tables'):
#                 # 选择第一个表进行预览
#                 table_name = result['tables'][0]['table_name']
#             else:
#                 raise HTTPException(
#                     status_code=400,
#                     detail="请指定要预览的表名称"
#                 )
#
#         # 构建预览查询
#         query = f"SELECT * FROM {table_name}"
#         if database:
#             query = f"SELECT * FROM {database}.{table_name}"
#
#         result = await data_integration_service.execute_query(
#             source_name=source_name,
#             query=query,
#             database=database,
#             limit=limit
#         )
#
#         if result.get('success'):
#             # 获取表结构信息
#             schema_result = await data_integration_service.get_table_schema(
#                 source_name, table_name, database
#             )
#
#             preview_data = {
#                 "source_name": source_name,
#                 "database": database,
#                 "table_name": table_name,
#                 "preview_data": result.get('results', []),
#                 "row_count": result.get('row_count', 0),
#                 "schema": schema_result.get('schema', {}) if schema_result.get('success') else {},
#                 "execution_time_ms": result.get('execution_time_ms', 0),
#                 "previewed_at": datetime.now()
#             }
#
#             return create_response(
#                 data=preview_data,
#                 message=f"数据预览成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '数据预览失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"数据预览失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"数据预览失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/statistics", summary="获取数据源统计信息")
# async def get_data_source_statistics(source_name: str):
#     """获取数据源的统计信息"""
#     try:
#         # 获取数据库列表
#         databases_result = await data_integration_service.get_databases(source_name)
#         if not databases_result.get('success'):
#             raise HTTPException(
#                 status_code=404,
#                 detail=databases_result.get('error', '数据源不存在')
#             )
#
#         databases = databases_result.get('databases', [])
#         total_tables = 0
#         tables_by_database = {}
#
#         # 统计每个数据库的表数量
#         for db in databases:
#             try:
#                 tables_result = await data_integration_service.get_tables(source_name, db)
#                 if tables_result.get('success'):
#                     db_table_count = tables_result.get('count', 0)
#                     total_tables += db_table_count
#                     tables_by_database[db] = db_table_count
#                 else:
#                     tables_by_database[db] = 0
#             except:
#                 tables_by_database[db] = 0
#
#         # 测试连接状态
#         test_result = await data_integration_service.test_data_source(source_name)
#
#         statistics = {
#             "source_name": source_name,
#             "connection_status": "connected" if test_result.get('success') else "disconnected",
#             "database_type": test_result.get('database_type', '未知'),
#             "version": test_result.get('version', '未知'),
#             "total_databases": len(databases),
#             "total_tables": total_tables,
#             "tables_by_database": tables_by_database,
#             "last_test": test_result.get('test_time', datetime.now()),
#             "response_time_ms": test_result.get('response_time_ms', 0),
#             "collected_at": datetime.now()
#         }
#
#         if not test_result.get('success'):
#             statistics["error"] = test_result.get('error', '连接失败')
#
#         return create_response(
#             data=statistics,
#             message=f"获取数据源 {source_name} 统计信息成功"
#         )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取数据源统计信息失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取数据源统计信息失败: {str(e)}"
#         )
#
#
# @router.post("/batch/test", summary="批量测试数据源连接")
# async def batch_test_connections(
#         source_names: List[str] = Body(..., description="数据源名称列表")
# ):
#     """批量测试多个数据源的连接状态"""
#     try:
#         results = {}
#
#         for source_name in source_names:
#             try:
#                 test_result = await data_integration_service.test_data_source(source_name)
#                 results[source_name] = test_result
#             except Exception as e:
#                 results[source_name] = {
#                     "success": False,
#                     "error": str(e),
#                     "test_time": datetime.now()
#                 }
#
#         # 统计结果
#         total_tested = len(results)
#         successful = sum(1 for result in results.values() if result.get('success'))
#         failed = total_tested - successful
#
#         summary = {
#             "total_tested": total_tested,
#             "successful": successful,
#             "failed": failed,
#             "success_rate": round((successful / total_tested) * 100, 1) if total_tested > 0 else 0,
#             "test_results": results,
#             "tested_at": datetime.now()
#         }
#
#         return create_response(
#             data=summary,
#             message=f"批量连接测试完成，成功 {successful}/{total_tested}"
#         )
#     except Exception as e:
#         logger.error(f"批量测试连接失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"批量测试连接失败: {str(e)}"
#         )
#
#
# @router.post("/sources/excel/upload", summary="上传Excel文件创建数据源")
# async def upload_excel_source(
#         name: str = Form(..., description="数据源名称"),
#         file: UploadFile = File(..., description="Excel文件"),
#         description: Optional[str] = Form(None, description="描述信息")
# ):
#     """上传Excel文件并创建数据源"""
#     try:
#         from app.utils.data_integration_clients import excel_service
#
#         # 验证文件类型
#         if not file.filename.lower().endswith(('.xlsx', '.xls')):
#             raise HTTPException(
#                 status_code=400,
#                 detail="不支持的文件类型，请上传.xlsx或.xls文件"
#             )
#
#         # 验证文件大小 (限制为50MB)
#         file_size = 0
#         content = await file.read()
#         file_size = len(content)
#         await file.seek(0)  # 重置文件指针
#
#         if file_size > 50 * 1024 * 1024:  # 50MB
#             raise HTTPException(
#                 status_code=400,
#                 detail="文件大小超过限制（50MB）"
#             )
#
#         # 创建Excel数据源
#         result = await excel_service.create_excel_source(name, file, description)
#
#         if result.get('success'):
#             # 添加到连接管理器
#             config = result['config']
#             add_result = await data_integration_service.add_data_source(
#                 name=name,
#                 db_type='excel',
#                 config=config
#             )
#
#             if add_result.get('success'):
#                 return create_response(
#                     data={
#                         "source_info": add_result,
#                         "upload_info": result['upload_info']
#                     },
#                     message=f"Excel数据源 {name} 创建成功"
#                 )
#             else:
#                 # 如果添加数据源失败，删除上传的文件
#                 await excel_service.delete_excel_source(config)
#                 raise HTTPException(
#                     status_code=500,
#                     detail=add_result.get('error', '创建数据源失败')
#                 )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '上传Excel文件失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"上传Excel文件失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"上传Excel文件失败: {str(e)}"
#         )
#
#
# @router.get("/sources/excel/files", summary="获取已上传的Excel文件列表")
# async def list_excel_files():
#     """获取已上传的Excel文件列表"""
#     try:
#         from app.utils.data_integration_clients import excel_service
#
#         files = await excel_service.list_uploaded_files()
#         return create_response(
#             data={
#                 "files": files,
#                 "total_count": len(files)
#             },
#             message="获取Excel文件列表成功"
#         )
#     except Exception as e:
#         logger.error(f"获取Excel文件列表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取Excel文件列表失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/sheets", summary="获取Excel工作表列表")
# async def get_excel_sheets(source_name: str):
#     """获取Excel文件的工作表列表"""
#     try:
#         result = await data_integration_service.get_tables(source_name)
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"获取 {source_name} 工作表列表成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=404,
#                 detail=result.get('error', '获取工作表列表失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"获取Excel工作表列表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"获取Excel工作表列表失败: {str(e)}"
#         )
#
#
# @router.get("/sources/{source_name}/sheets/{sheet_name}/preview", summary="预览Excel工作表数据")
# async def preview_excel_sheet(
#         source_name: str,
#         sheet_name: str,
#         limit: int = Query(10, ge=1, le=100, description="预览行数")
# ):
#     """预览Excel工作表数据"""
#     try:
#         client = data_integration_service.connection_manager.get_client(source_name)
#         if not client:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"数据源 {source_name} 不存在"
#             )
#
#         # 检查是否为Excel客户端
#         if not hasattr(client, 'get_data_preview'):
#             raise HTTPException(
#                 status_code=400,
#                 detail="该数据源不支持预览功能"
#             )
#
#         result = await client.get_data_preview(sheet_name, limit)
#
#         if result.get('success'):
#             return create_response(
#                 data=result,
#                 message=f"预览工作表 {sheet_name} 成功"
#             )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '预览失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"预览Excel工作表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"预览Excel工作表失败: {str(e)}"
#         )
#
#
# @router.delete("/sources/{source_name}/excel", summary="删除Excel数据源和文件")
# async def delete_excel_source(source_name: str):
#     """删除Excel数据源和对应的文件"""
#     try:
#         client = data_integration_service.connection_manager.get_client(source_name)
#         if not client:
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"数据源 {source_name} 不存在"
#             )
#
#         # 获取文件配置
#         config = client.config if hasattr(client, 'config') else {}
#
#         # 删除文件
#         from app.utils.data_integration_clients import excel_service
#         delete_result = await excel_service.delete_excel_source(config)
#
#         # 删除数据源
#         source_result = await data_integration_service.remove_data_source(source_name)
#
#         return create_response(
#             data={
#                 "file_deleted": delete_result.get('success', False),
#                 "source_deleted": source_result.get('success', False),
#                 "file_message": delete_result.get('message', ''),
#                 "source_message": source_result.get('message', '')
#             },
#             message=f"Excel数据源 {source_name} 删除成功"
#         )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"删除Excel数据源失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"删除Excel数据源失败: {str(e)}"
#         )
#
#
# @router.post("/sources/{source_name}/sheets/{sheet_name}/export", summary="导出Excel工作表数据")
# async def export_excel_sheet(
#         source_name: str,
#         sheet_name: str,
#         export_format: str = Query("json", description="导出格式: json, csv"),
#         limit: Optional[int] = Query(None, description="导出行数限制")
# ):
#     """导出Excel工作表数据"""
#     try:
#         # 构建查询
#         query = f"SELECT * FROM {sheet_name}"
#         if limit:
#             query += f" LIMIT {limit}"
#
#         result = await data_integration_service.execute_query(
#             source_name=source_name,
#             query=query,
#             limit=limit or 1000
#         )
#
#         if result.get('success'):
#             data = result['results']
#
#             if export_format.lower() == 'csv':
#                 import io
#                 import csv
#                 from fastapi.responses import StreamingResponse
#
#                 # 创建CSV内容
#                 output = io.StringIO()
#                 if data:
#                     writer = csv.DictWriter(output, fieldnames=data[0].keys())
#                     writer.writeheader()
#                     writer.writerows(data)
#
#                 csv_content = output.getvalue()
#                 output.close()
#
#                 return StreamingResponse(
#                     io.BytesIO(csv_content.encode('utf-8')),
#                     media_type="text/csv",
#                     headers={
#                         "Content-Disposition": f"attachment; filename={sheet_name}.csv"
#                     }
#                 )
#             else:
#                 # 返回JSON格式
#                 return create_response(
#                     data={
#                         "sheet_name": sheet_name,
#                         "export_format": export_format,
#                         "data": data,
#                         "row_count": len(data),
#                         "exported_at": datetime.now()
#                     },
#                     message=f"导出工作表 {sheet_name} 成功"
#                 )
#         else:
#             raise HTTPException(
#                 status_code=400,
#                 detail=result.get('error', '导出失败')
#             )
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"导出Excel工作表失败: {e}")
#         raise HTTPException(
#             status_code=500,
#             detail=f"导出Excel工作表失败: {str(e)}"
#         )
#
#
# # 在 get_supported_database_types 方法中添加Excel支持：
# async def get_supported_database_types(self) -> List[Dict[str, Any]]:
#     """获取支持的数据库类型（包括Excel）"""
#     types = [
#         # ... 其他数据库类型 ...
#
#         {
#             "type": "excel",
#             "name": "Microsoft Excel",
#             "description": "Excel电子表格文件数据源",
#             "default_port": None,
#             "features": ["多工作表支持", "数据预览", "文件上传", "格式检测", "数据导出"],
#             "connection_example": {
#                 "file_path": "/path/to/excel/file.xlsx",
#                 "upload_method": "文件上传"
#             },
#             "driver": "pandas + openpyxl",
#             "vendor": "Microsoft Corporation",
#             "license": "商业许可",
#             "use_cases": ["数据导入", "报表分析", "临时数据存储", "数据交换"],
#             "pros": ["易于使用", "广泛支持", "可视化编辑", "公式计算"],
#             "cons": ["性能有限", "并发支持差", "数据量限制"],
#             "supported_versions": [".xlsx", ".xls"],
#             "file_size_limit": "50MB",
#             "upload_formats": ["xlsx", "xls"]
#         }
#     ]
#     return types
