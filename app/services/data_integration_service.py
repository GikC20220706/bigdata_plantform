# """
# Data Integration Service
# 数据集成服务层，提供统一的数据源管理和操作接口
# """
#
# import asyncio
# from datetime import datetime, timedelta
# from typing import Dict, List, Optional, Any
# from loguru import logger
#
# from app.utils.data_integration_clients import (
#     DatabaseClientFactory,
#     connection_manager,
#     DatabaseClient
# )
# from app.schemas.base import BaseResponse
# from config.settings import settings
#
#
# class DataIntegrationService:
#     """数据集成服务"""
#
#     def __init__(self):
#         self.connection_manager = connection_manager
#         self._initialize_default_connections()
#
#     def _initialize_default_connections(self):
#         """初始化默认连接配置"""
#         if not settings.use_real_clusters:
#             logger.info("Mock模式：跳过真实数据库连接初始化")
#             return
#
#         # 默认数据源配置
#         default_configs = {
#             'mysql_demo': {
#                 'type': 'mysql',
#                 'config': {
#                     'host': 'localhost',
#                     'port': 3306,
#                     'username': 'root',
#                     'password': 'password',
#                     'database': 'demo'
#                 }
#             },
#             'hive_warehouse': {
#                 'type': 'hive',
#                 'config': {
#                     'host': settings.HIVE_SERVER_HOST,
#                     'port': settings.HIVE_SERVER_PORT,
#                     'username': settings.HIVE_USERNAME,
#                     'password': settings.HIVE_PASSWORD,
#                     'database': 'default'
#                 }
#             },
#             'doris_cluster': {
#                 'type': 'doris',
#                 'config': {
#                     'host': settings.DORIS_FE_HOST,
#                     'port': settings.DORIS_FE_PORT,
#                     'username': settings.DORIS_USERNAME,
#                     'password': settings.DORIS_PASSWORD,
#                     'database': 'demo'
#                 }
#             }
#         }
#
#         # 初始化连接
#         for name, conn_info in default_configs.items():
#             try:
#                 self.connection_manager.add_client(
#                     name,
#                     conn_info['type'],
#                     conn_info['config']
#                 )
#                 logger.info(f"初始化数据源连接: {name}")
#             except Exception as e:
#                 logger.warning(f"初始化数据源 {name} 失败: {e}")
#
#     async def get_data_sources_overview(self) -> Dict[str, Any]:
#         """获取数据源概览"""
#         try:
#             clients = self.connection_manager.list_clients()
#
#             if not clients and not settings.use_real_clusters:
#                 # Mock数据
#                 return {
#                     "total_sources": 6,
#                     "active_connections": 5,
#                     "failed_connections": 1,
#                     "supported_types": DatabaseClientFactory.get_supported_types(),
#                     "sources_by_type": {
#                         "mysql": 2,
#                         "hive": 1,
#                         "doris": 1,
#                         "kingbase": 1,
#                         "tidb": 1
#                     },
#                     "data_volume_estimate": "125.6GB",
#                     "last_sync": datetime.now(),
#                     "health_status": "良好"
#                 if failed_count == 0 else "部分异常" if active_count > 0 else "异常"
#                 }
#         except Exception as e:
#             logger.error(f"获取数据源概览失败: {e}")
#             return {
#                 "total_sources": 0,
#                 "active_connections": 0,
#                 "failed_connections": 0,
#                 "supported_types": DatabaseClientFactory.get_supported_types(),
#                 "sources_by_type": {},
#                 "error": str(e)
#             }
#
#     async def add_data_source(self, name: str, db_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
#         """添加数据源"""
#         try:
#             if db_type == "excel":
#                 required_fields = ['file_path']  # 或 file，视你的实现
#             else:
#                 required_fields = ['host', 'username']
#             missing_fields = [field for field in required_fields if field not in config]
#             if missing_fields:
#                 return {
#                     "success": False,
#                     "error": f"缺少必要配置项: {', '.join(missing_fields)}"
#                 }
#
#             # 创建客户端
#             self.connection_manager.add_client(name, db_type, config)
#
#             # 测试连接
#             client = self.connection_manager.get_client(name)
#             test_result = await client.test_connection()
#
#             return {
#                 "success": True,
#                 "name": name,
#                 "type": db_type,
#                 "test_result": test_result,
#                 "created_at": datetime.now()
#             }
#         except Exception as e:
#             logger.error(f"添加数据源失败 {name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def remove_data_source(self, name: str) -> Dict[str, Any]:
#         """移除数据源"""
#         try:
#             if name not in self.connection_manager.list_clients():
#                 return {
#                     "success": False,
#                     "error": f"数据源 {name} 不存在"
#                 }
#
#             self.connection_manager.remove_client(name)
#             return {
#                 "success": True,
#                 "message": f"数据源 {name} 已移除"
#             }
#         except Exception as e:
#             logger.error(f"移除数据源失败 {name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def test_data_source(self, name: str) -> Dict[str, Any]:
#         """测试数据源连接"""
#         try:
#             client = self.connection_manager.get_client(name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {name} 不存在"
#                 }
#
#             result = await client.test_connection()
#             return result
#         except Exception as e:
#             logger.error(f"测试数据源连接失败 {name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e),
#                 "test_time": datetime.now()
#             }
#
#     async def get_databases(self, source_name: str) -> Dict[str, Any]:
#         """获取数据源的数据库列表"""
#         try:
#             client = self.connection_manager.get_client(source_name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {source_name} 不存在"
#                 }
#
#             databases = await client.get_databases()
#             return {
#                 "success": True,
#                 "source_name": source_name,
#                 "databases": databases,
#                 "count": len(databases),
#                 "retrieved_at": datetime.now()
#             }
#         except Exception as e:
#             logger.error(f"获取数据库列表失败 {source_name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def get_tables(self, source_name: str, database: str = None) -> Dict[str, Any]:
#         """获取数据源的表列表"""
#         try:
#             client = self.connection_manager.get_client(source_name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {source_name} 不存在"
#                 }
#
#             tables = await client.get_tables(database)
#             return {
#                 "success": True,
#                 "source_name": source_name,
#                 "database": database,
#                 "tables": tables,
#                 "count": len(tables),
#                 "retrieved_at": datetime.now()
#             }
#         except Exception as e:
#             logger.error(f"获取表列表失败 {source_name}.{database}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def get_table_schema(self, source_name: str, table_name: str, database: str = None) -> Dict[str, Any]:
#         """获取表结构"""
#         try:
#             client = self.connection_manager.get_client(source_name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {source_name} 不存在"
#                 }
#
#             schema = await client.get_table_schema(table_name, database)
#             return {
#                 "success": True,
#                 "source_name": source_name,
#                 "database": database,
#                 "table_name": table_name,
#                 "schema": schema,
#                 "retrieved_at": datetime.now()
#             }
#         except Exception as e:
#             logger.error(f"获取表结构失败 {source_name}.{database}.{table_name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def get_table_metadata(self, source_name: str, table_name: str, database: str = None) -> Dict[str, Any]:
#         """获取表的完整元数据"""
#         try:
#             client = self.connection_manager.get_client(source_name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {source_name} 不存在"
#                 }
#
#             metadata = await client.get_table_metadata(table_name, database)
#             return {
#                 "success": True,
#                 "source_name": source_name,
#                 "metadata": metadata
#             }
#         except Exception as e:
#             logger.error(f"获取表元数据失败 {source_name}.{database}.{table_name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def execute_query(self, source_name: str, query: str, database: str = None, schema: str = "public", limit: int = 100) -> Dict[
#         str, Any]:
#         """执行查询 支持schema (模式名)"""
#         try:
#             client = self.connection_manager.get_client(source_name)
#             if not client:
#                 return {
#                     "success": False,
#                     "error": f"数据源 {source_name} 不存在"
#                 }
#
#             # 添加LIMIT限制以防止大量数据返回
#             if limit and "limit" not in query.lower():
#                 if query.strip().endswith(';'):
#                     query = query.strip()[:-1]
#                 query = f"{query} LIMIT {limit}"
#
#             start_time = datetime.now()
#
#             results = await client.execute_query(query, database,schema)
#             end_time = datetime.now()
#
#             return {
#                 "success": True,
#                 "source_name": source_name,
#                 "database": database,
#                 "schema": schema,
#                 "query": query,
#                 "results": results,
#                 "row_count": len(results),
#                 "execution_time_ms": int((end_time - start_time).total_seconds() * 1000),
#                 "executed_at": start_time
#             }
#         except Exception as e:
#             logger.error(f"查询执行失败 {source_name}: {e}")
#             return {
#                 "success": False,
#                 "error": str(e),
#                 "query": query
#             }
#
#     async def get_data_sources_list(self) -> List[Dict[str, Any]]:
#         """获取所有数据源列表"""
#         try:
#             clients = self.connection_manager.list_clients()
#
#             if not clients and not settings.use_real_clusters:
#                 # 返回Mock数据
#                 return [
#                     {
#                         "name": "MySQL-Demo",
#                         "type": "mysql",
#                         "status": "connected",
#                         "host": "localhost",
#                         "port": 3306,
#                         "database": "demo",
#                         "description": "MySQL演示数据库",
#                         "tables_count": 15,
#                         "last_test": datetime.now() - timedelta(minutes=5),
#                         "created_at": datetime.now() - timedelta(days=7)
#                     },
#                     {
#                         "name": "Hive-Warehouse",
#                         "type": "hive",
#                         "status": "connected",
#                         "host": "hadoop101",
#                         "port": 10000,
#                         "database": "default",
#                         "description": "Hive数据仓库",
#                         "tables_count": 128,
#                         "last_test": datetime.now() - timedelta(minutes=3),
#                         "created_at": datetime.now() - timedelta(days=30)
#                     },
#                     {
#                         "name": "Doris-OLAP",
#                         "type": "doris",
#                         "status": "connected",
#                         "host": "hadoop101",
#                         "port": 9030,
#                         "database": "demo",
#                         "description": "Doris OLAP引擎",
#                         "tables_count": 45,
#                         "last_test": datetime.now() - timedelta(minutes=2),
#                         "created_at": datetime.now() - timedelta(days=10)
#                     },
#                     {
#                         "name": "Kingbase-ES",
#                         "type": "kingbase",
#                         "status": "disconnected",
#                         "host": "192.168.1.100",
#                         "port": 54321,
#                         "database": "test",
#                         "description": "人大金仓数据库",
#                         "tables_count": 0,
#                         "last_test": datetime.now() - timedelta(hours=2),
#                         "created_at": datetime.now() - timedelta(days=5),
#                         "error": "连接超时"
#                     }
#                 ]
#
#             # 获取真实数据源信息
#             sources_list = []
#             connection_results = await self.connection_manager.test_all_connections()
#
#             for name in clients:
#                 client = self.connection_manager.get_client(name)
#                 test_result = connection_results.get(name, {})
#
#                 source_info = {
#                     "name": name,
#                     "type": client.__class__.__name__.replace('Client', '').lower(),
#                     "status": "connected" if test_result.get('success') else "disconnected",
#                     "host": client.config.get('host', '未知'),
#                     "port": client.config.get('port', 0),
#                     "database": client.config.get('database', ''),
#                     "description": f"{test_result.get('database_type', '未知')} 数据库",
#                     "last_test": test_result.get('test_time', datetime.now()),
#                     "version": test_result.get('version', '未知')
#                 }
#
#                 if not test_result.get('success'):
#                     source_info["error"] = test_result.get('error', '连接失败')
#
#                 # 获取表数量（仅对连接成功的）
#                 if test_result.get('success'):
#                     try:
#                         tables_result = await self.get_tables(name)
#                         source_info["tables_count"] = tables_result.get('count', 0)
#                     except:
#                         source_info["tables_count"] = 0
#                 else:
#                     source_info["tables_count"] = 0
#
#                 sources_list.append(source_info)
#
#             return sources_list
#         except Exception as e:
#             logger.error(f"获取数据源列表失败: {e}")
#             return []
#
#     async def search_tables(self, keyword: str = None, source_name: str = None, table_type: str = None) -> Dict[
#         str, Any]:
#         """搜索表"""
#         try:
#             all_tables = []
#             clients_to_search = [source_name] if source_name else self.connection_manager.list_clients()
#
#             for client_name in clients_to_search:
#                 try:
#                     tables_result = await self.get_tables(client_name)
#                     if tables_result.get('success'):
#                         for table in tables_result['tables']:
#                             table['source_name'] = client_name
#
#                             # 应用过滤条件
#                             if keyword and keyword.lower() not in table['table_name'].lower():
#                                 continue
#                             if table_type and table.get('table_type', '').lower() != table_type.lower():
#                                 continue
#
#                             all_tables.append(table)
#                 except Exception as e:
#                     logger.warning(f"搜索表时跳过数据源 {client_name}: {e}")
#                     continue
#
#             return {
#                 "success": True,
#                 "tables": all_tables,
#                 "total_count": len(all_tables),
#                 "search_criteria": {
#                     "keyword": keyword,
#                     "source_name": source_name,
#                     "table_type": table_type
#                 },
#                 "searched_at": datetime.now()
#             }
#         except Exception as e:
#             logger.error(f"搜索表失败: {e}")
#             return {
#                 "success": False,
#                 "error": str(e)
#             }
#
#     async def get_supported_database_types(self) -> List[Dict[str, Any]]:
#         """获取支持的数据库类型"""
#         return [
#             {
#                 "type": "mysql",
#                 "name": "MySQL",
#                 "description": "世界最流行的开源关系数据库管理系统",
#                 "default_port": 3306,
#                 "features": ["ACID事务", "主从复制", "分区表", "触发器", "存储过程", "全文索引"],
#                 "connection_example": {
#                     "host": "localhost",
#                     "port": 3306,
#                     "username": "root",
#                     "password": "password",
#                     "database": "demo",
#                     "charset": "utf8mb4"
#                 },
#                 "driver": "aiomysql",
#                 "vendor": "Oracle Corporation",
#                 "license": "GPL/商业双重许可",
#                 "use_cases": ["Web应用", "电商系统", "内容管理", "小到中型应用"],
#                 "pros": ["易于使用", "社区活跃", "文档丰富", "生态完善"],
#                 "cons": ["大数据处理能力有限", "复杂查询性能一般"],
#                 "supported_versions": ["5.7+", "8.0+"]
#             },
#             {
#                 "type": "kingbase",
#                 "name": "人大金仓 KingbaseES",
#                 "description": "国产自主可控的企业级关系数据库管理系统",
#                 "default_port": 54321,
#                 "features": ["PostgreSQL兼容", "高可用集群", "国密算法", "安全认证", "分布式架构", "在线扩容"],
#                 "connection_example": {
#                     "host": "kingbase.example.com",
#                     "port": 54321,
#                     "username": "system",
#                     "password": "kingbase_pwd",
#                     "database": "test"
#                 },
#                 "driver": "asyncpg (PostgreSQL兼容)",
#                 "vendor": "北京人大金仓信息技术股份有限公司",
#                 "license": "商业许可",
#                 "use_cases": ["政府信息化", "金融核心系统", "电信运营", "大型企业应用"],
#                 "pros": ["自主可控", "安全性高", "技术支持完善", "符合国产化要求"],
#                 "cons": ["生态相对较小", "第三方工具支持有限"],
#                 "supported_versions": ["V8.6+", "V8.7+"],
#                 "compliance": ["等保三级", "国密标准", "信创认证"]
#             },
#             {
#                 "type": "hive",
#                 "name": "Apache Hive",
#                 "description": "基于Hadoop的数据仓库软件，提供数据汇总、查询和分析",
#                 "default_port": 10000,
#                 "features": ["SQL接口", "分区表", "外部表", "UDF支持", "MapReduce集成", "Tez/Spark引擎"],
#                 "connection_example": {
#                     "host": "hadoop101",
#                     "port": 10000,
#                     "username": "bigdata",
#                     "password": "hive_pwd",
#                     "database": "default",
#                     "auth": "CUSTOM"
#                 },
#                 "driver": "pyhive",
#                 "vendor": "Apache Software Foundation",
#                 "license": "Apache License 2.0",
#                 "use_cases": ["数据仓库", "ETL处理", "大数据分析", "数据挖掘"],
#                 "pros": ["处理大规模数据", "SQL兼容性好", "生态丰富", "扩展性强"],
#                 "cons": ["实时性较差", "小数据查询效率低", "复杂性高"],
#                 "supported_versions": ["2.3+", "3.1+"],
#                 "dependencies": ["Hadoop", "HDFS", "YARN"]
#             },
#             {
#                 "type": "doris",
#                 "name": "Apache Doris",
#                 "description": "现代化的MPP分析型数据库，支持实时数据分析",
#                 "default_port": 9030,
#                 "features": ["MPP架构", "向量化执行", "实时分析", "高并发查询", "弹性扩展", "MySQL兼容"],
#                 "connection_example": {
#                     "host": "doris-fe.example.com",
#                     "port": 9030,
#                     "username": "root",
#                     "password": "doris_pwd",
#                     "database": "demo"
#                 },
#                 "driver": "aiomysql (MySQL兼容)",
#                 "vendor": "Apache Software Foundation",
#                 "license": "Apache License 2.0",
#                 "use_cases": ["实时数据分析", "OLAP查询", "报表系统", "数据大屏"],
#                 "pros": ["查询性能优秀", "支持实时导入", "运维简单", "MySQL兼容"],
#                 "cons": ["相对较新", "社区规模中等", "复杂事务支持有限"],
#                 "supported_versions": ["1.2+", "2.0+"],
#                 "architecture": ["Frontend(FE)", "Backend(BE)"]
#             },
#             {
#                 "type": "dameng",
#                 "name": "达梦数据库 (DM)",
#                 "description": "国产大型通用关系数据库管理系统",
#                 "default_port": 5236,
#                 "features": ["完全自主研发", "高性能事务处理", "高安全等级", "集群架构", "国密支持", "多版本并发控制"],
#                 "connection_example": {
#                     "host": "dameng.example.com",
#                     "port": 5236,
#                     "username": "SYSDBA",
#                     "password": "dameng_pwd",
#                     "database": "DAMENG"
#                 },
#                 "driver": "dmPython (官方驱动)",
#                 "vendor": "达梦数据库股份有限公司",
#                 "license": "商业许可",
#                 "use_cases": ["金融核心系统", "政务信息化", "电信运营", "制造业ERP"],
#                 "pros": ["完全自主", "性能优异", "安全性高", "技术支持好"],
#                 "cons": ["成本较高", "生态相对封闭", "学习成本高"],
#                 "supported_versions": ["DM7+", "DM8+"],
#                 "compliance": ["等保四级", "国密认证", "军B+认证", "EAL4+"]
#             },
#             {
#                 "type": "gbase",
#                 "name": "南大通用 GBase",
#                 "description": "国产分布式数据库，支持HTAP混合负载",
#                 "default_port": 5432,
#                 "features": ["分布式架构", "线性扩展", "HTAP融合", "PostgreSQL兼容", "MPP查询", "事务一致性"],
#                 "connection_example": {
#                     "host": "gbase.example.com",
#                     "port": 5432,
#                     "username": "gbase",
#                     "password": "gbase_pwd",
#                     "database": "testdb"
#                 },
#                 "driver": "asyncpg (PostgreSQL兼容)",
#                 "vendor": "天津南大通用数据技术股份有限公司",
#                 "license": "商业许可",
#                 "use_cases": ["数据仓库", "实时分析", "大数据平台", "云原生应用"],
#                 "pros": ["分布式能力强", "HTAP支持", "PostgreSQL兼容", "扩展性好"],
#                 "cons": ["技术复杂度高", "运维要求高", "成本较高"],
#                 "supported_versions": ["8a+", "8c+", "8s+"],
#                 "deployment": ["集中式", "分布式", "云原生"]
#             },
#             {
#                 "type": "tidb",
#                 "name": "TiDB",
#                 "description": "开源分布式HTAP数据库，兼容MySQL协议",
#                 "default_port": 4000,
#                 "features": ["水平扩展", "强一致性", "MySQL兼容", "HTAP支持", "云原生", "自动故障恢复"],
#                 "connection_example": {
#                     "host": "tidb.example.com",
#                     "port": 4000,
#                     "username": "root",
#                     "password": "tidb_pwd",
#                     "database": "test"
#                 },
#                 "driver": "aiomysql (MySQL兼容)",
#                 "vendor": "PingCAP",
#                 "license": "Apache License 2.0",
#                 "use_cases": ["互联网应用", "金融科技", "游戏业务", "新零售"],
#                 "pros": ["开源免费", "MySQL兼容", "弹性扩展", "HTAP能力"],
#                 "cons": ["资源消耗较大", "运维复杂", "学习曲线陡峭"],
#                 "supported_versions": ["6.0+", "7.0+"],
#                 "components": ["TiDB Server", "TiKV", "PD", "TiFlash"],
#                 "cloud_services": ["TiDB Cloud", "各大云厂商托管服务"]
#             }
#         ]
#
# # 全局数据集成服务实例
# data_integration_service = DataIntegrationService()
