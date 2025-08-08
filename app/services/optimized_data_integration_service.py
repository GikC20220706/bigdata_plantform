# app/services/optimized_data_integration_service.py
"""
优化后的数据集成服务 - 支持高并发和多层缓存
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from loguru import logger
import json
from pathlib import Path
import os
from app.utils.data_integration_clients import (
    DatabaseClientFactory,
    connection_manager,
)
from app.utils.integration_cache import (
    integration_cache,
    cache_connection_status,
    cache_table_schema,
    cache_data_preview
)
from app.models.data_source import DataSource, DataSourceConnection
from app.utils.database import get_db
from config.settings import settings
from sqlalchemy import text


class OptimizedDataIntegrationService:
    """优化后的数据集成服务"""

    def __init__(self):
        self.connection_manager = connection_manager
        self.cache_manager = integration_cache
        self._initialize_default_connections()

    def _initialize_default_connections(self):
        """初始化默认连接配置"""
        if not settings.use_real_clusters:
            logger.info("Mock模式：跳过真实数据库连接初始化")
            return

        # 从数据库加载已保存的连接配置
        self._load_saved_connections()

    def _load_saved_connections(self):
        """从数据库加载已保存的连接配置 - 使用同步数据库会话"""
        try:
            from app.utils.database import get_sync_db_session
            from app.models.data_source import DataSource
            import json

            # 使用同步数据库会话
            db = get_sync_db_session()
            try:
                saved_sources = db.query(DataSource).filter(DataSource.is_active == True).all()
                loaded_count = 0
                for source in saved_sources:
                    try:
                        if source.connection_config:
                            config = json.loads(source.connection_config) if isinstance(
                                source.connection_config, str) else source.connection_config
                            self.connection_manager.add_client(
                                source.name,
                                source.source_type,
                                config
                            )
                            logger.info(f"✅ 加载已保存的数据源: {source.name}")
                            loaded_count += 1
                    except Exception as e:
                        logger.error(f"❌ 加载数据源 {source.name} 失败: {e}")

                logger.info(f"成功加载 {loaded_count} 个数据源连接配置")

            finally:
                db.close()

        except Exception as e:
            logger.error(f"❌ 从数据库加载连接配置失败: {e}")
            logger.info("将继续启动，但需要手动配置数据源连接")

    @cache_table_schema(ttl=1800)  # 30分钟缓存
    async def get_table_schema(self, source_name: str, table_name: str, database: str = None, schema: str = None) -> Dict[str, Any]:
        """获取表结构 - 长期缓存"""
        try:
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            schema = await client.get_table_schema(table_name, database,schema)
            return {
                "success": True,
                "source_name": source_name,
                "database": database,
                "table_name": table_name,
                "schema": schema,
                "retrieved_at": datetime.now(),
                "cached": True
            }
        except Exception as e:
            logger.error(f"获取表结构失败 {source_name}.{database}.{table_name}: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    @cache_data_preview(ttl=300)  # 5分钟缓存
    async def execute_query(self, source_name: str, query: str, database: str = None, schema: str = None,
                            limit: int = 100) -> Dict[str, Any]:
        """执行查询 - 短期缓存"""
        try:
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            # 安全检查：确保有LIMIT限制
            if limit and "limit" not in query.lower() and "select" in query.lower():
                if query.strip().endswith(';'):
                    query = query.strip()[:-1]
                query = f"{query} LIMIT {limit}"

            start_time = datetime.now()
            results = await client.execute_query(query, database, schema)
            end_time = datetime.now()

            return {
                "success": True,
                "source_name": source_name,
                "database": database,
                "schema": schema,
                "query": query,
                "results": results,
                "row_count": len(results),
                "execution_time_ms": int((end_time - start_time).total_seconds() * 1000),
                "executed_at": start_time,
                "cached": True
            }
        except Exception as e:
            logger.error(f"查询执行失败 {source_name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "query": query
            }

    async def get_data_sources_list(self) -> List[Dict[str, Any]]:
        """获取所有数据源列表 - 混合缓存策略"""
        try:
            # 从缓存获取基础列表
            cache_key = "sources_list"

            async def fetch_sources_data():
                return await self._fetch_sources_from_clients()

            sources = await self.cache_manager.get_cached_data(
                cache_key,
                fetch_sources_data,
                {'redis': 120}  # 2分钟缓存
            )

            # 异步更新数据库中的统计信息
            asyncio.create_task(self._update_sources_statistics(sources))

            return sources

        except Exception as e:
            logger.error(f"获取数据源列表失败: {e}")
            return []

    async def _fetch_sources_from_clients(self) -> List[Dict[str, Any]]:
        """从客户端获取数据源信息"""
        clients = self.connection_manager.list_clients()

        if not clients and not settings.use_real_clusters:
            # 返回Mock数据
            return await self._get_mock_sources_list()

        # 并行获取连接状态
        connection_results = await self._parallel_test_connections(clients)

        sources_list = []
        for name in clients:
            client = self.connection_manager.get_client(name)
            test_result = connection_results.get(name, {})

            source_info = {
                "name": name,
                "type": client.__class__.__name__.replace('Client', '').lower(),
                "status": "connected" if test_result.get('success') else "disconnected",
                "host": client.config.get('host', '未知'),
                "port": client.config.get('port', 0),
                "database": client.config.get('database', ''),
                "description": f"{test_result.get('database_type', '未知')} 数据库",
                "last_test": test_result.get('test_time', datetime.now()),
                "version": test_result.get('version', '未知'),
                "tables_count": 0  # 延迟加载
            }

            if not test_result.get('success'):
                source_info["error"] = test_result.get('error', '连接失败')

            sources_list.append(source_info)

        return sources_list

    async def _get_mock_sources_list(self) -> List[Dict[str, Any]]:
        """获取Mock数据源列表"""
        return [
            {
                "name": "MySQL-Demo",
                "type": "mysql",
                "status": "connected",
                "host": "localhost",
                "port": 3306,
                "database": "demo",
                "description": "MySQL演示数据库",
                "tables_count": 15,
                "last_test": datetime.now() - timedelta(minutes=5),
                "created_at": datetime.now() - timedelta(days=7)
            },
            {
                "name": "Hive-Warehouse",
                "type": "hive",
                "status": "connected",
                "host": "hadoop101",
                "port": 10000,
                "database": "default",
                "description": "Hive数据仓库",
                "tables_count": 128,
                "last_test": datetime.now() - timedelta(minutes=3),
                "created_at": datetime.now() - timedelta(days=30)
            },
            # ... 其他Mock数据
        ]

    async def _update_sources_statistics(self, sources: List[Dict[str, Any]]):
        """异步更新数据源统计信息"""
        try:
            for source in sources:
                if source.get('status') == 'connected':
                    # 异步获取表数量等统计信息
                    asyncio.create_task(self._update_single_source_stats(source['name']))
        except Exception as e:
            logger.error(f"更新数据源统计信息失败: {e}")

    async def _update_single_source_stats(self, source_name: str):
        """更新单个数据源的统计信息"""
        try:
            tables_result = await self.get_tables(source_name)
            if tables_result.get('success'):
                table_count = tables_result.get('count', 0)

                # 更新数据库记录
                db = next(get_db())
                data_source = db.query(DataSource).filter(DataSource.name == source_name).first()
                if data_source:
                    # 这里可以添加表数量等统计字段到数据源模型
                    # data_source.tables_count = table_count
                    db.commit()
                db.close()

        except Exception as e:
            logger.debug(f"更新数据源统计信息失败 {source_name}: {e}")

    async def batch_test_connections(self, source_names: List[str]) -> Dict[str, Any]:
        """批量测试连接 - 高效并发版本"""
        try:
            # 使用缓存和并发处理
            results = {}

            # 分批处理，避免过载
            batch_size = 10
            for i in range(0, len(source_names), batch_size):
                batch = source_names[i:i + batch_size]
                batch_results = await self._parallel_test_connections(batch)
                results.update(batch_results)

            # 统计结果
            total_tested = len(results)
            successful = sum(1 for result in results.values() if result.get('success'))
            failed = total_tested - successful

            # 异步记录测试结果
            for name, result in results.items():
                asyncio.create_task(self._record_connection_test(name, result))

            summary = {
                "total_tested": total_tested,
                "successful": successful,
                "failed": failed,
                "success_rate": round((successful / total_tested) * 100, 1) if total_tested > 0 else 0,
                "test_results": results,
                "tested_at": datetime.now()
            }

            return summary
        except Exception as e:
            logger.error(f"批量测试连接失败: {e}")
            raise

    async def get_health_status(self) -> Dict[str, Any]:
        """获取数据集成模块健康状态"""
        try:
            # 获取概览信息（使用缓存）
            overview = await self.get_data_sources_overview()

            total_sources = overview.get('total_sources', 0)
            active_connections = overview.get('active_connections', 0)
            failed_connections = overview.get('failed_connections', 0)

            # 计算健康分数
            if total_sources == 0:
                health_score = 100
                status = "healthy"
            else:
                health_score = (active_connections / total_sources) * 100
                if health_score >= 90:
                    status = "healthy"
                elif health_score >= 70:
                    status = "warning"
                else:
                    status = "critical"

            # 获取缓存统计
            cache_stats = self.cache_manager.get_cache_stats()

            health_data = {
                "status": status,
                "health_score": round(health_score, 1),
                "total_sources": total_sources,
                "active_connections": active_connections,
                "failed_connections": failed_connections,
                "uptime_percentage": health_score,
                "last_check": datetime.now(),
                "issues": [],
                "cache_performance": cache_stats,
                "performance_metrics": {
                    "average_response_time": "< 100ms",
                    "cache_hit_rate": f"{cache_stats.get('memory_hit_rate', 0):.1f}%",
                    "concurrent_connections": len(self.connection_manager.list_clients())
                }
            }

            # 添加问题描述
            if failed_connections > 0:
                health_data["issues"].append(f"{failed_connections} 个数据源连接失败")

            if total_sources == 0:
                health_data["issues"].append("未配置任何数据源")

            # 检查缓存性能
            if cache_stats.get('cache_miss_rate', 0) > 50:
                health_data["issues"].append("缓存命中率较低，可能影响性能")

            return health_data
        except Exception as e:
            logger.error(f"获取健康状态失败: {e}")
            return {
                "status": "error",
                "error": str(e),
                "last_check": datetime.now()
            }

    # 其他方法保持不变，但可以添加适当的缓存装饰器
    async def get_databases(self, source_name: str) -> Dict[str, Any]:
        """获取数据库列表 - 添加缓存"""
        cache_key = f"databases_{source_name}"

        async def fetch_databases():
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            databases = await client.get_databases()
            return {
                "success": True,
                "source_name": source_name,
                "databases": databases,
                "count": len(databases),
                "retrieved_at": datetime.now()
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_databases,
            {'redis': 600}  # 10分钟缓存
        )

    async def get_tables(self, source_name: str, database: str = None, schema: str = None,
                         limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """获取表列表 - 添加缓存和分页支持"""
        # 缓存key包含分页信息
        cache_key = f"tables_{source_name}_{database or 'default'}_{schema or 'default'}_{limit}_{offset}"

        async def fetch_tables():
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            # 调用客户端的分页方法
            tables = await client.get_tables(database, schema, limit, offset)

            # 获取总数（如果客户端支持）
            total_count = 0
            if hasattr(client, 'get_tables_count'):
                total_count = await client.get_tables_count(database, schema)
            else:
                # 如果客户端不支持get_tables_count，估算总数
                if len(tables) == limit:
                    # 如果返回的数量等于limit，说明可能还有更多数据
                    total_count = offset + len(tables) + 1  # 估算值
                else:
                    total_count = offset + len(tables)  # 实际总数

            return {
                "success": True,
                "source_name": source_name,
                "database": database,
                "schema": schema,
                "tables": tables,
                "count": len(tables),
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + len(tables) < total_count,
                "retrieved_at": datetime.now()
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_tables,
            {'redis': 300}  # 5分钟缓存
        )

    # 可选：添加获取总数的独立方法（用于前端分页计算）
    async def get_tables_count(self, source_name: str, database: str = None, schema: str = None) -> Dict[str, Any]:
        """获取表总数"""
        cache_key = f"tables_count_{source_name}_{database or 'default'}_{schema or 'default'}"

        async def fetch_count():
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {"success": False, "error": f"数据源 {source_name} 不存在"}

            if hasattr(client, 'get_tables_count'):
                count = await client.get_tables_count(database, schema)
            else:
                # 降级：获取所有表然后计数（适用于表不多的情况）
                tables = await client.get_tables(database, schema, 999999, 0)
                count = len(tables)

            return {
                "success": True,
                "source_name": source_name,
                "database": database,
                "schema": schema,
                "total_count": count
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_count,
            {'redis': 600}  # 10分钟缓存（总数变化较少）
        )

    async def get_data_sources_overview(self) -> Dict[str, Any]:
        """获取数据源概览"""
        try:
            clients = self.connection_manager.list_clients()

            # 如果没有客户端，返回基础信息
            if not clients:
                return {
                    "total_sources": 0,
                    "active_connections": 0,
                    "failed_connections": 0,
                    "supported_types": ["mysql", "postgresql", "hive", "doris", "kingbase"],
                    "sources_by_type": {},
                    "data_volume_estimate": "0GB",
                    "last_sync": datetime.now(),
                    "health_status": "正常"
                }

            # 并行测试连接
            connection_results = await self._parallel_test_connections(clients)

            # 统计结果
            total_sources = len(clients)
            active_connections = sum(1 for result in connection_results.values() if result.get('success'))
            failed_connections = total_sources - active_connections

            # 按类型统计
            sources_by_type = {}
            for client_name in clients:
                client = self.connection_manager.get_client(client_name)
                client_type = client.__class__.__name__.replace('Client', '').lower()
                sources_by_type[client_type] = sources_by_type.get(client_type, 0) + 1

            return {
                "total_sources": total_sources,
                "active_connections": active_connections,
                "failed_connections": failed_connections,
                "supported_types": ["mysql", "postgresql", "hive", "doris", "kingbase"],
                "sources_by_type": sources_by_type,
                "data_volume_estimate": f"{total_sources * 10}GB",
                "last_sync": datetime.now(),
                "health_status": "良好" if failed_connections == 0 else "部分异常" if active_connections > 0 else "异常"
            }

        except Exception as e:
            logger.error(f"获取数据源概览失败: {e}")
            return {
                "total_sources": 0,
                "active_connections": 0,
                "failed_connections": 0,
                "supported_types": ["mysql", "postgresql", "hive", "doris", "kingbase"],
                "sources_by_type": {},
                "data_volume_estimate": "0GB",
                "last_sync": datetime.now(),
                "health_status": "异常",
                "error": str(e)
            }

    async def _parallel_test_connections(self, client_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """并行测试连接"""
        results = {}
        try:
            # 限制并发数量
            semaphore = asyncio.Semaphore(5)

            async def test_single_connection(name):
                async with semaphore:
                    try:
                        client = self.connection_manager.get_client(name)
                        if client:
                            result = await client.test_connection()
                            return name, result
                        else:
                            return name, {"success": False, "error": "客户端不存在"}
                    except Exception as e:
                        return name, {"success": False, "error": str(e)}

            # 并行执行测试
            tasks = [test_single_connection(name) for name in client_names[:10]]  # 限制最多10个
            test_results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in test_results:
                if isinstance(result, tuple) and len(result) == 2:
                    name, test_result = result
                    results[name] = test_result

        except Exception as e:
            logger.error(f"并行测试连接失败: {e}")

        return results

    async def _estimate_total_data_volume(self, client_names: List[str]) -> str:
        """估算总数据量"""
        try:
            # 尝试从真实集群获取数据量
            from app.utils.hadoop_client import HDFSClient

            try:
                hdfs_client = HDFSClient()
                storage_info = hdfs_client.get_storage_info()
                if storage_info and storage_info.get('total_size', 0) > 0:
                    total_gb = storage_info['total_size'] / (1024 ** 3)  # 转换为GB
                    return f"{total_gb:.1f}GB"
            except:
                pass

            # 备用估算
            total_gb = len(client_names) * 50  # 生产环境估算更大
            return f"{total_gb}GB"
        except:
            return "未知"

    async def _get_mock_overview(self) -> Dict[str, Any]:
        """获取Mock概览数据"""
        return {
            "total_sources": 6,
            "active_connections": 5,
            "failed_connections": 1,
            "supported_types": DatabaseClientFactory.get_supported_types(),
            "sources_by_type": {
                "mysql": 2,
                "hive": 1,
                "doris": 1,
                "kingbase": 1,
                "tidb": 1
            },
            "data_volume_estimate": "125.6GB",
            "last_sync": datetime.now(),
            "health_status": "良好",
            "cache_info": self.cache_manager.get_cache_stats()
        }

    async def add_data_source(self, name: str, db_type: str, config: Dict[str, Any], description: str = "") -> Dict[
        str, Any]:
        try:
            logger.info(f"开始添加数据源: name={name}, type={db_type}")

            # 1. 参数验证
            if not name or not db_type or not config:
                return {
                    "success": False,
                    "error": "参数不完整: 需要name, db_type, config"
                }

            # 2. 检查数据源是否已存在
            if self.connection_manager.get_client(name):
                return {
                    "success": False,
                    "error": f"数据源 '{name}' 已存在"
                }

            # 3. 临时添加到连接管理器进行测试
            logger.info(f"临时添加到连接管理器: {name}")
            success = self.connection_manager.add_client(name, db_type, config)
            if not success:
                return {
                    "success": False,
                    "error": f"不支持的数据库类型: {db_type}"
                }

            # 4. 测试连接 - 关键步骤
            logger.info(f"开始连接测试: {name}")
            client = self.connection_manager.get_client(name)
            test_result = await client.test_connection()

            logger.info(f"连接测试结果: {name} -> {test_result.get('success', False)}")

            # 5. 关键修复：连接测试失败时的处理
            if not test_result.get('success'):
                error_msg = test_result.get('error', '未知连接错误')
                logger.warning(f"连接测试失败: {name}, 原因: {error_msg}")

                # 从连接管理器中移除失败的连接
                try:
                    self.connection_manager.remove_client(name)
                    logger.info(f"已清理失败的连接: {name}")
                except Exception as remove_error:
                    logger.error(f"清理失败连接时出错: {remove_error}")

                # 返回失败结果
                return {
                    "success": False,
                    "error": f"数据源连接测试失败: {error_msg}",
                    "test_result": test_result,
                    "connection_details": {
                        "host": config.get('host'),
                        "port": config.get('port'),
                        "database": config.get('database')
                    }
                }

            # 6. 连接测试成功 - 保存到数据库
            logger.info(f"连接测试成功，开始保存到数据库: {name}")
            try:
                await self._save_data_source_to_db(name, db_type, config, test_result, description)
                logger.info(f"数据源保存到数据库成功: {name}")
            except Exception as db_error:
                logger.error(f"保存到数据库失败: {name}, 错误: {db_error}")

                # 数据库保存失败，清理连接管理器
                try:
                    self.connection_manager.remove_client(name)
                    logger.info(f"已清理保存失败的连接: {name}")
                except Exception as cleanup_error:
                    logger.error(f"清理连接时出错: {cleanup_error}")

                return {
                    "success": False,
                    "error": f"保存数据源配置失败: {str(db_error)}",
                    "test_result": test_result
                }

            # 7. 清除相关缓存
            try:
                await self.cache_manager.invalidate_cache(pattern=name)
                await self.cache_manager.invalidate_cache(pattern="overview")
                logger.info(f"缓存清理成功: {name}")
            except Exception as cache_error:
                logger.warning(f"清除缓存失败: {cache_error}")

            # 8. 成功完成
            logger.info(f"数据源添加完全成功: {name}")
            return {
                "success": True,
                "name": name,
                "type": db_type,
                "status": "connected",
                "test_result": test_result,
                "created_at": datetime.now(),
                "message": "数据源连接测试成功并已保存"
            }

        except Exception as e:
            logger.error(f"❌ 添加数据源异常 {name}: {e}")
            import traceback
            logger.error(f"异常堆栈: {traceback.format_exc()}")

            # 异常情况下清理连接管理器
            try:
                self.connection_manager.remove_client(name)
                logger.info(f"✅ 异常清理连接成功: {name}")
            except Exception as cleanup_error:
                logger.error(f"⚠️ 异常清理连接失败: {cleanup_error}")

            return {
                "success": False,
                "error": f"添加数据源时发生异常: {str(e)}"
            }

    async def _save_data_source_to_db(self, name: str, db_type: str, config: Dict[str, Any],
                                      test_result: Dict[str, Any],
                                      description: str = ""):
        """保存数据源配置到数据库"""
        try:
            from app.utils.database import get_sync_db_session
            from sqlalchemy import text
            import json

            # 使用同步数据库会话
            db = get_sync_db_session()

            try:
                # 检查表是否存在
                result = db.execute(text("SHOW TABLES LIKE 'data_sources'"))
                if not result.fetchone():
                    logger.warning("data_sources表不存在，跳过数据库保存")
                    return

                # 检查是否已存在
                result = db.execute(text("SELECT id FROM data_sources WHERE name = :name"),
                                    {"name": name})
                existing = result.fetchone()

                if existing:
                    # 更新现有记录
                    db.execute(text("""
                        UPDATE data_sources
                        SET source_type = :db_type, connection_config = :config, status = :status, 
                            last_connection_test = NOW(), description = :description, is_active = TRUE
                        WHERE name = :name
                    """), {
                        "db_type": db_type,
                        "config": json.dumps(config),
                        "status": "connected" if test_result.get('success') else "disconnected",
                        "description": description,
                        "name": name
                    })
                else:
                    # 创建新记录
                    db.execute(text("""
                        INSERT INTO data_sources 
                        (name, display_name, source_type, connection_config, status, description, is_active, last_connection_test)
                        VALUES (:name, :display_name, :db_type, :config, :status, :description, TRUE, NOW())
                    """), {
                        "name": name,
                        "display_name": name,
                        "db_type": db_type,
                        "config": json.dumps(config),
                        "status": "connected" if test_result.get('success') else "disconnected",
                        "description": description
                    })

                db.commit()
                logger.info(f"数据源 {name} 保存到数据库成功")

            finally:
                db.close()

        except Exception as e:
            logger.error(f"保存数据源到数据库失败: {e}")
            if 'db' in locals():
                try:
                    db.rollback()
                    db.close()
                except:
                    pass

    @cache_connection_status(ttl=30)
    async def test_data_source(self, name: str) -> Dict[str, Any]:
        """测试数据源连接 - 缓存优化"""
        try:
            client = self.connection_manager.get_client(name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {name} 不存在"
                }

            result = await client.test_connection()

            # 异步记录测试结果到数据库
            asyncio.create_task(self._record_connection_test(name, result))

            return result
        except Exception as e:
            logger.error(f"测试数据源连接失败 {name}: {e}")
            return {
                "success": False,
                "error": str(e),
                "test_time": datetime.now()
            }

    async def _record_connection_test(self, name: str, test_result: Dict[str, Any]):
        """异步记录连接测试结果"""
        try:
            db = next(get_db())
            data_source = db.query(DataSource).filter(DataSource.name == name).first()

            if data_source:
                # 更新数据源状态
                data_source.status = "online" if test_result.get('success') else "offline"
                data_source.last_connection_test = datetime.now()

                # 记录连接历史
                connection_record = DataSourceConnection(
                    data_source_id=data_source.id,
                    connection_timestamp=datetime.now(),
                    connection_type="test",
                    success=test_result.get('success', False),
                    response_time_ms=test_result.get('response_time_ms', 0),
                    error_message=test_result.get('error') if not test_result.get('success') else None
                )
                db.add(connection_record)
                db.commit()

            db.close()
        except Exception as e:
            logger.error(f"记录连接测试结果失败: {e}")

    async def remove_data_source(self, name: str) -> Dict[str, Any]:
        """删除数据源"""
        try:
            logger.info(f"开始删除数据源: {name}")

            # 检查数据源是否存在
            client_exists = self.connection_manager.get_client(name) is not None
            logger.info(f"数据源是否存在: {client_exists}")

            if not client_exists:
                return {
                    "success": False,
                    "error": f"数据源 {name} 不存在"
                }

            # 从连接管理器中移除
            logger.info(f"步骤1: 从连接管理器移除 {name}")
            self.connection_manager.remove_client(name)
            logger.info(f"连接管理器移除完成: {name}")

            # 从数据库中软删除
            try:
                logger.info(f"步骤2: 开始数据库软删除 {name}")
                await self._remove_from_db(name)
                logger.info(f"数据库软删除成功: {name}")
            except Exception as db_error:
                logger.error(f"数据库删除失败 {name}: {db_error}")
                # 即使数据库删除失败，连接已移除，仍然认为删除成功

            logger.info(f"步骤3: 删除操作完成 {name}")
            return {
                "success": True,
                "message": f"数据源 {name} 删除成功"
            }

        except Exception as e:
            logger.error(f"删除数据源异常 {name}: {e}")
            import traceback
            logger.error(f"删除异常堆栈: {traceback.format_exc()}")
            return {
                "success": False,
                "error": str(e) if str(e) else "删除过程中发生未知错误"
            }
    async def _remove_data_source_from_db(self, name: str) -> bool:
        """从数据库删除数据源（软删除）"""
        try:
            db = next(get_db())
            data_source = db.query(DataSource).filter(DataSource.name == name).first()

            if data_source:
                # 软删除：设置为非活跃状态
                data_source.is_active = False
                data_source.status = "deleted"
                db.commit()
                logger.info(f"数据源 {name} 已从数据库软删除")
                return True
            else:
                logger.warning(f"数据库中未找到数据源 {name}")
                return False

            db.close()

        except Exception as e:
            logger.error(f"从数据库删除数据源失败: {e}")
            if 'db' in locals():
                db.rollback()
                db.close()
            return False

    async def _remove_from_db(self, name: str):
        """从数据库中移除数据源 - 调试版本"""
        try:
            logger.info(f"_remove_from_db: 开始处理 {name}")

            from app.utils.database import get_sync_db_session
            from sqlalchemy import text

            logger.info(f"_remove_from_db: 获取数据库会话")
            db = get_sync_db_session()

            try:
                logger.info(f"_remove_from_db: 执行SQL更新 {name}")
                # 软删除 - 使用正确的SQLAlchemy占位符格式
                result = db.execute(text("""
                    UPDATE data_sources 
                    SET is_active = FALSE, status = 'deleted'
                    WHERE name = :name
                """), {"name": name})

                logger.info(f"_remove_from_db: SQL执行结果 rowcount={result.rowcount}")
                db.commit()
                logger.info(f"_remove_from_db: 数据库提交成功 {name}")

            except Exception as db_error:
                logger.error(f"_remove_from_db: 数据库操作失败 {name}: {db_error}")
                import traceback
                logger.error(f"_remove_from_db: 数据库异常堆栈: {traceback.format_exc()}")
                db.rollback()
                raise
            finally:
                logger.info(f"_remove_from_db: 关闭数据库连接")
                db.close()

        except Exception as e:
            logger.error(f"_remove_from_db: 整体异常 {name}: {e}")
            import traceback
            logger.error(f"_remove_from_db: 整体异常堆栈: {traceback.format_exc()}")
    async def get_table_metadata(self, source_name: str, table_name: str, database: str = None, schema: str = None) -> Dict[str, Any]:
        """获取表的完整元数据 - 缓存优化"""
        cache_key = f"metadata_{source_name}_{database or 'default'}_{table_name}"

        async def fetch_metadata():
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            metadata = await client.get_table_metadata(table_name, database,schema)
            return {
                "success": True,
                "source_name": source_name,
                "metadata": metadata,
                "retrieved_at": datetime.now()
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_metadata,
            {'redis': 1800}  # 30分钟缓存
        )

    async def search_tables(self, keyword: str = None, source_name: str = None, table_type: str = None) -> Dict[
        str, Any]:
        """搜索表 - 智能缓存"""
        cache_key = f"search_{keyword or 'all'}_{source_name or 'all'}_{table_type or 'all'}"

        async def fetch_search_results():
            all_tables = []
            clients_to_search = [source_name] if source_name else self.connection_manager.list_clients()

            # 并行搜索多个数据源
            search_tasks = []
            for client_name in clients_to_search[:10]:  # 限制并发数
                search_tasks.append(self._search_single_source(client_name, keyword, table_type))

            search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

            for result in search_results:
                if isinstance(result, list):
                    all_tables.extend(result)

            return {
                "success": True,
                "tables": all_tables,
                "total_count": len(all_tables),
                "search_criteria": {
                    "keyword": keyword,
                    "source_name": source_name,
                    "table_type": table_type
                },
                "searched_at": datetime.now()
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_search_results,
            {'redis': 600}  # 10分钟缓存
        )

    async def _search_single_source(self, client_name: str, keyword: str, table_type: str) -> List[Dict[str, Any]]:
        """搜索单个数据源"""
        try:
            tables_result = await self.get_tables(client_name)
            if not tables_result.get('success'):
                return []

            tables = []
            for table in tables_result['tables']:
                table['source_name'] = client_name

                # 应用过滤条件
                if keyword and keyword.lower() not in table['table_name'].lower():
                    continue
                if table_type and table.get('table_type', '').lower() != table_type.lower():
                    continue

                tables.append(table)

            return tables
        except Exception as e:
            logger.warning(f"搜索数据源 {client_name} 时出错: {e}")
            return []

    async def get_supported_database_types(self) -> List[Dict[str, Any]]:
        """获取支持的数据库类型 - 静态缓存"""
        cache_key = "supported_types"

        async def fetch_types():
            from app.utils.data_integration_clients import DatabaseClientFactory
            return DatabaseClientFactory.get_supported_types()

        types = await self.cache_manager.get_cached_data(
            cache_key,
            fetch_types,
            {'redis': 3600}  # 1小时缓存
        )

        return types

    async def preview_data_source(self, source_name: str, table_name: str = None, database: str = None,
                                  limit: int = 10) -> \
            Dict[str, Any]:
        """预览数据源数据"""
        try:
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            # 如果没有指定表名，获取第一个表
            if not table_name:
                tables_result = await self.get_tables(source_name, database)
                if not tables_result.get('success') or not tables_result.get('tables'):
                    return {
                        "success": False,
                        "error": "没有可用的表进行预览"
                    }
                # 获取第一个表的名称
                tables = tables_result['tables']
                if tables:
                    table_name = tables[0]['table_name']  # 修复：从tables结果中获取table_name
                else:
                    return {
                        "success": False,
                        "error": "数据源中没有找到任何表"
                    }

            # 执行查询预览
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            result = await self.execute_query(source_name, query, database, limit=limit)

            if result.get('success'):
                return {
                    "success": True,
                    "source_name": source_name,
                    "table_name": table_name,
                    "database": database,
                    "preview_data": result.get('results', []),
                    "row_count": len(result.get('results', [])),
                    "limit": limit
                }
            else:
                return result

        except Exception as e:
            logger.error(f"预览数据源失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def get_data_source_statistics(self, source_name: str) -> Dict[str, Any]:
        """获取数据源统计信息 - 缓存优化"""
        cache_key = f"statistics_{source_name}"

        async def fetch_statistics():
            # 获取数据库列表
            databases_result = await self.get_databases(source_name)
            if not databases_result.get('success'):
                return {
                    "success": False,
                    "error": databases_result.get('error', '数据源不存在')
                }

            databases = databases_result.get('databases', [])
            total_tables = 0
            tables_by_database = {}

            # 并行统计每个数据库的表数量
            table_tasks = []
            for db in databases[:5]:  # 限制并发数，只统计前5个数据库
                table_tasks.append(self._count_tables_in_database(source_name, db))

            table_results = await asyncio.gather(*table_tasks, return_exceptions=True)

            for i, (db, result) in enumerate(zip(databases[:5], table_results)):
                if isinstance(result, dict) and result.get('success'):
                    db_table_count = result.get('count', 0)
                    total_tables += db_table_count
                    tables_by_database[db] = db_table_count
                else:
                    tables_by_database[db] = 0

            # 测试连接状态
            test_result = await self.test_data_source(source_name)

            statistics = {
                "source_name": source_name,
                "connection_status": "connected" if test_result.get('success') else "disconnected",
                "database_type": test_result.get('database_type', '未知'),
                "version": test_result.get('version', '未知'),
                "total_databases": len(databases),
                "total_tables": total_tables,
                "tables_by_database": tables_by_database,
                "last_test": test_result.get('test_time', datetime.now()),
                "response_time_ms": test_result.get('response_time_ms', 0),
                "collected_at": datetime.now()
            }

            if not test_result.get('success'):
                statistics["error"] = test_result.get('error', '连接失败')

            return {
                "success": True,
                **statistics
            }

        return await self.cache_manager.get_cached_data(
            cache_key,
            fetch_statistics,
            {'redis': 900}  # 15分钟缓存
        )

    async def _count_tables_in_database(self, source_name: str, database: str) -> Dict[str, Any]:
        """统计单个数据库的表数量"""
        try:
            tables_result = await self.get_tables(source_name, database)
            return tables_result
        except Exception as e:
            return {"success": False, "error": str(e), "count": 0}

    # Excel相关方法
    async def upload_excel_source(self, name: str, file, description: str = None) -> Dict[str, Any]:
        """上传Excel文件创建数据源"""
        try:
            from app.utils.data_integration_clients import excel_service

            # 验证文件类型
            if not file.filename.lower().endswith(('.xlsx', '.xls')):
                return {
                    "success": False,
                    "error": "不支持的文件类型，请上传.xlsx或.xls文件"
                }

            # 验证文件大小 (限制为50MB)
            content = await file.read()
            file_size = len(content)
            await file.seek(0)  # 重置文件指针

            if file_size > 50 * 1024 * 1024:  # 50MB
                return {
                    "success": False,
                    "error": "文件大小超过限制（50MB）"
                }

            # 创建Excel数据源
            result = await excel_service.create_excel_source(name, file, description)

            if result.get('success'):
                # 添加到连接管理器
                config = result['config']
                add_result = await self.add_data_source(
                    name=name,
                    db_type='excel',
                    config=config
                )

                if add_result.get('success'):
                    # 清除相关缓存
                    await self.cache_manager.invalidate_cache(pattern="overview")

                    return {
                        "success": True,
                        "source_info": add_result,
                        "upload_info": result['upload_info']
                    }
                else:
                    # 如果添加数据源失败，删除上传的文件
                    await excel_service.delete_excel_source(config)
                    return {
                        "success": False,
                        "error": add_result.get('error', '创建数据源失败')
                    }
            else:
                return result

        except Exception as e:
            logger.error(f"上传Excel文件失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def list_excel_files(self) -> List[Dict[str, Any]]:
        """获取Excel文件列表"""
        try:
            # 返回空列表，实际实现可以扫描上传目录
            return []
        except Exception as e:
            logger.error(f"获取Excel文件列表失败: {e}")
            return []

    async def get_excel_sheets(self, source_name: str) -> Dict[str, Any]:
        """获取Excel工作表列表"""
        try:
            result = await self.get_tables(source_name)
            return result
        except Exception as e:
            logger.error(f"获取Excel工作表列表失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def preview_excel_sheet(self, source_name: str, sheet_name: str, limit: int = 10) -> Dict[str, Any]:
        """预览Excel工作表数据"""
        try:
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            # 检查是否为Excel客户端
            if not hasattr(client, 'get_data_preview'):
                return {
                    "success": False,
                    "error": "该数据源不支持预览功能"
                }

            result = await client.get_data_preview(sheet_name, limit)
            return result
        except Exception as e:
            logger.error(f"预览Excel工作表失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def delete_excel_source(self, source_name: str) -> Dict[str, Any]:
        """删除Excel数据源和对应的文件"""
        try:
            client = self.connection_manager.get_client(source_name)
            if not client:
                return {
                    "success": False,
                    "error": f"数据源 {source_name} 不存在"
                }

            # 获取文件配置
            config = client.config if hasattr(client, 'config') else {}

            # 删除文件
            from app.utils.data_integration_clients import excel_service
            delete_result = await excel_service.delete_excel_source(config)

            # 删除数据源
            source_result = await self.remove_data_source(source_name)

            return {
                "success": True,
                "file_deleted": delete_result.get('success', False),
                "source_deleted": source_result.get('success', False),
                "file_message": delete_result.get('message', ''),
                "source_message": source_result.get('message', '')
            }
        except Exception as e:
            logger.error(f"删除Excel数据源失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def export_excel_sheet(self, source_name: str, sheet_name: str, export_format: str = "json",
                                 limit: int = None) -> \
            Dict[str, Any]:
        """导出Excel工作表数据"""
        try:
            # 构建查询
            query = f"SELECT * FROM {sheet_name}"
            if limit:
                query += f" LIMIT {limit}"

            result = await self.execute_query(
                source_name=source_name,
                query=query,
                limit=limit or 1000
            )

            if result.get('success'):
                data = result['results']

                return {
                    "success": True,
                    "sheet_name": sheet_name,
                    "export_format": export_format,
                    "data": data,
                    "row_count": len(data),
                    "exported_at": datetime.now()
                }
            else:
                return result

        except Exception as e:
            logger.error(f"导出Excel工作表失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def _load_real_cluster_connections(self):
        """加载真实集群连接"""
        try:
            # 加载Hive连接
            if settings.HIVE_SERVER_HOST:
                hive_config = {
                    "host": settings.HIVE_SERVER_HOST,
                    "port": settings.HIVE_SERVER_PORT,
                    "username": settings.HIVE_USERNAME,
                    "password": settings.HIVE_PASSWORD,
                    "database": settings.HIVE_DATABASE or "default"
                }
                self.connection_manager.add_client("Production-Hive", "hive", hive_config)
                logger.info("✅ 加载Hive生产连接")

            # 加载HDFS连接（如果有客户端支持）
            if settings.HDFS_NAMENODE:
                hdfs_config = {
                    "namenode": settings.HDFS_NAMENODE,
                    "user": settings.HDFS_USER
                }
                # 如果有HDFS客户端实现，在这里添加
                logger.info("✅ HDFS配置已读取")

        except Exception as e:
            logger.error(f"加载真实集群连接失败: {e}")

    async def _get_production_fallback_overview(self) -> Dict[str, Any]:
        """生产环境备用概览数据"""
        return {
            "total_sources": 2,
            "active_connections": 1,
            "failed_connections": 1,
            "supported_types": DatabaseClientFactory.get_supported_types(),
            "sources_by_type": {"hive": 1, "hdfs": 1},
            "data_volume_estimate": "100.0GB",
            "last_sync": datetime.now(),
            "health_status": "部分异常",
            "error": "部分数据源连接失败",
            "cluster_info": {
                "hive_host": settings.HIVE_SERVER_HOST,
                "hdfs_namenode": settings.HDFS_NAMENODE
            }
        }


_service_instance = None

def get_optimized_data_integration_service():
    """获取数据集成服务实例"""
    global _service_instance
    if _service_instance is None:
        _service_instance = OptimizedDataIntegrationService()
    return _service_instance


