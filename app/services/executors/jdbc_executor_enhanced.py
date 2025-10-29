"""
JDBC执行器 - 执行SQL查询和执行（增强版）
支持多种数据库、连接池、事务控制
"""
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, AsyncEngine
from sqlalchemy import text
from loguru import logger
import asyncio
from datetime import datetime

from .base_executor import JobExecutor


# 全局连接池管理器
class ConnectionPoolManager:
    """数据库连接池管理器"""

    def __init__(self):
        self._pools: Dict[int, AsyncEngine] = {}
        self._lock = asyncio.Lock()

    async def get_engine(
        self,
        datasource_id: int,
        datasource_config: Dict[str, Any]
    ) -> AsyncEngine:
        """获取或创建数据库引擎（带连接池）"""
        async with self._lock:
            # 如果连接池已存在，直接返回
            if datasource_id in self._pools:
                return self._pools[datasource_id]

            # 创建新的连接池
            engine = await self._create_engine(datasource_config)
            self._pools[datasource_id] = engine

            logger.info(f"创建数据库连接池: datasource_id={datasource_id}")
            return engine

    async def _create_engine(self, config: Dict[str, Any]) -> AsyncEngine:
        """创建数据库引擎"""
        db_type = config.get('type', 'mysql')
        host = config.get('host')
        port = config.get('port', 3306)
        database = config.get('database')
        username = config.get('username')
        password = config.get('password')
        charset = config.get('charset', 'utf8mb4')

        # 连接池配置
        pool_size = config.get('poolSize', 5)
        max_overflow = config.get('maxOverflow', 10)
        pool_timeout = config.get('poolTimeout', 30)
        pool_recycle = config.get('poolRecycle', 3600)

        # 构建连接URL
        if db_type == 'mysql':
            driver = 'aiomysql'
            url = f"mysql+{driver}://{username}:{password}@{host}:{port}/{database}?charset={charset}"
        elif db_type == 'postgresql':
            driver = 'asyncpg'
            url = f"postgresql+{driver}://{username}:{password}@{host}:{port}/{database}"
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

        # 创建引擎（带连接池）
        engine = create_async_engine(
            url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            pool_pre_ping=True,  # 连接前检查
            echo=False
        )

        return engine

    async def close_all(self):
        """关闭所有连接池"""
        async with self._lock:
            for datasource_id, engine in self._pools.items():
                await engine.dispose()
                logger.info(f"关闭数据库连接池: datasource_id={datasource_id}")

            self._pools.clear()

    async def close_pool(self, datasource_id: int):
        """关闭指定的连接池"""
        async with self._lock:
            if datasource_id in self._pools:
                engine = self._pools.pop(datasource_id)
                await engine.dispose()
                logger.info(f"关闭数据库连接池: datasource_id={datasource_id}")


# 全局连接池管理器实例
connection_pool_manager = ConnectionPoolManager()


class JDBCExecutorEnhanced(JobExecutor):
    """JDBC执行器 - 增强版"""

    def __init__(self):
        super().__init__("jdbc_executor_enhanced")

    async def execute(
        self,
        db: AsyncSession,
        work_config: Dict[str, Any],
        instance_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行SQL"""
        try:
            datasource_id = work_config.get('dataSourceId')
            sql = work_config.get('sql')
            sql_type = work_config.get('type', 'query')  # query, execute
            timeout = work_config.get('timeout', 300)
            use_transaction = work_config.get('useTransaction', False)  # 是否使用事务
            params = work_config.get('params', {})  # SQL参数

            if not datasource_id:
                return {
                    "success": False,
                    "message": "数据源ID不能为空",
                    "data": None,
                    "error": "dataSourceId is required"
                }

            if not sql:
                return {
                    "success": False,
                    "message": "SQL不能为空",
                    "data": None,
                    "error": "SQL is required"
                }

            # 获取数据源配置
            datasource_config = await self._get_datasource_config(db, datasource_id)
            if not datasource_config:
                return {
                    "success": False,
                    "message": f"数据源不存在: {datasource_id}",
                    "data": None,
                    "error": f"Datasource {datasource_id} not found"
                }

            # 上下文变量替换
            if context:
                sql = self._replace_variables(sql, context)
                params = self._replace_dict_variables(params, context)

            # 获取数据库引擎（带连接池）
            engine = await connection_pool_manager.get_engine(
                datasource_id, datasource_config
            )

            # 执行SQL
            if sql_type == 'query':
                result = await self._execute_query(
                    engine, sql, params, timeout
                )
            else:
                result = await self._execute_update(
                    engine, sql, params, timeout, use_transaction
                )

            return result

        except Exception as e:
            logger.error(f"JDBC执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def _execute_query(
        self,
        engine: AsyncEngine,
        sql: str,
        params: Dict,
        timeout: int
    ) -> Dict[str, Any]:
        """执行查询"""
        try:
            start_time = datetime.now()

            async with engine.connect() as conn:
                # 执行查询
                result = await conn.execute(text(sql), params)

                # 获取列名
                columns = list(result.keys())

                # 获取数据
                rows = []
                for row in result:
                    rows.append(dict(zip(columns, row)))

                elapsed = (datetime.now() - start_time).total_seconds()

                return {
                    "success": True,
                    "message": f"查询成功，返回 {len(rows)} 行",
                    "data": {
                        "columns": columns,
                        "rows": rows,
                        "rowCount": len(rows),
                        "elapsed": round(elapsed, 3)
                    },
                    "error": None
                }

        except Exception as e:
            logger.error(f"SQL查询失败: {e}")
            return {
                "success": False,
                "message": "查询失败",
                "data": None,
                "error": str(e)
            }

    async def _execute_update(
        self,
        engine: AsyncEngine,
        sql: str,
        params: Dict,
        timeout: int,
        use_transaction: bool
    ) -> Dict[str, Any]:
        """执行更新（INSERT/UPDATE/DELETE）"""
        try:
            start_time = datetime.now()

            async with engine.begin() as conn:
                # 执行SQL
                result = await conn.execute(text(sql), params)

                affected_rows = result.rowcount

                elapsed = (datetime.now() - start_time).total_seconds()

                # 事务会自动提交

                return {
                    "success": True,
                    "message": f"执行成功，影响 {affected_rows} 行",
                    "data": {
                        "affectedRows": affected_rows,
                        "elapsed": round(elapsed, 3)
                    },
                    "error": None
                }

        except Exception as e:
            logger.error(f"SQL执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def _get_datasource_config(
        self,
        db: AsyncSession,
        datasource_id: int
    ) -> Optional[Dict[str, Any]]:
        """获取数据源配置"""
        from app.models.data_source import DataSource
        from sqlalchemy import select

        try:
            # 查询数据源
            result = await db.execute(
                select(DataSource).where(
                    DataSource.id == datasource_id,
                    DataSource.status == 1  # 只查询启用的数据源
                )
            )
            datasource = result.scalar_one_or_none()

            if not datasource:
                logger.error(f"数据源不存在或已禁用: {datasource_id}")
                return None

            # 转换为配置字典
            return datasource.to_config_dict()

        except Exception as e:
            logger.error(f"获取数据源配置失败: {e}")
            return None

    def _replace_variables(self, text: str, context: Dict) -> str:
        """替换SQL中的变量 ${varName}"""
        if not text or not context:
            return text

        import re
        pattern = r'\$\{(\w+)\}'

        def replacer(match):
            var_name = match.group(1)
            return str(context.get(var_name, match.group(0)))

        return re.sub(pattern, replacer, text)

    def _replace_dict_variables(self, data: Dict, context: Dict) -> Dict:
        """替换字典中的变量"""
        if not data or not context:
            return data

        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = self._replace_variables(value, context)
            else:
                result[key] = value

        return result

    async def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证配置"""
        errors = []

        if not config.get('dataSourceId'):
            errors.append("缺少dataSourceId数据源ID")

        if not config.get('sql'):
            errors.append("缺少sql")

        sql_type = config.get('type', 'query')
        if sql_type not in ['query', 'execute']:
            errors.append(f"不支持的SQL类型: {sql_type}")

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }


# 导出连接池管理器，供应用关闭时使用
__all__ = ['JDBCExecutorEnhanced', 'connection_pool_manager']