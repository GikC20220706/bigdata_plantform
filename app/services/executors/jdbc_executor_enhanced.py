"""
JDBC执行器 - 执行SQL查询和执行（增强版）
支持多种数据库、连接池、事务控制
"""
from typing import Dict, Any, Optional, List
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
        from urllib.parse import quote_plus
        db_type = config.get('type', 'mysql').lower()
        host = config.get('host')
        port = config.get('port')
        database = config.get('database')
        username = config.get('username')
        password = config.get('password')

        encoded_username = quote_plus(username) if username else ''
        encoded_password = quote_plus(password) if password else ''

        # 连接池配置
        pool_size = config.get('poolSize', 5)
        max_overflow = config.get('maxOverflow', 10)
        pool_timeout = config.get('poolTimeout', 30)
        pool_recycle = config.get('poolRecycle', 3600)

        # 根据数据库类型构建连接URL
        if db_type == 'mysql':
            driver = 'aiomysql'
            charset = config.get('charset', 'utf8mb4')
            if not port:
                port = 3306
            url = f"mysql+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}?charset={charset}"

        elif db_type == 'postgresql':
            driver = 'asyncpg'
            if not port:
                port = 5432
            url = f"postgresql+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}"

        elif db_type == 'kingbase':
            # kingbase 数据源不应该走到这里,因为应该在上层就路由到kingBaseExecutor了
            logger.error(f"kingbase数据源错误地使用了JDBC执行器")
            raise ValueError(
                f"kingbase数据库不支持JDBC连接方式。"
                f"系统检测到配置错误,Hive数据源应该使用kingBaseExecutor。"
                f"请检查作业配置或联系管理员"
            )

        elif db_type == 'doris':
            # Doris 使用 MySQL 协议
            driver = 'aiomysql'
            charset = config.get('charset', 'utf8mb4')
            if not port:
                port = 9030  # Doris FE 查询端口
            url = f"mysql+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}?charset={charset}"
            logger.info(f"Doris连接URL: {url.replace(encoded_password, '***')}")


        elif db_type == 'hive':
            # Hive 数据源不应该走到这里,因为应该在上层就路由到HiveExecutor了
            # 如果走到这里说明路由逻辑有问题
            logger.error(f"Hive数据源错误地使用了JDBC执行器")
            raise ValueError(
                f"Hive数据库不支持JDBC连接方式。"
                f"系统检测到配置错误,Hive数据源应该使用HiveExecutor。"
                f"请检查作业配置或联系管理员"
            )

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

        logger.info(f"成功创建{db_type}数据库连接池")
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
            use_transaction = work_config.get('useTransaction', False)
            params = work_config.get('params', {})

            # 参数验证
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

            # 获取数据库引擎(带连接池)
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
            # ✅ 顶层异常也要正确处理
            error_str = str(e)
            error_type = type(e).__name__

            logger.error(f"JDBC执行失败: {error_str}", exc_info=True)

            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": error_str,
                "error_type": error_type
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
                result = await conn.execute(text(sql), params)

                # ✅ 获取列名
                columns = list(result.keys())

                # ✅ 获取数据行
                rows = [dict(row._mapping) for row in result.fetchall()]

            elapsed = (datetime.now() - start_time).total_seconds()

            return {
                "success": True,
                "message": f"查询成功，返回 {len(rows)} 行数据",
                "data": {
                    "columns": columns,  # ✅ 添加列名
                    "rows": rows,
                    "rowCount": len(rows),
                    "elapsed": round(elapsed, 3)
                },
                "error": None
            }

        except Exception as e:
            # ✅ 确保查询失败也返回正确的状态
            error_str = str(e)
            error_type = type(e).__name__

            # 提供友好的错误消息
            if "syntax error" in error_str.lower():
                message = "SQL语法错误"
            elif "table" in error_str.lower() and "doesn't exist" in error_str.lower():
                message = "表不存在"
            elif "column" in error_str.lower() and "doesn't exist" in error_str.lower():
                message = "列不存在"
            elif "connection" in error_str.lower():
                message = "数据库连接失败"
            else:
                message = "查询失败"

            logger.error(f"{message}: {error_str}")

            return {
                "success": False,
                "message": message,
                "data": None,
                "error": error_str,
                "error_type": error_type
            }

    async def _execute_update(
            self,
            engine: AsyncEngine,
            sql: str,
            params: Dict,
            timeout: int,
            use_transaction: bool
    ) -> Dict[str, Any]:
        """执行更新(INSERT/UPDATE/DELETE) - 支持多条SQL语句"""
        try:
            start_time = datetime.now()

            # 分割多条SQL语句
            sql_statements = self._split_sql_statements(sql)

            if not sql_statements:
                return {
                    "success": False,
                    "message": "没有有效的SQL语句",
                    "data": None,
                    "error": "No valid SQL statements"
                }

            total_affected_rows = 0
            executed_count = 0
            failed_statement = None

            if use_transaction:
                # 使用事务执行多条语句
                async with engine.begin() as conn:
                    for i, stmt in enumerate(sql_statements):
                        failed_statement = stmt
                        result = await conn.execute(text(stmt), params)
                        total_affected_rows += result.rowcount
                        executed_count += 1
                        failed_statement = None
            else:
                # 不使用事务,逐条执行
                async with engine.connect() as conn:
                    for i, stmt in enumerate(sql_statements):
                        failed_statement = stmt
                        result = await conn.execute(text(stmt), params)
                        await conn.commit()
                        total_affected_rows += result.rowcount
                        executed_count += 1
                        failed_statement = None

            elapsed = (datetime.now() - start_time).total_seconds()

            return {
                "success": True,
                "message": f"执行成功,共执行 {executed_count} 条语句,影响 {total_affected_rows} 行",
                "data": {
                    "affectedRows": total_affected_rows,
                    "executedStatements": executed_count,
                    "elapsed": round(elapsed, 3)
                },
                "error": None
            }

        except Exception as e:
            # ✅ 关键修复: 正确返回失败状态
            error_str = str(e)
            error_type = type(e).__name__

            # 提供友好的错误消息
            if "already exists" in error_str.lower():
                message = "数据库对象已存在"
            elif "access denied" in error_str.lower() or "permission" in error_str.lower():
                message = "权限不足"
            elif "syntax error" in error_str.lower():
                message = "SQL语法错误"
            elif "connection" in error_str.lower():
                message = "数据库连接失败"
            elif "duplicate" in error_str.lower():
                message = "数据重复"
            else:
                message = "SQL执行失败"

            # 如果是多条SQL,指出哪条失败了
            if len(sql_statements) > 1 and failed_statement:
                message = f"{message}(第{executed_count + 1}/{len(sql_statements)}条)"
                logger.error(f"{message}: {error_str}")
                logger.error(f"失败的SQL: {failed_statement}")
            else:
                logger.error(f"{message}: {error_str}")

            return {
                "success": False,
                "message": message,
                "data": {
                    "executedStatements": executed_count,
                    "totalStatements": len(sql_statements),
                    "failedStatement": failed_statement if failed_statement else None
                },
                "error": error_str,
                "error_type": error_type
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
                    DataSource.is_active == True  # 只查询启用的数据源
                )
            )
            datasource = result.scalar_one_or_none()

            if not datasource:
                logger.error(f"数据源不存在或已禁用: {datasource_id}")
                return None

            # 从 connection_config 中解析配置
            connection_config = datasource.connection_config or {}

            # 构建执行器所需的配置格式
            config = {
                "type": datasource.source_type,  # mysql, postgresql 等
                "host": connection_config.get("host"),
                "port": connection_config.get("port", 3306),
                "database": connection_config.get("database"),
                "username": connection_config.get("username"),
                "password": connection_config.get("password"),
                "charset": connection_config.get("charset", "utf8mb4"),
                "poolSize": connection_config.get("poolSize", 5),
                "maxOverflow": connection_config.get("maxOverflow", 10),
                "poolTimeout": connection_config.get("poolTimeout", 30),
                "poolRecycle": connection_config.get("poolRecycle", 3600)
            }

            # 验证必需字段
            if not all([config["host"], config["database"], config["username"]]):
                logger.error(f"数据源配置不完整: {datasource_id}")
                return None

            return config

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

    def _split_sql_statements(self, sql: str) -> List[str]:
        """
        分割多条SQL语句

        Args:
            sql: 包含多条SQL的字符串

        Returns:
            List[str]: SQL语句列表
        """
        # 移除注释
        lines = []
        for line in sql.split('\n'):
            line = line.strip()
            # 跳过注释行和空行
            if line and not line.startswith('--') and not line.startswith('#'):
                lines.append(line)

        if not lines:
            return []

        # 合并所有行
        sql_content = ' '.join(lines)

        # 按分号分割
        statements = []
        for stmt in sql_content.split(';'):
            stmt = stmt.strip()
            if stmt:  # 只保留非空语句
                statements.append(stmt)

        return statements

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