"""
Hive执行器 - 执行Hive SQL查询和执行
支持HiveServer2连接、查询、执行等操作
"""
from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
from datetime import datetime
import re

from app.services.executors.base_executor import JobExecutor


class HiveExecutor(JobExecutor):
    """Hive执行器"""

    def __init__(self):
        super().__init__("hive_executor")

    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行Hive SQL"""
        try:
            datasource_id = work_config.get('dataSourceId')
            sql = work_config.get('sql')
            sql_type = work_config.get('type', 'query')  # query, execute
            timeout = work_config.get('timeout', 300)
            params = work_config.get('params', {})

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

            # 执行SQL
            if sql_type == 'query':
                result = await self._execute_query(
                    datasource_config, sql, timeout
                )
            else:
                result = await self._execute_update(
                    datasource_config, sql, timeout
                )

            return result

        except Exception as e:
            logger.error(f"Hive执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def _execute_query(
            self,
            config: Dict[str, Any],
            sql: str,
            timeout: int
    ) -> Dict[str, Any]:
        """执行Hive查询"""
        try:
            from pyhive import hive
            from TCLIService.ttypes import TOperationState
            import asyncio

            start_time = datetime.now()

            # 移除SQL末尾的分号(Hive不支持)
            sql = sql.strip()
            if sql.endswith(';'):
                sql = sql[:-1].strip()
                logger.info(f"已移除SQL末尾的分号: {sql}")

            # 在线程池中执行同步的Hive操作
            def sync_query():
                # 连接HiveServer2
                conn = hive.Connection(
                    host=config['host'],
                    port=config['port'],
                    username=config['username'],
                    database=config.get('database', 'default'),
                    auth=config.get('auth', 'CUSTOM'),
                    password=config.get('password')
                )

                cursor = conn.cursor()

                try:
                    # 执行查询
                    cursor.execute(sql)

                    # 获取列名
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []

                    # 获取数据
                    rows = []
                    for row in cursor.fetchall():
                        row_dict = {}
                        for i, col in enumerate(columns):
                            value = row[i]
                            # 处理特殊类型
                            if value is None:
                                row_dict[col] = None
                            elif isinstance(value, (datetime,)):
                                row_dict[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                row_dict[col] = value
                        rows.append(row_dict)

                    return {
                        "columns": columns,
                        "rows": rows,
                        "rowCount": len(rows)
                    }

                finally:
                    cursor.close()
                    conn.close()

            # 在线程池中执行
            loop = asyncio.get_event_loop()
            result_data = await loop.run_in_executor(None, sync_query)

            elapsed = (datetime.now() - start_time).total_seconds()

            return {
                "success": True,
                "message": f"查询成功，返回 {result_data['rowCount']} 行",
                "data": {
                    "columns": result_data['columns'],
                    "rows": result_data['rows'],
                    "rowCount": result_data['rowCount'],
                    "elapsed": round(elapsed, 3)
                },
                "error": None
            }

        except Exception as e:
            logger.error(f"Hive查询失败: {e}")
            return {
                "success": False,
                "message": "查询失败",
                "data": None,
                "error": str(e)
            }

    async def _execute_update(
            self,
            config: Dict[str, Any],
            sql: str,
            timeout: int
    ) -> Dict[str, Any]:
        """执行Hive更新操作（DDL/DML）- 支持多条SQL语句"""
        try:
            from pyhive import hive
            import asyncio

            start_time = datetime.now()

            # 分割多条SQL语句
            sql_statements = self._split_sql_statements(sql)

            # 移除每条语句末尾的分号(Hive不支持)
            sql_statements = [stmt.strip().rstrip(';') for stmt in sql_statements]

            if not sql_statements:
                return {
                    "success": False,
                    "message": "没有有效的SQL语句",
                    "data": None,
                    "error": "No valid SQL statements"
                }

            # 在线程池中执行同步的Hive操作
            def sync_execute():
                # 连接HiveServer2
                conn = hive.Connection(
                    host=config['host'],
                    port=config['port'],
                    username=config['username'],
                    database=config.get('database', 'default'),
                    auth=config.get('auth', 'CUSTOM'),
                    password=config.get('password')
                )

                cursor = conn.cursor()
                executed_count = 0
                total_affected_rows = 0

                try:
                    for stmt in sql_statements:
                        # 执行语句
                        cursor.execute(stmt)
                        executed_count += 1

                        # 尝试获取影响行数(某些DDL语句可能没有rowcount)
                        if hasattr(cursor, 'rowcount') and cursor.rowcount is not None:
                            total_affected_rows += cursor.rowcount

                    return {
                        "executedStatements": executed_count,
                        "affectedRows": total_affected_rows
                    }

                finally:
                    cursor.close()
                    conn.close()

            # 在线程池中执行
            loop = asyncio.get_event_loop()
            result_data = await loop.run_in_executor(None, sync_execute)

            elapsed = (datetime.now() - start_time).total_seconds()

            return {
                "success": True,
                "message": f"执行成功，共执行 {result_data['executedStatements']} 条语句",
                "data": {
                    "executedStatements": result_data['executedStatements'],
                    "affectedRows": result_data['affectedRows'],
                    "elapsed": round(elapsed, 3)
                },
                "error": None
            }

        except Exception as e:
            logger.error(f"Hive执行失败: {e}")
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
                    DataSource.is_active == True
                )
            )
            datasource = result.scalar_one_or_none()

            if not datasource:
                logger.error(f"数据源不存在或已禁用: {datasource_id}")
                return None

            # 从 connection_config 中解析配置
            connection_config = datasource.connection_config or {}

            # 构建Hive执行器所需的配置格式
            config = {
                "type": "hive",
                "host": connection_config.get("host"),
                "port": connection_config.get("port", 10000),
                "database": connection_config.get("database", "default"),
                "username": connection_config.get("username"),
                "password": connection_config.get("password", ""),
                "auth": connection_config.get("auth", "CUSTOM")
            }

            # 验证必需字段
            if not all([config["host"], config["username"]]):
                logger.error(f"Hive数据源配置不完整: {datasource_id}")
                return None

            return config

        except Exception as e:
            logger.error(f"获取Hive数据源配置失败: {e}")
            return None

    def _replace_variables(self, text: str, context: Dict) -> str:
        """替换SQL中的变量 ${varName}"""
        if not text or not context:
            return text

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
            if stmt:  # 只保留非空语句,且不包含分号
                statements.append(stmt)

        return statements

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


# 导出
__all__ = ['HiveExecutor']