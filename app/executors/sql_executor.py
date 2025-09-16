"""
SQL任务执行器
支持多种数据库的SQL任务执行
"""

import asyncio
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Any
from loguru import logger

from .base_executor import BaseExecutor, ExecutionContext, ExecutionResult, ExecutionStatus
from app.services.optimized_data_integration_service import get_optimized_data_integration_service
from app.utils.data_integration_clients import DatabaseClient


class SQLExecutor(BaseExecutor):
    """SQL任务执行器"""

    def __init__(self):
        super().__init__("SQLExecutor")
        self.integration_service = get_optimized_data_integration_service()

        # SQL执行配置
        self.max_rows_limit = 10000  # 默认最大返回行数
        self.query_timeout = 300  # 默认查询超时5分钟
        self.transaction_timeout = 600  # 事务超时10分钟

        logger.info("SQL执行器初始化完成")

    def validate_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证SQL任务配置

        Args:
            task_config: 任务配置

        Returns:
            bool: 配置是否有效
        """
        required_fields = ["task_id"]

        # 检查必要字段
        for field in required_fields:
            if field not in task_config:
                logger.error(f"SQL任务配置缺少必要字段: {field}")
                return False

        # 检查SQL语句或文件
        if not task_config.get("sql") and not task_config.get("sql_file"):
            logger.error("SQL任务必须指定sql语句或sql_file文件路径")
            return False

        # 检查数据库连接配置
        if not task_config.get("connection") and not task_config.get("data_source"):
            logger.error("SQL任务必须指定connection连接配置或data_source数据源名称")
            return False

        return True

    async def execute_task(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        执行SQL任务

        Args:
            task_config: SQL任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        start_time = datetime.now()
        task_id = task_config["task_id"]

        logger.info(f"开始执行SQL任务: {task_id}")

        try:
            # 1. 准备SQL语句
            sql_statements = await self._prepare_sql_statements(task_config, context)

            # 2. 获取数据库客户端
            client = await self._get_database_client(task_config, context)

            # 3. 执行SQL语句
            execution_results = []
            total_affected_rows = 0

            for i, sql in enumerate(sql_statements):
                logger.info(f"执行第 {i + 1}/{len(sql_statements)} 条SQL语句")
                logger.debug(f"SQL: {sql[:200]}...")  # 只记录前200字符

                sql_result = await self._execute_single_sql(
                    client, sql, task_config, context
                )

                execution_results.append(sql_result)

                if sql_result.get("affected_rows"):
                    total_affected_rows += sql_result["affected_rows"]

                # 检查是否有错误
                if not sql_result.get("success", False):
                    raise Exception(sql_result.get("error", "SQL执行失败"))

            # 4. 构建成功结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result = ExecutionResult(
                status=ExecutionStatus.SUCCESS,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                exit_code=0,
                task_result={
                    "executed_statements": len(sql_statements),
                    "total_affected_rows": total_affected_rows,
                    "execution_results": execution_results,
                    "database_type": task_config.get("connection", {}).get("type", "unknown")
                }
            )

            logger.info(f"SQL任务 {task_id} 执行成功，耗时 {duration:.2f}秒")
            return result

        except Exception as e:
            # 5. 构建失败结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_msg = str(e)
            logger.error(f"SQL任务 {task_id} 执行失败: {error_msg}")

            result = ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                exit_code=1,
                error_message=error_msg,
                exception_details={
                    "type": type(e).__name__,
                    "message": error_msg
                }
            )

            return result

    async def _prepare_sql_statements(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> List[str]:
        """
        准备SQL语句

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            List[str]: SQL语句列表
        """
        sql_statements = []

        # 从配置中获取SQL语句
        if task_config.get("sql"):
            sql_content = task_config["sql"]
        elif task_config.get("sql_file"):
            sql_file = task_config["sql_file"]

            # 支持相对路径
            if not os.path.isabs(sql_file):
                sql_file = os.path.join(context.working_directory or ".", sql_file)

            if not os.path.exists(sql_file):
                raise FileNotFoundError(f"SQL文件不存在: {sql_file}")

            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
        else:
            raise ValueError("未指定SQL语句或文件")

        # 处理SQL模板变量替换
        sql_content = self._process_sql_template(sql_content, context)

        # 分割多条SQL语句
        sql_statements = self._split_sql_statements(sql_content)

        if not sql_statements:
            raise ValueError("未找到有效的SQL语句")

        logger.info(f"准备了 {len(sql_statements)} 条SQL语句")
        return sql_statements

    def _process_sql_template(self, sql_content: str, context: ExecutionContext) -> str:
        """
        处理SQL模板变量

        Args:
            sql_content: SQL内容
            context: 执行上下文

        Returns:
            str: 处理后的SQL内容
        """
        # 支持常见的Airflow模板变量
        template_vars = {
            "ds": context.execution_date,  # 执行日期 YYYY-MM-DD
            "ds_nodash": context.execution_date.replace("-", ""),  # 无短横线日期
            "task_instance_key_str": f"{context.dag_id}__{context.task_id}__{context.execution_date}",
            "dag_id": context.dag_id,
            "task_id": context.task_id,
            "execution_date": context.execution_date,
            "retry_number": str(context.retry_number)
        }

        # 添加环境变量
        template_vars.update(context.environment_variables)

        # 简单的模板替换
        for key, value in template_vars.items():
            sql_content = sql_content.replace(f"{{{{ {key} }}}}", str(value))
            sql_content = sql_content.replace(f"${{{key}}}", str(value))

        return sql_content

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        分割多条SQL语句

        Args:
            sql_content: SQL内容

        Returns:
            List[str]: SQL语句列表
        """
        # 移除注释和空行
        lines = []
        for line in sql_content.split('\n'):
            line = line.strip()
            if line and not line.startswith('--') and not line.startswith('#'):
                lines.append(line)

        if not lines:
            return []

        # 按分号分割语句
        sql_content = ' '.join(lines)
        statements = []

        for stmt in sql_content.split(';'):
            stmt = stmt.strip()
            if stmt:
                statements.append(stmt)

        return statements

    async def _get_database_client(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> DatabaseClient:
        """
        获取数据库客户端

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            DatabaseClient: 数据库客户端
        """
        # 方式1: 使用已配置的数据源
        if task_config.get("data_source"):
            data_source_name = task_config["data_source"]
            client = self.integration_service.connection_manager.get_client(data_source_name)

            if not client:
                raise ValueError(f"数据源 {data_source_name} 不存在")

            return client

        # 方式2: 使用临时连接配置
        if task_config.get("connection"):
            conn_config = task_config["connection"]

            # 创建临时数据源名称
            temp_source_name = f"temp_sql_{context.task_id}_{context.execution_date.replace('-', '')}"

            # 添加临时数据源
            success = self.integration_service.connection_manager.add_client(
                temp_source_name,
                conn_config.get("type", "mysql"),
                conn_config
            )

            if not success:
                raise ValueError("创建临时数据库连接失败")

            client = self.integration_service.connection_manager.get_client(temp_source_name)

            if not client:
                raise ValueError("获取临时数据库客户端失败")

            return client

        raise ValueError("未指定有效的数据库连接配置")

    async def _execute_single_sql(
            self,
            client: DatabaseClient,
            sql: str,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        执行单条SQL语句

        Args:
            client: 数据库客户端
            sql: SQL语句
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            start_time = datetime.now()

            # 判断SQL类型
            sql_type = self._detect_sql_type(sql)

            if sql_type in ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN"]:
                # 查询类SQL
                result = await self._execute_query_sql(client, sql, task_config)
            else:
                # 更新类SQL (INSERT, UPDATE, DELETE, CREATE, etc.)
                result = await self._execute_update_sql(client, sql, task_config)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            result.update({
                "sql": sql,
                "sql_type": sql_type,
                "duration_seconds": duration,
                "success": True
            })

            logger.debug(f"SQL执行成功，耗时 {duration:.2f}秒")
            return result

        except Exception as e:
            error_msg = str(e)
            logger.error(f"SQL执行失败: {error_msg}")

            return {
                "sql": sql,
                "success": False,
                "error": error_msg,
                "error_type": type(e).__name__
            }

    async def _execute_query_sql(
            self,
            client: DatabaseClient,
            sql: str,
            task_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行查询类SQL
        """
        # 获取配置参数
        limit = task_config.get("limit", self.max_rows_limit)
        fetch_data = task_config.get("fetch_data", True)

        # 修改：检查client是否支持limit参数
        if fetch_data:
            try:
                # 先尝试不带limit参数执行
                rows = await client.execute_query(sql)

                # 如果获取到数据且设置了limit，手动截取
                if rows and limit:
                    rows = rows[:limit]

                return {
                    "row_count": len(rows) if rows else 0,
                    "data": rows[:100] if rows else [],  # 只返回前100行数据
                    "total_rows": len(rows) if rows else 0,
                    "limited": len(rows) >= limit if rows and limit else False
                }
            except TypeError as e:
                # 如果报类型错误，说明可能是limit参数问题，尝试其他方式
                logger.warning(f"execute_query不支持limit参数: {e}")
                rows = await client.execute_query(sql)
                if rows and limit:
                    rows = rows[:limit]
                return {
                    "row_count": len(rows) if rows else 0,
                    "data": rows[:100] if rows else [],
                    "total_rows": len(rows) if rows else 0,
                    "limited": len(rows) >= limit if rows and limit else False
                }
        else:
            # 只获取行数，不返回数据
            count_sql = f"SELECT COUNT(*) as count FROM ({sql}) as count_query"
            count_result = await client.execute_query(count_sql)

            return {
                "row_count": count_result[0]["count"] if count_result else 0,
                "data": [],
                "data_fetched": False
            }

    async def _execute_update_sql(
            self,
            client: DatabaseClient,
            sql: str,
            task_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        执行更新类SQL

        Args:
            client: 数据库客户端
            sql: SQL语句
            task_config: 任务配置

        Returns:
            Dict[str, Any]: 执行结果
        """
        # 这里需要根据具体的数据库客户端实现来执行
        # 由于当前的client接口主要是查询，我们需要扩展它

        try:
            # 尝试执行更新SQL
            if hasattr(client, 'execute_update'):
                affected_rows = await client.execute_update(sql)
            else:
                # 如果没有专门的更新方法，使用查询方法
                # 注意：这可能不会返回受影响的行数
                await client.execute_query(sql, limit=1)
                affected_rows = -1  # 表示未知

            return {
                "affected_rows": affected_rows,
                "operation_type": "update"
            }

        except Exception as e:
            # 重新抛出异常，让上层处理
            raise e

    def _detect_sql_type(self, sql: str) -> str:
        """
        检测SQL语句类型

        Args:
            sql: SQL语句

        Returns:
            str: SQL类型
        """
        sql_upper = sql.strip().upper()

        if sql_upper.startswith("SELECT"):
            return "SELECT"
        elif sql_upper.startswith("INSERT"):
            return "INSERT"
        elif sql_upper.startswith("UPDATE"):
            return "UPDATE"
        elif sql_upper.startswith("DELETE"):
            return "DELETE"
        elif sql_upper.startswith("CREATE"):
            return "CREATE"
        elif sql_upper.startswith("DROP"):
            return "DROP"
        elif sql_upper.startswith("ALTER"):
            return "ALTER"
        elif sql_upper.startswith("SHOW"):
            return "SHOW"
        elif sql_upper.startswith("DESCRIBE") or sql_upper.startswith("DESC"):
            return "DESCRIBE"
        elif sql_upper.startswith("EXPLAIN"):
            return "EXPLAIN"
        elif sql_upper.startswith("CALL"):
            return "CALL"
        else:
            return "UNKNOWN"

    def get_supported_task_types(self) -> List[str]:
        """
        获取支持的任务类型

        Returns:
            List[str]: 支持的任务类型
        """
        return ["sql", "query", "database"]

    async def validate_sql_syntax(self, sql: str, database_type: str = "mysql") -> Dict[str, Any]:
        """
        验证SQL语法（简单验证）

        Args:
            sql: SQL语句
            database_type: 数据库类型

        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            # 基本的SQL语法检查
            sql = sql.strip()

            if not sql:
                return {"valid": False, "error": "SQL语句为空"}

            if not sql.endswith(';'):
                # SQL可以不以分号结尾
                pass

            # 检查危险操作
            dangerous_keywords = ["DROP DATABASE", "DROP SCHEMA", "TRUNCATE"]
            sql_upper = sql.upper()

            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    return {
                        "valid": False,
                        "error": f"检测到危险操作: {keyword}",
                        "warning": True
                    }

            return {"valid": True, "sql_type": self._detect_sql_type(sql)}

        except Exception as e:
            return {"valid": False, "error": str(e)}

    async def explain_query(
            self,
            sql: str,
            client: DatabaseClient
    ) -> Dict[str, Any]:
        """
        获取查询执行计划

        Args:
            sql: SQL查询语句
            client: 数据库客户端

        Returns:
            Dict[str, Any]: 执行计划信息
        """
        try:
            explain_sql = f"EXPLAIN {sql}"
            explain_result = await client.execute_query(explain_sql)

            return {
                "success": True,
                "execution_plan": explain_result,
                "original_sql": sql
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "original_sql": sql
            }

    async def get_table_info(
            self,
            table_name: str,
            client: DatabaseClient,
            schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取表信息

        Args:
            table_name: 表名
            client: 数据库客户端
            schema: 模式名

        Returns:
            Dict[str, Any]: 表信息
        """
        try:
            # 获取表结构
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            describe_sql = f"DESCRIBE {full_table_name}"

            columns_info = await client.execute_query(describe_sql)

            # 获取表统计信息
            count_sql = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
            count_result = await client.execute_query(count_sql)
            row_count = count_result[0]["row_count"] if count_result else 0

            return {
                "success": True,
                "table_name": table_name,
                "schema": schema,
                "columns": columns_info,
                "row_count": row_count
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name,
                "schema": schema
            }

    async def dry_run(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        SQL任务的试运行（验证模式）

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 试运行结果
        """
        try:
            # 1. 验证配置
            if not self.validate_config(task_config):
                return {"success": False, "error": "任务配置无效"}

            # 2. 准备SQL语句
            sql_statements = await self._prepare_sql_statements(task_config, context)

            # 3. 获取数据库客户端
            client = await self._get_database_client(task_config, context)

            # 4. 验证每条SQL
            validation_results = []
            for sql in sql_statements:
                sql_type = self._detect_sql_type(sql)
                validation = await self.validate_sql_syntax(sql)

                result = {
                    "sql": sql[:100] + "..." if len(sql) > 100 else sql,
                    "sql_type": sql_type,
                    "validation": validation
                }

                # 对于查询类SQL，可以获取执行计划
                if sql_type == "SELECT":
                    explain_result = await self.explain_query(sql, client)
                    result["execution_plan"] = explain_result

                validation_results.append(result)

            return {
                "success": True,
                "total_statements": len(sql_statements),
                "validation_results": validation_results,
                "database_connection": "ok"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }


# 创建SQL执行器实例并注册
sql_executor = SQLExecutor()

# 注册到执行器注册表
from .base_executor import executor_registry

executor_registry.register("sql", sql_executor)
executor_registry.register("query", sql_executor)  # 别名
executor_registry.register("database", sql_executor)  # 别名