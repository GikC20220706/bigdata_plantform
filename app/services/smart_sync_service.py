# app/services/smart_sync_service.py
"""
智能数据同步服务 - 支持拖拽式操作和自动建表
"""

import asyncio
import json
import tempfile
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from loguru import logger

from app.services.datax_service import EnhancedSyncService
from app.services.optimized_data_integration_service import get_optimized_data_integration_service
from app.utils.response import create_response


class SmartSyncService:
    """智能数据同步服务"""

    def __init__(self):
        self.datax_service = EnhancedSyncService()
        self.integration_service = get_optimized_data_integration_service()

    async def analyze_sync_plan(self, sync_request: Dict[str, Any]) -> Dict[str, Any]:
        """分析同步计划，自动生成同步策略"""
        try:
            source_name = sync_request['source_name']
            target_name = sync_request['target_name']
            tables = sync_request['tables']
            sync_mode = sync_request.get('sync_mode', 'single')

            # 获取源和目标数据源配置
            source_config = await self._get_data_source_config(source_name)
            target_config = await self._get_data_source_config(target_name)

            if not source_config or not target_config:
                return {
                    "success": False,
                    "error": "数据源配置不存在"
                }

            # 保存源数据库类型，供字段长度分析使用
            self._current_source_type = source_config.get('type', 'mysql')

            # 分析每个表的同步计划
            sync_plans = []
            total_estimated_time = 0
            total_estimated_rows = 0

            for table_info in tables:
                source_table = table_info['source_table']
                target_table = table_info['target_table']

                logger.info(f"分析表: {source_table} -> {target_table}")

                # 保存当前表信息
                self._current_source_name = source_name
                self._current_table_name = source_table
                self._current_target_type = target_config['type']

                # 获取源表元数据
                source_metadata = await self.integration_service.get_table_metadata(
                    source_name, source_table
                )

                if not source_metadata.get('success'):
                    logger.error(f"获取源表 {source_table} 元数据失败")
                    continue

                table_meta = source_metadata['metadata']

                # 检查目标表是否存在
                target_exists = await self._check_target_table_exists(
                    target_name, target_table
                )

                # 生成同步策略
                strategy = await self._generate_sync_strategy(
                    source_config, target_config, table_meta, target_exists
                )

                # 估算同步时间和数据量
                estimated_rows = table_meta.get('statistics', {}).get('row_count', 0)
                estimated_time = self._estimate_sync_time(estimated_rows, source_config['type'], target_config['type'])

                sync_plan = {
                    "source_table": source_table,
                    "target_table": target_table,
                    "estimated_rows": estimated_rows,
                    "estimated_time_minutes": estimated_time,
                    "target_exists": target_exists,
                    "strategy": strategy,
                    "schema_mapping": await self._generate_schema_mapping(table_meta, target_config['type']),
                    "warnings": self._check_compatibility_warnings(table_meta, source_config, target_config)
                }

                sync_plans.append(sync_plan)
                total_estimated_time += estimated_time
                total_estimated_rows += estimated_rows

            return {
                "success": True,
                "sync_mode": sync_mode,
                "source_name": source_name,
                "target_name": target_name,
                "total_tables": len(sync_plans),
                "total_estimated_rows": total_estimated_rows,
                "total_estimated_time_minutes": total_estimated_time,
                "sync_plans": sync_plans,
                "global_strategy": self._determine_global_strategy(sync_mode, sync_plans),
                "recommended_parallel_jobs": self._recommend_parallel_jobs(total_estimated_rows),
                "analysis_time": datetime.now()
            }

        except Exception as e:
            logger.error(f"分析同步计划失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def execute_smart_sync(self, sync_plan: Dict[str, Any]) -> Dict[str, Any]:
        """执行智能同步"""
        try:
            sync_id = f"smart_sync_{int(datetime.now().timestamp())}"
            logger.info(f"开始执行智能同步: {sync_id}")

            # 预检查
            precheck_result = await self._precheck_sync_conditions(sync_plan)
            if not precheck_result['success']:
                return precheck_result

            # 创建目标表（如果需要）
            table_creation_results = await self._create_target_tables(sync_plan)

            # 执行数据同步
            sync_results = []
            successful_syncs = 0
            failed_syncs = 0

            for plan in sync_plan['sync_plans']:
                try:
                    logger.info(f"开始同步表: {plan['source_table']} -> {plan['target_table']}")

                    # 生成DataX配置
                    datax_config = await self._generate_datax_config(sync_plan, plan)
                    logger.info(f"DataX配置生成成功: {datax_config}")

                    # 执行同步
                    logger.info(f"开始执行DataX同步任务...")
                    sync_result = await self.datax_service.execute_sync_task(datax_config)
                    logger.info(f"DataX执行结果: {sync_result}")

                    if sync_result.get('success'):
                        successful_syncs += 1
                        logger.info(f"表 {plan['source_table']} 同步成功")
                        # 验证数据完整性
                        verification = await self._verify_sync_integrity(sync_plan, plan)
                        sync_result['verification'] = verification
                        logger.info(f"数据验证结果: {verification}")
                    else:
                        failed_syncs += 1
                        error_msg = sync_result.get('error', '未知错误')
                        logger.error(f"表 {plan['source_table']} 同步失败: {error_msg}")

                    sync_results.append({
                        "table": plan['source_table'],
                        "target_table": plan['target_table'],
                        "result": sync_result
                    })

                except Exception as e:
                    failed_syncs += 1
                    error_msg = f"同步异常: {str(e)}"
                    logger.error(f"表 {plan['source_table']} 同步异常: {error_msg}")
                    logger.error(f"异常详情: {traceback.format_exc()}")

                    sync_results.append({
                        "table": plan['source_table'],
                        "target_table": plan['target_table'],
                        "result": {
                            "success": False,
                            "error": error_msg,
                            "error_type": "exception",
                            "traceback": traceback.format_exc()
                        }
                    })

            # 生成同步报告
            sync_report = {
                "sync_id": sync_id,
                "success": failed_syncs == 0,
                "total_tables": len(sync_plan['sync_plans']),
                "successful_syncs": successful_syncs,
                "failed_syncs": failed_syncs,
                "sync_results": sync_results,
                "table_creation_results": table_creation_results,
                "execution_time": datetime.now(),
                "summary": self._generate_sync_summary(sync_results)
            }

            return sync_report

        except Exception as e:
            logger.error(f"执行智能同步失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def _create_target_tables(self, sync_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """自动创建目标表"""
        creation_results = []
        target_name = sync_plan['target_name']

        for plan in sync_plan['sync_plans']:
            if not plan['target_exists']:
                try:
                    # 生成建表SQL
                    create_sql = await self._generate_create_table_sql(
                        plan['schema_mapping'],
                        plan['target_table'],
                        sync_plan['target_name']
                    )

                    # 执行建表
                    result = await self._execute_create_table(target_name, create_sql)

                    creation_results.append({
                        "table": plan['target_table'],
                        "success": result['success'],
                        "sql": create_sql,
                        "message": result.get('message', '')
                    })

                except Exception as e:
                    creation_results.append({
                        "table": plan['target_table'],
                        "success": False,
                        "error": str(e)
                    })

        return creation_results

    async def _generate_create_table_sql(self, schema_mapping: Dict[str, Any],
                                         table_name: str, target_source: str) -> str:
        """生成建表SQL"""
        target_config = await self._get_data_source_config(target_source)
        target_type = target_config['type'].lower()

        columns = []
        for col in schema_mapping['columns']:
            col_name = col['name']
            col_type = col['target_type']
            nullable = "NULL" if col.get('nullable', True) else "NOT NULL"

            # 根据数据库类型使用正确的引号格式
            if target_type == 'kingbase':
                columns.append(f'    "{col_name}" {col_type} {nullable}')
            elif target_type == 'mysql':
                columns.append(f'    `{col_name}` {col_type} {nullable}')
            elif target_type == 'doris':
                columns.append(f'    `{col_name}` {col_type} {nullable}')
            else:
                # PostgreSQL, Oracle等使用双引号或不用引号
                columns.append(f'    "{col_name}" {col_type} {nullable}')
        newline = '\n'
        column_definitions = f',{newline}'.join(columns)

        if target_type == 'mysql':
            sql = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
        {column_definitions}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""".strip()

        elif target_type == 'doris':
            # Doris建表语句
            first_column = schema_mapping['columns'][0]['name']
            sql = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
        {column_definitions}
        ) ENGINE=OLAP
        DUPLICATE KEY(`{first_column}`)
        DISTRIBUTED BY HASH(`{first_column}`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );""".strip()

        elif target_type == 'postgresql':
            sql = f"""CREATE TABLE IF NOT EXISTS "{table_name}" (
        {column_definitions}
        );""".strip()

        elif target_type == 'kingbase':
            sql = f'''CREATE TABLE IF NOT EXISTS "{table_name}" (
            {column_definitions}
            );'''.strip()

        elif target_type == 'hive':
            sql = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
        {column_definitions}
        ) 
        STORED AS TEXTFILE
        LOCATION '/user/hive/warehouse/{table_name}';""".strip()

        elif target_type == 'oracle':
            sql = f"""CREATE TABLE "{table_name}" (
        {column_definitions}
        );""".strip()

        else:
            # 通用SQL
            sql = f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
        {column_definitions}
        );""".strip()

        logger.info(f"生成{target_type}建表SQL: {sql}")
        return sql

    async def _generate_schema_mapping(self, table_metadata: Dict[str, Any], target_type: str) -> Dict[str, Any]:
        """生成schema映射"""
        source_columns = table_metadata.get('schema', {}).get('columns', [])

        if not source_columns:
            logger.error("源表没有字段信息")
            raise ValueError("源表字段信息缺失")

        mapped_columns = []
        total_columns = len(source_columns)

        # 如果是MySQL目标且字段很多，分析实际字段长度
        field_lengths = {}
        if target_type.lower() == 'mysql' and total_columns > 30:
            logger.info(f"MySQL目标表有{total_columns}个字段，开始智能长度分析...")
            try:
                # 获取当前处理的源表信息
                source_name = getattr(self, '_current_source_name', None)
                table_name = getattr(self, '_current_table_name', None)

                if source_name and table_name:
                    # 传递字段信息进行长度分析
                    field_lengths = await self._analyze_field_lengths(source_name, table_name, source_columns)
                else:
                    logger.warning("无法获取源表信息，跳过智能长度分析")
            except Exception as e:
                logger.warning(f"智能长度分析失败: {e}")

        for i, col in enumerate(source_columns):
            source_type = col.get('data_type', 'VARCHAR').upper()
            col_name = col.get('name', col.get('column_name', f'column_{i}'))

            if not col_name:
                col_name = f'column_{i}'

            # 使用智能长度或默认映射
            if col_name in field_lengths and target_type.lower() == 'mysql':
                recommended_length = field_lengths[col_name]
                if recommended_length == 'TEXT':
                    target_col_type = 'TEXT'
                else:
                    target_col_type = f'VARCHAR({recommended_length})'
                logger.info(f"字段 {col_name} 使用智能长度: {target_col_type}")
            else:
                target_col_type = self._map_data_type(source_type, target_type)

                # 如果没有智能分析，但字段很多，适当减少长度
                if (target_type.lower() == 'mysql' and 'VARCHAR(255)' in target_col_type and
                        total_columns > 50 and col_name not in field_lengths):
                    target_col_type = 'VARCHAR(120)'  # 适度减少
                    logger.info(f"字段 {col_name} 使用默认优化长度: {target_col_type}")

            mapped_columns.append({
                "name": col_name,
                "source_type": source_type,
                "target_type": target_col_type,
                "nullable": col.get('is_nullable', True),
                "length": col.get('character_maximum_length'),
                "precision": col.get('numeric_precision'),
                "scale": col.get('numeric_scale')
            })

        logger.info(f"字段映射生成完成，共 {len(mapped_columns)} 个字段")

        return {
            "columns": mapped_columns,
            "mapping_strategy": "intelligent" if field_lengths else "auto",
            "total_columns": len(mapped_columns),
            "analyzed_fields": len(field_lengths)
        }

    def _map_data_type(self, source_type: str, target_type: str) -> str:
        """数据类型映射 - 扩展版支持多种数据库"""

        # 完整的数据类型映射表
        type_mappings = {
            # MySQL 作为源数据库的映射
            'mysql': {
                'mysql': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'DATETIME': 'DATETIME', 'TIMESTAMP': 'TIMESTAMP',
                    'DECIMAL': 'DECIMAL', 'FLOAT': 'FLOAT', 'DOUBLE': 'DOUBLE',
                    'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT', 'MEDIUMINT': 'MEDIUMINT',
                    'CHAR': 'CHAR', 'LONGTEXT': 'LONGTEXT', 'MEDIUMTEXT': 'MEDIUMTEXT',
                    'DATE': 'DATE', 'TIME': 'TIME', 'YEAR': 'YEAR',
                    'BINARY': 'BINARY', 'VARBINARY': 'VARBINARY', 'BLOB': 'BLOB'
                },
                'postgresql': {
                    'INT': 'INTEGER', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'DATETIME': 'TIMESTAMP', 'TIMESTAMP': 'TIMESTAMP',
                    'DECIMAL': 'NUMERIC', 'FLOAT': 'REAL', 'DOUBLE': 'DOUBLE PRECISION',
                    'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT', 'MEDIUMINT': 'INTEGER',
                    'CHAR': 'CHAR', 'LONGTEXT': 'TEXT', 'MEDIUMTEXT': 'TEXT',
                    'DATE': 'DATE', 'TIME': 'TIME', 'YEAR': 'INTEGER',
                    'BINARY': 'BYTEA', 'VARBINARY': 'BYTEA', 'BLOB': 'BYTEA'
                },
                'hive': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'STRING',
                    'TEXT': 'STRING', 'DATETIME': 'TIMESTAMP', 'TIMESTAMP': 'TIMESTAMP',
                    'DECIMAL': 'DECIMAL', 'FLOAT': 'FLOAT', 'DOUBLE': 'DOUBLE',
                    'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT', 'MEDIUMINT': 'INT',
                    'CHAR': 'CHAR', 'LONGTEXT': 'STRING', 'MEDIUMTEXT': 'STRING',
                    'DATE': 'DATE', 'TIME': 'STRING', 'YEAR': 'INT',
                    'BINARY': 'BINARY', 'VARBINARY': 'BINARY', 'BLOB': 'BINARY'
                },
                'doris': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'STRING', 'DATETIME': 'DATETIME', 'TIMESTAMP': 'DATETIME',
                    'DECIMAL': 'DECIMAL', 'FLOAT': 'FLOAT', 'DOUBLE': 'DOUBLE',
                    'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT', 'MEDIUMINT': 'INT',
                    'CHAR': 'CHAR', 'LONGTEXT': 'STRING', 'MEDIUMTEXT': 'STRING',
                    'DATE': 'DATE', 'TIME': 'STRING', 'YEAR': 'INT',
                    'BINARY': 'STRING', 'VARBINARY': 'STRING', 'BLOB': 'STRING'
                },
                'kingbase': {
                    'INT': 'INTEGER', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'DATETIME': 'TIMESTAMP', 'TIMESTAMP': 'TIMESTAMP',
                    'DECIMAL': 'NUMERIC', 'FLOAT': 'REAL', 'DOUBLE': 'DOUBLE PRECISION',
                    'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT', 'MEDIUMINT': 'INTEGER',
                    'CHAR': 'CHAR', 'LONGTEXT': 'TEXT', 'MEDIUMTEXT': 'TEXT',
                    'DATE': 'DATE', 'TIME': 'TIME', 'YEAR': 'INTEGER',
                    'BINARY': 'BYTEA', 'VARBINARY': 'BYTEA', 'BLOB': 'BYTEA'
                }
            },

            # KingBase 作为源数据库的映射
            'kingbase': {
                'mysql': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'TIMESTAMP': 'DATETIME', 'TIMESTAMPTZ': 'TIMESTAMP',
                    'NUMERIC': 'DECIMAL', 'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE',
                    'SMALLINT': 'SMALLINT', 'CHAR': 'CHAR', 'BOOLEAN': 'TINYINT',
                    'DATE': 'DATE', 'TIME': 'TIME', 'INTERVAL': 'VARCHAR(50)',
                    'BYTEA': 'BLOB', 'UUID': 'VARCHAR(36)', 'JSON': 'JSON',
                    'JSONB': 'JSON', 'ARRAY': 'TEXT', 'SERIAL': 'INT AUTO_INCREMENT'
                },
                'hive': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'STRING',
                    'TEXT': 'STRING', 'TIMESTAMP': 'TIMESTAMP', 'TIMESTAMPTZ': 'TIMESTAMP',
                    'NUMERIC': 'DECIMAL', 'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE',
                    'SMALLINT': 'SMALLINT', 'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN',
                    'DATE': 'DATE', 'TIME': 'STRING', 'INTERVAL': 'STRING',
                    'BYTEA': 'BINARY', 'UUID': 'STRING', 'JSON': 'STRING',
                    'JSONB': 'STRING', 'ARRAY': 'ARRAY<STRING>', 'SERIAL': 'INT'
                },
                'doris': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'STRING', 'TIMESTAMP': 'DATETIME', 'TIMESTAMPTZ': 'DATETIME',
                    'NUMERIC': 'DECIMAL', 'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE',
                    'SMALLINT': 'SMALLINT', 'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN',
                    'DATE': 'DATE', 'TIME': 'STRING', 'INTERVAL': 'STRING',
                    'BYTEA': 'STRING', 'UUID': 'VARCHAR(36)', 'JSON': 'JSON',
                    'JSONB': 'JSON', 'ARRAY': 'STRING', 'SERIAL': 'INT'
                },
                'kingbase': {
                    'INTEGER': 'INTEGER', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'TIMESTAMP': 'TIMESTAMP', 'TIMESTAMPTZ': 'TIMESTAMPTZ',
                    'NUMERIC': 'NUMERIC', 'REAL': 'REAL', 'DOUBLE PRECISION': 'DOUBLE PRECISION',
                    'SMALLINT': 'SMALLINT', 'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN',
                    'DATE': 'DATE', 'TIME': 'TIME', 'INTERVAL': 'INTERVAL',
                    'BYTEA': 'BYTEA', 'UUID': 'UUID', 'JSON': 'JSON',
                    'JSONB': 'JSONB', 'ARRAY': 'ARRAY', 'SERIAL': 'SERIAL'
                }
            },

            # Oracle 作为源数据库的映射
            'oracle': {
                'mysql': {
                    'NUMBER': 'DECIMAL', 'VARCHAR2': 'VARCHAR', 'CHAR': 'CHAR',
                    'CLOB': 'LONGTEXT', 'BLOB': 'LONGBLOB', 'DATE': 'DATETIME',
                    'TIMESTAMP': 'TIMESTAMP', 'RAW': 'VARBINARY', 'LONG': 'LONGTEXT',
                    'NVARCHAR2': 'VARCHAR', 'NCHAR': 'CHAR', 'NCLOB': 'LONGTEXT'
                },
                'hive': {
                    'NUMBER': 'DECIMAL', 'VARCHAR2': 'STRING', 'CHAR': 'CHAR',
                    'CLOB': 'STRING', 'BLOB': 'BINARY', 'DATE': 'TIMESTAMP',
                    'TIMESTAMP': 'TIMESTAMP', 'RAW': 'BINARY', 'LONG': 'STRING',
                    'NVARCHAR2': 'STRING', 'NCHAR': 'CHAR', 'NCLOB': 'STRING'
                },
                'doris': {
                    'NUMBER': 'DECIMAL', 'VARCHAR2': 'VARCHAR', 'CHAR': 'CHAR',
                    'CLOB': 'STRING', 'BLOB': 'STRING', 'DATE': 'DATETIME',
                    'TIMESTAMP': 'DATETIME', 'RAW': 'STRING', 'LONG': 'STRING',
                    'NVARCHAR2': 'VARCHAR', 'NCHAR': 'CHAR', 'NCLOB': 'STRING'
                },
                'kingbase': {
                    'NUMBER': 'NUMERIC', 'VARCHAR2': 'VARCHAR', 'CHAR': 'CHAR',
                    'CLOB': 'TEXT', 'BLOB': 'BYTEA', 'DATE': 'TIMESTAMP',
                    'TIMESTAMP': 'TIMESTAMP', 'RAW': 'BYTEA', 'LONG': 'TEXT',
                    'NVARCHAR2': 'VARCHAR', 'NCHAR': 'CHAR', 'NCLOB': 'TEXT'
                }
            },

            # PostgreSQL 作为源数据库的映射
            'postgresql': {
                'mysql': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'TIMESTAMP': 'DATETIME', 'NUMERIC': 'DECIMAL',
                    'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE', 'SMALLINT': 'SMALLINT',
                    'CHAR': 'CHAR', 'BOOLEAN': 'TINYINT', 'DATE': 'DATE',
                    'TIME': 'TIME', 'BYTEA': 'BLOB', 'UUID': 'VARCHAR(36)',
                    'JSON': 'JSON', 'JSONB': 'JSON'
                },
                'hive': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'STRING',
                    'TEXT': 'STRING', 'TIMESTAMP': 'TIMESTAMP', 'NUMERIC': 'DECIMAL',
                    'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE', 'SMALLINT': 'SMALLINT',
                    'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN', 'DATE': 'DATE',
                    'TIME': 'STRING', 'BYTEA': 'BINARY', 'UUID': 'STRING',
                    'JSON': 'STRING', 'JSONB': 'STRING'
                },
                'doris': {
                    'INTEGER': 'INT', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'STRING', 'TIMESTAMP': 'DATETIME', 'NUMERIC': 'DECIMAL',
                    'REAL': 'FLOAT', 'DOUBLE PRECISION': 'DOUBLE', 'SMALLINT': 'SMALLINT',
                    'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN', 'DATE': 'DATE',
                    'TIME': 'STRING', 'BYTEA': 'STRING', 'UUID': 'VARCHAR(36)',
                    'JSON': 'JSON', 'JSONB': 'JSON'
                },
                'kingbase': {
                    'INTEGER': 'INTEGER', 'BIGINT': 'BIGINT', 'VARCHAR': 'VARCHAR',
                    'TEXT': 'TEXT', 'TIMESTAMP': 'TIMESTAMP', 'NUMERIC': 'NUMERIC',
                    'REAL': 'REAL', 'DOUBLE PRECISION': 'DOUBLE PRECISION', 'SMALLINT': 'SMALLINT',
                    'CHAR': 'CHAR', 'BOOLEAN': 'BOOLEAN', 'DATE': 'DATE',
                    'TIME': 'TIME', 'BYTEA': 'BYTEA', 'UUID': 'UUID',
                    'JSON': 'JSON', 'JSONB': 'JSONB'
                }
            },

            # Hive 作为源数据库的映射
            'hive': {
                'mysql': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'STRING': 'TEXT',
                    'DOUBLE': 'DOUBLE', 'FLOAT': 'FLOAT', 'DECIMAL': 'DECIMAL',
                    'BOOLEAN': 'TINYINT', 'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT',
                    'TIMESTAMP': 'TIMESTAMP', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'VARCHAR', 'BINARY': 'BLOB', 'ARRAY': 'JSON',
                    'MAP': 'JSON', 'STRUCT': 'JSON'
                },
                'kingbase': {
                    'INT': 'INTEGER', 'BIGINT': 'BIGINT', 'STRING': 'TEXT',
                    'DOUBLE': 'DOUBLE PRECISION', 'FLOAT': 'REAL', 'DECIMAL': 'NUMERIC',
                    'BOOLEAN': 'BOOLEAN', 'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT',
                    'TIMESTAMP': 'TIMESTAMP', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'VARCHAR', 'BINARY': 'BYTEA', 'ARRAY': 'JSONB',
                    'MAP': 'JSONB', 'STRUCT': 'JSONB'
                },
                'doris': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'STRING': 'STRING',
                    'DOUBLE': 'DOUBLE', 'FLOAT': 'FLOAT', 'DECIMAL': 'DECIMAL',
                    'BOOLEAN': 'BOOLEAN', 'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT',
                    'TIMESTAMP': 'DATETIME', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'VARCHAR', 'BINARY': 'STRING', 'ARRAY': 'JSON',
                    'MAP': 'JSON', 'STRUCT': 'JSON'
                }
            },

            # Doris 作为源数据库的映射
            'doris': {
                'mysql': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'STRING': 'TEXT',
                    'DOUBLE': 'DOUBLE', 'FLOAT': 'FLOAT', 'DECIMAL': 'DECIMAL',
                    'BOOLEAN': 'TINYINT', 'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT',
                    'DATETIME': 'DATETIME', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'VARCHAR', 'JSON': 'JSON'
                },
                'hive': {
                    'INT': 'INT', 'BIGINT': 'BIGINT', 'STRING': 'STRING',
                    'DOUBLE': 'DOUBLE', 'FLOAT': 'FLOAT', 'DECIMAL': 'DECIMAL',
                    'BOOLEAN': 'BOOLEAN', 'TINYINT': 'TINYINT', 'SMALLINT': 'SMALLINT',
                    'DATETIME': 'TIMESTAMP', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'STRING', 'JSON': 'STRING'
                },
                'kingbase': {
                    'INT': 'INTEGER', 'BIGINT': 'BIGINT', 'STRING': 'TEXT',
                    'DOUBLE': 'DOUBLE PRECISION', 'FLOAT': 'REAL', 'DECIMAL': 'NUMERIC',
                    'BOOLEAN': 'BOOLEAN', 'TINYINT': 'SMALLINT', 'SMALLINT': 'SMALLINT',
                    'DATETIME': 'TIMESTAMP', 'DATE': 'DATE', 'CHAR': 'CHAR',
                    'VARCHAR': 'VARCHAR', 'JSON': 'JSONB'
                }
            }
        }

        # 获取源数据库类型的映射
        source_mappings = type_mappings.get(source_type.lower(), {})
        target_mappings = source_mappings.get(target_type.lower(), {})

        # 返回映射后的类型，如果找不到则返回默认类型
        mapped_type = target_mappings.get(source_type.upper())

        if mapped_type:
            return mapped_type
        else:
            # 默认映射策略
            default_mappings = {
                'mysql': 'VARCHAR(255)',
                'postgresql': 'VARCHAR(255)',
                'kingbase': 'VARCHAR(255)',
                'hive': 'STRING',
                'doris': 'VARCHAR(255)',
                'oracle': 'VARCHAR2(255)'
            }
            return default_mappings.get(target_type.lower(), 'VARCHAR(255)')

    async def _generate_datax_config(self, sync_plan: Dict[str, Any], table_plan: Dict[str, Any]) -> Dict[str, Any]:
        """生成DataX配置"""
        try:
            source_config = await self._get_data_source_config(sync_plan['source_name'])
            target_config = await self._get_data_source_config(sync_plan['target_name'])

            if not source_config:
                raise ValueError(f"无法获取源数据源配置: {sync_plan['source_name']}")
            if not target_config:
                raise ValueError(f"无法获取目标数据源配置: {sync_plan['target_name']}")

            # 保存当前处理的表信息，供其他方法使用
            self._current_source_name = sync_plan['source_name']
            self._current_table_name = table_plan['source_table']
            self._current_target_type = target_config['type']

            # 获取字段映射信息
            schema_mapping = table_plan.get('schema_mapping', {})
            columns_mapping = schema_mapping.get('columns', [])

            if not columns_mapping:
                logger.error(f"表 {table_plan['source_table']} 没有字段映射信息")
                raise ValueError(f"表 {table_plan['source_table']} 缺少字段映射信息")

            # 生成明确的字段列表
            source_columns = [col['name'] for col in columns_mapping]
            target_columns = [col['name'] for col in columns_mapping]

            if not source_columns:
                raise ValueError(f"源表 {table_plan['source_table']} 字段列表为空")
            if not target_columns:
                raise ValueError(f"目标表 {table_plan['target_table']} 字段列表为空")

            logger.info(f"字段映射: 源字段({len(source_columns)})={source_columns[:5]}...")
            logger.info(f"字段映射: 目标字段({len(target_columns)})={target_columns[:5]}...")

            # 确定写入模式
            write_mode = self._determine_write_mode(table_plan, sync_plan.get('sync_mode', 'full'))

            datax_config = {
                "id": f"sync_{sync_plan['source_name']}_{table_plan['source_table']}",
                "name": f"{table_plan['source_table']} -> {table_plan['target_table']}",
                "source": {
                    **source_config,
                    "table": table_plan['source_table'],
                    "columns": source_columns
                },
                "target": {
                    **target_config,
                    "table": table_plan['target_table'],
                    "write_mode": write_mode,
                    "columns": target_columns
                },
                "sync_type": "full",
                "parallel_jobs": sync_plan.get('recommended_parallel_jobs', 4),
                "schema_mapping": table_plan['schema_mapping']
            }

            logger.info(f"DataX配置生成完成，源字段数: {len(source_columns)}, 目标字段数: {len(target_columns)}")
            return datax_config

        except Exception as e:
            logger.error(f"DataX配置生成失败: {str(e)}")
            raise e

    async def _analyze_field_lengths(self, source_name: str, table_name: str, columns: List[Dict]) -> Dict[str, int]:
        """分析字段实际使用的最大长度"""
        field_lengths = {}

        try:
            logger.info(f"开始分析表 {table_name} 的字段长度...")

            # 筛选出可能需要长度分析的字段
            text_columns = []
            for col in columns:
                col_name = col.get('name', '')
                source_type = col.get('source_type', '').upper()
                if source_type in ['VARCHAR', 'CHAR', 'TEXT', 'CHARACTER VARYING']:
                    text_columns.append(col_name)

            if not text_columns:
                logger.info("没有需要分析长度的文本字段")
                return field_lengths

            logger.info(f"需要分析长度的字段: {text_columns}")

            # 分批分析字段（避免SQL太长）
            batch_size = 10
            for i in range(0, len(text_columns), batch_size):
                batch_columns = text_columns[i:i + batch_size]

                # 构建长度查询SQL
                length_queries = []
                for col in batch_columns:
                    # 根据不同数据库使用不同的长度函数
                    if hasattr(self, '_current_source_type'):
                        source_type = getattr(self, '_current_source_type', 'mysql').lower()
                    else:
                        source_type = 'mysql'  # 默认

                    if source_type in ['kingbase', 'postgresql']:
                        length_queries.append(f'MAX(LENGTH("{col}")) as "{col}_len"')
                    else:
                        length_queries.append(f'MAX(LENGTH(`{col}`)) as {col}_len')

                if length_queries:
                    sql = f"SELECT {', '.join(length_queries)} FROM {table_name} LIMIT 1"
                    logger.info(f"执行长度分析SQL: {sql}")

                    result = await self.integration_service.execute_query(
                        source_name=source_name,
                        query=sql,
                        limit=1
                    )

                    if result.get('success') and result.get('results'):
                        row = result['results'][0]
                        for col in batch_columns:
                            length_key = f"{col}_len"
                            actual_length = row.get(length_key, 0) or 0

                            # 智能推荐长度
                            if actual_length == 0:
                                recommended_length = 100  # 如果没有数据，使用默认值
                            elif actual_length <= 50:
                                recommended_length = 100  # 短字段，给一些余量
                            elif actual_length <= 200:
                                recommended_length = min(actual_length + 100, 300)  # 中等字段
                            elif actual_length <= 1000:
                                recommended_length = min(actual_length + 200, 1200)  # 较长字段
                            else:
                                recommended_length = 'TEXT'  # 很长的字段用TEXT

                            field_lengths[col] = recommended_length
                            logger.info(f"字段 {col} - 实际最大长度: {actual_length}, 推荐: {recommended_length}")
                    else:
                        logger.warning(f"字段长度分析失败: {result.get('error', '未知错误')}")
                        # 如果分析失败，给所有字段设置默认值
                        for col in batch_columns:
                            field_lengths[col] = 150

        except Exception as e:
            logger.error(f"分析字段长度异常: {e}")
            # 异常时给所有文本字段设置保守的默认值
            for col in columns:
                if col.get('source_type', '').upper() in ['VARCHAR', 'CHAR', 'TEXT']:
                    field_lengths[col.get('name', '')] = 150

        logger.info(f"字段长度分析完成，结果: {field_lengths}")
        return field_lengths
    def _determine_write_mode(self, table_plan: Dict[str, Any], sync_mode: str) -> str:
        """根据情况决定写入模式"""
        target_exists = table_plan.get('target_exists', False)
        strategy = table_plan.get('strategy', 'full_copy')

        if not target_exists:
            return "insert"
        elif strategy == 'incremental_update':
            return "insert"
        elif sync_mode == 'full' or strategy in ['full_copy', 'batch_insert']:
            return "replace"
        else:
            return "insert"

    async def _verify_sync_integrity(self, sync_plan: Dict[str, Any],
                                     table_plan: Dict[str, Any]) -> Dict[str, Any]:
        """验证同步完整性"""
        try:
            source_name = sync_plan['source_name']
            target_name = sync_plan['target_name']
            source_table = table_plan['source_table']
            target_table = table_plan['target_table']

            # 获取源表行数
            source_count_result = await self.integration_service.execute_query(
                source_name, f"SELECT COUNT(*) as cnt FROM {source_table}"
            )

            # 获取目标表行数
            target_count_result = await self.integration_service.execute_query(
                target_name, f"SELECT COUNT(*) as cnt FROM {target_table}"
            )

            if (source_count_result.get('success') and target_count_result.get('success')):
                source_count = source_count_result['results'][0]['cnt']
                target_count = target_count_result['results'][0]['cnt']

                return {
                    "success": True,
                    "source_rows": source_count,
                    "target_rows": target_count,
                    "integrity_check": source_count == target_count,
                    "data_loss": max(0, source_count - target_count),
                    "verification_time": datetime.now()
                }
            else:
                return {
                    "success": False,
                    "error": "无法获取行数进行验证"
                }

        except Exception as e:
            return {
                "success": False,
                "error": f"验证失败: {str(e)}"
            }

    def _estimate_sync_time(self, rows: int, source_type: str, target_type: str) -> int:
        """估算同步时间（分钟）"""
        # 基于经验的同步速度估算（行/分钟）
        base_speed = {
            ('mysql', 'mysql'): 50000,
            ('mysql', 'hive'): 30000,
            ('oracle', 'mysql'): 40000,
            ('postgresql', 'hive'): 35000
        }

        speed = base_speed.get((source_type.lower(), target_type.lower()), 25000)
        estimated_minutes = max(1, rows // speed)

        return estimated_minutes

    def _recommend_parallel_jobs(self, total_rows: int) -> int:
        """推荐并行作业数"""
        if total_rows < 100000:
            return 2
        elif total_rows < 1000000:
            return 4
        elif total_rows < 10000000:
            return 6
        else:
            return 8

    def _check_compatibility_warnings(self, table_meta: Dict[str, Any],
                                      source_config: Dict[str, Any],
                                      target_config: Dict[str, Any]) -> List[str]:
        """检查兼容性警告"""
        warnings = []

        # 检查字符集兼容性
        if source_config['type'] == 'mysql' and target_config['type'] == 'kingbase':
            warnings.append("MySQL到kingbase可能存在字符集转换问题")

        # 检查大表警告
        row_count = table_meta.get('statistics', {}).get('row_count', 0)
        if row_count > 10000000:
            warnings.append(f"大表同步（{row_count:,}行），建议在低峰期执行")

        # 检查数据类型兼容性
        columns = table_meta.get('schema', {}).get('columns', [])
        for col in columns:
            if col.get('data_type', '').upper() in ['JSON', 'JSONB']:
                col_name = col.get('name', col.get('column_name', '未知字段'))
                warnings.append(f"列 {col_name} 使用JSON类型，请确保目标数据库支持")

        return warnings

    async def _get_data_source_config(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取数据源配置"""
        try:
            # 从实际的数据集成服务获取数据源配置
            sources_list = await self.integration_service.get_data_sources_list_basic()

            # 查找指定名称的数据源
            target_source = None
            for source in sources_list:
                if source.get('name') == source_name:
                    target_source = source
                    break

            if not target_source:
                logger.error(f"未找到数据源: {source_name}")
                return None

            # 🔧 修复：确保类型映射正确
            source_type = target_source.get('type', '').lower()
            if not source_type:
                logger.error(f"数据源 {source_name} 类型为空")
                return None

            # 映射数据源类型
            type_mapping = {
                'mysql': 'mysql',
                'kingbase': 'kingbase',
                'hive': 'hive',
                'postgresql': 'postgresql',
                'oracle': 'oracle',
                'doris': 'doris'
            }

            mapped_type = type_mapping.get(source_type, source_type)

            # 🔧 重要修复：确保密码不为空
            password = target_source.get('password', '')
            if not password:
                # 如果密码为空，尝试从其他地方获取或使用默认值
                logger.warning(f"数据源 {source_name} 密码为空，请检查配置")

            username = target_source.get('username', '')
            if not username:
                logger.warning(f"数据源 {source_name} 用户名为空，请检查配置")

            config = {
                "type": mapped_type,
                "host": target_source.get('host', ''),
                "port": target_source.get('port', 3306),
                "database": target_source.get('database', ''),
                "username": username,
                "password": password,
            }
            # 🆕 Doris特殊配置
            if mapped_type == 'doris':
                # Doris需要额外的HTTP端口配置
                config['http_port'] = 8060  # FE HTTP端口
                # Doris的查询端口通常是9030
                if config['port'] == 3306:  # 如果是默认MySQL端口，改为Doris端口
                    config['port'] = 9030
            # 🔧 验证必要字段
            required_fields = ['host', 'username', 'password']
            missing_fields = [field for field in required_fields if not config.get(field)]

            if missing_fields:
                logger.error(f"数据源 {source_name} 缺少必要字段: {missing_fields}")
                logger.error(f"当前配置: {config}")
                return None

            logger.info(f"获取数据源配置成功: {source_name} -> {mapped_type}")
            logger.info(
                f"配置详情: host={config['host']}, username={config['username']}, password={'***' if config['password'] else 'EMPTY'}")

            return config

        except Exception as e:
            logger.error(f"获取数据源配置失败 {source_name}: {e}")
            return None

    async def _check_target_table_exists(self, target_name: str, table_name: str) -> bool:
        """检查目标表是否存在"""
        try:
            result = await self.integration_service.get_tables(target_name)
            if result.get('success'):
                tables = result.get('tables', [])
                return any(t.get('table_name') == table_name for t in tables)
            return False
        except:
            return False

    async def _execute_create_table(self, target_name: str, create_sql: str) -> Dict[str, Any]:
        """执行建表SQL"""
        try:
            logger.info(f"开始执行建表SQL，目标数据源: {target_name}")
            logger.info(f"建表SQL: {create_sql}")

            # 执行建表SQL
            result = await self.integration_service.execute_query(
                source_name=target_name,
                query=create_sql,
                database=None,
                schema=None,
                limit=None
            )

            logger.info(f"建表SQL执行结果: {result}")

            if result.get('success'):
                logger.info("建表SQL执行成功，等待验证表是否存在...")
                import asyncio
                await asyncio.sleep(2)

                # 提取表名
                if 'CREATE TABLE IF NOT EXISTS `' in create_sql:
                    table_name = create_sql.split('`')[1]
                elif 'CREATE TABLE IF NOT EXISTS "' in create_sql:
                    table_name = create_sql.split('"')[1]
                else:
                    # 其他情况的表名提取
                    parts = create_sql.split()
                    table_name = parts[5] if len(parts) > 5 else "unknown"

                logger.info(f"提取的表名: {table_name}")

                # 验证表是否真的存在
                verify_sql = f"SELECT 1 FROM `{table_name}` WHERE 1=2"
                verify_result = await self.integration_service.execute_query(
                    source_name=target_name,
                    query=verify_sql,
                    database=None,
                    schema=None,
                    limit=1
                )

                logger.info(f"表存在验证结果: {verify_result}")

                if verify_result.get('success'):
                    logger.info(f"表 {table_name} 创建并验证成功")
                    return {
                        "success": True,
                        "message": f"表 {table_name} 创建成功并验证通过",
                        "sql": create_sql,
                        "table_name": table_name
                    }
                else:
                    logger.error(f"表 {table_name} 创建后验证失败: {verify_result.get('error')}")
                    return {
                        "success": False,
                        "message": f"表创建后验证失败: {verify_result.get('error')}",
                        "sql": create_sql,
                        "table_name": table_name
                    }
            else:
                error_msg = result.get('error', '创建失败')
                logger.error(f"建表SQL执行失败: {error_msg}")
                return {
                    "success": False,
                    "message": f"建表失败: {error_msg}",
                    "sql": create_sql
                }

        except Exception as e:
            logger.error(f"建表过程异常: {e}")
            import traceback
            logger.error(f"异常详情: {traceback.format_exc()}")
            return {
                "success": False,
                "message": f"建表异常: {str(e)}",
                "sql": create_sql
            }

    async def _generate_sync_strategy(self, source_config: Dict[str, Any],
                                      target_config: Dict[str, Any],
                                      table_meta: Dict[str, Any],
                                      target_exists: bool) -> str:
        """生成同步策略"""
        source_type = source_config.get('type', '').lower()
        target_type = target_config.get('type', '').lower()
        row_count = table_meta.get('statistics', {}).get('row_count', 0)

        if target_exists:
            return "incremental_update"  # 目标表存在，增量更新
        elif row_count > 10000000:
            return "batch_insert"  # 大表，批量插入
        elif source_type == target_type:
            return "direct_copy"  # 同类型数据库，直接复制
        else:
            return "full_copy"  # 不同类型，全量复制

    def _determine_global_strategy(self, sync_mode: str, sync_plans: List[Dict]) -> str:
        """确定全局同步策略"""
        if sync_mode == "single":
            return "单表同步"
        elif sync_mode == "multiple":
            return f"多表同步({len(sync_plans)}张表)"
        elif sync_mode == "database":
            return "整库同步"
        else:
            return "自定义同步"

    async def _precheck_sync_conditions(self, sync_plan: Dict[str, Any]) -> Dict[str, Any]:
        """预检查同步条件"""
        try:
            # 检查源数据源连接
            source_name = sync_plan.get('source_name')
            target_name = sync_plan.get('target_name')

            if not source_name or not target_name:
                return {
                    "success": False,
                    "error": "缺少源或目标数据源名称"
                }

            # 简单的连接检查
            source_config = await self._get_data_source_config(source_name)
            target_config = await self._get_data_source_config(target_name)

            if not source_config or not target_config:
                return {
                    "success": False,
                    "error": "数据源配置验证失败"
                }

            return {
                "success": True,
                "message": "预检查通过"
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"预检查失败: {str(e)}"
            }

    def _generate_sync_summary(self, sync_results: List[Dict]) -> Dict[str, Any]:
        """生成同步摘要"""
        total_tables = len(sync_results)
        successful_tables = len([r for r in sync_results if r.get('result', {}).get('success')])
        failed_tables = total_tables - successful_tables

        return {
            "total_tables": total_tables,
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "success_rate": f"{(successful_tables / total_tables * 100):.1f}%" if total_tables > 0 else "0%",
            "summary_message": f"共{total_tables}张表，成功{successful_tables}张，失败{failed_tables}张"
        }


# 全局智能同步服务实例
smart_sync_service = SmartSyncService()
