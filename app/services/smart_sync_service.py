# app/services/smart_sync_service.py
"""
智能数据同步服务 - 支持拖拽式操作和自动建表
"""

import asyncio
import json
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from loguru import logger

from app.services.datax_service import EnhancedSyncService
from app.services.optimized_data_integration_service import optimized_data_integration_service
from app.utils.response import create_response


class SmartSyncService:
    """智能数据同步服务"""

    def __init__(self):
        self.datax_service = EnhancedSyncService()
        self.integration_service = optimized_data_integration_service

    async def analyze_sync_plan(self, sync_request: Dict[str, Any]) -> Dict[str, Any]:
        """分析同步计划，自动生成同步策略"""
        try:
            source_name = sync_request['source_name']
            target_name = sync_request['target_name']
            tables = sync_request['tables']  # [{'source_table': 'users', 'target_table': 'users_copy'}]
            sync_mode = sync_request.get('sync_mode', 'single')

            # 获取源和目标数据源配置
            source_config = await self._get_data_source_config(source_name)
            target_config = await self._get_data_source_config(target_name)

            if not source_config or not target_config:
                return {
                    "success": False,
                    "error": "数据源配置不存在"
                }

            # 分析每个表的同步计划
            sync_plans = []
            total_estimated_time = 0
            total_estimated_rows = 0

            for table_info in tables:
                source_table = table_info['source_table']
                target_table = table_info['target_table']

                # 获取源表元数据
                source_metadata = await self.integration_service.get_table_metadata(
                    source_name, source_table
                )

                if not source_metadata.get('success'):
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
                    # 生成DataX配置
                    datax_config = await self._generate_datax_config(sync_plan, plan)

                    # 执行同步
                    sync_result = await self.datax_service.execute_sync_task(datax_config)

                    if sync_result.get('success'):
                        successful_syncs += 1
                        # 验证数据完整性
                        verification = await self._verify_sync_integrity(sync_plan, plan)
                        sync_result['verification'] = verification
                    else:
                        failed_syncs += 1

                    sync_results.append({
                        "table": plan['source_table'],
                        "target_table": plan['target_table'],
                        "result": sync_result
                    })

                except Exception as e:
                    failed_syncs += 1
                    sync_results.append({
                        "table": plan['source_table'],
                        "target_table": plan['target_table'],
                        "result": {
                            "success": False,
                            "error": str(e)
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

            columns.append(f"    {col_name} {col_type} {nullable}")

        # 🔧 修复：先定义换行符变量，避免在f-string中使用反斜杠
        newline = '\n'
        column_definitions = f',{newline}'.join(columns)

        if target_type == 'mysql':
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definitions}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;""".strip()

        elif target_type == 'postgresql':
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definitions}
    );""".strip()

        elif target_type == 'hive':
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definitions}
    ) 
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/{table_name}';""".strip()

        else:
            # 通用SQL
            sql = f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {column_definitions}
    );""".strip()

        return sql

    async def _generate_schema_mapping(self, table_metadata: Dict[str, Any],
                                       target_type: str) -> Dict[str, Any]:
        """生成schema映射"""
        source_columns = table_metadata.get('schema', {}).get('columns', [])
        mapped_columns = []

        for col in source_columns:
            source_type = col.get('data_type', 'VARCHAR').upper()
            target_col_type = self._map_data_type(source_type, target_type)

            mapped_columns.append({
                "name": col['column_name'],
                "source_type": source_type,
                "target_type": target_col_type,
                "nullable": col.get('is_nullable', True),
                "length": col.get('character_maximum_length'),
                "precision": col.get('numeric_precision'),
                "scale": col.get('numeric_scale')
            })

        return {
            "columns": mapped_columns,
            "mapping_strategy": "auto",
            "type_conversions": self._get_type_conversion_summary(mapped_columns)
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

    async def _generate_datax_config(self, sync_plan: Dict[str, Any],
                                     table_plan: Dict[str, Any]) -> Dict[str, Any]:
        """生成DataX配置"""
        source_config = await self._get_data_source_config(sync_plan['source_name'])
        target_config = await self._get_data_source_config(sync_plan['target_name'])

        return {
            "id": f"sync_{sync_plan['source_name']}_{table_plan['source_table']}",
            "name": f"{table_plan['source_table']} -> {table_plan['target_table']}",
            "source": {
                **source_config,
                "table": table_plan['source_table'],
                "query": f"SELECT * FROM {table_plan['source_table']}"
            },
            "target": {
                **target_config,
                "table": table_plan['target_table'],
                "write_mode": "insert"
            },
            "sync_type": "full",
            "parallel_jobs": sync_plan.get('recommended_parallel_jobs', 4),
            "schema_mapping": table_plan['schema_mapping']
        }

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
                source_name, f"SELECT COUNT(*) as cnt FROM {source_table}", limit=1
            )

            # 获取目标表行数
            target_count_result = await self.integration_service.execute_query(
                target_name, f"SELECT COUNT(*) as cnt FROM {target_table}", limit=1
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
        if source_config['type'] == 'mysql' and target_config['type'] == 'postgresql':
            warnings.append("MySQL到PostgreSQL可能存在字符集转换问题")

        # 检查大表警告
        row_count = table_meta.get('statistics', {}).get('row_count', 0)
        if row_count > 10000000:
            warnings.append(f"大表同步（{row_count:,}行），建议在低峰期执行")

        # 检查数据类型兼容性
        columns = table_meta.get('schema', {}).get('columns', [])
        for col in columns:
            if col.get('data_type', '').upper() in ['JSON', 'JSONB']:
                warnings.append(f"列 {col['column_name']} 使用JSON类型，请确保目标数据库支持")

        return warnings

    async def _get_data_source_config(self, source_name: str) -> Optional[Dict[str, Any]]:
        """获取数据源配置"""
        # 这里应该从数据库或配置中获取数据源信息
        # 暂时返回模拟数据
        mock_configs = {
            "MySQL-Production": {
                "type": "mysql",
                "host": "192.168.1.100",
                "port": 3306,
                "database": "production",
                "username": "user",
                "password": "password"
            },
            "Hive-Warehouse": {
                "type": "hive",
                "host": "192.168.1.101",
                "port": 10000,
                "database": "default",
                "username": "hive",
                "password": "hive"
            }
        }
        return mock_configs.get(source_name)

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
            result = await self.integration_service.execute_query(
                target_name, create_sql
            )
            return {
                "success": result.get('success', False),
                "message": "表创建成功" if result.get('success') else result.get('error', '创建失败')
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"建表失败: {str(e)}"
            }


# 全局智能同步服务实例
smart_sync_service = SmartSyncService()