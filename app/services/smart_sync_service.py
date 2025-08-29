# app/services/smart_sync_service.py
"""
智能数据同步服务 - 支持拖拽式操作和自动建表
"""

import asyncio
import json
import os
import tempfile
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from loguru import logger

from app.services.datax_service import EnhancedSyncService
from app.services.optimized_data_integration_service import get_optimized_data_integration_service
from app.utils.response import create_response
from app.models.sync_history import SyncHistory, SyncTableHistory
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc, func
from app.utils.database import get_async_db


class SmartSyncService:
    """智能数据同步服务"""

    def __init__(self):
        self.datax_service = EnhancedSyncService()
        self.integration_service = get_optimized_data_integration_service()

    async def get_sync_history(
            self,
            page: int = 1,
            page_size: int = 20,
            status: Optional[str] = None,
            source_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """从数据库获取真实的同步历史记录"""
        try:
            # 获取数据库连接
            async with get_async_db() as db:
                # 构建查询条件
                conditions = []
                if status:
                    conditions.append(SyncHistory.status == status)
                if source_name:
                    conditions.append(SyncHistory.source_name.like(f"%{source_name}%"))

                # 查询总数
                count_query = select(func.count(SyncHistory.id))
                if conditions:
                    count_query = count_query.where(and_(*conditions))

                total_result = await db.execute(count_query)
                total = total_result.scalar()

                # 分页查询
                query = select(SyncHistory).order_by(desc(SyncHistory.created_at))
                if conditions:
                    query = query.where(and_(*conditions))

                offset = (page - 1) * page_size
                query = query.offset(offset).limit(page_size)

                result = await db.execute(query)
                records = result.scalars().all()

                # 构建响应数据
                history_list = []
                for record in records:
                    history_list.append({
                        "sync_id": record.sync_id,
                        "source_name": record.source_name,
                        "target_name": record.target_name,
                        "status": record.status,
                        "start_time": record.start_time,
                        "end_time": record.end_time,
                        "duration_seconds": record.duration_seconds,
                        "total_tables": record.total_tables,
                        "success_tables": record.success_tables,
                        "failed_tables": record.failed_tables,
                        "total_records": record.total_records,
                        "created_by": record.created_by,
                        "current_step": record.current_step,
                        "progress": record.progress,
                        "error_message": record.error_message
                    })

                return {
                    "history": history_list,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "has_more": offset + page_size < total,
                    "total_pages": (total + page_size - 1) // page_size
                }

        except Exception as e:
            logger.error(f"从数据库获取同步历史失败: {e}")
            return {
                "history": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "has_more": False,
                "error": str(e)
            }

    async def save_sync_history(self, sync_data: Dict[str, Any]) -> bool:
        """保存同步历史到数据库"""
        try:
            async with get_async_db() as db:
                sync_record = SyncHistory(
                    sync_id=sync_data.get('sync_id'),
                    source_name=sync_data.get('source_name'),
                    target_name=sync_data.get('target_name'),
                    status=sync_data.get('status', 'running'),
                    start_time=sync_data.get('start_time'),
                    end_time=sync_data.get('end_time'),
                    duration_seconds=sync_data.get('duration_seconds'),
                    total_tables=sync_data.get('total_tables', 0),
                    success_tables=sync_data.get('success_tables', 0),
                    failed_tables=sync_data.get('failed_tables', 0),
                    total_records=sync_data.get('total_records', 0),
                    sync_mode=sync_data.get('sync_mode'),
                    parallel_jobs=sync_data.get('parallel_jobs', 1),
                    created_by=sync_data.get('created_by'),
                    error_message=sync_data.get('error_message'),
                    sync_result=sync_data.get('sync_result'),
                    current_step=sync_data.get('current_step'),
                    progress=sync_data.get('progress', 0)
                )

                db.add(sync_record)
                await db.commit()

                logger.info(f"同步历史保存成功: {sync_data.get('sync_id')}")
                return True

        except Exception as e:
            logger.error(f"保存同步历史失败: {e}")
            await db.rollback()
            return False

    async def update_sync_status(self, sync_id: str, status_data: Dict[str, Any]) -> bool:
        """更新数据库中的同步状态"""
        try:
            async with get_async_db() as db:
                # 查找现有记录
                result = await db.execute(
                    select(SyncHistory).where(SyncHistory.sync_id == sync_id)
                )
                record = result.scalar_one_or_none()

                if record:
                    # 更新现有记录
                    for key, value in status_data.items():
                        if hasattr(record, key):
                            setattr(record, key, value)
                    record.updated_at = datetime.now()
                else:
                    # 创建新记录
                    record = SyncHistory(sync_id=sync_id, **status_data)
                    db.add(record)

                await db.commit()
                return True

        except Exception as e:
            logger.error(f"更新同步状态失败: {e}")
            await db.rollback()
            return False
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

            # 检测目标类型
            target_config = await self._get_data_source_config(sync_plan['target_name'])
            is_hive_target = (target_config and target_config.get('type', '').lower() == 'hive')

            logger.info(f"目标类型: {target_config.get('type', 'unknown')}, Hive目标: {is_hive_target}")

            # 🔧 修复：正确的执行顺序
            hdfs_result = None
            table_creation_results = []

            if is_hive_target:
                logger.info("=== Hive同步流程开始 ===")

                logger.info("步骤1: 创建Hive外部表...")
                table_creation_results = await self._create_hive_external_tables(sync_plan)

                # 检查建表结果
                table_creation_success = all(result.get('success', False) for result in table_creation_results)
                if not table_creation_success:
                    logger.error("Hive外部表创建失败，终止同步")
                    return {
                        "success": False,
                        "error": "Hive外部表创建失败",
                        "table_creation_results": table_creation_results
                    }

                logger.info("步骤2: 创建HDFS目录...")
                hdfs_result = await self._create_hdfs_directories(sync_plan)

                # 检查HDFS目录创建结果
                if not hdfs_result.get('success', False):
                    logger.error("HDFS目录创建失败，终止同步")
                    return {
                        "success": False,
                        "error": "HDFS目录创建失败",
                        "hdfs_result": hdfs_result,
                        "table_creation_results": table_creation_results
                    }

                logger.info("Hive表和HDFS目录准备完成，开始数据同步...")

            else:
                logger.info("普通数据库目标，创建常规表...")
                table_creation_results = await self._create_target_tables(sync_plan)

            # 执行数据同步
            sync_results = []
            successful_syncs = 0
            failed_syncs = 0

            logger.info("步骤3: 开始执行数据同步...")

            for plan in sync_plan['sync_plans']:
                try:
                    logger.info(f"同步表: {plan['source_table']} -> {plan['target_table']}")

                    # 🔧 彻底清理配置，防止循环引用
                    clean_sync_plan = {
                        'source_name': sync_plan['source_name'],
                        'target_name': sync_plan['target_name'],
                        'sync_mode': sync_plan.get('sync_mode', 'full'),
                        'recommended_parallel_jobs': sync_plan.get('recommended_parallel_jobs', 4)
                    }

                    clean_plan = {
                        'source_table': plan['source_table'],
                        'target_table': plan['target_table'],
                        'target_exists': plan.get('target_exists', False),
                        'strategy': plan.get('strategy', 'full_copy')
                        # 🔧 故意不包含 schema_mapping，避免55个字段的复杂对象
                    }

                    logger.info(
                        f"清理后的配置 - 源表: {clean_plan['source_table']}, 目标表: {clean_plan['target_table']}")

                    # 生成DataX配置
                    datax_config = await self._generate_datax_config(clean_sync_plan, clean_plan)
                    logger.info(f"DataX配置生成成功")

                    # 执行同步
                    logger.info(f"⚡ 开始执行DataX同步任务...")
                    sync_result = await self.datax_service.execute_sync_task(datax_config)
                    logger.info(f"DataX执行结果: success={sync_result.get('success')}")

                    if sync_result.get('success'):
                        successful_syncs += 1
                        logger.info(f"表 {plan['source_table']} 同步成功")

                        # 🔧 修复：只有DataX同步成功后才进行Hive后续操作
                        if is_hive_target:
                            logger.info("步骤4: 刷新Hive分区...")
                            partition_result = await self._refresh_hive_partition(sync_plan, plan)
                            sync_result['partition_refresh'] = partition_result
                            logger.info(f"Hive分区刷新结果: {partition_result.get('success')}")

                        # 验证数据完整性
                        # verification = await self._verify_sync_integrity(sync_plan, plan)
                        # sync_result['verification'] = verification
                        # logger.info(f"🔍 数据验证结果: {verification}")

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
                "hdfs_directories": hdfs_result if is_hive_target else None,
                "execution_time": datetime.now(),
                "summary": self._generate_sync_summary(sync_results),
                "workflow": "hive" if is_hive_target else "standard"
            }

            if is_hive_target:
                logger.info("=== Hive同步流程完成 ===")

            return sync_report

        except Exception as e:
            logger.error(f"执行智能同步失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def _is_hive_target(self, sync_plan: Dict[str, Any]) -> bool:
        """异步判断是否为Hive目标"""
        try:
            target_config = await self._get_data_source_config(sync_plan['target_name'])
            return target_config and target_config.get('type', '').lower() == 'hive'
        except:
            return False

    async def _create_hive_external_tables(self, sync_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """创建Hive外部表"""
        creation_results = []
        target_name = sync_plan['target_name']

        for plan in sync_plan['sync_plans']:
            try:
                # 生成Hive外部表SQL
                create_sql = await self._generate_hive_external_table_sql(
                    plan['schema_mapping'],
                    plan['target_table'],
                    sync_plan['target_name']
                )

                # 执行建表
                result = await self._execute_hive_table_creation(target_name, create_sql, plan['target_table'])

                creation_results.append({
                    "table": plan['target_table'],
                    "success": result['success'],
                    "sql": create_sql,
                    "message": result.get('message', '')
                })

            except Exception as e:
                logger.error(f"创建Hive外部表失败 {plan['target_table']}: {e}")
                creation_results.append({
                    "table": plan['target_table'],
                    "success": False,
                    "error": str(e)
                })

        return creation_results

    async def _generate_hive_external_table_sql(self, schema_mapping: Dict[str, Any],
                                                table_name: str, target_source: str) -> str:
        """生成Hive外部表SQL - 修复版本"""
        target_config = await self._get_data_source_config(target_source)
        columns = schema_mapping.get('columns', [])

        if not columns:
            raise ValueError("缺少字段映射信息")

        # 构建字段定义
        column_definitions = []
        for col in columns:
            col_name = col['name']
            # 去掉双引号，Hive字段名不需要引号
            clean_col_name = col_name.strip('"')
            # 将字段类型转换为Hive类型
            hive_type = self._convert_to_hive_type(col['target_type'])
            column_definitions.append(f"    {clean_col_name} {hive_type}")

        str1 = ',\n'.join(column_definitions)  # 字段定义部分
        database = target_config.get('database', 'default')

        # 🔧 重要修复：表名处理逻辑
        if table_name.startswith('ODS_'):
            final_table_name = table_name  # 如果已经有ODS_前缀，直接使用
        else:
            final_table_name = f"ODS_{table_name}"  # 添加ODS_前缀

        # 🔧 关键修复：生成正确的LOCATION路径，与DataX路径一致
        base_path = target_config.get('base_path', '/user/hive/warehouse')
        if database != 'default':
            table_location = f"{base_path}/{database}.db/{final_table_name}"
        else:
            table_location = f"{base_path}/{final_table_name}"

        # 🔧 按照你的建表格式 + 修复分区和ORC
        sql = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database.upper()}.{final_table_name.upper()} (
    {str1}
    ) PARTITIONED BY (dt string) 
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '\\t' 
    STORED AS ORC  
    LOCATION '{table_location}'
    TBLPROPERTIES ('orc.compress' = 'snappy')"""

        logger.info(f"生成Hive外部表SQL: {sql}")
        logger.info(f"表位置: {table_location}")
        return sql

    def _convert_to_hive_type(self, datax_type: str) -> str:
        """将DataX字段类型转换为Hive类型"""
        type_mapping = {
            'VARCHAR': 'STRING',
            'TEXT': 'STRING',
            'INT': 'INT',
            'INTEGER': 'INT',
            'BIGINT': 'BIGINT',
            'DECIMAL': 'DECIMAL(10,2)',
            'DOUBLE': 'DOUBLE',
            'FLOAT': 'FLOAT',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'TIMESTAMP': 'TIMESTAMP',
            'BOOLEAN': 'BOOLEAN'
        }

        # 提取基础类型（去掉长度等）
        base_type = datax_type.split('(')[0].upper()
        return type_mapping.get(base_type, 'STRING')

    async def _execute_hive_table_creation(self, target_name: str, create_sql: str, table_name: str) -> Dict[str, Any]:
        """执行Hive建表SQL"""
        try:
            logger.info(f"开始创建Hive外部表: {table_name}")
            logger.info(f"建表SQL: {create_sql}")

            # 使用集成服务执行SQL
            result = await self.integration_service.execute_query(
                source_name=target_name,
                query=create_sql,
                database=None,
                schema=None,
                limit=1
            )

            logger.info(f"Hive建表执行结果: {result}")

            return {
                "success": result.get('success', False),
                "message": f"Hive外部表 {table_name} 创建成功" if result.get('success') else result.get('error',
                                                                                                        '创建失败'),
                "sql": create_sql
            }

        except Exception as e:
            logger.error(f"Hive建表失败: {e}")
            return {
                "success": False,
                "message": f"Hive建表失败: {str(e)}",
                "sql": create_sql
            }

    async def _refresh_hive_partition(self, sync_plan: Dict[str, Any], table_plan: Dict[str, Any]) -> Dict[str, Any]:
        """刷新Hive分区"""
        try:
            target_config = await self._get_data_source_config(sync_plan['target_name'])
            database = target_config.get('database', 'default')

            # 🔧 修复：使用与DataX配置一致的表名处理逻辑
            original_table_name = table_plan['target_table']
            if original_table_name.startswith('ODS_'):
                final_table_name = original_table_name
            else:
                final_table_name = f"ODS_{original_table_name}"

            # 获取分区值
            from datetime import datetime
            partition_value = datetime.now().strftime('%Y-%m-%d')

            # 🔧 修复：构建正确的分区路径
            base_path = target_config.get('base_path', '/user/hive/warehouse')
            if database != 'default':
                partition_location = f"{base_path}/{database}.db/{final_table_name}/dt={partition_value}"
            else:
                partition_location = f"{base_path}/{final_table_name}/dt={partition_value}"

            # 🔧 修复：添加分区时指定LOCATION
            add_partition_sql = f"""ALTER TABLE {database.upper()}.{final_table_name.upper()} 
    ADD IF NOT EXISTS PARTITION (dt='{partition_value}') 
    LOCATION '{partition_location}'"""

            logger.info(f"添加分区SQL: {add_partition_sql}")

            # 执行添加分区
            result = await self.integration_service.execute_query(
                source_name=sync_plan['target_name'],
                query=add_partition_sql,
                limit=1
            )

            if result.get('success'):
                # 刷新元数据
                repair_sql = f"MSCK REPAIR TABLE {database.upper()}.{final_table_name.upper()}"
                repair_result = await self.integration_service.execute_query(
                    source_name=sync_plan['target_name'],
                    query=repair_sql,
                    limit=1
                )

                return {
                    "success": True,
                    "partition_added": True,
                    "partition_value": partition_value,
                    "partition_location": partition_location,  # 🆕 添加位置信息
                    "table_name": final_table_name,  # 🆕 添加表名信息
                    "metadata_refreshed": repair_result.get('success', False),
                    "message": f"分区 dt={partition_value} 添加成功"
                }
            else:
                return {
                    "success": False,
                    "error": f"添加分区失败: {result.get('error')}"
                }

        except Exception as e:
            logger.error(f"刷新Hive分区失败: {e}")
            return {
                "success": False,
                "error": f"刷新分区失败: {str(e)}"
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

            # 🔧 关键修复：手动构建配置，避免循环引用
            final_source_config = {
                'type': source_config.get('type'),
                'host': source_config.get('host'),
                'port': source_config.get('port'),
                'database': source_config.get('database'),
                'username': source_config.get('username'),
                'password': source_config.get('password'),
                'table': table_plan['source_table']
            }

            final_target_config = {
                'type': target_config.get('type'),
                'host': target_config.get('host'),
                'port': target_config.get('port'),
                'database': target_config.get('database'),
                'username': target_config.get('username'),
                'password': target_config.get('password')
            }

            # Hive特殊处理
            target_table_name = table_plan['target_table']
            if target_config['type'].lower() == 'hive':
                # 处理表名：确保ODS_前缀
                original_table_name = table_plan['target_table']
                if original_table_name.startswith('ODS_'):
                    final_table_name = original_table_name
                else:
                    final_table_name = f"ODS_{original_table_name}"

                # 生成HDFS路径
                database = target_config.get('database', 'default')
                base_path = target_config.get('base_path', '/user/hive/warehouse')

                # 生成当前日期分区
                from datetime import datetime
                current_date = datetime.now().strftime('%Y-%m-%d')
                partition_value = current_date

                # 构建完整的HDFS路径
                if database != 'default':
                    base_table_path = f"{base_path}/{database}.db/{final_table_name}"
                else:
                    base_table_path = f"{base_path}/{final_table_name}"

                hdfs_path = f"{base_table_path}/dt={partition_value}"

                # 手动添加Hive字段
                final_target_config['hdfs_path'] = hdfs_path
                final_target_config['table'] = final_table_name
                final_target_config['partition_column'] = 'dt'
                final_target_config['partition_value'] = partition_value
                final_target_config['storage_format'] = 'ORC'
                final_target_config['compression'] = 'snappy'
                final_target_config['namenode_host'] = target_config.get('namenode_host', '192.142.76.242')
                final_target_config['namenode_port'] = target_config.get('namenode_port', '8020')

                target_table_name = final_table_name
                logger.info(f"Hive目标检测到，生成路径: {hdfs_path}")
                logger.info(f"最终表名: {final_table_name}")
                logger.info(f"分区信息: dt={partition_value}")
            else:
                final_target_config['table'] = target_table_name

            # 处理Hive源配置
            if source_config.get('type', '').lower() == 'hive':
                final_source_config['namenode_host'] = source_config.get('namenode_host')
                final_source_config['namenode_port'] = source_config.get('namenode_port')
                final_source_config['base_path'] = source_config.get('base_path')
                final_source_config['file_type'] = source_config.get('file_type', 'orc')
                final_source_config['field_delimiter'] = source_config.get('field_delimiter', '\t')

            # 🔧 简化字段处理：直接从table_plan获取，不使用schema_mapping
            schema_mapping = table_plan.get('schema_mapping', {})
            columns_mapping = schema_mapping.get('columns', [])

            if not columns_mapping:
                # 🔧 如果没有schema_mapping，使用简单的字段列表
                logger.warning(f"表 {table_plan['source_table']} 没有schema_mapping，使用简化字段配置")
                # 生成简单的字段配置
                simple_columns = [f"col_{i}" for i in range(10)]  # 假设10个字段
                source_columns = simple_columns
                target_columns = simple_columns
            else:
                # 过滤掉分区字段，只保留字段名
                data_columns = [col for col in columns_mapping if col['name'].lower() != 'dt']
                source_columns = [col['name'] for col in data_columns]
                target_columns = [col['name'] for col in data_columns]

            logger.info(f"字段配置: 源字段({len(source_columns)})={source_columns[:5]}...")
            logger.info(f"字段配置: 目标字段({len(target_columns)})={target_columns[:5]}...")

            if not source_columns or not target_columns:
                raise ValueError("字段列表为空")

            # 确定写入模式
            write_mode = self._determine_write_mode(table_plan, sync_plan.get('sync_mode', 'full'))

            # 手动添加字段配置
            final_source_config['columns'] = source_columns
            final_target_config['columns'] = target_columns
            final_target_config['write_mode'] = write_mode

            # 🔧 构建最终的DataX配置，完全去掉schema_mapping引用
            datax_config = {
                "id": f"sync_{sync_plan['source_name']}_{table_plan['source_table']}",
                "name": f"{table_plan['source_table']} -> {target_table_name}",
                "source": final_source_config,
                "target": final_target_config,
                "sync_type": "full",
                "parallel_jobs": sync_plan.get('recommended_parallel_jobs', 4)
                # 🔧 完全删除 schema_mapping 引用
            }

            logger.info(f"DataX配置生成完成，源字段数: {len(source_columns)}, 目标字段数: {len(target_columns)}")
            try:
                import json
                test_json = json.dumps(datax_config, ensure_ascii=False)
                logger.info("JSON序列化测试通过")
            except Exception as e:
                logger.error(f"JSON序列化失败: {e}")
                raise
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

    async def _create_hdfs_directories(self, sync_plan: Dict[str, Any]) -> Dict[str, Any]:
        """创建HDFS目录"""
        try:
            target_config = await self._get_data_source_config(sync_plan['target_name'])

            if target_config.get('type', '').lower() != 'hive':
                return {"success": True, "message": "非Hive目标，跳过HDFS目录创建"}

            # 获取namenode信息
            namenode_host = target_config.get('namenode_host', '192.142.76.242')
            namenode_port = target_config.get('namenode_port', '8020')

            logger.info(f"HDFS连接信息: {namenode_host}:{namenode_port}")

            created_paths = []
            failed_paths = []

            for plan in sync_plan['sync_plans']:
                try:
                    # 生成与DataX配置一致的路径
                    original_table_name = plan['target_table']
                    if original_table_name.startswith('ODS_'):
                        final_table_name = original_table_name
                    else:
                        final_table_name = f"ODS_{original_table_name}"

                    database = target_config.get('database', 'default')
                    base_path = target_config.get('base_path', '/user/hive/warehouse')

                    # 生成当前日期分区
                    from datetime import datetime
                    current_date = datetime.now().strftime('%Y-%m-%d')

                    # 构建完整路径（与DataX配置中的路径生成逻辑一致）
                    if database != 'default':
                        base_table_path = f"{base_path}/{database}.db/{final_table_name}"
                    else:
                        base_table_path = f"{base_path}/{final_table_name}"

                    partition_path = f"{base_table_path}/dt={current_date}"

                    logger.info(f"准备创建HDFS路径: {partition_path}")

                    # 🔧 使用你提供的方法创建HDFS目录
                    success = await self._create_hdfs_path(namenode_host, namenode_port, partition_path)

                    if success:
                        created_paths.append(partition_path)
                        logger.info(f"HDFS目录创建成功: {partition_path}")
                    else:
                        failed_paths.append(partition_path)
                        logger.error(f"HDFS目录创建失败: {partition_path}")

                except Exception as e:
                    error_msg = f"{plan['target_table']}: {str(e)}"
                    logger.error(f"创建HDFS目录异常: {error_msg}")
                    failed_paths.append(error_msg)

            return {
                "success": len(failed_paths) == 0,
                "created_paths": created_paths,
                "failed_paths": failed_paths,
                "total_paths": len(created_paths) + len(failed_paths),
                "namenode": f"{namenode_host}:{namenode_port}",
                "message": f"HDFS目录创建完成，成功{len(created_paths)}个，失败{len(failed_paths)}个"
            }

        except Exception as e:
            logger.error(f"HDFS目录创建异常: {e}")
            return {
                "success": False,
                "error": f"HDFS目录创建异常: {str(e)}"
            }

    async def _create_hdfs_path(self, namenode_host: str, namenode_port: str, hdfs_path: str) -> bool:
        """创建单个HDFS路径 - 使用WebHDFS API"""
        try:
            import aiohttp
            import asyncio

            # WebHDFS API 端口通常是 9870 (Hadoop 3.x) 或 50070 (Hadoop 2.x)
            webhdfs_port = 9870  # 你可以根据实际情况调整

            # 构建WebHDFS URL
            webhdfs_url = f"http://{namenode_host}:{webhdfs_port}/webhdfs/v1{hdfs_path}?op=MKDIRS&user.name=bigdata"

            logger.info(f"使用WebHDFS创建目录: {webhdfs_url}")

            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.put(webhdfs_url) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('boolean'):
                            logger.info(f"HDFS目录创建成功: {hdfs_path}")
                            return True
                        else:
                            logger.error(f"HDFS目录创建失败: {hdfs_path}, 响应: {result}")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"WebHDFS请求失败: {response.status}, 错误: {error_text}")
                        return False

        except Exception as e:
            logger.error(f"WebHDFS API调用异常: {e}")
            return False

    async def _verify_hdfs_path(self, namenode_host: str, namenode_port: str, hdfs_path: str) -> bool:
        """验证HDFS路径是否存在 - 使用WebHDFS API"""
        try:
            import aiohttp

            webhdfs_port = 9870
            webhdfs_url = f"http://{namenode_host}:{webhdfs_port}/webhdfs/v1{hdfs_path}?op=GETFILESTATUS&user.name=bigdata"

            logger.info(f"验证HDFS路径: {webhdfs_url}")

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(webhdfs_url) as response:
                    if response.status == 200:
                        logger.info(f"HDFS路径验证成功: {hdfs_path}")
                        return True
                    else:
                        logger.warning(f"HDFS路径不存在或验证失败: {hdfs_path}")
                        return False

        except Exception as e:
            logger.error(f"HDFS路径验证异常: {e}")
            return False

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

            # 🆕 Hive特殊配置
            if mapped_type == 'hive':
                config.update({
                    'namenode_host': target_source.get('namenode_host', '192.142.76.242'),
                    'namenode_port': target_source.get('namenode_port', '8020'),
                    'base_path': target_source.get('base_path', '/user/hive/warehouse'),
                    'storage_format': 'ORC',
                    'compression': 'snappy',
                    'field_delimiter': target_source.get('field_delimiter', '\t'),
                    'add_ods_prefix': target_source.get('add_ods_prefix', True),
                    'partition_column': 'dt'
                })

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

    def _deep_clean_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """深度清理配置，移除循环引用和不可序列化对象"""
        import copy
        import json

        if not isinstance(config, dict):
            return config

        clean_config = {}
        exclude_keys = {'__class__', '__dict__', 'class', 'classLoader', 'module'}

        for key, value in config.items():
            if key in exclude_keys or callable(value):
                continue

            try:
                if isinstance(value, dict):
                    clean_config[key] = self._deep_clean_config(value)
                elif isinstance(value, list):
                    clean_config[key] = [
                        self._deep_clean_config(item) if isinstance(item, dict) else item
                        for item in value
                    ]
                else:
                    # 测试是否可序列化
                    json.dumps(value)
                    clean_config[key] = value
            except (TypeError, ValueError, UnicodeDecodeError):
                # 如果不能序列化，转为字符串
                clean_config[key] = str(value) if value is not None else None

        return clean_config

# 全局智能同步服务实例
smart_sync_service = SmartSyncService()
