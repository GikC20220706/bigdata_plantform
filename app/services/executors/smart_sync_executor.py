"""
数据同步执行器 - 对接smart-sync系统
"""
from typing import Dict, Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from .base_executor import JobExecutor
from app.services.data_sync_adapter_service import data_sync_adapter_service


class SmartSyncExecutor(JobExecutor):
    """数据同步执行器 - 复用smart-sync"""

    def __init__(self):
        super().__init__("smart_sync_executor")

    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行数据同步作业"""
        try:
            logger.info(f"🚀 开始执行数据同步: {instance_id}")
            logger.info(f"📋 配置: {work_config}")

            # 1. 查询数据源信息
            from app.models.data_source import DataSource
            from sqlalchemy import select
            import os

            source_result = await db.execute(
                select(DataSource).where(DataSource.id == work_config['sourceId'])
            )
            source_ds = source_result.scalar_one_or_none()

            target_result = await db.execute(
                select(DataSource).where(DataSource.id == work_config['targetId'])
            )
            target_ds = target_result.scalar_one_or_none()

            if not source_ds or not target_ds:
                raise ValueError("源或目标数据源不存在")

            # 从 connection_config 中提取配置
            source_conn_config = source_ds.connection_config or {}
            target_conn_config = target_ds.connection_config or {}

            # 2. 构建源配置
            source_config = {
                'type': source_ds.source_type.lower(),
                'name': source_ds.name,
                'table': work_config['sourceTable'],
                'columns': [col['code'] for col in work_config.get('sourceColumns', [])],
                'where': work_config.get('whereCondition', '')
            }

            # MySQL/PostgreSQL等需要连接信息
            if source_ds.source_type.lower() in ['mysql', 'postgresql', 'kingbase']:
                source_config.update({
                    'host': source_conn_config.get('host'),
                    'port': source_conn_config.get('port', 3306),
                    'database': source_conn_config.get('database'),
                    'username': source_conn_config.get('username'),
                    'password': source_conn_config.get('password')
                })

            # 3. 构建目标配置
            target_config = {
                'type': target_ds.source_type.lower(),
                'name': target_ds.name,
                'table': work_config['targetTable'],
                'columns': [col['code'] for col in work_config.get('targetColumns', [])]
            }

            # ✅ Hive 特殊处理
            if target_ds.source_type.lower() == 'hive':
                # 从环境变量或配置中获取 Hive 相关信息
                namenode_host = os.getenv('HIVE_SERVER_HOST', '192.142.76.242')
                namenode_port = os.getenv('HADOOP_NAMENODE_PORT', '8020')
                database = target_conn_config.get('database', 'default')

                # 生成当前日期分区
                from datetime import datetime
                current_date = datetime.now().strftime('%Y-%m-%d')

                # 生成 HDFS 路径
                base_path = '/user/hive/warehouse'
                table_name = work_config['targetTable']

                if database and database != 'default':
                    hdfs_path = f"{base_path}/{database}.db/{table_name}/dt={current_date}"
                else:
                    hdfs_path = f"{base_path}/{table_name}/dt={current_date}"

                target_config.update({
                    'namenode_host': namenode_host,
                    'namenode_port': namenode_port,
                    'database': database,
                    'hdfs_path': hdfs_path,
                    'file_type': 'orc',
                    'file_name': f'{table_name}_data',
                    'partition_column': 'dt',
                    'partition_value': current_date,
                    'compression': 'snappy'
                })

                logger.info(f"✅ Hive配置: namenode={namenode_host}:{namenode_port}")
                logger.info(f"✅ HDFS路径: {hdfs_path}")

            else:
                # MySQL/PostgreSQL等
                target_config.update({
                    'host': target_conn_config.get('host'),
                    'port': target_conn_config.get('port', 3306),
                    'database': target_conn_config.get('database'),
                    'username': target_conn_config.get('username'),
                    'password': target_conn_config.get('password')
                })

            # 4. 构建 DataX 配置
            from app.services.datax_service import DataXIntegrationService
            datax_service = DataXIntegrationService()

            sync_config = {
                'task_id': instance_id,
                'source': source_config,
                'target': target_config,
                'column_mapping': work_config.get('columnMapping', []),
                'sync_mode': work_config.get('syncMode', 'replace')
            }

            logger.info(f"📦 DataX配置构建完成")

            # 5. 执行同步
            result = await datax_service.create_sync_task(sync_config)

            if result.get('success'):
                return {
                    "success": True,
                    "message": "数据同步成功",
                    "data": result,
                    "error": None
                }
            else:
                return {
                    "success": False,
                    "message": "数据同步失败",
                    "data": None,
                    "error": result.get('error')
                }

        except Exception as e:
            logger.error(f"❌ 数据同步执行失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "message": "同步失败",
                "data": None,
                "error": str(e)
            }

    async def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证配置"""
        errors = []

        if not config.get('sourceDBId'):
            errors.append("缺少sourceDBId")

        if not config.get('targetDBId'):
            errors.append("缺少targetDBId")

        sync_mode = config.get('syncMode', 'single')

        if sync_mode == 'single':
            if not config.get('sourceTable'):
                errors.append("缺少sourceTable")
            if not config.get('targetTable'):
                errors.append("缺少targetTable")
        elif sync_mode == 'multi':
            tables = config.get('tables', [])
            if not tables:
                errors.append("多表同步模式下缺少tables配置")
            else:
                for i, table in enumerate(tables):
                    if not table.get('sourceTable'):
                        errors.append(f"表{i + 1}缺少sourceTable")
                    if not table.get('targetTable'):
                        errors.append(f"表{i + 1}缺少targetTable")
        else:
            errors.append(f"不支持的同步模式: {sync_mode}")

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }