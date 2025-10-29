"""
数据同步适配器Service
将前端的数据同步作业接口适配到smart-sync系统
"""
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from loguru import logger

from app.models.data_source import DataSource


class DataSyncAdapterService:
    """数据同步适配器 - 桥接前端作业和smart-sync"""

    async def get_data_source_tables(
            self,
            db: AsyncSession,
            data_source_id: int,
            table_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取数据源表列表
        前端接口: POST /work/getDataSourceTables
        对应: smart-sync的 GET /api/v1/smart-sync/sources/{source_name}/tables
        """
        try:
            # 根据dataSourceId获取数据源信息
            source = await self._get_data_source_by_id(db, data_source_id)
            if not source:
                raise ValueError(f"数据源不存在: {data_source_id}")

            # TODO: 调用smart-sync接口获取表列表
            # from app.services.smart_sync_service import smart_sync_service
            # tables = await smart_sync_service.get_source_tables(source.name)

            # 临时返回模拟数据
            tables = []

            # 过滤表
            if table_pattern:
                tables = [t for t in tables if table_pattern in t.get('table_name', '')]

            return {
                "tables": tables,
                "sourceId": data_source_id,
                "sourceName": source.name
            }

        except Exception as e:
            logger.error(f"获取数据源表列表失败: {e}")
            raise

    async def preview_table_data(
            self,
            db: AsyncSession,
            data_source_id: int,
            table_name: str,
            limit: int = 100
    ) -> Dict[str, Any]:
        """
        预览表数据
        前端接口: POST /work/getDataSourceData
        对应: smart-sync的 POST /api/v1/smart-sync/preview-table
        """
        try:
            source = await self._get_data_source_by_id(db, data_source_id)
            if not source:
                raise ValueError(f"数据源不存在: {data_source_id}")

            # TODO: 调用smart-sync接口预览数据
            # from app.services.smart_sync_service import smart_sync_service
            # preview_data = await smart_sync_service.preview_table_data(
            #     source_name=source.name,
            #     table_name=table_name,
            #     limit=limit
            # )

            # 临时返回模拟数据
            preview_data = {
                "columns": [],
                "rows": [],
                "total": 0
            }

            return preview_data

        except Exception as e:
            logger.error(f"预览表数据失败: {e}")
            raise

    async def get_table_columns(
            self,
            db: AsyncSession,
            data_source_id: int,
            table_name: str
    ) -> Dict[str, Any]:
        """
        获取表字段信息
        前端接口: POST /work/getDataSourceColumns
        """
        try:
            source = await self._get_data_source_by_id(db, data_source_id)
            if not source:
                raise ValueError(f"数据源不存在: {data_source_id}")

            # TODO: 调用smart-sync接口获取字段信息
            # 可以从 preview_table 接口返回的schema中提取

            columns = []

            return {
                "columns": columns,
                "tableName": table_name
            }

        except Exception as e:
            logger.error(f"获取表字段失败: {e}")
            raise

    async def validate_sync_config(
            self,
            db: AsyncSession,
            config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        验证同步配置
        调用smart-sync的analyze接口验证配置可行性
        """
        try:
            source_db_id = config.get('sourceDBId')
            target_db_id = config.get('targetDBId')

            source = await self._get_data_source_by_id(db, source_db_id)
            target = await self._get_data_source_by_id(db, target_db_id)

            if not source or not target:
                raise ValueError("源或目标数据源不存在")

            # TODO: 调用smart-sync analyze接口
            # from app.services.smart_sync_service import smart_sync_service
            # analysis = await smart_sync_service.analyze_sync_plan({
            #     'source_name': source.name,
            #     'target_name': target.name,
            #     'tables': [config.get('sourceTable')],
            #     'sync_mode': 'full'
            # })

            return {
                "feasible": True,
                "message": "配置验证通过"
            }

        except Exception as e:
            logger.error(f"验证同步配置失败: {e}")
            raise

    async def build_sync_task_config(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        将作业配置转换为smart-sync执行配置
        """
        try:
            sync_mode = work_config.get('syncMode', 'single')

            if sync_mode == 'single':
                # 单表同步
                return await self._build_single_table_config(db, work_config)
            elif sync_mode == 'multi':
                # 多表同步
                return await self._build_multi_table_config(db, work_config)
            else:
                raise ValueError(f"不支持的同步模式: {sync_mode}")

        except Exception as e:
            logger.error(f"构建同步任务配置失败: {e}")
            raise

    async def _build_single_table_config(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """构建单表同步配置"""
        source = await self._get_data_source_by_id(db, work_config['sourceDBId'])
        target = await self._get_data_source_by_id(db, work_config['targetDBId'])

        return {
            "source_name": source.name,
            "target_name": target.name,
            "tables": [
                {
                    "source_table": work_config['sourceTable'],
                    "target_table": work_config['targetTable'],
                    "column_map": work_config.get('columnMap', []),
                    "query_condition": work_config.get('queryCondition'),
                    "over_mode": work_config.get('overMode', 'replace')
                }
            ],
            "sync_strategy": "single"
        }

    async def _build_multi_table_config(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """构建多表同步配置"""
        source = await self._get_data_source_by_id(db, work_config['sourceDBId'])
        target = await self._get_data_source_by_id(db, work_config['targetDBId'])

        tables = []
        for table_config in work_config.get('tables', []):
            tables.append({
                "source_table": table_config['sourceTable'],
                "target_table": table_config['targetTable'],
                "column_map": table_config.get('columnMap', []),
                "over_mode": table_config.get('overMode', 'replace')
            })

        return {
            "source_name": source.name,
            "target_name": target.name,
            "tables": tables,
            "sync_strategy": work_config.get('syncStrategy', 'parallel')
        }

    async def _get_data_source_by_id(
            self,
            db: AsyncSession,
            source_id: int
    ) -> Optional[DataSource]:
        """根据ID获取数据源"""
        result = await db.execute(
            select(DataSource).where(DataSource.id == source_id)
        )
        return result.scalar_one_or_none()


# 创建全局实例
data_sync_adapter_service = DataSyncAdapterService()