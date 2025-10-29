"""
数据同步执行器 - 对接smart-sync系统
"""
from typing import Dict, Any, Optional
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
            # 验证配置
            validation = await self.validate_config(work_config)
            if not validation["valid"]:
                return {
                    "success": False,
                    "message": "配置验证失败",
                    "data": None,
                    "error": ", ".join(validation["errors"])
                }

            # 构建smart-sync配置
            sync_config = await data_sync_adapter_service.build_sync_task_config(
                db, work_config
            )

            # TODO: 调用smart-sync执行接口
            # from app.services.smart_sync_service import smart_sync_service
            # result = await smart_sync_service.execute_sync(sync_config)

            # 临时返回模拟结果
            logger.info(f"执行数据同步: {sync_config}")

            return {
                "success": True,
                "message": "数据同步成功",
                "data": {
                    "syncMode": work_config.get('syncMode'),
                    "sourceTable": work_config.get('sourceTable'),
                    "targetTable": work_config.get('targetTable'),
                    "rowsSynced": 0,
                    "duration": 0
                },
                "error": None
            }

        except Exception as e:
            logger.error(f"数据同步执行失败: {e}")
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