# app/api/v1/smart_sync.py
"""
智能数据同步API - 支持拖拽式可视化操作
"""
import asyncio

from fastapi import APIRouter, HTTPException, BackgroundTasks, Body, Query
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from app.services.smart_sync_service import smart_sync_service
from app.utils.response import create_response
from loguru import logger

router = APIRouter()


class TableSyncRequest(BaseModel):
    """表同步请求模型"""
    source_table: str = Field(..., description="源表名")
    target_table: str = Field(..., description="目标表名")
    create_if_not_exists: bool = Field(True, description="目标表不存在时是否自动创建")


class SmartSyncRequest(BaseModel):
    """智能同步请求模型"""
    source_name: str = Field(..., description="源数据源名称")
    target_name: str = Field(..., description="目标数据源名称")
    tables: List[TableSyncRequest] = Field(..., description="要同步的表列表")
    sync_mode: str = Field("multiple", description="同步模式: single/multiple/database")
    auto_create_tables: bool = Field(True, description="是否自动创建目标表")
    parallel_jobs: Optional[int] = Field(None, description="并行作业数，不指定则自动推荐")


@router.get("/sources/available", summary="获取可用的数据源列表")
async def get_available_sources():
    """获取所有可用的数据源，用于拖拽界面"""
    try:
        # 修复1: 使用新的基础数据源列表方法
        sources_result = await smart_sync_service.integration_service.get_data_sources_list_basic()

        # 修复2: 不查询表信息，避免阻塞
        enhanced_sources = []
        for source in sources_result:
            enhanced_source = {
                **source,
                "tables": [],  # 不查询表信息
                "total_tables": "点击查看",  # 延迟加载
                "lazy_load": True
            }
            enhanced_sources.append(enhanced_source)

        return create_response(
            data={
                "sources": enhanced_sources,
                "total_sources": len(enhanced_sources),
                "connected_sources": len([s for s in enhanced_sources if s.get('status') == 'connected'])
            },
            message="获取可用数据源成功"
        )

    except Exception as e:
        logger.error(f"获取可用数据源失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取可用数据源失败: {str(e)}")


@router.get("/sources/{source_name}/tables", summary="获取指定数据源的表列表")
async def get_source_tables(source_name: str, limit: int = Query(50, description="限制返回表数量")):
    """按需获取数据源表列表"""
    try:
        tables_result = await smart_sync_service.integration_service.get_tables(
            source_name, limit=limit
        )

        if tables_result.get('success'):
            return create_response(
                data=tables_result,
                message=f"获取数据源 {source_name} 的表列表成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=tables_result.get('error', '获取表列表失败')
            )
    except Exception as e:
        logger.error(f"获取表列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取表列表失败: {str(e)}")

@router.post("/analyze", summary="分析同步计划")
async def analyze_sync_plan(request: SmartSyncRequest):
    """分析用户拖拽的同步计划，生成智能建议"""
    try:
        # 转换请求格式
        sync_request = {
            "source_name": request.source_name,
            "target_name": request.target_name,
            "tables": [
                {
                    "source_table": table.source_table,
                    "target_table": table.target_table
                }
                for table in request.tables
            ],
            "sync_mode": request.sync_mode,
            "auto_create_tables": request.auto_create_tables,
            "parallel_jobs": request.parallel_jobs
        }

        # 执行分析
        analysis_result = await smart_sync_service.analyze_sync_plan(sync_request)

        if analysis_result.get('success'):
            return create_response(
                data=analysis_result,
                message="同步计划分析完成"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=analysis_result.get('error', '分析同步计划失败')
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"分析同步计划失败: {e}")
        raise HTTPException(status_code=500, detail=f"分析同步计划失败: {str(e)}")


@router.post("/execute", summary="执行智能同步")
async def execute_smart_sync(
        background_tasks: BackgroundTasks,
        sync_plan: Dict[str, Any] = Body(..., description="经过分析的同步计划")
):
    """执行智能数据同步"""
    try:
        # 验证同步计划
        if not sync_plan.get('sync_plans'):
            raise HTTPException(status_code=400, detail="同步计划为空")

        # 创建同步任务ID
        sync_id = f"smart_sync_{int(datetime.now().timestamp())}"

        # 在后台执行同步
        background_tasks.add_task(
            _execute_sync_background,
            sync_id,
            sync_plan
        )

        return create_response(
            data={
                "sync_id": sync_id,
                "status": "started",
                "total_tables": len(sync_plan['sync_plans']),
                "estimated_time_minutes": sync_plan.get('total_estimated_time_minutes', 0)
            },
            message="智能同步任务已启动"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启动智能同步失败: {e}")
        raise HTTPException(status_code=500, detail=f"启动智能同步失败: {str(e)}")


@router.get("/status/{sync_id}", summary="获取同步状态")
async def get_sync_status(sync_id: str):
    """获取智能同步任务的执行状态"""
    try:
        # 从Redis或数据库获取同步状态
        status = await _get_sync_status_from_cache(sync_id)

        if not status:
            raise HTTPException(status_code=404, detail="同步任务不存在")

        return create_response(
            data=status,
            message="获取同步状态成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取同步状态失败: {str(e)}")


@router.post("/preview-table", summary="预览表数据")
async def preview_table_data(
        source_name: str = Body(..., description="数据源名称"),
        table_name: str = Body(..., description="表名"),
        limit: int = Body(10, description="预览行数")
):
    """预览表数据，支持拖拽前的数据查看"""
    try:
        result = await smart_sync_service.integration_service.preview_data_source(
            source_name=source_name,
            table_name=table_name,
            limit=limit
        )

        if result.get('success'):
            return create_response(
                data=result,
                message="表数据预览成功"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get('error', '预览表数据失败')
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"预览表数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"预览表数据失败: {str(e)}")


@router.post("/validate-target", summary="验证目标配置")
async def validate_target_config(
        target_name: str = Body(..., description="目标数据源名称"),
        target_tables: List[str] = Body(..., description="目标表名列表")
):
    """验证目标数据源和表配置"""
    try:
        # 修改：使用新的基础数据源列表方法
        sources_result = await smart_sync_service.integration_service.get_data_sources_list_basic()
        target_source = next((s for s in sources_result if s['name'] == target_name), None)

        if not target_source:
            raise HTTPException(status_code=404, detail=f"目标数据源 {target_name} 不存在")

        if target_source.get('status') != 'connected':
            raise HTTPException(status_code=400, detail=f"目标数据源 {target_name} 连接失败")

        # 检查目标表状态
        tables_result = await smart_sync_service.integration_service.get_tables(target_name)
        existing_tables = []
        if tables_result.get('success'):
            existing_table_names = [t.get('table_name', t.get('name', '')) for t in tables_result.get('tables', [])]
            existing_tables = [t for t in target_tables if t in existing_table_names]

        validation_result = {
            "target_source_valid": True,
            "target_source_info": {
                "name": target_source['name'],
                "type": target_source['type'],
                "host": target_source.get('host', ''),
                "status": target_source['status']
            },
            "table_validations": [
                {
                    "table_name": table,
                    "exists": table in existing_tables,
                    "can_create": True,  # 假设都可以创建
                    "validation_status": "exists" if table in existing_tables else "will_create"
                }
                for table in target_tables
            ],
            "summary": {
                "total_tables": len(target_tables),
                "existing_tables": len(existing_tables),
                "new_tables": len(target_tables) - len(existing_tables)
            }
        }

        return create_response(
            data=validation_result,
            message="目标配置验证完成"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"验证目标配置失败: {e}")
        raise HTTPException(status_code=500, detail=f"验证目标配置失败: {str(e)}")


@router.get("/templates", summary="获取同步模板")
async def get_sync_templates():
    """获取常用的同步模板，方便用户快速配置"""
    try:
        templates = [
            {
                "id": "mysql_to_hive",
                "name": "MySQL到Hive数据仓库",
                "description": "将MySQL业务数据同步到Hive数据仓库",
                "source_type": "mysql",
                "target_type": "hive",
                "recommended_settings": {
                    "sync_mode": "multiple",
                    "parallel_jobs": 4,
                    "auto_create_tables": True
                },
                "common_tables": ["users", "orders", "products", "categories"]
            },
            {
                "id": "oracle_to_mysql",
                "name": "Oracle到MySQL迁移",
                "description": "Oracle系统数据迁移到MySQL",
                "source_type": "oracle",
                "target_type": "mysql",
                "recommended_settings": {
                    "sync_mode": "database",
                    "parallel_jobs": 6,
                    "auto_create_tables": True
                },
                "common_tables": ["employees", "departments", "salaries"]
            },
            {
                "id": "postgresql_to_hive",
                "name": "PostgreSQL到Hive分析",
                "description": "PostgreSQL业务数据同步到Hive进行分析",
                "source_type": "postgresql",
                "target_type": "hive",
                "recommended_settings": {
                    "sync_mode": "multiple",
                    "parallel_jobs": 4,
                    "auto_create_tables": True
                },
                "common_tables": ["events", "sessions", "user_actions"]
            }
        ]

        return create_response(
            data={"templates": templates},
            message="获取同步模板成功"
        )

    except Exception as e:
        logger.error(f"获取同步模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取同步模板失败: {str(e)}")


@router.get("/history", summary="获取同步历史")
async def get_sync_history(
        page: int = 1,
        page_size: int = 20,
        status: Optional[str] = None,
        source_name: Optional[str] = None
):
    """获取智能同步的历史记录"""
    try:
        # 模拟历史记录数据
        mock_history = [
            {
                "sync_id": f"smart_sync_{1640000000 + i}",
                "source_name": f"MySQL-{i % 3 + 1}",
                "target_name": f"Hive-{i % 2 + 1}",
                "status": ["completed", "failed", "running"][i % 3],
                "start_time": datetime.now().replace(hour=i % 24, minute=i % 60),
                "end_time": datetime.now().replace(hour=(i + 1) % 24, minute=(i + 10) % 60) if i % 3 != 2 else None,
                "total_tables": i % 5 + 1,
                "success_tables": i % 5 if i % 3 == 0 else (i % 5 + 1) // 2,
                "total_records": (i + 1) * 10000,
                "created_by": "用户A" if i % 2 == 0 else "用户B"
            }
            for i in range(50)
        ]

        # 应用过滤条件
        if status:
            mock_history = [h for h in mock_history if h['status'] == status]
        if source_name:
            mock_history = [h for h in mock_history if source_name.lower() in h['source_name'].lower()]

        # 分页
        total = len(mock_history)
        offset = (page - 1) * page_size
        paginated_history = mock_history[offset:offset + page_size]

        return create_response(
            data={
                "history": paginated_history,
                "total": total,
                "page": page,
                "page_size": page_size,
                "has_more": offset + page_size < total
            },
            message="获取同步历史成功"
        )

    except Exception as e:
        logger.error(f"获取同步历史失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取同步历史失败: {str(e)}")


# 后台任务函数
async def _execute_sync_background(sync_id: str, sync_plan: Dict[str, Any]):
    """后台执行同步任务"""
    try:
        # 更新状态为执行中
        await _update_sync_status(sync_id, {
            "status": "running",
            "start_time": datetime.now(),
            "progress": 0,
            "current_step": "开始同步..."
        })

        # 执行智能同步
        result = await smart_sync_service.execute_smart_sync(sync_plan)

        # 更新最终状态
        final_status = {
            "status": "completed" if result.get('success') else "failed",
            "end_time": datetime.now(),
            "progress": 100,
            "result": result,
            "current_step": "同步完成" if result.get('success') else "同步失败"
        }

        await _update_sync_status(sync_id, final_status)

        logger.info(f"智能同步任务完成: {sync_id}, 成功: {result.get('success')}")

    except Exception as e:
        logger.error(f"后台同步任务失败 {sync_id}: {e}")
        await _update_sync_status(sync_id, {
            "status": "error",
            "end_time": datetime.now(),
            "progress": 0,
            "error": str(e),
            "current_step": "执行异常"
        })


async def _update_sync_status(sync_id: str, status_data: Dict[str, Any]):
    """更新同步状态到缓存"""
    try:
        # 这里应该存储到Redis或数据库
        # 目前使用内存存储（仅用于演示）
        if not hasattr(_update_sync_status, 'cache'):
            _update_sync_status.cache = {}

        if sync_id not in _update_sync_status.cache:
            _update_sync_status.cache[sync_id] = {}

        _update_sync_status.cache[sync_id].update(status_data)

    except Exception as e:
        logger.error(f"更新同步状态失败: {e}")


async def _get_sync_status_from_cache(sync_id: str) -> Optional[Dict[str, Any]]:
    """从缓存获取同步状态"""
    try:
        if hasattr(_update_sync_status, 'cache'):
            return _update_sync_status.cache.get(sync_id)
        return None
    except Exception as e:
        logger.error(f"获取同步状态失败: {e}")
        return None

async def _get_source_tables_safe(integration_service, source_name: str, timeout: int = 10):
    """安全获取数据源表信息，带超时保护"""
    try:
        # 使用asyncio.wait_for添加超时保护
        return await asyncio.wait_for(
            integration_service.get_tables(source_name, limit=100),  # 限制查询100张表
            timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.warning(f"获取数据源 {source_name} 表信息超时({timeout}秒)")
        return {
            "success": False,
            "error": "查询超时",
            "tables": []
        }
    except Exception as e:
        logger.warning(f"获取数据源 {source_name} 表信息异常: {e}")
        return {
            "success": False,
            "error": str(e),
            "tables": []
        }