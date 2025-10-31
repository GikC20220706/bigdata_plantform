"""
作业API端点
提供作业的增删改查、配置管理、运行控制等功能
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.schemas.job_workflow import (
    JobWorkCreate, JobWorkUpdate, JobWorkCopy, JobWorkConfigSave,
    JobWorkPageQuery, JobWorkRun, JobWorkStop
)
from app.services.job_work_service import job_work_service

router = APIRouter(prefix="/job-work", tags=["数据开发-作业"])


# ==================== 作业CRUD ====================

@router.post("/add", summary="创建作业")
async def create_work(
    data: JobWorkCreate,
    db: AsyncSession = Depends(get_async_db)
):
    """在作业流中创建新作业"""
    try:
        result = await job_work_service.create_work(db, data)

        return create_response(
            data=result,
            message="创建作业成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/page", summary="分页查询作业列表")
async def page_works(
    workflowId: int = Query(..., description="作业流ID"),
    page: int = Query(0, ge=0, description="页码（从0开始）"),
    pageSize: int = Query(10, ge=1, le=100, description="每页大小"),
    searchKeyWord: Optional[str] = Query(None, description="搜索关键词"),
    db: AsyncSession = Depends(get_async_db)
):
    """分页获取作业列表"""
    try:
        query_params = JobWorkPageQuery(
            workflowId=workflowId,
            page=page,
            pageSize=pageSize,
            searchKeyWord=searchKeyWord
        )

        result = await job_work_service.page_works(db, query_params)

        # 转换为响应格式
        content = []
        for work in result["content"]:
            content.append({
                "id": work.id,
                "workflowId": work.workflow_id,
                "name": work.name,
                "workType": work.work_type.value,
                "remark": work.remark,
                "status": work.status.value,
                "executor": work.executor,
                "createDateTime": work.created_at.isoformat() if work.created_at else None,
                "updateDateTime": work.updated_at.isoformat() if work.updated_at else None
            })

        return create_response(
            data={
                "content": content,
                "total": result["total"],
                "page": result["page"],
                "pageSize": result["pageSize"]
            },
            message="查询成功"
        )
    except Exception as e:
        logger.error(f"分页查询作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detail/{work_id}", summary="获取作业详情")
async def get_work_detail(
    work_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业详细信息"""
    try:
        work = await job_work_service.get_work_by_id(db, work_id)

        if not work:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={
                "id": work.id,
                "workflowId": work.workflow_id,
                "name": work.name,
                "workType": work.work_type.value,
                "remark": work.remark,
                "status": work.status.value,
                "config": work.config,
                "executor": work.executor,
                "createDateTime": work.created_at.isoformat() if work.created_at else None,
                "updateDateTime": work.updated_at.isoformat() if work.updated_at else None
            },
            message="获取成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取作业详情失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update", summary="更新作业")
async def update_work(
    data: JobWorkUpdate,
    db: AsyncSession = Depends(get_async_db)
):
    """更新作业基本信息"""
    try:
        work = await job_work_service.update_work(db, data)

        if not work:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={"id": work.id},
            message="更新成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete/{work_id}", summary="删除作业")
async def delete_work(
    work_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """删除作业"""
    try:
        success = await job_work_service.delete_work(db, work_id)

        if not success:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={"id": work_id},
            message="删除成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/copy", summary="复制作业")
async def copy_work(
    data: JobWorkCopy,
    db: AsyncSession = Depends(get_async_db)
):
    """复制现有作业"""
    try:
        result = await job_work_service.copy_work(db, data)

        return create_response(
            data=result,
            message="复制作业成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"复制作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 作业配置管理 ====================

@router.post("/saveConfig", summary="保存作业配置")
async def save_work_config(
    data: JobWorkConfigSave,
    db: AsyncSession = Depends(get_async_db)
):
    """保存作业的详细配置"""
    try:
        work = await job_work_service.save_work_config(db, data.workId, data.config)

        if not work:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={"workId": work.id},
            message="保存配置成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"保存作业配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/getConfig/{work_id}", summary="获取作业配置")
async def get_work_config(
    work_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业的详细配置"""
    try:
        config = await job_work_service.get_work_config(db, work_id)

        if config is None:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(data=config, message="获取成功")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取作业配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 状态管理 ====================

@router.post("/publish/{work_id}", summary="发布作业")
async def publish_work(
    work_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """发布作业上线"""
    try:
        work = await job_work_service.publish_work(db, work_id)

        if not work:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={"id": work.id, "status": work.status.value},
            message="发布成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"发布作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/offline/{work_id}", summary="下线作业")
async def offline_work(
    work_id: int,
    db: AsyncSession = Depends(get_async_db)
):
    """下线作业"""
    try:
        work = await job_work_service.offline_work(db, work_id)

        if not work:
            raise HTTPException(status_code=404, detail="作业不存在")

        return create_response(
            data={"id": work.id, "status": work.status.value},
            message="下线成功"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"下线作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 运行控制 ====================

@router.post("/run", summary="运行作业")
async def run_work(
    data: JobWorkRun,
    db: AsyncSession = Depends(get_async_db)
):
    """运行单个作业（用于测试）"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.run_single_work(
            db=db,
            work_id=data.workId,
            trigger_user=None,  # TODO: 从认证中获取
            context=data.context
        )

        # ✅ 确保返回的是字符串ID，不是对象
        return create_response(
            data={
                "workInstanceId": result["workInstanceId"],  # 字符串
                "workflowInstanceId": result.get("workflowInstanceId"),
                "status": result["status"],
                "message": result.get("message", "作业已提交运行")
            },
            message="作业已提交运行"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"运行作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop", summary="停止作业")
async def stop_work(
    data: JobWorkStop,
    db: AsyncSession = Depends(get_async_db)
):
    """停止正在运行的作业"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.stop_work(
            db=db,
            work_instance_id=data.workInstanceId
        )

        return create_response(
            data=result,
            message=result["message"]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"停止作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instance/{work_instance_id}", summary="获取作业实例状态")
async def get_work_instance_status(
    work_instance_id: str,
    db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的详细状态"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.get_work_instance_status(
            db=db,
            work_instance_id=work_instance_id
        )

        return create_response(
            data=result,
            message="获取成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取作业实例状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instance/{work_instance_id}/log", summary="获取作业实例日志")
async def get_work_instance_log(
        work_instance_id: str,
        log_type: str = Query("all", regex="^(submit|running|all)$", description="日志类型"),
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的执行日志"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        # 获取完整的实例信息（包括状态）
        instance_status = await job_work_run_service.get_work_instance_status(
            db=db,
            work_instance_id=work_instance_id
        )

        # 获取日志
        result = await job_work_run_service.get_work_instance_log(
            db=db,
            work_instance_id=work_instance_id,
            log_type=log_type
        )

        # ✅ 转换为前端期望的格式
        logs = result.get("logs", {})

        return create_response(
            data={
                "log": logs.get("submitLog", "") if log_type in ["submit", "all"] else logs.get("runningLog", ""),
                "status": instance_status.get("status"),
                "submitLog": logs.get("submitLog", ""),
                "runningLog": logs.get("runningLog", ""),
                "resultData": instance_status.get("resultData")
            },
            message="获取成功"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取作业日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 作业类型定义 ====================

@router.get("/types", summary="获取支持的作业类型列表")
async def get_work_types():
    """获取所有支持的作业类型"""
    work_types = [
        {
            "type": "EXE_JDBC",
            "name": "JDBC执行作业",
            "category": "SQL",
            "description": "执行SQL语句（INSERT/UPDATE/DELETE）",
            "configFields": ["dataSourceId", "sql", "timeout"]
        },
        {
            "type": "QUERY_JDBC",
            "name": "JDBC查询作业",
            "category": "SQL",
            "description": "执行SQL查询（SELECT）",
            "configFields": ["dataSourceId", "sql", "resultHandler"]
        },
        {
            "type": "SPARK_SQL",
            "name": "SparkSQL查询作业",
            "category": "SQL",
            "description": "在Spark集群上执行SQL",
            "configFields": ["clusterId", "sql", "saveMode"]
        },
        {
            "type": "DATA_SYNC_JDBC",
            "name": "数据同步作业",
            "category": "数据同步",
            "description": "数据库之间的数据同步",
            "configFields": ["sourceDBId", "targetDBId", "tables", "syncMode"]
        },
        {
            "type": "EXCEL_SYNC_JDBC",
            "name": "Excel导入作业",
            "category": "数据同步",
            "description": "从Excel文件导入数据到数据库",
            "configFields": ["excelFileId", "targetDBId", "targetTable"]
        },
        {
            "type": "DB_MIGRATE",
            "name": "整库迁移作业",
            "category": "数据同步",
            "description": "整个数据库的迁移",
            "configFields": ["sourceDBId", "targetDBId", "migrateMode"]
        },
        {
            "type": "BASH",
            "name": "Bash脚本作业",
            "category": "脚本",
            "description": "执行Bash shell脚本",
            "configFields": ["script", "workDir", "envVars"]
        },
        {
            "type": "PYTHON",
            "name": "Python脚本作业",
            "category": "脚本",
            "description": "执行Python脚本",
            "configFields": ["script", "pythonVersion", "dependencies"]
        },
        {
            "type": "CURL",
            "name": "HTTP请求作业",
            "category": "API",
            "description": "发送HTTP请求",
            "configFields": ["url", "method", "headers", "body"]
        },
        {
            "type": "API",
            "name": "API调用作业",
            "category": "API",
            "description": "调用RESTful API",
            "configFields": ["apiUrl", "method", "params", "auth"]
        },
        {
            "type": "SPARK_JAR",
            "name": "Spark Jar作业",
            "category": "大数据",
            "description": "运行自定义Spark应用",
            "configFields": ["jarFileId", "mainClass", "args", "sparkConf"]
        },
        {
            "type": "FLINK_SQL",
            "name": "Flink SQL作业",
            "category": "大数据",
            "description": "执行Flink SQL",
            "configFields": ["clusterId", "sql", "checkpointInterval"]
        },
        {
            "type": "FLINK_JAR",
            "name": "Flink Jar作业",
            "category": "大数据",
            "description": "运行自定义Flink应用",
            "configFields": ["jarFileId", "mainClass", "args", "flinkConf"]
        }
    ]

    return create_response(
        data={"workTypes": work_types},
        message="获取成功"
    )