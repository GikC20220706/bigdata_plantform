"""
作业API端点
提供作业的增删改查、配置管理、运行控制等功能
"""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Body, BackgroundTasks
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.models import JobWorkInstance, JobInstanceStatus
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
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db)
):
    """运行单个作业（用于测试）"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.run_single_work(
            db=db,
            work_id=data.workId,
            trigger_user=None,  # TODO: 从认证中获取
            context=data.context,
            background_tasks=background_tasks
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

        # 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(JobWorkInstance.instance_id == work_instance_id)
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        # 直接从数据库读取日志
        response_data = {
            "status": work_instance.status.value,
            "submitLog": work_instance.submit_log or "",
            "runningLog": work_instance.running_log or ""
        }

        # 根据 log_type 设置 log 字段(前端期望的字段名)
        if log_type == "submit" or log_type == "all":
            response_data["log"] = work_instance.submit_log or ""
        elif log_type == "running":
            response_data["log"] = work_instance.running_log or ""
            # 运行日志前端期望的字段名是 yarnLog
            response_data["yarnLog"] = work_instance.running_log or ""

        return create_response(
            data=response_data,
            message="获取成功"
        )
    except HTTPException:
        raise
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

@router.get("/instance/{work_instance_id}/result", summary="获取作业实例结果")
async def get_work_instance_result(
        work_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的执行结果"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        result = await job_work_run_service.get_work_instance_result(
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
        logger.error(f"获取作业实例结果失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{work_instance_id}/rerun", summary="重跑作业实例")
async def rerun_work_instance(
        work_instance_id: str,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db)
):
    """基于作业实例重新运行作业"""
    try:
        from app.services.job_work_run_service import job_work_run_service

        # 获取原作业实例
        result = await db.execute(
            select(JobWorkInstance).where(
                JobWorkInstance.instance_id == work_instance_id
            )
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(
                status_code=404,
                detail=f"作业实例不存在: {work_instance_id}"
            )

        # 使用原作业的work_id重新运行
        new_result = await job_work_run_service.run_single_work(
            db=db,
            work_id=work_instance.work_id,
            trigger_user=None,
            context={},
            background_tasks=background_tasks
        )

        logger.info(
            f"重跑作业: 原实例={work_instance_id}, "
            f"新实例={new_result['workInstanceId']}"
        )

        return create_response(
            data={
                "originalInstanceId": work_instance_id,
                "newInstanceId": new_result["workInstanceId"],
                "workflowInstanceId": new_result.get("workflowInstanceId"),
                "status": new_result["status"],
                "message": "作业已重新提交运行"
            },
            message="作业已重新提交运行"
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"重跑作业失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/instance/{work_instance_id}", summary="删除作业实例")
async def delete_work_instance(
        work_instance_id: str,
        force: bool = Query(False, description="是否强制删除"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除作业实例

    - 默认只能删除已完成的实例（SUCCESS、FAIL、ABORT）
    - PENDING（等待中）状态的作业也可以删除
    - 只有 RUNNING（运行中）状态的作业不能删除
    - 使用 force=true 可以强制删除任何状态的实例
    """
    try:
        from sqlalchemy import select, delete
        from app.models import JobWorkInstance

        # 1. 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(
                JobWorkInstance.instance_id == work_instance_id
            )
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        # 2. 检查状态 - 关键修改
        if not force:
            # ✅ 只有真正在运行中的作业不能删除
            # PENDING（等待中）、SUCCESS、FAIL、ABORT 都可以删除
            if work_instance.status == JobInstanceStatus.RUNNING:
                raise HTTPException(
                    status_code=400,
                    detail=f"作业实例正在运行中，无法删除。如需强制删除，请使用force参数。"
                )

            # ✅ PENDING 状态也可以删除
            # 因为它还没开始执行，删除是安全的

        # 3. 删除作业实例
        await db.execute(
            delete(JobWorkInstance).where(
                JobWorkInstance.instance_id == work_instance_id
            )
        )

        await db.commit()

        logger.info(f"作业实例删除成功: {work_instance_id}")

        return create_response(
            data={
                "workInstanceId": work_instance_id,
                "workName": work_instance.work_name,
                "status": work_instance.status.value
            },
            message="作业实例删除成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"删除作业实例失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/instance/{work_instance_id}/jsonpath", summary="获取作业实例JsonPath解析结果")
async def get_work_instance_jsonpath(
        work_instance_id: str,
        db: AsyncSession = Depends(get_async_db)
):
    """获取作业实例的JsonPath解析结果"""
    try:
        from app.services.job_work_run_service import job_work_run_service
        from sqlalchemy import select
        from app.models import JobWorkInstance

        # 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(JobWorkInstance.instance_id == work_instance_id)
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        # 获取结果数据
        result_data = work_instance.result_data

        if not result_data:
            return create_response(data=[], message="暂无结果数据")

        # 解析JsonPath
        import json

        # 如果result_data是字符串,先解析
        if isinstance(result_data, str):
            try:
                result_data = json.loads(result_data)
            except:
                pass

        # 提取所有可能的路径
        paths = []

        def extract_paths(obj, current_path="$"):
            """递归提取所有JsonPath路径"""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{current_path}.{key}"

                    # 格式化值的显示
                    if isinstance(value, (dict, list)):
                        display_value = f"[{type(value).__name__}]"
                    elif isinstance(value, str):
                        display_value = value[:100] if len(str(value)) > 100 else value
                    else:
                        display_value = str(value)

                    paths.append({
                        "value": display_value,
                        "jsonPath": new_path,
                        "copyValue": new_path  # ✅ 添加 copyValue 字段用于复制
                    })

                    # 递归处理嵌套结构
                    if isinstance(value, (dict, list)):
                        extract_paths(value, new_path)

            elif isinstance(obj, list):
                for i, item in enumerate(obj[:10]):  # 只处理前10个元素
                    new_path = f"{current_path}[{i}]"

                    # 格式化值的显示
                    if isinstance(item, (dict, list)):
                        display_value = f"[{type(item).__name__}]"
                    elif isinstance(item, str):
                        display_value = item[:100] if len(str(item)) > 100 else item
                    else:
                        display_value = str(item)

                    paths.append({
                        "value": display_value,
                        "jsonPath": new_path,
                        "copyValue": new_path  # ✅ 添加 copyValue 字段用于复制
                    })

                    # 递归处理嵌套结构
                    if isinstance(item, (dict, list)):
                        extract_paths(item, new_path)

        extract_paths(result_data)

        return create_response(
            data=paths[:100],  # 最多返回100条
            message="获取成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取JsonPath解析结果失败: {e}")
        import traceback
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{work_instance_id}/tablepath", summary="获取表格路径解析")
async def get_work_instance_tablepath(
        work_instance_id: str,
        table_row: int = Body(..., description="表格行号"),
        table_col: int = Body(..., description="表格列号"),
        db: AsyncSession = Depends(get_async_db)
):
    """获取表格特定位置的数据"""
    try:
        from sqlalchemy import select
        from app.models import JobWorkInstance
        import json

        # 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(JobWorkInstance.instance_id == work_instance_id)
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        result_data = work_instance.result_data
        if not result_data:
            return create_response(data={"value": "", "copyValue": ""}, message="暂无结果数据")

        # 解析result_data
        if isinstance(result_data, str):
            try:
                result_data = json.loads(result_data)
            except:
                pass

        # 尝试从不同的数据结构中提取表格数据
        table_data = None

        # 情况1: resultData包含columns和rows
        if isinstance(result_data, dict):
            if 'columns' in result_data and 'rows' in result_data:
                columns = result_data['columns']
                rows = result_data['rows']
                table_data = rows
            elif 'data' in result_data:
                # 情况2: data字段包含数组
                if isinstance(result_data['data'], list):
                    table_data = result_data['data']
        elif isinstance(result_data, list):
            # 情况3: result_data直接是数组
            table_data = result_data

        if not table_data or table_row >= len(table_data):
            return create_response(
                data={"value": "", "copyValue": ""},
                message="行号超出范围"
            )

        row_data = table_data[table_row]

        # 获取列值
        if isinstance(row_data, dict):
            keys = list(row_data.keys())
            if table_col >= len(keys):
                return create_response(
                    data={"value": "", "copyValue": ""},
                    message="列号超出范围"
                )
            col_key = keys[table_col]
            value = row_data[col_key]
            json_path = f"$.data[{table_row}].{col_key}"
        elif isinstance(row_data, list):
            if table_col >= len(row_data):
                return create_response(
                    data={"value": "", "copyValue": ""},
                    message="列号超出范围"
                )
            value = row_data[table_col]
            json_path = f"$.data[{table_row}][{table_col}]"
        else:
            return create_response(
                data={"value": str(row_data), "copyValue": ""},
                message="不支持的数据格式"
            )

        return create_response(
            data={
                "value": str(value),
                "copyValue": json_path
            },
            message="获取成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取表格路径失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/instance/{work_instance_id}/regexpath", summary="获取正则匹配解析")
async def get_work_instance_regexpath(
        work_instance_id: str,
        regex_str: str = Body(..., description="正则表达式"),
        db: AsyncSession = Depends(get_async_db)
):
    """使用正则表达式提取数据"""
    try:
        from sqlalchemy import select
        from app.models import JobWorkInstance
        import json
        import re

        # 获取作业实例
        result = await db.execute(
            select(JobWorkInstance).where(JobWorkInstance.instance_id == work_instance_id)
        )
        work_instance = result.scalar_one_or_none()

        if not work_instance:
            raise HTTPException(status_code=404, detail="作业实例不存在")

        result_data = work_instance.result_data
        if not result_data:
            return create_response(
                data={"value": "", "copyValue": ""},
                message="暂无结果数据"
            )

        # 转换为字符串
        if isinstance(result_data, dict) or isinstance(result_data, list):
            text = json.dumps(result_data, ensure_ascii=False)
        else:
            text = str(result_data)

        # 执行正则匹配
        try:
            matches = re.findall(regex_str, text)
            if matches:
                # 如果有多个匹配,返回第一个
                value = matches[0] if isinstance(matches[0], str) else ', '.join(matches[0])
                return create_response(
                    data={
                        "value": value,
                        "copyValue": regex_str
                    },
                    message=f"匹配成功,找到 {len(matches)} 个结果"
                )
            else:
                return create_response(
                    data={"value": "无匹配结果", "copyValue": regex_str},
                    message="未找到匹配项"
                )
        except re.error as e:
            return create_response(
                data={"value": f"正则表达式错误: {str(e)}", "copyValue": ""},
                message="正则表达式格式错误"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"正则匹配失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))