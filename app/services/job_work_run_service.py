"""
单个作业运行控制Service
支持独立运行单个作业（用于测试）
"""
import asyncio
import uuid
from datetime import datetime
from typing import Dict, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from loguru import logger
from sqlalchemy.orm import selectinload

from app.models.job_work import JobWork
from app.models.job_instance import (
    JobWorkInstance, JobInstanceStatus, JobTriggerType, JobWorkflowInstance
)
from app.services.executors import executor_manager


class JobWorkRunService:
    """单个作业运行控制服务"""

    async def run_single_work(
            self,
            db: AsyncSession,
            work_id: int,
            trigger_user: Optional[str] = None,
            context: Optional[Dict[str, Any]] = None,
            background_tasks=None
    ) -> Dict[str, Any]:
        """
        单独运行作业（用于测试）
        """
        try:
            # 1. 获取作业
            work = await self._get_work(db, work_id)
            if not work:
                raise ValueError(f"作业不存在: {work_id}")

            # 2. 获取所属作业流
            workflow = work.workflow
            if not workflow:
                raise ValueError(f"作业流不存在")

            # 3. 创建临时作业流实例（用于单独测试作业）
            workflow_instance_id = f"WF_SINGLE_{uuid.uuid4().hex[:16].upper()}"

            workflow_instance = JobWorkflowInstance(
                workflow_instance_id=workflow_instance_id,
                workflow_id=workflow.id,
                workflow_name=workflow.name,
                status=JobInstanceStatus.RUNNING,
                trigger_type=JobTriggerType.MANUAL,
                last_modified_by=trigger_user,  # ✅ 改为 last_modified_by
                start_datetime=datetime.now()
            )
            db.add(workflow_instance)
            await db.commit()
            await db.refresh(workflow_instance)

            # 4. 创建作业实例
            work_instance_id = f"WORK_{uuid.uuid4().hex[:16].upper()}"

            work_instance = JobWorkInstance(
                instance_id=work_instance_id,
                workflow_instance_id=workflow_instance_id,
                work_id=work.id,
                work_name=work.name,
                work_type=work.work_type.value,
                status=JobInstanceStatus.PENDING
            )

            db.add(work_instance)
            await db.commit()
            await db.refresh(work_instance)

            # 5. 使用BackgroundTasks而不是asyncio.create_task
            if background_tasks:
                background_tasks.add_task(
                    self._execute_work_async,
                    work_instance_id,
                    work,
                    context or {}
                )
            else:
                # 如果没有background_tasks,就同步执行
                await self._execute_work_async(
                    work_instance_id,
                    work,
                    context or {}
                )

            return {
                "workInstanceId": work_instance_id,
                "workflowInstanceId": workflow_instance_id,
                "status": JobInstanceStatus.RUNNING.value,
                "message": "作业已提交运行"
            }

        except Exception as e:
            logger.error(f"运行作业失败: {e}")
            raise


    async def stop_work(
            self,
            db: AsyncSession,
            work_instance_id: str
    ) -> Dict[str, Any]:
        """
        停止作业

        Args:
            db: 数据库会话
            work_instance_id: 作业实例ID

        Returns:
            {
                "workInstanceId": str,
                "status": str,
                "message": str
            }
        """
        try:
            # 获取作业实例
            work_instance = await self._get_work_instance(db, work_instance_id)
            if not work_instance:
                raise ValueError(f"作业实例不存在: {work_instance_id}")

            # 检查状态
            if work_instance.status != JobInstanceStatus.RUNNING:
                return {
                    "workInstanceId": work_instance_id,
                    "status": work_instance.status.value,
                    "message": f"作业当前状态为 {work_instance.status.value}，无法停止"
                }

            # 更新状态为中止
            work_instance.status = JobInstanceStatus.ABORT
            work_instance.end_datetime = datetime.now()
            work_instance.error_message = "用户手动停止"

            await db.commit()

            logger.info(f"作业已停止: {work_instance_id}")

            return {
                "workInstanceId": work_instance_id,
                "status": JobInstanceStatus.ABORT.value,
                "message": "作业已停止"
            }

        except Exception as e:
            logger.error(f"停止作业失败: {e}")
            raise

    async def get_work_instance_status(
            self,
            db: AsyncSession,
            work_instance_id: str
    ) -> Dict[str, Any]:
        """获取作业实例状态"""
        try:
            work_instance = await self._get_work_instance(db, work_instance_id)
            if not work_instance:
                raise ValueError(f"作业实例不存在: {work_instance_id}")

            return {
                "workInstanceId": work_instance.instance_id,
                "workflowInstanceId": work_instance.workflow_instance_id,
                "workId": work_instance.work_id,
                "workName": work_instance.work_name,
                "workType": work_instance.work_type,
                "status": work_instance.status.value,
                "startDatetime": work_instance.start_datetime.isoformat() if work_instance.start_datetime else None,
                "endDatetime": work_instance.end_datetime.isoformat() if work_instance.end_datetime else None,
                "errorMessage": work_instance.error_message,
                "resultData": work_instance.result_data
            }

        except Exception as e:
            logger.error(f"获取作业实例状态失败: {e}")
            raise

    async def get_work_instance_log(
            self,
            db: AsyncSession,
            work_instance_id: str,
            log_type: str = "all"
    ) -> Dict[str, Any]:
        """获取作业实例日志"""
        try:
            work_instance = await self._get_work_instance(db, work_instance_id)
            if not work_instance:
                raise ValueError(f"作业实例不存在: {work_instance_id}")

            result = {
                "instanceId": work_instance_id,
                "status": work_instance.status.value
            }

            if log_type == "submit" or log_type == "all":
                # 前端期望的字段名是 log
                result["log"] = work_instance.submit_log or ""
                result["submitLog"] = work_instance.submit_log or ""

            if log_type == "running":
                # 运行日志前端期望的字段名是 yarnLog
                result["yarnLog"] = work_instance.running_log or ""
                result["runningLog"] = work_instance.running_log or ""

            return result

        except Exception as e:
            logger.error(f"获取作业实例日志失败: {e}")
            raise

    async def get_work_instance_result(
            self,
            db: AsyncSession,
            work_instance_id: str
    ) -> Dict[str, Any]:
        """获取作业实例结果"""
        try:
            work_instance = await self._get_work_instance(db, work_instance_id)
            if not work_instance:
                raise ValueError(f"作业实例不存在: {work_instance_id}")

            return {
                "instanceId": work_instance_id,
                "status": work_instance.status.value,
                "resultData": work_instance.result_data or {}
            }

        except Exception as e:
            logger.error(f"获取作业实例结果失败: {e}")
            raise
    # ==================== 私有方法 ====================

    async def _get_work(
            self,
            db: AsyncSession,
            work_id: int
    ) -> Optional[JobWork]:
        """获取作业"""
        result = await db.execute(
            select(JobWork)
            .where(JobWork.id == work_id)
            .options(
                selectinload(JobWork.workflow),
                selectinload(JobWork.instances)
            )
        )
        return result.scalar_one_or_none()

    async def _get_work_instance(
            self,
            db: AsyncSession,
            work_instance_id: str
    ) -> Optional[JobWorkInstance]:
        """获取作业实例"""
        result = await db.execute(
            select(JobWorkInstance).where(
                JobWorkInstance.instance_id == work_instance_id
            )
        )
        return result.scalar_one_or_none()

    def _serialize_result_data(self, data: Any) -> Any:
        """
        递归序列化结果数据,将 datetime 对象转换为字符串

        Args:
            data: 需要序列化的数据

        Returns:
            序列化后的数据
        """
        import json
        from datetime import datetime, date

        def datetime_handler(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        # 将数据转换为 JSON 字符串再转回来,确保所有 datetime 都被转换
        try:
            json_str = json.dumps(data, default=datetime_handler, ensure_ascii=False)
            return json.loads(json_str)
        except Exception as e:
            logger.warning(f"序列化结果数据失败: {e}, 返回原数据")
            return data
    async def _execute_work_async(
            self,
            work_instance_id: str,
            work: JobWork,
            context: Dict[str, Any]
    ):
        """异步执行作业（后台任务）"""
        from app.utils.database import async_session_maker
        from datetime import datetime

        try:
            async with async_session_maker() as db:
                # 获取作业实例
                work_instance = await self._get_work_instance(db, work_instance_id)

                # ✅ 生成详细的提交日志
                submit_log_lines = [
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 开始提交作业",
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 开始检测运行环境",
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 检测运行环境完成",
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 开始执行作业"
                ]

                # 更新状态为运行中
                work_instance.status = JobInstanceStatus.RUNNING
                work_instance.start_datetime = datetime.now()
                work_instance.submit_log = "\n".join(submit_log_lines)
                await db.commit()

                # 获取执行器
                executor = executor_manager.get_executor(work.executor)
                if not executor:
                    raise ValueError(f"找不到执行器: {work.executor}")

                # ✅ 添加执行前的日志
                submit_log_lines.append(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 执行参数检查SQL:"
                )

                # 根据作业类型添加具体信息
                if work.work_type.value in ['QUERY_JDBC', 'EXE_JDBC']:
                    sql = work.config.get('sql', '')
                    # 如果SQL太长，进行格式化
                    formatted_sql = sql.strip()
                    submit_log_lines.append(formatted_sql)

                    # 添加数据源信息
                    datasource_id = work.config.get('dataSourceId')
                    if datasource_id:
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 使用数据源ID: {datasource_id}"
                        )

                submit_log_lines.append(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 执行查询SQL:"
                )
                submit_log_lines.append(work.config.get('sql', '').strip())

                # 更新提交日志
                work_instance.submit_log = "\n".join(submit_log_lines)
                await db.commit()

                # 执行作业
                result = await executor.execute(
                    db, work.config or {}, work_instance_id, context
                )

                # 更新执行结果
                await db.refresh(work_instance)

                # ✅ 更新运行日志
                running_log_lines = []

                if result.get('success'):
                    work_instance.status = JobInstanceStatus.SUCCESS
                    # 序列化 result_data,将 datetime 转换为字符串
                    result_data = result.get('data')
                    if result_data:
                        work_instance.result_data = self._serialize_result_data(result_data)
                    else:
                        work_instance.result_data = result_data

                    # 添加成功日志
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 查询SQL执行成功"
                    )

                    # 添加结果统计
                    if result.get('data') and isinstance(result['data'], dict):
                        row_count = result['data'].get('rowCount', 0)
                        elapsed = result['data'].get('elapsed', 0)
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 返回行数: {row_count}"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 执行时间: {elapsed}秒"
                        )

                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 数据保存成功"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} INFO : 执行成功"
                    )

                    work_instance.submit_log = "\n".join(submit_log_lines)
                    work_instance.running_log = "执行成功"
                else:
                    work_instance.status = JobInstanceStatus.FAIL
                    work_instance.error_message = result.get('error')

                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} ERROR : 执行失败: {result.get('error')}"
                    )
                    work_instance.submit_log = "\n".join(submit_log_lines)
                    work_instance.running_log = f"执行失败: {result.get('error')}"

                work_instance.end_datetime = datetime.now()
                await db.commit()

                logger.info(
                    f"作业执行完成: {work.name} "
                    f"(状态: {work_instance.status.value})"
                )

        except Exception as e:
            logger.error(f"执行作业失败: {e}")
            try:
                async with async_session_maker() as db:
                    work_instance = await self._get_work_instance(db, work_instance_id)
                    work_instance.status = JobInstanceStatus.FAIL
                    work_instance.end_datetime = datetime.now()
                    work_instance.error_message = str(e)

                    error_log = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} ERROR : 执行异常: {str(e)}"
                    work_instance.submit_log = (work_instance.submit_log or "") + "\n" + error_log
                    work_instance.running_log = f"执行异常: {str(e)}"

                    await db.commit()
            except:
                pass

# 创建全局实例
job_work_run_service = JobWorkRunService()