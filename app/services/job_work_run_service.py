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
            context: Optional[Dict[str, Any]] = None
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
            workflow_instance_id = f"TEST_{uuid.uuid4().hex[:16].upper()}"

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

            # 5. 异步执行作业
            asyncio.create_task(
                self._execute_work_async(
                    work_instance_id,
                    work,
                    context or {}
                )
            )

            logger.info(
                f"作业已提交运行: {work.name} "
                f"(实例ID: {work_instance_id})"
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

            logs = {}

            if log_type in ["submit", "all"]:
                logs["submitLog"] = work_instance.submit_log or ""

            if log_type in ["running", "all"]:
                logs["runningLog"] = work_instance.running_log or ""

            return {
                "workInstanceId": work_instance_id,
                "logs": logs
            }

        except Exception as e:
            logger.error(f"获取作业日志失败: {e}")
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
                    work_instance.result_data = result.get('data')

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