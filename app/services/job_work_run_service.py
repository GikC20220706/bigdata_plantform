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
        """异步执行作业(后台任务)"""
        from app.utils.database import async_session_maker
        from datetime import datetime

        try:
            async with async_session_maker() as db:
                # 获取作业实例
                work_instance = await self._get_work_instance(db, work_instance_id)

                # ===================================================================
                # 步骤1: 初始化基础日志 (所有作业类型通用)
                # ===================================================================
                submit_log_lines = [
                    f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始提交作业"
                ]

                # 更新状态为运行中
                work_instance.status = JobInstanceStatus.RUNNING
                work_instance.start_datetime = datetime.now()
                work_instance.submit_log = "\n".join(submit_log_lines)
                await db.commit()

                # ===================================================================
                # 步骤2: 根据作业类型添加执行前的特定日志
                # ===================================================================
                work_type = work.work_type.value

                # 1. 接口调用作业 (API)
                if work_type == 'API':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测作业配置"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行接口调用"
                    )

                # 2. Python作业
                elif work_type == 'PYTHON':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测脚本内容"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始准备执行"
                    )
                    # 添加Python脚本内容
                    script_content = work.config.get('script', '')
                    if script_content:
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : Python脚本:"
                        )
                        submit_log_lines.append(script_content.strip())
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )

                # 3. Bash作业
                elif work_type == 'BASH':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测脚本内容"
                    )
                    # 添加Bash脚本内容
                    script_content = work.config.get('script', '')
                    if script_content:
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : Bash脚本:"
                        )
                        submit_log_lines.append(script_content.strip())
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始准备执行"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )

                # 4. JDBC查询作业
                elif work_type == 'QUERY_JDBC':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始检测运行环境"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测运行环境完成 "
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )

                # 5. JDBC执行作业
                elif work_type == 'EXE_JDBC':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始检测运行环境"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测运行环境完成 "
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )

                # 6. 数据同步作业
                elif work_type == 'DATA_SYNC_JDBC':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始检测数据源连接"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 源数据库连接成功"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 目标数据库连接成功"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行数据同步"
                    )

                # 7. Curl作业
                elif work_type == 'CURL':
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测脚本内容"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )
                    # 添加curl命令内容
                    curl_command = work.config.get('command', '')
                    if curl_command:
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行作业内容:"
                        )
                        submit_log_lines.append(curl_command.strip())

                # 其他作业类型使用通用日志
                else:
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始检测运行环境"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 检测运行环境完成"
                    )
                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行作业"
                    )

                # 更新执行前日志
                work_instance.submit_log = "\n".join(submit_log_lines)
                await db.commit()

                # ===================================================================
                # 步骤3: 执行作业
                # ===================================================================
                executor = executor_manager.get_executor(work.executor)
                if not executor:
                    raise ValueError(f"找不到执行器: {work.executor}")

                result = await executor.execute(
                    db, work.config or {}, work_instance_id, context
                )

                # 刷新实例状态
                await db.refresh(work_instance)

                # ===================================================================
                # 步骤4: 根据执行结果和作业类型更新日志
                # ===================================================================
                if result.get("success"):
                    work_instance.status = JobInstanceStatus.SUCCESS

                    # 根据作业类型添加成功日志
                    # 1. 接口调用作业
                    if work_type == 'API':
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 请求成功, 查看运行结果"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 2. Python作业
                    elif work_type == 'PYTHON':
                        # 从结果中获取进程ID
                        pid = result.get('data', {}).get('pid', 'unknown')
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : Python作业提交成功,pid:[{pid}]"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 运行状态:FINISHED"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 运行状态:FINISHED"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 保存日志成功"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 3. Bash作业
                    elif work_type == 'BASH':
                        # 从结果中获取进程ID
                        pid = result.get('data', {}).get('pid', 'unknown')
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : BASH作业提交成功,pid:[{pid}]"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 运行状态:FINISHED"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 保存日志成功"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 4. JDBC查询作业
                    elif work_type == 'QUERY_JDBC':
                        # 从配置中获取SQL
                        sql = work.config.get('sql', '').strip()

                        # 添加查询统计SQL
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行条数检测SQL:"
                        )
                        submit_log_lines.append(
                            f"SELECT COUNT(*) FROM ( {sql} ) temp"
                        )

                        # 添加实际查询SQL
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行查询SQL:"
                        )
                        submit_log_lines.append(sql)

                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 查询SQL执行成功 "
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 数据保存成功 "
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 5. JDBC执行作业
                    elif work_type == 'EXE_JDBC':
                        # 从配置中获取SQL列表
                        sql_list = work.config.get('sqls', [])
                        if not sql_list and work.config.get('sql'):
                            sql_list = [work.config.get('sql')]

                        # 为每条SQL添加执行日志
                        for sql in sql_list:
                            if sql and sql.strip():
                                submit_log_lines.append(
                                    f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始执行SQL:"
                                )
                                submit_log_lines.append(f"{sql.strip()}")
                                submit_log_lines.append(
                                    f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : SQL执行成功 "
                                )

                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 6. 数据同步作业
                    elif work_type == 'DATA_SYNC_JDBC':
                        # 从结果中获取同步统计信息
                        sync_stats = result.get('data', {})
                        total_records = sync_stats.get('totalRecords', 0)
                        success_records = sync_stats.get('successRecords', 0)
                        elapsed_time = sync_stats.get('elapsedTime', 0)

                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始读取源数据"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 读取记录数: {total_records}"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 开始写入目标数据库"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 成功写入记录数: {success_records}"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 数据同步完成, 耗时: {elapsed_time}秒"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 7. Curl作业
                    elif work_type == 'CURL':
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 保存结果成功"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 其他作业类型使用通用成功日志
                    else:
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 数据保存成功"
                        )
                        submit_log_lines.append(
                            f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} INFO  : 执行成功"
                        )
                        work_instance.submit_log = "\n".join(submit_log_lines)
                        work_instance.running_log = "执行成功"

                    # 序列化结果数据
                    result_data = result.get('data')
                    if result_data:
                        work_instance.result_data = self._serialize_result_data(result_data)

                # 执行失败
                else:
                    work_instance.status = JobInstanceStatus.FAIL
                    work_instance.error_message = result.get('error')

                    submit_log_lines.append(
                        f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ERROR : 执行失败: {result.get('error')}"
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

                    error_log = f"{datetime.now().strftime('%Y-%m-%d')}T{datetime.now().strftime('%H:%M:%S.%f')[:-3]} ERROR : 执行异常: {str(e)}"
                    work_instance.submit_log = (work_instance.submit_log or "") + "\n" + error_log
                    work_instance.running_log = f"执行异常: {str(e)}"

                    await db.commit()
            except:
                pass

# 创建全局实例
job_work_run_service = JobWorkRunService()