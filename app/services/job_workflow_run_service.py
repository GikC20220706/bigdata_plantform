"""
作业流运行控制Service
负责作业流的执行、调度、状态管理
"""
import uuid
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from loguru import logger

from app.models.job_workflow import JobWorkflow
from app.models.job_work import JobWork
from app.models.job_instance import (
    JobWorkflowInstance, JobWorkInstance,
    JobInstanceStatus, JobTriggerType
)
from app.services.executors import executor_manager


class JobWorkflowRunService:
    """作业流运行控制服务"""

    # ==================== 作业流运行 ====================

    async def run_workflow(
            self,
            db: AsyncSession,
            workflow_id: int,
            trigger_type: JobTriggerType = JobTriggerType.MANUAL,
            trigger_user: Optional[str] = None,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        运行作业流

        Args:
            db: 数据库会话
            workflow_id: 作业流ID
            trigger_type: 触发类型
            trigger_user: 触发用户
            context: 执行上下文（变量、参数等）

        Returns:
            {
                "workflowInstanceId": str,
                "status": str,
                "message": str
            }
        """
        try:
            # 1. 获取作业流配置
            workflow = await self._get_workflow_with_works(db, workflow_id)
            if not workflow:
                raise ValueError(f"作业流不存在: {workflow_id}")

            # 2. 创建作业流实例
            workflow_instance = await self._create_workflow_instance(
                db, workflow, trigger_type, trigger_user
            )

            # 3. 解析流程图配置
            execution_plan = await self._parse_workflow_config(workflow)

            # 4. 创建作业实例
            work_instances = await self._create_work_instances(
                db, workflow_instance, workflow.works
            )

            # 5. 异步执行作业流（后台任务）
            asyncio.create_task(
                self._execute_workflow_async(
                    workflow_instance.workflow_instance_id,
                    execution_plan,
                    work_instances,
                    context or {}
                )
            )

            logger.info(
                f"作业流已提交运行: {workflow.name} "
                f"(实例ID: {workflow_instance.workflow_instance_id})"
            )

            return {
                "workflowInstanceId": workflow_instance.workflow_instance_id,
                "status": JobInstanceStatus.RUNNING.value,
                "message": "作业流已提交运行"
            }

        except Exception as e:
            logger.error(f"运行作业流失败: {e}")
            raise

    async def abort_workflow(
            self,
            db: AsyncSession,
            workflow_instance_id: str
    ) -> Dict[str, Any]:
        """
        中止作业流

        Args:
            db: 数据库会话
            workflow_instance_id: 作业流实例ID

        Returns:
            {
                "workflowInstanceId": str,
                "status": str,
                "message": str
            }
        """
        try:
            # 1. 获取作业流实例
            workflow_instance = await self._get_workflow_instance(
                db, workflow_instance_id
            )
            if not workflow_instance:
                raise ValueError(f"作业流实例不存在: {workflow_instance_id}")

            # 2. 检查状态
            if workflow_instance.status not in [
                JobInstanceStatus.PENDING,
                JobInstanceStatus.RUNNING
            ]:
                return {
                    "workflowInstanceId": workflow_instance_id,
                    "status": workflow_instance.status.value,
                    "message": f"作业流当前状态为 {workflow_instance.status.value}，无法中止"
                }

            # 3. 更新状态为中止中
            workflow_instance.status = JobInstanceStatus.ABORTING
            await db.commit()

            # 4. 中止所有运行中的作业实例
            work_instances = await self._get_work_instances_by_workflow(
                db, workflow_instance_id
            )

            for work_instance in work_instances:
                if work_instance.status == JobInstanceStatus.RUNNING:
                    work_instance.status = JobInstanceStatus.ABORT
                    work_instance.end_datetime = datetime.now()
                    work_instance.error_message = "作业流已中止"

            # 5. 更新作业流实例状态
            workflow_instance.status = JobInstanceStatus.ABORT
            workflow_instance.end_datetime = datetime.now()
            workflow_instance.error_message = "用户手动中止"

            await db.commit()

            logger.info(f"作业流已中止: {workflow_instance_id}")

            return {
                "workflowInstanceId": workflow_instance_id,
                "status": JobInstanceStatus.ABORT.value,
                "message": "作业流已中止"
            }

        except Exception as e:
            logger.error(f"中止作业流失败: {e}")
            raise

    async def get_workflow_instance_status(
            self,
            db: AsyncSession,
            workflow_instance_id: str
    ) -> Dict[str, Any]:
        """获取作业流实例状态"""
        try:
            workflow_instance = await self._get_workflow_instance(
                db, workflow_instance_id
            )
            if not workflow_instance:
                raise ValueError(f"作业流实例不存在: {workflow_instance_id}")

            # 获取所有作业实例
            work_instances = await self._get_work_instances_by_workflow(
                db, workflow_instance_id
            )

            # 统计作业状态
            status_counts = {
                "total": len(work_instances),
                "pending": 0,
                "running": 0,
                "success": 0,
                "fail": 0,
                "abort": 0
            }

            for work_instance in work_instances:
                status_key = work_instance.status.value.lower()
                if status_key in status_counts:
                    status_counts[status_key] += 1

            return {
                "workflowInstanceId": workflow_instance_id,
                "workflowName": workflow_instance.workflow_name,
                "status": workflow_instance.status.value,
                "triggerType": workflow_instance.trigger_type.value,
                "startDatetime": workflow_instance.start_datetime.isoformat() if workflow_instance.start_datetime else None,
                "endDatetime": workflow_instance.end_datetime.isoformat() if workflow_instance.end_datetime else None,
                "errorMessage": workflow_instance.error_message,
                "workStatus": status_counts,
                "works": [
                    {
                        "instanceId": wi.instance_id,
                        "workName": wi.work_name,
                        "workType": wi.work_type,
                        "status": wi.status.value,
                        "startDatetime": wi.start_datetime.isoformat() if wi.start_datetime else None,
                        "endDatetime": wi.end_datetime.isoformat() if wi.end_datetime else None,
                        "errorMessage": wi.error_message
                    }
                    for wi in work_instances
                ]
            }

        except Exception as e:
            logger.error(f"获取作业流实例状态失败: {e}")
            raise

    # ==================== 私有方法 ====================

    async def _get_workflow_with_works(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Optional[JobWorkflow]:
        """获取作业流及其作业"""
        from sqlalchemy.orm import selectinload

        result = await db.execute(
            select(JobWorkflow)
            .options(selectinload(JobWorkflow.works))
            .where(JobWorkflow.id == workflow_id)
        )
        return result.scalar_one_or_none()

    async def _create_workflow_instance(
            self,
            db: AsyncSession,
            workflow: JobWorkflow,
            trigger_type: JobTriggerType,
            trigger_user: Optional[str]
    ) -> JobWorkflowInstance:
        """创建作业流实例"""
        workflow_instance = JobWorkflowInstance(
            workflow_instance_id=f"WF_{uuid.uuid4().hex[:16].upper()}",
            workflow_id=workflow.id,
            workflow_name=workflow.name,
            status=JobInstanceStatus.PENDING,
            trigger_type=trigger_type,
            start_datetime=datetime.now(),
            last_modified_by=trigger_user
        )

        db.add(workflow_instance)
        await db.commit()
        await db.refresh(workflow_instance)

        return workflow_instance

    async def _create_work_instances(
            self,
            db: AsyncSession,
            workflow_instance: JobWorkflowInstance,
            works: List[JobWork]
    ) -> Dict[int, JobWorkInstance]:
        """创建作业实例"""
        work_instances = {}

        for work in works:
            work_instance = JobWorkInstance(
                instance_id=f"WORK_{uuid.uuid4().hex[:16].upper()}",
                workflow_instance_id=workflow_instance.workflow_instance_id,
                work_id=work.id,
                work_name=work.name,
                work_type=work.work_type.value,
                status=JobInstanceStatus.PENDING
            )

            db.add(work_instance)
            work_instances[work.id] = work_instance

        await db.commit()

        # 刷新所有实例
        for work_instance in work_instances.values():
            await db.refresh(work_instance)

        return work_instances

    async def _parse_workflow_config(
            self,
            workflow: JobWorkflow
    ) -> Dict[str, Any]:
        """
        解析流程图配置，生成执行计划

        web_config 格式示例:
        [
            {"id": "node1", "type": "start", "x": 100, "y": 100},
            {"id": "node2", "type": "work", "workId": 1, "x": 300, "y": 100},
            {"id": "edge1", "type": "edge", "source": "node1", "target": "node2"}
        ]
        """
        web_config = workflow.web_config or []

        # 提取节点和边
        nodes = []
        edges = []
        work_nodes = {}

        for item in web_config:
            item_type = item.get('type', '')

            if item_type == 'work':
                nodes.append(item)
                work_id = item.get('workId')
                if work_id:
                    work_nodes[item['id']] = work_id
            elif item_type == 'edge':
                edges.append(item)

        # 构建依赖关系图
        dependency_graph = {}  # work_id -> [依赖的work_id列表]
        work_order = []  # 执行顺序

        for node_id, work_id in work_nodes.items():
            dependencies = []

            # 找出所有指向该节点的边
            for edge in edges:
                if edge.get('target') == node_id:
                    source_node_id = edge.get('source')
                    if source_node_id in work_nodes:
                        dependencies.append(work_nodes[source_node_id])

            dependency_graph[work_id] = dependencies

        # 拓扑排序得到执行顺序
        work_order = self._topological_sort(dependency_graph)

        return {
            "workOrder": work_order,
            "dependencies": dependency_graph,
            "totalWorks": len(work_nodes)
        }

    def _topological_sort(self, graph: Dict[int, List[int]]) -> List[int]:
        """拓扑排序 - 确定执行顺序"""
        # 简单实现：没有依赖的先执行
        result = []
        visited = set()

        def visit(node):
            if node in visited:
                return
            visited.add(node)

            # 先访问依赖节点
            for dep in graph.get(node, []):
                visit(dep)

            result.append(node)

        # 访问所有节点
        for node in graph.keys():
            visit(node)

        return result

    async def _execute_workflow_async(
            self,
            workflow_instance_id: str,
            execution_plan: Dict[str, Any],
            work_instances: Dict[int, JobWorkInstance],
            context: Dict[str, Any]
    ):
        """异步执行作业流（后台任务）"""
        from app.utils.database import async_session_maker

        try:
            async with async_session_maker() as db:
                # 更新作业流状态为运行中
                workflow_instance = await self._get_workflow_instance(
                    db, workflow_instance_id
                )
                workflow_instance.status = JobInstanceStatus.RUNNING
                await db.commit()

                # 按照执行计划依次执行作业
                work_order = execution_plan.get('workOrder', [])

                for work_id in work_order:
                    work_instance = work_instances.get(work_id)
                    if not work_instance:
                        continue

                    # 检查作业流是否被中止
                    await db.refresh(workflow_instance)
                    if workflow_instance.status == JobInstanceStatus.ABORTING:
                        logger.info(f"作业流已中止: {workflow_instance_id}")
                        break

                    # 执行作业
                    await self._execute_work(db, work_instance, context)

                    # 如果作业失败，根据策略决定是否继续
                    await db.refresh(work_instance)
                    if work_instance.status == JobInstanceStatus.FAIL:
                        logger.error(f"作业执行失败: {work_instance.work_name}")
                        # TODO: 根据失败策略决定是否继续
                        break

                # 更新作业流最终状态
                await self._update_workflow_final_status(db, workflow_instance_id)

        except Exception as e:
            logger.error(f"执行作业流失败: {e}")
            # 更新失败状态
            try:
                async with async_session_maker() as db:
                    workflow_instance = await self._get_workflow_instance(
                        db, workflow_instance_id
                    )
                    workflow_instance.status = JobInstanceStatus.FAIL
                    workflow_instance.end_datetime = datetime.now()
                    workflow_instance.error_message = str(e)
                    await db.commit()
            except:
                pass

    async def _execute_work(
            self,
            db: AsyncSession,
            work_instance: JobWorkInstance,
            context: Dict[str, Any]
    ):
        """执行单个作业"""
        try:
            # 获取作业配置
            work_result = await db.execute(
                select(JobWork).where(JobWork.id == work_instance.work_id)
            )
            work = work_result.scalar_one_or_none()
            if not work:
                raise ValueError(f"作业不存在: {work_instance.work_id}")

            # 更新状态为运行中
            work_instance.status = JobInstanceStatus.RUNNING
            work_instance.start_datetime = datetime.now()
            await db.commit()

            # 获取执行器并执行
            executor = executor_manager.get_executor(work.executor)
            if not executor:
                raise ValueError(f"找不到执行器: {work.executor}")

            result = await executor.execute(
                db, work.config or {}, work_instance.instance_id, context
            )

            # 更新执行结果
            if result.get('success'):
                work_instance.status = JobInstanceStatus.SUCCESS
                work_instance.result_data = result.get('data')
            else:
                work_instance.status = JobInstanceStatus.FAIL
                work_instance.error_message = result.get('error')

            work_instance.end_datetime = datetime.now()
            await db.commit()

            logger.info(
                f"作业执行完成: {work_instance.work_name} "
                f"(状态: {work_instance.status.value})"
            )

        except Exception as e:
            logger.error(f"执行作业失败: {e}")
            work_instance.status = JobInstanceStatus.FAIL
            work_instance.end_datetime = datetime.now()
            work_instance.error_message = str(e)
            await db.commit()

    async def _update_workflow_final_status(
            self,
            db: AsyncSession,
            workflow_instance_id: str
    ):
        """更新作业流最终状态"""
        workflow_instance = await self._get_workflow_instance(
            db, workflow_instance_id
        )

        # 获取所有作业实例
        work_instances = await self._get_work_instances_by_workflow(
            db, workflow_instance_id
        )

        # 判断最终状态
        has_fail = any(
            wi.status == JobInstanceStatus.FAIL
            for wi in work_instances
        )
        all_success = all(
            wi.status == JobInstanceStatus.SUCCESS
            for wi in work_instances
        )

        if workflow_instance.status == JobInstanceStatus.ABORTING:
            workflow_instance.status = JobInstanceStatus.ABORT
        elif has_fail:
            workflow_instance.status = JobInstanceStatus.FAIL
        elif all_success:
            workflow_instance.status = JobInstanceStatus.SUCCESS
        else:
            workflow_instance.status = JobInstanceStatus.FAIL

        workflow_instance.end_datetime = datetime.now()
        await db.commit()

    async def _get_workflow_instance(
            self,
            db: AsyncSession,
            workflow_instance_id: str
    ) -> Optional[JobWorkflowInstance]:
        """获取作业流实例"""
        result = await db.execute(
            select(JobWorkflowInstance).where(
                JobWorkflowInstance.workflow_instance_id == workflow_instance_id
            )
        )
        return result.scalar_one_or_none()

    async def _get_work_instances_by_workflow(
            self,
            db: AsyncSession,
            workflow_instance_id: str
    ) -> List[JobWorkInstance]:
        """获取作业流的所有作业实例"""
        result = await db.execute(
            select(JobWorkInstance).where(
                JobWorkInstance.workflow_instance_id == workflow_instance_id
            )
        )
        return result.scalars().all()


# 创建全局实例
job_workflow_run_service = JobWorkflowRunService()