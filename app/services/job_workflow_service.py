"""
作业流Service - 业务逻辑层
"""
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import selectinload
from loguru import logger

from app.models.job_workflow import JobWorkflow, JobWorkflowStatus
from app.models.job_work import JobWork, JobWorkType, JobWorkStatus
from app.models.job_instance import JobWorkflowInstance, JobWorkInstance, JobInstanceStatus
from app.schemas.job_workflow import (
    JobWorkflowCreate, JobWorkflowUpdate, JobWorkflowPageQuery,
    JobWorkCreate, JobWorkUpdate, JobWorkPageQuery
)


class JobWorkflowService:
    """作业流服务"""

    # ==================== 作业流CRUD ====================

    async def create_workflow(
            self,
            db: AsyncSession,
            data: JobWorkflowCreate,
            username: Optional[str] = None
    ) -> JobWorkflow:
        """创建作业流"""
        try:
            workflow = JobWorkflow(
                name=data.name,
                remark=data.remark,
                status=JobWorkflowStatus.DRAFT,
                create_username=username
            )

            db.add(workflow)
            await db.commit()
            await db.refresh(workflow)

            logger.info(f"创建作业流成功: {workflow.name} (ID: {workflow.id})")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"创建作业流失败: {e}")
            raise

    async def get_workflow_by_id(
            self,
            db: AsyncSession,
            workflow_id: int,
            load_works: bool = False
    ) -> Optional[JobWorkflow]:
        """根据ID获取作业流"""
        try:
            query = select(JobWorkflow).where(JobWorkflow.id == workflow_id)

            if load_works:
                query = query.options(selectinload(JobWorkflow.works))

            result = await db.execute(query)
            return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"获取作业流失败: {e}")
            raise

    async def page_workflows(
            self,
            db: AsyncSession,
            query_params: JobWorkflowPageQuery
    ) -> Dict[str, Any]:
        """分页查询作业流"""
        try:
            # 构建查询
            query = select(JobWorkflow)
            count_query = select(func.count(JobWorkflow.id))

            # 关键词搜索
            if query_params.searchKeyWord:
                keyword = f"%{query_params.searchKeyWord}%"
                query = query.where(
                    or_(
                        JobWorkflow.name.like(keyword),
                        JobWorkflow.remark.like(keyword)
                    )
                )
                count_query = count_query.where(
                    or_(
                        JobWorkflow.name.like(keyword),
                        JobWorkflow.remark.like(keyword)
                    )
                )

            # 获取总数
            total_result = await db.execute(count_query)
            total = total_result.scalar()

            # 分页
            offset = query_params.page * query_params.pageSize
            query = query.order_by(desc(JobWorkflow.created_at))
            query = query.offset(offset).limit(query_params.pageSize)

            # 执行查询
            result = await db.execute(query)
            workflows = result.scalars().all()

            return {
                "content": workflows,
                "total": total,
                "page": query_params.page,
                "pageSize": query_params.pageSize
            }

        except Exception as e:
            logger.error(f"分页查询作业流失败: {e}")
            raise

    async def update_workflow(
            self,
            db: AsyncSession,
            workflow_id: int,
            data: JobWorkflowUpdate
    ) -> Optional[JobWorkflow]:
        """更新作业流"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            if data.name is not None:
                workflow.name = data.name
            if data.remark is not None:
                workflow.remark = data.remark

            await db.commit()
            await db.refresh(workflow)

            logger.info(f"更新作业流成功: {workflow.name} (ID: {workflow.id})")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"更新作业流失败: {e}")
            raise

    async def delete_workflow(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> bool:
        """删除作业流"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return False

            await db.delete(workflow)
            await db.commit()

            logger.info(f"删除作业流成功: ID={workflow_id}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除作业流失败: {e}")
            raise

    # ==================== 流程图配置 ====================

    async def save_workflow_config(
            self,
            db: AsyncSession,
            workflow_id: int,
            web_config: List[Dict[str, Any]]
    ) -> Optional[JobWorkflow]:
        """保存流程图配置"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            workflow.web_config = web_config
            await db.commit()
            await db.refresh(workflow)

            logger.info(f"保存流程图配置成功: {workflow.name}")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"保存流程图配置失败: {e}")
            raise

    async def get_workflow_config(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Optional[Dict[str, Any]]:
        """获取流程图配置"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            return {
                "webConfig": workflow.web_config or [],
                "cronConfig": workflow.cron_config,
                "alarmList": workflow.alarm_list,
                "otherConfig": workflow.other_config
            }

        except Exception as e:
            logger.error(f"获取流程图配置失败: {e}")
            raise

    async def save_workflow_setting(
            self,
            db: AsyncSession,
            workflow_id: int,
            cron_config: Optional[Dict[str, Any]],
            alarm_list: Optional[List[Dict[str, Any]]],
            other_config: Optional[Dict[str, Any]]
    ) -> Optional[JobWorkflow]:
        """保存作业流设置"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            if cron_config is not None:
                workflow.cron_config = cron_config
            if alarm_list is not None:
                workflow.alarm_list = alarm_list
            if other_config is not None:
                workflow.other_config = other_config

            await db.commit()
            await db.refresh(workflow)

            logger.info(f"保存作业流设置成功: {workflow.name}")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"保存作业流设置失败: {e}")
            raise

    # ==================== 作业流状态管理 ====================

    async def publish_workflow(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Optional[JobWorkflow]:
        """发布作业流"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            workflow.status = JobWorkflowStatus.ONLINE
            await db.commit()
            await db.refresh(workflow)

            logger.info(f"发布作业流成功: {workflow.name}")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"发布作业流失败: {e}")
            raise

    async def offline_workflow(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Optional[JobWorkflow]:
        """下线作业流"""
        try:
            workflow = await self.get_workflow_by_id(db, workflow_id)
            if not workflow:
                return None

            workflow.status = JobWorkflowStatus.OFFLINE
            await db.commit()
            await db.refresh(workflow)

            logger.info(f"下线作业流成功: {workflow.name}")
            return workflow

        except Exception as e:
            await db.rollback()
            logger.error(f"下线作业流失败: {e}")
            raise


# 创建全局实例
job_workflow_service = JobWorkflowService()