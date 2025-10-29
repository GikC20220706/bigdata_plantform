"""
作业实例查询Service
提供作业流实例和作业实例的查询、统计功能
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, desc
from loguru import logger

from app.models.job_instance import (
    JobWorkflowInstance, JobWorkInstance,
    JobInstanceStatus, JobTriggerType
)


class JobInstanceService:
    """作业实例查询服务"""

    # ==================== 作业流实例查询 ====================

    async def list_workflow_instances(
            self,
            db: AsyncSession,
            workflow_id: Optional[int] = None,
            status: Optional[str] = None,
            trigger_type: Optional[str] = None,
            page: int = 0,
            page_size: int = 10
    ) -> Dict[str, Any]:
        """分页查询作业流实例列表"""
        try:
            # 构建查询条件
            conditions = []

            if workflow_id:
                conditions.append(JobWorkflowInstance.workflow_id == workflow_id)

            if status:
                conditions.append(JobWorkflowInstance.status == JobInstanceStatus(status))

            if trigger_type:
                conditions.append(JobWorkflowInstance.trigger_type == JobTriggerType(trigger_type))

            # 查询总数
            count_query = select(func.count(JobWorkflowInstance.id))
            if conditions:
                count_query = count_query.where(and_(*conditions))

            count_result = await db.execute(count_query)
            total = count_result.scalar()

            # 查询数据
            query = select(JobWorkflowInstance)
            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(desc(JobWorkflowInstance.created_at))
            query = query.offset(page * page_size).limit(page_size)

            result = await db.execute(query)
            instances = result.scalars().all()

            # 转换为响应格式
            content = []
            for instance in instances:
                content.append({
                    "workflowInstanceId": instance.workflow_instance_id,
                    "workflowId": instance.workflow_id,
                    "workflowName": instance.workflow_name,
                    "status": instance.status.value,
                    "triggerType": instance.trigger_type.value,
                    "startDatetime": instance.start_datetime.isoformat() if instance.start_datetime else None,
                    "endDatetime": instance.end_datetime.isoformat() if instance.end_datetime else None,
                    "lastModifiedBy": instance.last_modified_by,
                    "errorMessage": instance.error_message,
                    "createdAt": instance.created_at.isoformat() if instance.created_at else None
                })

            return {
                "content": content,
                "total": total,
                "page": page,
                "pageSize": page_size
            }

        except Exception as e:
            logger.error(f"查询作业流实例列表失败: {e}")
            raise

    # ==================== 作业实例查询 ====================

    async def list_work_instances(
            self,
            db: AsyncSession,
            workflow_instance_id: Optional[str] = None,
            work_id: Optional[int] = None,
            status: Optional[str] = None,
            page: int = 0,
            page_size: int = 10
    ) -> Dict[str, Any]:
        """分页查询作业实例列表"""
        try:
            # 构建查询条件
            conditions = []

            if workflow_instance_id:
                conditions.append(JobWorkInstance.workflow_instance_id == workflow_instance_id)

            if work_id:
                conditions.append(JobWorkInstance.work_id == work_id)

            if status:
                conditions.append(JobWorkInstance.status == JobInstanceStatus(status))

            # 查询总数
            count_query = select(func.count(JobWorkInstance.id))
            if conditions:
                count_query = count_query.where(and_(*conditions))

            count_result = await db.execute(count_query)
            total = count_result.scalar()

            # 查询数据
            query = select(JobWorkInstance)
            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(desc(JobWorkInstance.created_at))
            query = query.offset(page * page_size).limit(page_size)

            result = await db.execute(query)
            instances = result.scalars().all()

            # 转换为响应格式
            content = []
            for instance in instances:
                content.append({
                    "instanceId": instance.instance_id,
                    "workflowInstanceId": instance.workflow_instance_id,
                    "workId": instance.work_id,
                    "workName": instance.work_name,
                    "workType": instance.work_type,
                    "status": instance.status.value,
                    "startDatetime": instance.start_datetime.isoformat() if instance.start_datetime else None,
                    "endDatetime": instance.end_datetime.isoformat() if instance.end_datetime else None,
                    "errorMessage": instance.error_message,
                    "createdAt": instance.created_at.isoformat() if instance.created_at else None
                })

            return {
                "content": content,
                "total": total,
                "page": page,
                "pageSize": page_size
            }

        except Exception as e:
            logger.error(f"查询作业实例列表失败: {e}")
            raise

    # ==================== 统计查询 ====================

    async def get_workflow_statistics(
            self,
            db: AsyncSession,
            workflow_id: int,
            days: int = 7
    ) -> Dict[str, Any]:
        """获取作业流执行统计"""
        try:
            # 计算开始日期
            start_date = datetime.now() - timedelta(days=days)

            # 查询指定时间范围内的实例
            query = select(JobWorkflowInstance).where(
                and_(
                    JobWorkflowInstance.workflow_id == workflow_id,
                    JobWorkflowInstance.created_at >= start_date
                )
            )

            result = await db.execute(query)
            instances = result.scalars().all()

            # 统计各状态数量
            status_counts = {
                "total": len(instances),
                "success": 0,
                "fail": 0,
                "running": 0,
                "pending": 0,
                "abort": 0
            }

            # 计算执行时长
            durations = []

            for instance in instances:
                status_key = instance.status.value.lower()
                if status_key in status_counts:
                    status_counts[status_key] += 1

                # 计算执行时长（秒）
                if instance.start_datetime and instance.end_datetime:
                    duration = (instance.end_datetime - instance.start_datetime).total_seconds()
                    durations.append(duration)

            # 计算平均执行时长
            avg_duration = sum(durations) / len(durations) if durations else 0

            # 按日期统计
            daily_stats = {}
            for instance in instances:
                date_key = instance.created_at.strftime('%Y-%m-%d')
                if date_key not in daily_stats:
                    daily_stats[date_key] = {
                        "total": 0,
                        "success": 0,
                        "fail": 0
                    }

                daily_stats[date_key]["total"] += 1
                if instance.status == JobInstanceStatus.SUCCESS:
                    daily_stats[date_key]["success"] += 1
                elif instance.status == JobInstanceStatus.FAIL:
                    daily_stats[date_key]["fail"] += 1

            return {
                "workflowId": workflow_id,
                "days": days,
                "statusCounts": status_counts,
                "avgDuration": round(avg_duration, 2),
                "successRate": round(status_counts["success"] / status_counts["total"] * 100, 2) if status_counts[
                                                                                                        "total"] > 0 else 0,
                "dailyStats": daily_stats
            }

        except Exception as e:
            logger.error(f"获取作业流统计失败: {e}")
            raise

    async def get_work_statistics(
            self,
            db: AsyncSession,
            work_id: int,
            days: int = 7
    ) -> Dict[str, Any]:
        """获取作业执行统计"""
        try:
            # 计算开始日期
            start_date = datetime.now() - timedelta(days=days)

            # 查询指定时间范围内的实例
            query = select(JobWorkInstance).where(
                and_(
                    JobWorkInstance.work_id == work_id,
                    JobWorkInstance.created_at >= start_date
                )
            )

            result = await db.execute(query)
            instances = result.scalars().all()

            # 统计各状态数量
            status_counts = {
                "total": len(instances),
                "success": 0,
                "fail": 0,
                "running": 0,
                "pending": 0,
                "abort": 0
            }

            # 计算执行时长
            durations = []

            for instance in instances:
                status_key = instance.status.value.lower()
                if status_key in status_counts:
                    status_counts[status_key] += 1

                # 计算执行时长（秒）
                if instance.start_datetime and instance.end_datetime:
                    duration = (instance.end_datetime - instance.start_datetime).total_seconds()
                    durations.append(duration)

            # 计算平均执行时长
            avg_duration = sum(durations) / len(durations) if durations else 0

            # 按日期统计
            daily_stats = {}
            for instance in instances:
                date_key = instance.created_at.strftime('%Y-%m-%d')
                if date_key not in daily_stats:
                    daily_stats[date_key] = {
                        "total": 0,
                        "success": 0,
                        "fail": 0
                    }

                daily_stats[date_key]["total"] += 1
                if instance.status == JobInstanceStatus.SUCCESS:
                    daily_stats[date_key]["success"] += 1
                elif instance.status == JobInstanceStatus.FAIL:
                    daily_stats[date_key]["fail"] += 1

            return {
                "workId": work_id,
                "days": days,
                "statusCounts": status_counts,
                "avgDuration": round(avg_duration, 2),
                "successRate": round(status_counts["success"] / status_counts["total"] * 100, 2) if status_counts[
                                                                                                        "total"] > 0 else 0,
                "dailyStats": daily_stats
            }

        except Exception as e:
            logger.error(f"获取作业统计失败: {e}")
            raise


# 创建全局实例
job_instance_service = JobInstanceService()