"""
作业Service - 业务逻辑层
"""
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc
from loguru import logger

from app.models.job_work import JobWork, JobWorkType, JobWorkStatus
from app.models.job_workflow import JobWorkflow
from app.schemas.job_workflow import (
    JobWorkCreate, JobWorkUpdate, JobWorkPageQuery, JobWorkCopy
)


class JobWorkService:
    """作业服务"""

    # 作业类型到执行器的映射
    WORK_TYPE_EXECUTOR_MAP = {
        JobWorkType.EXE_JDBC: 'jdbc_executor_enhanced',
        JobWorkType.QUERY_JDBC: 'jdbc_executor_enhanced',
        JobWorkType.SPARK_SQL: 'spark_sql_executor',
        JobWorkType.DATA_SYNC_JDBC: 'smart_sync_executor',
        JobWorkType.EXCEL_SYNC_JDBC: 'excel_sync_executor',
        JobWorkType.DB_MIGRATE: 'db_migrate_executor',
        JobWorkType.BASH: 'bash_executor',
        JobWorkType.PYTHON: 'python_executor',
        JobWorkType.CURL: 'http_executor',
        JobWorkType.API: 'api_executor',
        JobWorkType.SPARK_JAR: 'spark_jar_executor',
        JobWorkType.FLINK_SQL: 'flink_sql_executor',
        JobWorkType.FLINK_JAR: 'flink_jar_executor',
    }

    # ==================== 作业CRUD ====================

    async def create_work(
            self,
            db: AsyncSession,
            data: JobWorkCreate
    ) -> Dict[str, Any]:
        """创建作业"""
        try:
            # 验证作业流是否存在
            workflow_result = await db.execute(
                select(JobWorkflow).where(JobWorkflow.id == data.workflowId)
            )
            workflow = workflow_result.scalar_one_or_none()
            if not workflow:
                raise ValueError(f"作业流不存在: {data.workflowId}")

            # 确定执行器
            try:
                work_type_enum = JobWorkType(data.workType)
                executor = self.WORK_TYPE_EXECUTOR_MAP.get(work_type_enum)
            except ValueError:
                raise ValueError(f"不支持的作业类型: {data.workType}")

            initial_config = {}

            # JDBC类作业：保存数据源ID
            if work_type_enum in [JobWorkType.EXE_JDBC, JobWorkType.QUERY_JDBC]:
                if data.datasourceId:
                    initial_config = {
                        "dataSourceId": data.datasourceId,
                        "sql": "",
                        "timeout": 300,
                        "type": "execute" if work_type_enum == JobWorkType.EXE_JDBC else "query"
                    }
                    logger.info(f"初始化JDBC作业配置: dataSourceId={data.datasourceId}")
                else:
                    logger.warning(f"创建JDBC作业但未指定数据源ID")

            # Spark/Flink SQL：保存集群信息
            elif work_type_enum in [JobWorkType.SPARK_SQL, JobWorkType.FLINK_SQL]:
                if data.clusterId or data.containerId:
                    initial_config = {
                        "clusterId": data.clusterId,
                        "containerId": data.containerId,
                        "sql": ""
                    }

            # Bash/Python：基础配置
            elif work_type_enum in [JobWorkType.BASH, JobWorkType.PYTHON]:
                initial_config = {
                    "script": "",
                    "timeout": 300
                }

            # HTTP/API：基础配置
            elif work_type_enum in [JobWorkType.CURL, JobWorkType.API]:
                initial_config = {
                    "url": "",
                    "method": "GET",
                    "timeout": 60
                }

            # 创建作业
            work = JobWork(
                workflow_id=data.workflowId,
                name=data.name,
                work_type=work_type_enum,
                remark=data.remark,
                status=JobWorkStatus.DRAFT,
                executor=executor,
                config=initial_config  # ✅ 使用初始化的配置
            )

            db.add(work)
            await db.commit()
            await db.refresh(work)

            logger.info(
                f"创建作业成功: {work.name} (ID: {work.id}, Type: {work.work_type}, "
                f"Config: {initial_config})"
            )

            return {
                "workId": work.id,
                "name": work.name
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"创建作业失败: {e}")
            raise

    async def get_work_by_id(
            self,
            db: AsyncSession,
            work_id: int
    ) -> Optional[JobWork]:
        """根据ID获取作业"""
        try:
            result = await db.execute(
                select(JobWork).where(JobWork.id == work_id)
            )
            return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"获取作业失败: {e}")
            raise

    async def page_works(
            self,
            db: AsyncSession,
            query_params: JobWorkPageQuery
    ) -> Dict[str, Any]:
        """分页查询作业"""
        try:
            # 构建查询
            query = select(JobWork).where(JobWork.workflow_id == query_params.workflowId)
            count_query = select(func.count(JobWork.id)).where(
                JobWork.workflow_id == query_params.workflowId
            )

            # 关键词搜索
            if query_params.searchKeyWord:
                keyword = f"%{query_params.searchKeyWord}%"
                query = query.where(
                    or_(
                        JobWork.name.like(keyword),
                        JobWork.remark.like(keyword)
                    )
                )
                count_query = count_query.where(
                    or_(
                        JobWork.name.like(keyword),
                        JobWork.remark.like(keyword)
                    )
                )

            # 获取总数
            total_result = await db.execute(count_query)
            total = total_result.scalar()

            # 分页
            offset = query_params.page * query_params.pageSize
            query = query.order_by(desc(JobWork.created_at))
            query = query.offset(offset).limit(query_params.pageSize)

            # 执行查询
            result = await db.execute(query)
            works = result.scalars().all()

            return {
                "content": works,
                "total": total,
                "page": query_params.page,
                "pageSize": query_params.pageSize
            }

        except Exception as e:
            logger.error(f"分页查询作业失败: {e}")
            raise

    async def update_work(
            self,
            db: AsyncSession,
            data: JobWorkUpdate
    ) -> Optional[JobWork]:
        """更新作业"""
        try:
            work = await self.get_work_by_id(db, data.workId)
            if not work:
                return None

            if data.name is not None:
                work.name = data.name
            if data.remark is not None:
                work.remark = data.remark

            await db.commit()
            await db.refresh(work)

            logger.info(f"更新作业成功: {work.name} (ID: {work.id})")
            return work

        except Exception as e:
            await db.rollback()
            logger.error(f"更新作业失败: {e}")
            raise

    async def delete_work(
            self,
            db: AsyncSession,
            work_id: int
    ) -> bool:
        """删除作业"""
        try:
            work = await self.get_work_by_id(db, work_id)
            if not work:
                return False

            await db.delete(work)
            await db.commit()

            logger.info(f"删除作业成功: ID={work_id}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除作业失败: {e}")
            raise

    async def copy_work(
            self,
            db: AsyncSession,
            data: JobWorkCopy
    ) -> Dict[str, Any]:
        """复制作业"""
        try:
            # 获取源作业
            source_work = await self.get_work_by_id(db, data.workId)
            if not source_work:
                raise ValueError(f"源作业不存在: {data.workId}")

            # 创建新作业
            new_work = JobWork(
                workflow_id=source_work.workflow_id,
                name=data.name,
                work_type=source_work.work_type,
                remark=source_work.remark,
                status=JobWorkStatus.DRAFT,
                executor=source_work.executor,
                config=source_work.config  # 复制配置
            )

            db.add(new_work)
            await db.commit()
            await db.refresh(new_work)

            logger.info(f"复制作业成功: {new_work.name} (源ID: {data.workId}, 新ID: {new_work.id})")

            return {
                "workId": new_work.id,
                "name": new_work.name
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"复制作业失败: {e}")
            raise

    # ==================== 作业配置 ====================

    async def save_work_config(
            self,
            db: AsyncSession,
            work_id: int,
            new_config: Dict[str, Any]
    ) -> Optional[JobWork]:
        """保存作业配置（智能合并）"""
        try:
            work = await self.get_work_by_id(db, work_id)
            if not work:
                return None

            # 获取原配置
            old_config = work.config or {}

            # 🆕 智能合并配置
            # 对于SQL类作业，保留dataSourceId等创建时的配置
            if work.work_type in [JobWorkType.EXE_JDBC, JobWorkType.QUERY_JDBC]:
                # 保留创建时的dataSourceId
                if 'dataSourceId' in old_config and 'dataSourceId' not in new_config:
                    new_config['dataSourceId'] = old_config['dataSourceId']

                # 确保有默认值
                if 'timeout' not in new_config:
                    new_config['timeout'] = 300
                if 'type' not in new_config:
                    new_config['type'] = 'execute' if work.work_type == JobWorkType.EXE_JDBC else 'query'

            # 对于Spark/Flink SQL，保留集群配置
            elif work.work_type in [JobWorkType.SPARK_SQL, JobWorkType.FLINK_SQL]:
                for key in ['clusterId', 'containerId', 'sparkConfig', 'flinkConfig']:
                    if key in old_config and key not in new_config:
                        new_config[key] = old_config[key]

            # 更新配置
            work.config = new_config

            await db.commit()
            await db.refresh(work)

            logger.info(f"保存作业配置成功: {work.name} (ID: {work.id})")
            return work

        except Exception as e:
            await db.rollback()
            logger.error(f"保存作业配置失败: {e}")
            raise

    async def get_work_config(
            self,
            db: AsyncSession,
            work_id: int
    ) -> Optional[Dict[str, Any]]:
        """获取作业配置"""
        try:
            work = await self.get_work_by_id(db, work_id)
            if not work:
                return None

            return {
                "id": work.id,
                "name": work.name,
                "workType": work.work_type.value,
                "remark": work.remark,
                "status": work.status.value,
                "config": work.config or {},
                "createDateTime": work.created_at.isoformat() if work.created_at else None,
                "updateDateTime": work.updated_at.isoformat() if work.updated_at else None
            }

        except Exception as e:
            logger.error(f"获取作业配置失败: {e}")
            raise

    # ==================== 作业状态管理 ====================

    async def publish_work(
            self,
            db: AsyncSession,
            work_id: int
    ) -> Optional[JobWork]:
        """发布作业"""
        try:
            work = await self.get_work_by_id(db, work_id)
            if not work:
                return None

            work.status = JobWorkStatus.ONLINE
            await db.commit()
            await db.refresh(work)

            logger.info(f"发布作业成功: {work.name}")
            return work

        except Exception as e:
            await db.rollback()
            logger.error(f"发布作业失败: {e}")
            raise

    async def offline_work(
            self,
            db: AsyncSession,
            work_id: int
    ) -> Optional[JobWork]:
        """下线作业"""
        try:
            work = await self.get_work_by_id(db, work_id)
            if not work:
                return None

            work.status = JobWorkStatus.OFFLINE
            await db.commit()
            await db.refresh(work)

            logger.info(f"下线作业成功: {work.name}")
            return work

        except Exception as e:
            await db.rollback()
            logger.error(f"下线作业失败: {e}")
            raise


# 创建全局实例
job_work_service = JobWorkService()