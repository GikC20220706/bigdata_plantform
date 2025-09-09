# app/services/user_cluster_service.py
"""
用户计算集群管理服务层
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, or_, func
from sqlalchemy.orm import selectinload
from loguru import logger

from app.models.user_cluster import UserCluster, ClusterType, ClusterStatus
from app.utils.response import create_response


class UserClusterService:
    """用户计算集群管理服务"""

    async def get_clusters_list(
            self,
            db: AsyncSession,
            page: int = 1,
            page_size: int = 10,
            search_keyword: str = ""
    ) -> Dict:
        """获取用户集群列表（分页）"""
        try:
            # 构建查询条件
            query = select(UserCluster)

            if search_keyword:
                query = query.where(
                    or_(
                        UserCluster.name.contains(search_keyword),
                        UserCluster.remark.contains(search_keyword)
                    )
                )

            # 计算总数
            count_query = select(func.count(UserCluster.id))
            if search_keyword:
                count_query = count_query.where(
                    or_(
                        UserCluster.name.contains(search_keyword),
                        UserCluster.remark.contains(search_keyword)
                    )
                )

            total_result = await db.execute(count_query)
            total = total_result.scalar()

            # 分页查询
            query = query.order_by(UserCluster.updated_at.desc())
            query = query.offset((page - 1) * page_size).limit(page_size)

            result = await db.execute(query)
            clusters = result.scalars().all()

            return {
                "content": [cluster.to_dict() for cluster in clusters],
                "totalElements": total,
                "currentPage": page,
                "pageSize": page_size,
                "totalPages": (total + page_size - 1) // page_size
            }

        except Exception as e:
            logger.error(f"获取集群列表失败: {e}")
            raise

    async def add_cluster(
            self,
            db: AsyncSession,
            name: str,
            cluster_type: str,
            remark: str = "",
            config: Dict = None
    ) -> Dict:
        """添加新集群"""
        try:
            # 检查名称是否重复
            existing = await db.execute(
                select(UserCluster).where(UserCluster.name == name)
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"集群名称 '{name}' 已存在")

            # 验证集群类型
            try:
                cluster_type_enum = ClusterType(cluster_type)
            except ValueError:
                raise ValueError(f"不支持的集群类型: {cluster_type}")

            # 创建集群记录
            new_cluster = UserCluster(
                name=name,
                cluster_type=cluster_type_enum,
                remark=remark,
                config=config or {},
                status=ClusterStatus.CHECKING
            )

            db.add(new_cluster)
            await db.commit()
            await db.refresh(new_cluster)

            # 异步检测集群状态
            asyncio.create_task(self._check_cluster_status(new_cluster.id))

            logger.info(f"成功添加集群: {name}")
            return new_cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"添加集群失败: {e}")
            raise

    async def update_cluster(
            self,
            db: AsyncSession,
            cluster_id: int,
            name: str = None,
            cluster_type: str = None,
            remark: str = None,
            config: Dict = None
    ) -> Dict:
        """更新集群信息"""
        try:
            # 获取集群
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 检查名称重复（如果更改了名称）
            if name and name != cluster.name:
                existing = await db.execute(
                    select(UserCluster).where(
                        and_(UserCluster.name == name, UserCluster.id != cluster_id)
                    )
                )
                if existing.scalar_one_or_none():
                    raise ValueError(f"集群名称 '{name}' 已存在")
                cluster.name = name

            # 更新其他字段
            if cluster_type:
                try:
                    cluster.cluster_type = ClusterType(cluster_type)
                except ValueError:
                    raise ValueError(f"不支持的集群类型: {cluster_type}")

            if remark is not None:
                cluster.remark = remark

            if config is not None:
                cluster.config = config

            cluster.updated_at = datetime.now()

            await db.commit()
            await db.refresh(cluster)

            logger.info(f"成功更新集群: {cluster.name}")
            return cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"更新集群失败: {e}")
            raise

    async def delete_cluster(self, db: AsyncSession, cluster_id: int) -> bool:
        """删除集群"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            cluster_name = cluster.name
            await db.delete(cluster)
            await db.commit()

            logger.info(f"成功删除集群: {cluster_name}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除集群失败: {e}")
            raise

    async def check_cluster(self, db: AsyncSession, cluster_id: int) -> Dict:
        """检测集群状态"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 模拟集群检测逻辑
            status, node_info, memory_info, storage_info = await self._perform_cluster_check(cluster)

            # 更新状态
            cluster.update_status_info(status, node_info, memory_info, storage_info)
            await db.commit()
            await db.refresh(cluster)

            return cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"检测集群失败: {e}")
            raise

    async def set_default_cluster(self, db: AsyncSession, cluster_id: int) -> bool:
        """设置默认集群"""
        try:
            # 先取消所有默认集群
            await db.execute(
                update(UserCluster).values(is_default=False)
            )

            # 设置新的默认集群
            result = await db.execute(
                update(UserCluster)
                .where(UserCluster.id == cluster_id)
                .values(is_default=True)
            )

            if result.rowcount == 0:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            await db.commit()
            logger.info(f"成功设置默认集群: {cluster_id}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"设置默认集群失败: {e}")
            raise

    async def get_cluster_summary(self, db: AsyncSession) -> Dict:
        """获取集群汇总信息（用于首页显示）"""
        try:
            # 统计各状态集群数量
            result = await db.execute(
                select(UserCluster.status, func.count(UserCluster.id))
                .group_by(UserCluster.status)
            )
            status_counts = dict(result.fetchall())

            total_clusters = sum(status_counts.values())
            available_clusters = status_counts.get(ClusterStatus.ACTIVE, 0)

            return {
                "total_clusters": total_clusters,
                "available_clusters": available_clusters,
                "unavailable_clusters": total_clusters - available_clusters,
                "status_breakdown": {
                    status.value: count for status, count in status_counts.items()
                }
            }

        except Exception as e:
            logger.error(f"获取集群汇总信息失败: {e}")
            return {
                "total_clusters": 0,
                "available_clusters": 0,
                "unavailable_clusters": 0,
                "status_breakdown": {}
            }

    async def _perform_cluster_check(self, cluster: UserCluster) -> Tuple[ClusterStatus, str, str, str]:
        """执行实际的集群检测"""
        try:
            # 这里应该根据集群类型实现真实的检测逻辑
            # 目前返回模拟数据
            await asyncio.sleep(1)  # 模拟检测时间

            if cluster.cluster_type == ClusterType.YARN:
                return ClusterStatus.ACTIVE, "2/3", "32GB/64GB", "1.2TB/2TB"
            elif cluster.cluster_type == ClusterType.KUBERNETES:
                return ClusterStatus.ACTIVE, "5/5", "64GB/128GB", "2.5TB/5TB"
            else:
                return ClusterStatus.ACTIVE, "1/1", "16GB/32GB", "500GB/1TB"

        except Exception as e:
            logger.error(f"集群检测失败: {e}")
            return ClusterStatus.ERROR, "0/0", "0GB", "0GB"

    async def _check_cluster_status(self, cluster_id: int):
        """后台异步检测集群状态"""
        try:
            # 这里应该实现真实的异步检测逻辑
            await asyncio.sleep(3)  # 模拟检测延迟
            logger.info(f"后台检测集群 {cluster_id} 完成")
        except Exception as e:
            logger.error(f"后台检测集群 {cluster_id} 失败: {e}")


# 创建全局服务实例
user_cluster_service = UserClusterService()