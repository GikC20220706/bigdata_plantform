# app/services/cluster_database_service.py
"""
生产级集群数据库服务 - 负责集群数据的数据库操作
专门处理集群、节点、指标的数据库CRUD操作和数据持久化
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy import and_, func, desc, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from loguru import logger

from app.models.cluster import Cluster, ClusterNode, ClusterMetric
from app.utils.metrics_collector import metrics_collector


class ClusterDatabaseService:
    """集群数据库服务 - 生产级数据持久化"""

    def __init__(self):
        """初始化集群数据库服务"""
        pass

    async def get_or_create_cluster(
            self,
            db: AsyncSession,
            cluster_name: str,
            cluster_type: str
    ) -> Cluster:
        """获取或创建集群记录"""
        try:
            # 查询已存在的集群
            stmt = select(Cluster).where(
                and_(
                    Cluster.name == cluster_name,
                    Cluster.cluster_type == cluster_type
                )
            )
            result = await db.execute(stmt)
            cluster = result.scalar_one_or_none()

            if not cluster:
                # 创建新的集群记录
                cluster = Cluster(
                    name=cluster_name,
                    cluster_type=cluster_type,
                    status="unknown",
                    is_active=True,
                    total_nodes=0,
                    active_nodes=0
                )
                db.add(cluster)
                await db.commit()
                await db.refresh(cluster)
                logger.info(f"创建新集群记录: {cluster_name} ({cluster_type})")

            return cluster

        except Exception as e:
            logger.error(f"获取或创建集群失败: {e}")
            await db.rollback()
            raise

    async def update_cluster_metrics(
            self,
            db: AsyncSession,
            cluster: Cluster,
            metrics_data: Dict
    ) -> None:
        """更新集群指标数据"""
        try:
            # 更新集群基本指标
            cluster.status = self._determine_status(metrics_data)
            cluster.total_nodes = metrics_data.get('total_nodes', 0)
            cluster.active_nodes = metrics_data.get('active_nodes', 0)
            cluster.cpu_usage = metrics_data.get('cpu_usage', 0.0)
            cluster.memory_usage = metrics_data.get('memory_usage', 0.0)
            cluster.disk_usage = metrics_data.get('disk_usage', 0.0)
            cluster.last_health_check = datetime.now()

            # 保存历史指标数据
            metric_record = ClusterMetric(
                cluster_id=cluster.id,
                metric_timestamp=datetime.now(),
                cpu_usage=metrics_data.get('cpu_usage', 0.0),
                memory_usage=metrics_data.get('memory_usage', 0.0),
                disk_usage=metrics_data.get('disk_usage', 0.0),
                total_nodes=metrics_data.get('total_nodes', 0),
                active_nodes=metrics_data.get('active_nodes', 0),
                additional_metrics=metrics_data.get('additional_metrics', {})
            )
            db.add(metric_record)

            await db.commit()
            logger.debug(f"集群 {cluster.name} 指标数据已更新")

        except Exception as e:
            logger.error(f"更新集群指标失败: {e}")
            await db.rollback()
            raise

    async def update_cluster_nodes(
            self,
            db: AsyncSession,
            cluster: Cluster,
            nodes_data: List[Dict]
    ) -> None:
        """更新集群节点信息"""
        try:
            for node_data in nodes_data:
                hostname = node_data.get('name', node_data.get('hostname', ''))
                ip_address = node_data.get('host', node_data.get('ip_address', ''))

                if not hostname or not ip_address:
                    continue

                # 查询现有节点
                stmt = select(ClusterNode).where(
                    and_(
                        ClusterNode.cluster_id == cluster.id,
                        ClusterNode.hostname == hostname
                    )
                )
                result = await db.execute(stmt)
                node = result.scalar_one_or_none()

                metrics = node_data.get('metrics', {})

                if not node:
                    # 创建新节点
                    node = ClusterNode(
                        cluster_id=cluster.id,
                        hostname=hostname,
                        ip_address=ip_address,
                        role=node_data.get('role', 'unknown'),
                        status='online' if metrics.get('success', False) else 'offline',
                        cpu_usage=metrics.get('cpu_usage', 0.0),
                        memory_usage=metrics.get('memory_usage', 0.0),
                        disk_usage=metrics.get('disk_usage', 0.0),
                        last_seen=datetime.now()
                    )
                    db.add(node)
                else:
                    # 更新现有节点
                    node.ip_address = ip_address
                    node.role = node_data.get('role', node.role)
                    node.status = 'online' if metrics.get('success', False) else 'offline'
                    node.cpu_usage = metrics.get('cpu_usage', 0.0)
                    node.memory_usage = metrics.get('memory_usage', 0.0)
                    node.disk_usage = metrics.get('disk_usage', 0.0)
                    node.last_seen = datetime.now()

            await db.commit()
            logger.debug(f"集群 {cluster.name} 节点信息已更新")

        except Exception as e:
            logger.error(f"更新节点信息失败: {e}")
            await db.rollback()
            raise

    async def get_cluster_with_nodes(
            self,
            db: AsyncSession,
            cluster_type: str
    ) -> Optional[Cluster]:
        """获取集群及其节点信息"""
        try:
            stmt = select(Cluster).options(
                selectinload(Cluster.nodes)
            ).where(
                Cluster.cluster_type == cluster_type
            ).order_by(desc(Cluster.last_health_check))

            result = await db.execute(stmt)
            cluster = result.scalar_one_or_none()

            return cluster

        except Exception as e:
            logger.error(f"查询集群信息失败: {e}")
            return None

    async def get_all_clusters(self, db: AsyncSession) -> List[Cluster]:
        """获取所有集群信息"""
        try:
            stmt = select(Cluster).options(
                selectinload(Cluster.nodes)
            ).where(
                Cluster.is_active == True
            ).order_by(Cluster.cluster_type, Cluster.name)

            result = await db.execute(stmt)
            clusters = result.scalars().all()

            return list(clusters)

        except Exception as e:
            logger.error(f"查询所有集群失败: {e}")
            return []

    async def get_cluster_historical_metrics(
            self,
            db: AsyncSession,
            cluster_id: int,
            hours: int = 24
    ) -> List[ClusterMetric]:
        """获取集群历史指标数据"""
        try:
            start_time = datetime.now() - timedelta(hours=hours)

            stmt = select(ClusterMetric).where(
                and_(
                    ClusterMetric.cluster_id == cluster_id,
                    ClusterMetric.metric_timestamp >= start_time
                )
            ).order_by(desc(ClusterMetric.metric_timestamp))

            result = await db.execute(stmt)
            metrics = result.scalars().all()

            return list(metrics)

        except Exception as e:
            logger.error(f"查询历史指标失败: {e}")
            return []

    async def cleanup_old_metrics(
            self,
            db: AsyncSession,
            days_to_keep: int = 30
    ) -> int:
        """清理旧的指标数据"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)

            stmt = select(func.count(ClusterMetric.id)).where(
                ClusterMetric.metric_timestamp < cutoff_date
            )
            result = await db.execute(stmt)
            count_to_delete = result.scalar()

            if count_to_delete > 0:
                delete_stmt = ClusterMetric.__table__.delete().where(
                    ClusterMetric.metric_timestamp < cutoff_date
                )
                await db.execute(delete_stmt)
                await db.commit()

                logger.info(f"清理了 {count_to_delete} 条历史指标数据")

            return count_to_delete

        except Exception as e:
            logger.error(f"清理历史数据失败: {e}")
            await db.rollback()
            return 0

    def _determine_status(self, metrics_data: Dict) -> str:
        """根据指标数据确定集群状态"""
        active_nodes = metrics_data.get('active_nodes', 0)
        total_nodes = metrics_data.get('total_nodes', 0)

        if total_nodes == 0:
            return "unknown"

        # 计算健康节点比例
        healthy_ratio = active_nodes / total_nodes if total_nodes > 0 else 0

        # 检查资源使用情况
        cpu_usage = metrics_data.get('cpu_usage', 0.0)
        memory_usage = metrics_data.get('memory_usage', 0.0)
        disk_usage = metrics_data.get('disk_usage', 0.0)

        # 状态判断逻辑
        if healthy_ratio < 0.5:
            return "error"
        elif healthy_ratio < 0.8 or cpu_usage > 90 or memory_usage > 90 or disk_usage > 95:
            return "warning"
        elif active_nodes == total_nodes and cpu_usage < 80 and memory_usage < 80:
            return "normal"
        else:
            return "warning"

    async def get_cluster_alerts(
            self,
            db: AsyncSession,
            cluster_type: Optional[str] = None
    ) -> List[Dict]:
        """获取集群告警信息（基于真实数据）"""
        try:
            alerts = []

            # 查询条件
            conditions = [Cluster.is_active == True]
            if cluster_type:
                conditions.append(Cluster.cluster_type == cluster_type)

            stmt = select(Cluster).options(
                selectinload(Cluster.nodes)
            ).where(and_(*conditions))

            result = await db.execute(stmt)
            clusters = result.scalars().all()

            for cluster in clusters:
                # 集群级别告警
                if cluster.status == "error":
                    alerts.append({
                        "id": f"cluster_{cluster.id}",
                        "cluster": cluster.name,
                        "level": "error",
                        "title": f"{cluster.name}集群状态异常",
                        "message": f"集群中有大量节点离线，当前状态: {cluster.status}",
                        "timestamp": cluster.last_health_check or datetime.now(),
                        "status": "active",
                        "metric_type": "cluster_health"
                    })

                # 资源使用告警
                if cluster.cpu_usage and cluster.cpu_usage > 90:
                    alerts.append({
                        "id": f"cpu_{cluster.id}",
                        "cluster": cluster.name,
                        "level": "warning",
                        "title": f"{cluster.name}集群CPU使用率过高",
                        "message": f"CPU使用率 {cluster.cpu_usage:.1f}%，建议检查负载",
                        "timestamp": cluster.last_health_check or datetime.now(),
                        "status": "active",
                        "metric_type": "cpu_usage"
                    })

                if cluster.memory_usage and cluster.memory_usage > 85:
                    alerts.append({
                        "id": f"memory_{cluster.id}",
                        "cluster": cluster.name,
                        "level": "warning",
                        "title": f"{cluster.name}集群内存使用率过高",
                        "message": f"内存使用率 {cluster.memory_usage:.1f}%，可能影响性能",
                        "timestamp": cluster.last_health_check or datetime.now(),
                        "status": "active",
                        "metric_type": "memory_usage"
                    })

                # 节点级别告警
                for node in cluster.nodes:
                    if node.status == 'offline':
                        alerts.append({
                            "id": f"node_{node.id}",
                            "cluster": cluster.name,
                            "level": "error",
                            "title": f"节点 {node.hostname} 离线",
                            "message": f"{cluster.name}集群节点 {node.hostname} ({node.ip_address}) 无法连接",
                            "timestamp": node.last_seen or datetime.now(),
                            "status": "active",
                            "metric_type": "node_offline"
                        })

            return alerts

        except Exception as e:
            logger.error(f"获取集群告警失败: {e}")
            return []


# 创建全局实例
cluster_db_service = ClusterDatabaseService()