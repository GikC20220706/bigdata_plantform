# app/models/cluster.py
"""
生产级集群数据模型 - 移除所有模拟数据，支持完整的集群生命周期管理
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, Float, DateTime,
    JSON, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.models.base import BaseModel


class ClusterStatus(str, Enum):
    """集群状态枚举"""
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    ERROR = "error"
    INITIALIZING = "initializing"


class ClusterType(str, Enum):
    """集群类型枚举"""
    HADOOP = "hadoop"
    FLINK = "flink"
    DORIS = "doris"


class NodeStatus(str, Enum):
    """节点状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    MAINTENANCE = "maintenance"


class Cluster(BaseModel):
    """
    集群主表 - 存储集群基础信息和配置
    """
    __tablename__ = "clusters"

    # 基础信息
    name = Column(String(100), nullable=False, unique=True, index=True, comment="集群名称")
    cluster_type = Column(String(20), nullable=False, index=True, comment="集群类型")
    description = Column(Text, comment="集群描述")
    version = Column(String(50), comment="集群版本")

    # 状态管理
    status = Column(String(20), nullable=False, default=ClusterStatus.ONLINE,
                    index=True, comment="集群状态")
    is_active = Column(Boolean, nullable=False, default=True,
                       index=True, comment="是否启用监控")

    # 配置信息
    config = Column(JSON, comment="集群配置信息")
    endpoints = Column(JSON, comment="集群服务端点")
    connection_config = Column(JSON, comment="连接配置(SSH等)")

    # 监控配置
    health_check_interval = Column(Integer, default=300, comment="健康检查间隔(秒)")
    metrics_collection_interval = Column(Integer, default=60, comment="指标收集间隔(秒)")
    alert_thresholds = Column(JSON, comment="告警阈值配置")

    # 统计信息(冗余字段，用于快速查询)
    total_nodes = Column(Integer, default=0, comment="总节点数")
    active_nodes = Column(Integer, default=0, comment="活跃节点数")
    last_health_check = Column(DateTime(timezone=True), comment="最后健康检查时间")
    last_metrics_update = Column(DateTime(timezone=True), comment="最后指标更新时间")

    # 关联关系
    nodes = relationship("ClusterNode", back_populates="cluster",
                         cascade="all, delete-orphan", lazy="select")
    metrics = relationship("ClusterMetric", back_populates="cluster",
                           cascade="all, delete-orphan")
    operations = relationship("ClusterOperation", back_populates="cluster",
                              cascade="all, delete-orphan")
    alerts = relationship("ClusterAlert", back_populates="cluster",
                          cascade="all, delete-orphan")

    # 索引
    __table_args__ = (
        Index('idx_cluster_type_status', 'cluster_type', 'status'),
        Index('idx_cluster_active', 'is_active', 'status'),
        Index('idx_cluster_health_check', 'last_health_check'),
    )


class ClusterNode(BaseModel):
    """
    集群节点表 - 存储节点详细信息
    """
    __tablename__ = "cluster_nodes"

    # 基础信息
    cluster_id = Column(Integer, ForeignKey('clusters.id', ondelete='CASCADE'),
                        nullable=False, index=True)
    node_name = Column(String(100), nullable=False, comment="节点名称")
    hostname = Column(String(255), nullable=False, comment="主机名")
    ip_address = Column(String(45), nullable=False, comment="IP地址")
    port = Column(Integer, default=22, comment="SSH端口")

    # 角色和状态
    role = Column(String(50), comment="节点角色(NameNode, DataNode等)")
    status = Column(String(20), nullable=False, default=NodeStatus.ACTIVE,
                    index=True, comment="节点状态")

    # 配置信息
    ssh_config = Column(JSON, comment="SSH连接配置")
    service_config = Column(JSON, comment="服务配置")
    hardware_info = Column(JSON, comment="硬件信息")

    # 监控状态
    last_heartbeat = Column(DateTime(timezone=True), comment="最后心跳时间")
    uptime_start = Column(DateTime(timezone=True), comment="启动时间")

    # 当前资源使用情况(缓存字段)
    cpu_usage = Column(Float, comment="CPU使用率")
    memory_usage = Column(Float, comment="内存使用率")
    disk_usage = Column(Float, comment="磁盘使用率")
    network_io = Column(JSON, comment="网络IO统计")

    # 关联关系
    cluster = relationship("Cluster", back_populates="nodes")
    metrics = relationship("ClusterMetric", back_populates="node",
                           cascade="all, delete-orphan")

    # 约束和索引
    __table_args__ = (
        UniqueConstraint('cluster_id', 'node_name', name='uk_cluster_node'),
        UniqueConstraint('cluster_id', 'ip_address', 'port', name='uk_cluster_endpoint'),
        Index('idx_node_cluster_status', 'cluster_id', 'status'),
        Index('idx_node_heartbeat', 'last_heartbeat'),
        Index('idx_node_role', 'role', 'status'),
    )


class ClusterMetric(BaseModel):
    """
    集群指标历史表 - 存储所有监控指标数据
    """
    __tablename__ = "cluster_metrics"

    # 关联信息
    cluster_id = Column(Integer, ForeignKey('clusters.id', ondelete='CASCADE'),
                        nullable=False, index=True)
    node_id = Column(Integer, ForeignKey('cluster_nodes.id', ondelete='CASCADE'),
                     nullable=True, index=True)

    # 指标信息
    metric_type = Column(String(50), nullable=False, index=True,
                         comment="指标类型(system, service, business)")
    metric_category = Column(String(50), nullable=False, index=True,
                             comment="指标分类(cpu, memory, disk, network)")
    metric_name = Column(String(100), nullable=False, comment="指标名称")

    # 指标数据
    metric_value = Column(Float, comment="数值型指标值")
    metric_data = Column(JSON, nullable=False, comment="完整指标数据")

    # 时间信息
    collected_at = Column(DateTime(timezone=True), nullable=False,
                          default=func.now(), index=True, comment="采集时间")

    # 关联关系
    cluster = relationship("Cluster", back_populates="metrics")
    node = relationship("ClusterNode", back_populates="metrics")

    # 索引
    __table_args__ = (
        Index('idx_metric_cluster_time', 'cluster_id', 'collected_at'),
        Index('idx_metric_node_time', 'node_id', 'collected_at'),
        Index('idx_metric_type_time', 'metric_type', 'collected_at'),
        Index('idx_metric_category', 'metric_category', 'collected_at'),
    )


class ClusterOperation(BaseModel):
    """
    集群操作记录表 - 记录所有集群操作和变更
    """
    __tablename__ = "cluster_operations"

    # 关联信息
    cluster_id = Column(Integer, ForeignKey('clusters.id', ondelete='SET NULL'),
                        nullable=True, index=True)

    # 操作信息
    operation_type = Column(String(50), nullable=False, index=True,
                            comment="操作类型(restart, config_change, scale等)")
    operation_name = Column(String(100), nullable=False, comment="操作名称")
    description = Column(Text, comment="操作描述")

    # 操作参数和结果
    parameters = Column(JSON, comment="操作参数")
    result_status = Column(String(20), nullable=False, index=True,
                           comment="执行结果(success, failed, partial)")
    result_data = Column(JSON, comment="执行结果数据")
    error_message = Column(Text, comment="错误信息")

    # 执行信息
    execution_duration = Column(Float, comment="执行耗时(秒)")
    operator = Column(String(100), comment="操作员")
    client_ip = Column(String(45), comment="客户端IP")

    # 关联关系
    cluster = relationship("Cluster", back_populates="operations")

    # 索引
    __table_args__ = (
        Index('idx_operation_cluster_time', 'cluster_id', 'created_at'),
        Index('idx_operation_type_status', 'operation_type', 'result_status'),
        Index('idx_operation_operator', 'operator', 'created_at'),
    )


class ClusterAlert(BaseModel):
    """
    集群告警表 - 存储告警信息和处理状态
    """
    __tablename__ = "cluster_alerts"

    # 关联信息
    cluster_id = Column(Integer, ForeignKey('clusters.id', ondelete='CASCADE'),
                        nullable=False, index=True)

    # 告警信息
    alert_type = Column(String(50), nullable=False, index=True,
                        comment="告警类型(cpu_high, memory_high, disk_full等)")
    severity = Column(String(20), nullable=False, index=True,
                      comment="告警级别(critical, warning, info)")
    title = Column(String(200), nullable=False, comment="告警标题")
    description = Column(Text, comment="告警描述")

    # 告警数据
    alert_data = Column(JSON, comment="告警相关数据")
    threshold_config = Column(JSON, comment="触发阈值配置")
    current_value = Column(Float, comment="当前值")

    # 状态管理
    status = Column(String(20), nullable=False, default="active", index=True,
                    comment="告警状态(active, acknowledged, resolved)")
    acknowledged_at = Column(DateTime(timezone=True), comment="确认时间")
    acknowledged_by = Column(String(100), comment="确认人")
    resolved_at = Column(DateTime(timezone=True), comment="解决时间")
    resolved_by = Column(String(100), comment="解决人")

    # 关联关系
    cluster = relationship("Cluster", back_populates="alerts")

    # 索引
    __table_args__ = (
        Index('idx_alert_cluster_status', 'cluster_id', 'status'),
        Index('idx_alert_type_severity', 'alert_type', 'severity'),
        Index('idx_alert_time', 'created_at', 'status'),
    )


class ClusterService(BaseModel):
    """
    集群服务表 - 管理集群中的各个服务组件
    """
    __tablename__ = "cluster_services"

    # 关联信息
    cluster_id = Column(Integer, ForeignKey('clusters.id', ondelete='CASCADE'),
                        nullable=False, index=True)
    node_id = Column(Integer, ForeignKey('cluster_nodes.id', ondelete='CASCADE'),
                     nullable=True, index=True)

    # 服务信息
    service_name = Column(String(100), nullable=False, comment="服务名称")
    service_type = Column(String(50), nullable=False, comment="服务类型")
    version = Column(String(50), comment="服务版本")

    # 配置和状态
    config = Column(JSON, comment="服务配置")
    status = Column(String(20), nullable=False, default="unknown", index=True,
                    comment="服务状态")

    # 端口和访问信息
    ports = Column(JSON, comment="服务端口配置")
    health_check_url = Column(String(500), comment="健康检查URL")
    web_ui_url = Column(String(500), comment="Web UI访问地址")

    # 监控信息
    last_check_at = Column(DateTime(timezone=True), comment="最后检查时间")
    start_time = Column(DateTime(timezone=True), comment="服务启动时间")

    # 关联关系
    cluster = relationship("Cluster")
    node = relationship("ClusterNode")

    # 约束和索引
    __table_args__ = (
        UniqueConstraint('cluster_id', 'node_id', 'service_name',
                         name='uk_cluster_node_service'),
        Index('idx_service_cluster_status', 'cluster_id', 'status'),
        Index('idx_service_type', 'service_type', 'status'),
    )


# ===================================================================
# 数据初始化和迁移脚本
# ===================================================================

class ClusterDataMigration:
    """数据迁移工具 - 从配置文件迁移到数据库"""

    @staticmethod
    async def migrate_from_config(session, config_file: str):
        """从nodes.json配置文件迁移数据"""
        import json
        from pathlib import Path

        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_file}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # 创建集群记录
        clusters_data = {
            'hadoop': {
                'name': 'Hadoop生产集群',
                'cluster_type': ClusterType.HADOOP,
                'description': 'Hadoop分布式存储和计算集群'
            },
            'flink': {
                'name': 'Flink实时计算集群',
                'cluster_type': ClusterType.FLINK,
                'description': 'Flink流处理集群'
            },
            'doris': {
                'name': 'Doris分析数据库集群',
                'cluster_type': ClusterType.DORIS,
                'description': 'Apache Doris OLAP数据库集群'
            }
        }

        created_clusters = {}

        for cluster_type, cluster_info in clusters_data.items():
            cluster = Cluster(**cluster_info)
            session.add(cluster)
            await session.flush()  # 获取ID
            created_clusters[cluster_type] = cluster

        # 迁移节点数据
        for node_config in config.get('nodes', []):
            # 根据节点角色确定集群类型
            cluster_type = ClusterDataMigration._determine_cluster_type(
                node_config.get('role', ''))

            if cluster_type not in created_clusters:
                continue

            cluster = created_clusters[cluster_type]

            node = ClusterNode(
                cluster_id=cluster.id,
                node_name=node_config['node_name'],
                hostname=node_config.get('hostname', node_config['node_name']),
                ip_address=node_config['ip_address'],
                port=node_config.get('ssh_config', {}).get('port', 22),
                role=node_config.get('role'),
                status=NodeStatus.ACTIVE if node_config.get('enabled', True)
                else NodeStatus.INACTIVE,
                ssh_config=node_config.get('ssh_config', {}),
                service_config=node_config.get('services', [])
            )
            session.add(node)

        await session.commit()
        return len(created_clusters), len(config.get('nodes', []))

    @staticmethod
    def _determine_cluster_type(role: str) -> str:
        """根据节点角色确定集群类型"""
        role_mapping = {
            'namenode': 'hadoop',
            'datanode': 'hadoop',
            'secondary_namenode': 'hadoop',
            'resourcemanager': 'hadoop',
            'nodemanager': 'hadoop',
            'jobmanager': 'flink',
            'taskmanager': 'flink',
            'frontend': 'doris',
            'backend': 'doris'
        }

        role_lower = role.lower().replace('-', '_').replace(' ', '_')
        return role_mapping.get(role_lower, 'hadoop')  # 默认hadoop


# ===================================================================
# 数据访问层 (Repository Pattern)
# ===================================================================

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update, delete, func, and_, or_
from sqlalchemy.orm import selectinload, joinedload
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta


class ClusterRepository:
    """集群数据访问层 - 提供所有数据库操作方法"""

    def __init__(self, session: AsyncSession):
        self.session = session

    # =============== 集群管理 ===============

    async def get_all_clusters(self, include_nodes: bool = False) -> List[Cluster]:
        """获取所有活跃集群"""
        stmt = select(Cluster).where(Cluster.status != 'deleted')

        if include_nodes:
            stmt = stmt.options(selectinload(Cluster.nodes))

        result = await self.session.execute(stmt.order_by(Cluster.created_at))
        return list(result.scalars().all())

    async def get_cluster_by_type(self, cluster_type: str,
                                  include_nodes: bool = False) -> Optional[Cluster]:
        """根据类型获取集群"""
        stmt = (select(Cluster)
                .where(Cluster.cluster_type == cluster_type)
                .where(Cluster.status != 'deleted'))

        if include_nodes:
            stmt = stmt.options(selectinload(Cluster.nodes))

        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def create_cluster(self, cluster_data: Dict[str, Any]) -> Cluster:
        """创建新集群"""
        cluster = Cluster(**cluster_data)
        self.session.add(cluster)
        await self.session.flush()
        return cluster

    async def update_cluster_status(self, cluster_id: int,
                                    status: str, **kwargs) -> bool:
        """更新集群状态"""
        update_data = {'status': status, 'updated_at': datetime.now()}
        update_data.update(kwargs)

        stmt = (update(Cluster)
                .where(Cluster.id == cluster_id)
                .values(**update_data))

        result = await self.session.execute(stmt)
        await self.session.commit()
        return result.rowcount > 0

    # =============== 节点管理 ===============

    async def get_cluster_nodes(self, cluster_id: int,
                                status_filter: Optional[str] = None) -> List[ClusterNode]:
        """获取集群节点"""
        stmt = select(ClusterNode).where(ClusterNode.cluster_id == cluster_id)

        if status_filter:
            stmt = stmt.where(ClusterNode.status == status_filter)

        result = await self.session.execute(stmt.order_by(ClusterNode.node_name))
        return list(result.scalars().all())

    async def update_node_metrics(self, node_id: int,
                                  metrics: Dict[str, Any]) -> bool:
        """更新节点资源指标"""
        update_data = {
            'cpu_usage': metrics.get('cpu_usage'),
            'memory_usage': metrics.get('memory_usage'),
            'disk_usage': metrics.get('disk_usage'),
            'last_heartbeat': datetime.now(),
            'updated_at': datetime.now()
        }

        # 过滤None值
        update_data = {k: v for k, v in update_data.items() if v is not None}

        stmt = (update(ClusterNode)
                .where(ClusterNode.id == node_id)
                .values(**update_data))

        result = await self.session.execute(stmt)
        await self.session.commit()
        return result.rowcount > 0

    # =============== 指标管理 ===============

    async def save_cluster_metrics(self, cluster_id: int,
                                   metrics: Dict[str, Any],
                                   node_id: Optional[int] = None) -> ClusterMetric:
        """保存集群指标数据"""
        metric = ClusterMetric(
            cluster_id=cluster_id,
            node_id=node_id,
            metric_type=metrics.get('metric_type', 'system'),
            metric_category=metrics.get('category', 'general'),
            metric_name=metrics.get('name', 'cluster_metrics'),
            metric_value=metrics.get('value'),
            metric_data=metrics,
            collected_at=datetime.now()
        )

        self.session.add(metric)
        await self.session.flush()
        return metric

    async def get_latest_metrics(self, cluster_id: int,
                                 hours: int = 1) -> List[ClusterMetric]:
        """获取最新指标数据"""
        since = datetime.now() - timedelta(hours=hours)

        stmt = (select(ClusterMetric)
                .where(ClusterMetric.cluster_id == cluster_id)
                .where(ClusterMetric.collected_at >= since)
                .order_by(ClusterMetric.collected_at.desc()))

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # =============== 操作记录 ===============

    async def log_operation(self, operation_data: Dict[str, Any]) -> ClusterOperation:
        """记录集群操作"""
        operation = ClusterOperation(**operation_data)
        self.session.add(operation)
        await self.session.flush()
        return operation

    async def get_operation_history(self, cluster_id: int,
                                    limit: int = 50) -> List[ClusterOperation]:
        """获取操作历史"""
        stmt = (select(ClusterOperation)
                .where(ClusterOperation.cluster_id == cluster_id)
                .order_by(ClusterOperation.created_at.desc())
                .limit(limit))

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # =============== 告警管理 ===============

    async def create_alert(self, alert_data: Dict[str, Any]) -> ClusterAlert:
        """创建告警"""
        alert = ClusterAlert(**alert_data)
        self.session.add(alert)
        await self.session.flush()
        return alert

    async def get_active_alerts(self, cluster_id: Optional[int] = None) -> List[ClusterAlert]:
        """获取活跃告警"""
        stmt = (select(ClusterAlert)
                .where(ClusterAlert.status == 'active'))

        if cluster_id:
            stmt = stmt.where(ClusterAlert.cluster_id == cluster_id)

        result = await self.session.execute(stmt.order_by(ClusterAlert.created_at.desc()))
        return list(result.scalars().all())

    # =============== 统计查询 ===============

    async def get_cluster_statistics(self) -> Dict[str, Any]:
        """获取集群统计信息"""
        # 集群总数统计
        cluster_stats = await self.session.execute(
            select(
                Cluster.cluster_type,
                Cluster.status,
                func.count(Cluster.id).label('count')
            )
            .where(Cluster.status != 'deleted')
            .group_by(Cluster.cluster_type, Cluster.status)
        )

        # 节点总数统计
        node_stats = await self.session.execute(
            select(
                func.count(ClusterNode.id).label('total_nodes'),
                func.count(func.nullif(ClusterNode.status, 'inactive')).label('active_nodes')
            )
        )

        # 最近告警统计
        recent_alerts = await self.session.execute(
            select(func.count(ClusterAlert.id))
            .where(ClusterAlert.created_at >= datetime.now() - timedelta(hours=24))
            .where(ClusterAlert.status == 'active')
        )

        return {
            'clusters': {row.cluster_type + '_' + row.status: row.count
                         for row in cluster_stats},
            'nodes': dict(node_stats.first()._asdict()),
            'recent_alerts': recent_alerts.scalar() or 0,
            'last_updated': datetime.now()
        }