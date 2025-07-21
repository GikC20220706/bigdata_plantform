"""
TODO 用于集群管理的数据库模型。该模块包含用于存储集群信息、节点详细信息和集群健康指标的模型。
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, Float, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import JSON

from .base import BaseModel


class Cluster(BaseModel):
    """
    Model for storing cluster information.

    Represents a big data cluster (Hadoop, Flink, Doris, etc.) with its
    configuration and current status.
    """
    __tablename__ = "clusters"

    # Basic cluster information
    name = Column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Cluster name (unique identifier)"
    )

    cluster_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="Type of cluster (hadoop, flink, doris, etc.)"
    )

    description = Column(
        Text,
        nullable=True,
        comment="Cluster description"
    )

    # Status and health
    status = Column(
        String(20),
        nullable=False,
        default="unknown",
        index=True,
        comment="Current cluster status (normal, warning, error, offline)"
    )

    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether the cluster is actively monitored"
    )

    # Node information
    total_nodes = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Total number of nodes in cluster"
    )

    active_nodes = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of currently active nodes"
    )

    # Resource metrics
    cpu_usage = Column(
        Float,
        nullable=True,
        comment="Average CPU usage percentage across cluster"
    )

    memory_usage = Column(
        Float,
        nullable=True,
        comment="Average memory usage percentage across cluster"
    )

    disk_usage = Column(
        Float,
        nullable=True,
        comment="Average disk usage percentage across cluster"
    )

    # Configuration and metadata
    config = Column(
        JSON,
        nullable=True,
        comment="Cluster configuration as JSON"
    )

    endpoints = Column(
        JSON,
        nullable=True,
        comment="Cluster service endpoints as JSON"
    )

    # Monitoring
    last_health_check = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of last health check"
    )

    health_check_interval = Column(
        Integer,
        nullable=False,
        default=300,
        comment="Health check interval in seconds"
    )

    # Relationships
    nodes = relationship("ClusterNode", back_populates="cluster", cascade="all, delete-orphan")
    metrics = relationship("ClusterMetric", back_populates="cluster", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Cluster(name='{self.name}', type='{self.cluster_type}', status='{self.status}')>"


class ClusterNode(BaseModel):
    """
    Model for individual nodes within a cluster.

    Stores detailed information about each node including its role,
    status, and resource metrics.
    """
    __tablename__ = "cluster_nodes"

    # Node identification
    cluster_id = Column(
        Integer,
        ForeignKey("clusters.id"),
        nullable=False,
        index=True,
        comment="Reference to parent cluster"
    )

    hostname = Column(
        String(255),
        nullable=False,
        index=True,
        comment="Node hostname"
    )

    ip_address = Column(
        String(45),  # IPv6 compatible
        nullable=False,
        index=True,
        comment="Node IP address"
    )

    # Node role and status
    role = Column(
        String(100),
        nullable=False,
        comment="Node role (NameNode, DataNode, TaskManager, etc.)"
    )

    status = Column(
        String(20),
        nullable=False,
        default="unknown",
        index=True,
        comment="Node status (online, offline, maintenance)"
    )

    # Resource information
    cpu_cores = Column(
        Integer,
        nullable=True,
        comment="Number of CPU cores"
    )

    memory_total = Column(
        Integer,
        nullable=True,
        comment="Total memory in MB"
    )

    disk_total = Column(
        Integer,
        nullable=True,
        comment="Total disk space in MB"
    )

    # Current metrics
    cpu_usage = Column(
        Float,
        nullable=True,
        comment="Current CPU usage percentage"
    )

    memory_usage = Column(
        Float,
        nullable=True,
        comment="Current memory usage percentage"
    )

    disk_usage = Column(
        Float,
        nullable=True,
        comment="Current disk usage percentage"
    )

    load_average = Column(
        Float,
        nullable=True,
        comment="System load average"
    )

    # Network and additional info
    network_io = Column(
        Float,
        nullable=True,
        comment="Network I/O rate"
    )

    last_seen = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last time node was seen online"
    )

    # Relationships
    cluster = relationship("Cluster", back_populates="nodes")

    def __repr__(self) -> str:
        return f"<ClusterNode(hostname='{self.hostname}', role='{self.role}', status='{self.status}')>"


class ClusterMetric(BaseModel):
    """
    Model for storing historical cluster metrics.

    Stores time-series data for cluster performance monitoring
    and historical analysis.
    """
    __tablename__ = "cluster_metrics"

    cluster_id = Column(
        Integer,
        ForeignKey("clusters.id"),
        nullable=False,
        index=True,
        comment="Reference to cluster"
    )

    # Timestamp for time-series data
    metric_timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="Timestamp when metrics were collected"
    )

    # Resource metrics
    cpu_usage = Column(
        Float,
        nullable=True,
        comment="CPU usage percentage at timestamp"
    )

    memory_usage = Column(
        Float,
        nullable=True,
        comment="Memory usage percentage at timestamp"
    )

    disk_usage = Column(
        Float,
        nullable=True,
        comment="Disk usage percentage at timestamp"
    )

    # Node status
    total_nodes = Column(
        Integer,
        nullable=True,
        comment="Total nodes at timestamp"
    )

    active_nodes = Column(
        Integer,
        nullable=True,
        comment="Active nodes at timestamp"
    )

    # Additional metrics as JSON
    additional_metrics = Column(
        JSON,
        nullable=True,
        comment="Additional cluster-specific metrics"
    )

    # Relationships
    cluster = relationship("Cluster", back_populates="metrics")

    def __repr__(self) -> str:
        return f"<ClusterMetric(cluster_id={self.cluster_id}, timestamp={self.metric_timestamp})>"

