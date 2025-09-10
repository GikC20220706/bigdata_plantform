"""
TODO 用于数据源管理的数据库模型。该模块包含用于管理数据源、连接和跨平台数据源元数据的模型，
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship

from .base import BaseModel


class DataSource(BaseModel):
    """完整的数据源模型"""
    __tablename__ = "data_sources"

    # 基本信息
    name = Column(String(255), nullable=False, unique=True, index=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)

    # 连接信息
    source_type = Column(String(50), nullable=False, index=True)
    connection_config = Column(JSON, nullable=True)  # 🔑 保存连接配置

    # 状态信息
    status = Column(String(20), nullable=False, default="unknown", index=True)
    is_active = Column(Boolean, nullable=False, default=True, index=True)  # 🔑 软删除标记

    # 监控信息
    last_connection_test = Column(DateTime(timezone=True), nullable=True)

    # 🔑 新增字段（如果没有的话）
    tables_count = Column(Integer, nullable=True, default=0)  # 表数量统计
    data_volume_estimate = Column(String(50), nullable=True)  # 数据量估计
    tags = Column(JSON, nullable=True)  # 标签

    # 关系
    connections = relationship("DataSourceConnection", back_populates="data_source", cascade="all, delete-orphan")
    custom_apis = relationship("CustomAPI", back_populates="data_source", cascade="all, delete-orphan")


class DataSourceConnection(BaseModel):
    """
    Model for tracking data source connection history.

    Stores historical information about connection attempts,
    success rates, and performance metrics.
    """
    __tablename__ = "data_source_connections"

    data_source_id = Column(
        Integer,
        ForeignKey("data_sources.id"),
        nullable=False,
        index=True,
        comment="Reference to data source"
    )

    # Connection attempt details
    connection_timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="When connection was attempted"
    )

    connection_type = Column(
        String(50),
        nullable=False,
        comment="Type of connection (test, sync, query)"
    )

    # Result
    success = Column(
        Boolean,
        nullable=False,
        index=True,
        comment="Whether connection was successful"
    )

    response_time_ms = Column(
        Integer,
        nullable=True,
        comment="Connection response time in milliseconds"
    )

    error_message = Column(
        Text,
        nullable=True,
        comment="Error message if connection failed"
    )

    # Additional details
    records_processed = Column(
        Integer,
        nullable=True,
        comment="Number of records processed (for sync operations)"
    )

    bytes_transferred = Column(
        Integer,
        nullable=True,
        comment="Bytes transferred during operation"
    )

    connection_metadata = Column(
        JSON,
        nullable=True,
        comment="Additional connection metadata"
    )

    # Relationships
    data_source = relationship("DataSource", back_populates="connections")

    def __repr__(self) -> str:
        return f"<DataSourceConnection(data_source_id={self.data_source_id}, success={self.success})>"
