"""
TODO 用于数据源管理的数据库模型。该模块包含用于管理数据源、连接和跨平台数据源元数据的模型，
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship

from .base import BaseModel


class DataSource(BaseModel):
    """
    Model for data source definitions.

    Represents external data sources that feed data into the platform,
    including databases, APIs, file systems, and streaming sources.
    """
    __tablename__ = "data_sources"

    # Basic information
    name = Column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Data source name (unique identifier)"
    )

    display_name = Column(
        String(255),
        nullable=False,
        comment="Human-readable display name"
    )

    description = Column(
        Text,
        nullable=True,
        comment="Data source description"
    )

    # Source type and category
    source_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="Type of data source (database, api, file, stream, etc.)"
    )

    source_category = Column(
        String(50),
        nullable=True,
        index=True,
        comment="Business category (finance, hr, crm, etc.)"
    )

    # Connection information
    connection_string = Column(
        Text,
        nullable=True,
        comment="Connection string or URL (encrypted)"
    )

    connection_config = Column(
        JSON,
        nullable=True,
        comment="Connection configuration as JSON"
    )

    # Authentication
    auth_type = Column(
        String(50),
        nullable=True,
        comment="Authentication type (none, basic, oauth, key, etc.)"
    )

    auth_config = Column(
        JSON,
        nullable=True,
        comment="Authentication configuration (encrypted)"
    )

    # Status and health
    status = Column(
        String(20),
        nullable=False,
        default="unknown",
        index=True,
        comment="Connection status (online, offline, error)"
    )

    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether the data source is active"
    )

    # Data characteristics
    data_format = Column(
        String(50),
        nullable=True,
        comment="Primary data format (json, csv, parquet, etc.)"
    )

    update_frequency = Column(
        String(50),
        nullable=True,
        comment="How often data is updated (realtime, hourly, daily, etc.)"
    )

    # Volume estimates
    estimated_records = Column(
        Integer,
        nullable=True,
        comment="Estimated number of records"
    )

    estimated_size_mb = Column(
        Integer,
        nullable=True,
        comment="Estimated data size in MB"
    )

    # Monitoring
    last_connection_test = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last successful connection test"
    )

    last_data_sync = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last successful data synchronization"
    )

    # Metadata
    schema_definition = Column(
        JSON,
        nullable=True,
        comment="Data schema definition as JSON"
    )

    tags = Column(
        JSON,
        nullable=True,
        comment="Tags for categorization and search"
    )

    # Relationships
    connections = relationship("DataSourceConnection", back_populates="data_source", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<DataSource(name='{self.name}', type='{self.source_type}', status='{self.status}')>"


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
