"""
Database models package for the Big Data Platform.

This package contains all SQLAlchemy models for the application,
organized by domain area (clusters, data sources, tasks, etc.).
"""

from .base import Base, BaseModel, TimestampMixin
from .cluster import Cluster, ClusterNode, ClusterMetric
from .data_source import DataSource, DataSourceConnection
from .task import TaskDefinition, TaskExecution, TaskSchedule

# Export all models for easy importing
__all__ = [
    # Base classes
    "Base",
    "BaseModel",
    "TimestampMixin",

    # Cluster models
    "Cluster",
    "ClusterNode",
    "ClusterMetric",

    # Data source models
    "DataSource",
    "DataSourceConnection",

    # Task models
    "TaskDefinition",
    "TaskExecution",
    "TaskSchedule",
]


# For Alembic migrations - import all models to ensure they're registered
def get_all_models():
    """Get all model classes for Alembic migrations."""
    return [
        Cluster, ClusterNode, ClusterMetric,
        DataSource, DataSourceConnection,
        TaskDefinition, TaskExecution, TaskSchedule
    ]