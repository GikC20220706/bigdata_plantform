# app/models/__init__.py
"""
Database models package for the Big Data Platform.

This package contains all SQLAlchemy models for the application,
organized by domain area (clusters, data sources, tasks, etc.).
"""

from .base import Base, BaseModel, TimestampMixin
from .cluster import Cluster, ClusterNode, ClusterMetric
from .data_source import DataSource, DataSourceConnection
from .task import TaskDefinition, TaskExecution, TaskSchedule
from .business_system import BusinessSystem, BusinessSystemDataSource
# 🆕 新增智能同步相关模型
from .sync_task import (
    SyncTask, SyncTableMapping, SyncExecution, SyncTableResult,
    DataSourceMetadata, SyncTemplate
)
# 添加计算集群
from .user_cluster import UserCluster, ClusterType, ClusterStatus
# 工作流编排相关模型
from .workflow import (
    WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition,
    WorkflowExecution, WorkflowNodeExecution, WorkflowTemplate,
    WorkflowVariable, WorkflowAlert,
    WorkflowStatus, NodeType, NodeStatus, TriggerType
)
from .custom_api import CustomAPI, APIParameter, APIAccessLog
from .sync_history import SyncHistory, SyncTableHistory

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
    "UserCluster",

    # Data source models
    "DataSource",
    "DataSourceConnection",
    "DataSourceMetadata",  # 🆕

    # Task models
    "TaskDefinition",
    "TaskExecution",
    "TaskSchedule",

    # Business system models
    "BusinessSystem",
    "BusinessSystemDataSource",

    # 🆕 Smart sync models
    "SyncTask",
    "SyncTableMapping",
    "SyncExecution",
    "SyncTableResult",
    "SyncTemplate",

    # Workflow models
    "WorkflowDefinition",
    "WorkflowNodeDefinition",
    "WorkflowEdgeDefinition",
    "WorkflowExecution",
    "WorkflowNodeExecution",
    "WorkflowTemplate",
    "WorkflowVariable",
    "WorkflowAlert",

    # Workflow enums
    "WorkflowStatus",
    "NodeType",
    "NodeStatus",
    "TriggerType",
    #Custom API
    "CustomAPI",
    "APIParameter",
    "APIAccessLog",

    "SyncHistory",        # 添加这一行
    "SyncTableHistory",
]


# For Alembic migrations - import all models to ensure they're registered
def get_all_models():
    """Get all model classes for Alembic migrations."""
    return [
        # Original models
        Cluster, ClusterNode, ClusterMetric,
        DataSource, DataSourceConnection,
        TaskDefinition, TaskExecution, TaskSchedule,
        BusinessSystem, BusinessSystemDataSource,
        # 🆕 New sync models
        SyncTask, SyncTableMapping, SyncExecution, SyncTableResult,
        DataSourceMetadata, SyncTemplate,
        WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition,
        WorkflowExecution, WorkflowNodeExecution, WorkflowTemplate,
        WorkflowVariable, WorkflowAlert,CustomAPI, APIParameter, APIAccessLog,UserCluster,SyncHistory, SyncTableHistory,
    ]