# app/models/__init__.py
"""
Database models package for the Big Data Platform.

This package contains all SQLAlchemy models for the application,
organized by domain area (clusters, data sources, tasks, etc.).
"""

from .base import Base, BaseModel, TimestampMixin
from .cluster import Cluster, ClusterNode, ClusterMetric
from .custom_api import CustomAPI, APIParameter, APIAccessLog
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
from .sync_history import SyncHistory, SyncTableHistory
from app.models.resource_file import ResourceFile, ResourceType
from .data_catalog import DataCatalog
from .data_asset import DataAsset, AssetColumn, AssetAccessLog
from .field_standard import FieldStandard
from .indicator_system import IndicatorSystem, IndicatorAssetRelation
from .api_user import APIUser, APIKey, APIUserPermission, UserType
from .job_workflow import JobWorkflow, JobWorkflowStatus
from .job_work import JobWork, JobWorkType, JobWorkStatus
from .job_instance import (
    JobWorkflowInstance, JobWorkInstance,
    JobInstanceStatus, JobTriggerType
)

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
    # API User & Security
    "APIUser",
    "APIKey",
    "APIUserPermission",
    "UserType",

    "SyncHistory",        # 添加这一行
    "SyncTableHistory",
    "ResourceFile",
    "ResourceType",
    # 数据资源目录模块
    "DataCatalog",
    "DataAsset",
    "AssetColumn",
    "AssetAccessLog",
    "FieldStandard",
    # 指标体系建设模块
    "IndicatorSystem",
    "IndicatorAssetRelation",
    # Job Workflow models (新增)
    "JobWorkflow",
    "JobWorkflowStatus",
    "JobWork",
    "JobWorkType",
    "JobWorkStatus",
    "JobWorkflowInstance",
    "JobWorkInstance",
    "JobInstanceStatus",
    "JobTriggerType",
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
        WorkflowVariable, WorkflowAlert,CustomAPI, APIParameter, APIAccessLog,APIUser,APIKey,APIUserPermission,UserType,
        UserCluster,SyncHistory, SyncTableHistory,ResourceType,ResourceFile,DataCatalog, DataAsset,
        AssetColumn, AssetAccessLog, FieldStandard,IndicatorSystem,IndicatorAssetRelation,
    ]