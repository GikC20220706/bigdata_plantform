"""
Schemas package for API request and response models.
"""

# Base schemas and enums
from .base import TaskStatus, Priority, ClusterStatus, BaseResponse
from .business_system import (
    BusinessSystemCreate,
    BusinessSystemUpdate,
    BusinessSystemResponse,
    BusinessSystemSummary,
    BusinessSystemListResponse,
    BusinessSystemSearchParams,
    BusinessSystemStatistics,
    BusinessSystemHealth,
    DataSourceAssociation,
    DataSourceAssociationCreate,
    DataSourceAssociationResponse,
    BusinessSystemBatchImport,
    BatchOperationResult,
    SystemType,
    CriticalityLevel,
    SystemStatus,
    SyncFrequency
)

# Cluster schemas
from .cluster import (
    ClusterInfo,
    ClusterCreateRequest,
    ClusterUpdateRequest,
    ClusterListResponse,
)

# Task schemas
from .task import (
    TaskExecution,
    TaskCreateRequest,
    TaskUpdateRequest,
    TaskSearchParams,
    TaskListResponse,
)

# Overview schemas
from .overview import (
    StatCard,
    DatabaseLayerStats,
    StorageInfo,
    DataQualityMetrics,
    OverviewStats,
    SystemHealth,
    OverviewResponse,
)

__all__ = [
    # Base
    "TaskStatus",
    "Priority",
    "ClusterStatus",
    "BaseResponse",

    # Cluster
    "ClusterInfo",
    "ClusterCreateRequest",
    "ClusterUpdateRequest",
    "ClusterListResponse",

    # Task
    "TaskExecution",
    "TaskCreateRequest",
    "TaskUpdateRequest",
    "TaskSearchParams",
    "TaskListResponse",

    # Overview
    "StatCard",
    "DatabaseLayerStats",
    "StorageInfo",
    "DataQualityMetrics",
    "OverviewStats",
    "SystemHealth",
    "OverviewResponse",

    # Business system schemas
    "BusinessSystemCreate",
    "BusinessSystemUpdate",
    "BusinessSystemResponse",
    "BusinessSystemSummary",
    "BusinessSystemListResponse",
    "BusinessSystemSearchParams",
    "BusinessSystemStatistics",
    "BusinessSystemHealth",
    "DataSourceAssociation",
    "DataSourceAssociationCreate",
    "DataSourceAssociationResponse",
    "BusinessSystemBatchImport",
    "BatchOperationResult",
    "SystemType",
    "CriticalityLevel",
    "SystemStatus",
    "SyncFrequency",
]