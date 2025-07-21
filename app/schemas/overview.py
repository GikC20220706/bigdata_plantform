"""
Overview and dashboard schemas for the Big Data Platform.
TODO 大数据平台的概览和总体架构
"""

from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel, Field
from .base import BaseResponse
from .cluster import ClusterInfo
from .task import TaskExecution
from .task import TaskSearchParams
from .task import TaskListResponse
from .task import TaskStatus

class StatCard(BaseModel):
    """Statistical card model for dashboard display."""
    title: str = Field(..., description="Card title")
    icon: str = Field(..., description="Icon identifier")
    value: str = Field(..., description="Display value")
    change: str = Field(..., description="Change description")
    change_type: str = Field(..., description="Change type: positive/negative")


class DatabaseLayerStats(BaseModel):
    """Database layer statistics model."""
    ods_tables: int = Field(..., description="Number of ODS layer tables")
    dwd_tables: int = Field(..., description="Number of DWD layer tables")
    dws_tables: int = Field(..., description="Number of DWS layer tables")
    ads_tables: int = Field(..., description="Number of ADS layer tables")
    other_tables: int = Field(..., description="Number of other tables")
    total_tables: int = Field(..., description="Total number of tables")


class StorageInfo(BaseModel):
    """Storage information model."""
    total_size: int = Field(..., ge=0, description="Total size in bytes")
    used_size: int = Field(..., ge=0, description="Used size in bytes")
    available_size: int = Field(..., ge=0, description="Available size in bytes")
    usage_percentage: float = Field(..., ge=0, le=100, description="Usage percentage")


class DataQualityMetrics(BaseModel):
    """Data quality metrics model."""
    completeness: float = Field(..., ge=0, le=100, description="Completeness score")
    accuracy: float = Field(..., ge=0, le=100, description="Accuracy score")
    consistency: float = Field(..., ge=0, le=100, description="Consistency score")
    timeliness: float = Field(..., ge=0, le=100, description="Timeliness score")
    overall_score: float = Field(..., ge=0, le=100, description="Overall quality score")


class OverviewStats(BaseModel):
    """Overview statistics information model."""
    business_systems: int = Field(..., ge=0, description="Number of connected business systems")
    total_tables: int = Field(..., ge=0, description="Total number of data tables")
    data_storage_size: str = Field(..., description="Total data storage size")
    realtime_jobs: int = Field(..., ge=0, description="Number of real-time jobs")
    api_calls_today: int = Field(..., ge=0, description="API calls count today")
    data_quality_score: float = Field(..., ge=0, le=100, description="Overall data quality score")

    # Extended statistics
    database_layers: DatabaseLayerStats = Field(..., description="Database layer statistics")
    new_systems_this_month: int = Field(..., ge=0, description="New systems added this month")
    new_tables_this_month: int = Field(..., ge=0, description="New tables added this month")
    storage_growth_this_month: str = Field(..., description="Storage growth this month")


class SystemHealth(BaseModel):
    """System health status model."""
    overall_status: str = Field(..., description="Overall system status")
    components: Dict[str, Any] = Field(..., description="Component status details")
    alerts: List[str] = Field(default_factory=list, description="Active alerts")
    last_check: datetime = Field(..., description="Last health check timestamp")


class OverviewResponse(BaseResponse):
    """Overview page response model."""
    stats: OverviewStats = Field(..., description="Overview statistics")
    stat_cards: List[StatCard] = Field(..., description="Dashboard stat cards")
    clusters: List[ClusterInfo] = Field(..., description="Cluster information")
    today_tasks: List[TaskExecution] = Field(..., description="Today's task executions")
