"""
TODO 与API请求和响应相关的集群相关模式
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from .base import ClusterStatus
from .task import TaskSearchParams


class ClusterInfo(BaseModel):
    """Cluster information model."""
    name: str = Field(..., description="Cluster name")
    status: ClusterStatus = Field(..., description="Cluster status")
    total_nodes: int = Field(..., description="Total number of nodes")
    active_nodes: int = Field(..., description="Number of active nodes")
    cpu_usage: float = Field(..., ge=0, le=100, description="CPU usage percentage")
    memory_usage: float = Field(..., ge=0, le=100, description="Memory usage percentage")
    disk_usage: Optional[float] = Field(
        None,
        ge=0,
        le=100,
        description="Disk usage percentage"
    )
    last_update: datetime = Field(..., description="Last update timestamp")


class ClusterCreateRequest(BaseModel):
    """Request model for creating a new cluster."""
    name: str = Field(..., description="Cluster name")
    nodes: List[str] = Field(..., description="List of node addresses")
    config: Optional[Dict[str, Any]] = Field(None, description="Cluster configuration")


class ClusterUpdateRequest(BaseModel):
    """Request model for updating cluster configuration."""
    status: Optional[ClusterStatus] = Field(None, description="New cluster status")
    config: Optional[Dict[str, Any]] = Field(None, description="Updated configuration")


class ClusterListResponse(BaseModel):
    """Response model for cluster list."""
    clusters: List[ClusterInfo] = Field(..., description="List of clusters")
    total: int = Field(..., ge=0, description="Total number of clusters")