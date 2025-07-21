"""
TODO 大数据平台的基础架构和通用枚举。
"""

from enum import Enum
from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Task execution status enumeration."""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"


class Priority(str, Enum):
    """Task priority enumeration."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ClusterStatus(str, Enum):
    """Cluster health status enumeration."""
    NORMAL = "normal"
    WARNING = "warning"
    ERROR = "error"
    OFFLINE = "offline"


class BaseResponse(BaseModel):
    """Base response model with common fields."""
    timestamp: datetime = Field(default_factory=datetime.now)
    success: bool = Field(default=True)
    message: Optional[str] = Field(None)