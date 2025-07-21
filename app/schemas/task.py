"""
TODO 与API请求和响应相关的任务相关模式。
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from .base import TaskStatus, Priority


class TaskExecution(BaseModel):
    """Task execution information model."""
    task_name: str = Field(..., description="Task name")
    business_system: str = Field(..., description="Business system name")
    data_source: str = Field(..., description="Data source identifier")
    target_table: str = Field(..., description="Target table name")
    execution_time: datetime = Field(..., description="Execution timestamp")
    status: TaskStatus = Field(..., description="Task execution status")
    priority: Priority = Field(..., description="Task priority level")
    data_volume: int = Field(..., ge=0, description="Number of records processed")
    duration: int = Field(..., ge=0, description="Execution duration in seconds")
    error_message: Optional[str] = Field(None, description="Error message if failed")


class TaskCreateRequest(BaseModel):
    """Request model for creating a new task."""
    task_name: str = Field(..., description="Task name")
    business_system: str = Field(..., description="Business system")
    data_source: str = Field(..., description="Data source")
    target_table: str = Field(..., description="Target table")
    priority: Priority = Field(default=Priority.MEDIUM, description="Task priority")
    schedule: Optional[str] = Field(None, description="Cron schedule expression")


class TaskUpdateRequest(BaseModel):
    """Request model for updating task configuration."""
    status: Optional[TaskStatus] = Field(None, description="New task status")
    priority: Optional[Priority] = Field(None, description="Updated priority")
    schedule: Optional[str] = Field(None, description="Updated schedule")


class TaskSearchParams(BaseModel):
    """Task search parameters model."""
    keyword: Optional[str] = Field(None, description="Search keyword")
    status: Optional[TaskStatus] = Field(None, description="Status filter")
    business_system: Optional[str] = Field(None, description="Business system filter")
    start_date: Optional[datetime] = Field(None, description="Start date filter")
    end_date: Optional[datetime] = Field(None, description="End date filter")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(20, ge=1, le=100, description="Items per page")


class TaskListResponse(BaseModel):
    """Task list response model."""
    tasks: List[TaskExecution] = Field(..., description="Task execution list")
    total: int = Field(..., ge=0, description="Total number of tasks")
    page: int = Field(..., ge=1, description="Current page number")
    page_size: int = Field(..., ge=1, description="Items per page")
    total_pages: int = Field(..., ge=1, description="Total number of pages")