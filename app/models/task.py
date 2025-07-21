"""
TODO 用于任务执行和管理的数据库模型。 本模块包含用于存储任务定义、执行、计划和任务相关元数据的型。
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, Float, ForeignKey, JSON
from sqlalchemy.orm import relationship

from .base import BaseModel


class TaskDefinition(BaseModel):
    """
    Model for task definitions.

    Represents reusable task templates that define what a task does,
    its parameters, and execution requirements.
    """
    __tablename__ = "task_definitions"

    # Basic task information
    name = Column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Task name (unique identifier)"
    )

    display_name = Column(
        String(255),
        nullable=False,
        comment="Human-readable task name"
    )

    description = Column(
        Text,
        nullable=True,
        comment="Task description and purpose"
    )

    # Task categorization
    task_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="Type of task (etl, sync, analysis, etc.)"
    )

    business_domain = Column(
        String(100),
        nullable=True,
        index=True,
        comment="Business domain (finance, hr, crm, etc.)"
    )

    # Source and target
    source_system = Column(
        String(255),
        nullable=True,
        comment="Source system or data source"
    )

    target_system = Column(
        String(255),
        nullable=True,
        comment="Target system or destination"
    )

    # Execution details
    execution_engine = Column(
        String(50),
        nullable=True,
        comment="Execution engine (spark, flink, hive, etc.)"
    )

    script_path = Column(
        String(500),
        nullable=True,
        comment="Path to execution script or code"
    )

    # Configuration
    default_config = Column(
        JSON,
        nullable=True,
        comment="Default task configuration as JSON"
    )

    parameters_schema = Column(
        JSON,
        nullable=True,
        comment="JSON schema for task parameters"
    )

    # Resource requirements
    required_cpu = Column(
        Float,
        nullable=True,
        comment="Required CPU cores"
    )

    required_memory_mb = Column(
        Integer,
        nullable=True,
        comment="Required memory in MB"
    )

    estimated_duration_minutes = Column(
        Integer,
        nullable=True,
        comment="Estimated execution duration in minutes"
    )

    # Status and metadata
    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether task definition is active"
    )

    version = Column(
        String(50),
        nullable=False,
        default="1.0",
        comment="Task definition version"
    )

    tags = Column(
        JSON,
        nullable=True,
        comment="Tags for categorization and search"
    )

    # Relationships
    executions = relationship("TaskExecution", back_populates="task_definition", cascade="all, delete-orphan")
    schedules = relationship("TaskSchedule", back_populates="task_definition", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<TaskDefinition(name='{self.name}', type='{self.task_type}')>"


class TaskExecution(BaseModel):
    """
    Model for individual task executions.

    Stores information about each time a task is executed,
    including status, metrics, and results.
    """
    __tablename__ = "task_executions"

    # Task reference
    task_definition_id = Column(
        Integer,
        ForeignKey("task_definitions.id"),
        nullable=False,
        index=True,
        comment="Reference to task definition"
    )

    # Execution identification
    execution_id = Column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique execution identifier"
    )

    # Timing
    started_at = Column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="When task execution started"
    )

    completed_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="When task execution completed"
    )

    duration_seconds = Column(
        Integer,
        nullable=True,
        comment="Execution duration in seconds"
    )

    # Status and result
    status = Column(
        String(20),
        nullable=False,
        default="pending",
        index=True,
        comment="Execution status (pending, running, success, failed, cancelled)"
    )

    exit_code = Column(
        Integer,
        nullable=True,
        comment="Task exit code"
    )

    # Data processing metrics
    records_processed = Column(
        Integer,
        nullable=True,
        comment="Number of records processed"
    )

    records_inserted = Column(
        Integer,
        nullable=True,
        comment="Number of records inserted"
    )

    records_updated = Column(
        Integer,
        nullable=True,
        comment="Number of records updated"
    )

    records_deleted = Column(
        Integer,
        nullable=True,
        comment="Number of records deleted"
    )

    bytes_processed = Column(
        Integer,
        nullable=True,
        comment="Bytes of data processed"
    )

    # Resource usage
    cpu_time_seconds = Column(
        Float,
        nullable=True,
        comment="CPU time consumed in seconds"
    )

    memory_peak_mb = Column(
        Integer,
        nullable=True,
        comment="Peak memory usage in MB"
    )

    # Configuration and context
    execution_config = Column(
        JSON,
        nullable=True,
        comment="Configuration used for this execution"
    )

    execution_context = Column(
        JSON,
        nullable=True,
        comment="Execution context (cluster, user, etc.)"
    )

    # Results and logs
    result_summary = Column(
        JSON,
        nullable=True,
        comment="Summary of execution results"
    )

    error_message = Column(
        Text,
        nullable=True,
        comment="Error message if execution failed"
    )

    log_file_path = Column(
        String(500),
        nullable=True,
        comment="Path to detailed execution logs"
    )

    # Relationships
    task_definition = relationship("TaskDefinition", back_populates="executions")

    def __repr__(self) -> str:
        return f"<TaskExecution(execution_id='{self.execution_id}', status='{self.status}')>"


class TaskSchedule(BaseModel):
    """
    Model for task scheduling information.

    Stores scheduling rules and metadata for automated task execution.
    """
    __tablename__ = "task_schedules"

    # Task reference
    task_definition_id = Column(
        Integer,
        ForeignKey("task_definitions.id"),
        nullable=False,
        index=True,
        comment="Reference to task definition"
    )

    # Schedule definition
    schedule_name = Column(
        String(255),
        nullable=False,
        comment="Schedule name"
    )

    cron_expression = Column(
        String(100),
        nullable=True,
        comment="Cron expression for scheduling"
    )

    schedule_type = Column(
        String(50),
        nullable=False,
        comment="Type of schedule (cron, interval, manual)"
    )

    # Schedule parameters
    interval_minutes = Column(
        Integer,
        nullable=True,
        comment="Interval in minutes for interval-based schedules"
    )

    start_date = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Schedule start date"
    )

    end_date = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Schedule end date (optional)"
    )

    # Status and control
    is_active = Column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether schedule is active"
    )

    max_retries = Column(
        Integer,
        nullable=False,
        default=3,
        comment="Maximum number of retry attempts"
    )

    retry_delay_minutes = Column(
        Integer,
        nullable=False,
        default=5,
        comment="Delay between retries in minutes"
    )

    # Execution tracking
    last_execution_time = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="Last time this schedule executed"
    )

    next_execution_time = Column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
        comment="Next scheduled execution time"
    )

    execution_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Total number of executions"
    )

    success_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="Number of successful executions"
    )

    # Configuration
    schedule_config = Column(
        JSON,
        nullable=True,
        comment="Additional schedule configuration"
    )

    # Relationships
    task_definition = relationship("TaskDefinition", back_populates="schedules")

    def __repr__(self) -> str:
        return f"<TaskSchedule(name='{self.schedule_name}', type='{self.schedule_type}', active={self.is_active})>"

