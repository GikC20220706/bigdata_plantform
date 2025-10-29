"""
作业流实例和作业实例数据库模型
用于记录执行历史
"""
from datetime import datetime
from sqlalchemy import Column, String, Integer, Text, DateTime, JSON, Enum as SQLEnum, ForeignKey, BigInteger, Boolean
from sqlalchemy.orm import relationship
from enum import Enum
from app.models.base import BaseModel


class JobInstanceStatus(str, Enum):
    """实例状态枚举"""
    PENDING = "PENDING"  # 等待中
    RUNNING = "RUNNING"  # 运行中
    SUCCESS = "SUCCESS"  # 成功
    FAIL = "FAIL"  # 失败
    ABORT = "ABORT"  # 已中止
    ABORTING = "ABORTING"  # 中止中


class JobTriggerType(str, Enum):
    """触发类型枚举"""
    MANUAL = "MANUAL"  # 手动触发
    SCHEDULE = "SCHEDULE"  # 定时调度
    API = "API"  # API调用


class JobWorkflowInstance(BaseModel):
    """作业流实例模型"""
    __tablename__ = "job_workflow_instances"

    # 实例ID（用于前端展示）
    workflow_instance_id = Column(String(100), nullable=False, unique=True, index=True, comment="作业流实例ID")

    # 关联的作业流
    workflow_id = Column(Integer, ForeignKey('job_workflows.id'), nullable=False, index=True, comment="作业流ID")
    workflow_name = Column(String(200), nullable=False, comment="作业流名称")

    # 状态
    status = Column(
        SQLEnum(JobInstanceStatus),
        nullable=False,
        default=JobInstanceStatus.PENDING,
        index=True,
        comment="执行状态"
    )

    # 触发信息
    trigger_type = Column(SQLEnum(JobTriggerType), nullable=False, default=JobTriggerType.MANUAL, comment="触发类型")

    # 时间信息
    start_datetime = Column(DateTime(timezone=True), nullable=True, comment="开始时间")
    end_datetime = Column(DateTime(timezone=True), nullable=True, comment="结束时间")

    # 执行人信息
    last_modified_by = Column(String(100), nullable=True, comment="最后修改人")

    # 执行日志路径
    log_path = Column(String(500), nullable=True, comment="日志路径")

    # 错误信息
    error_message = Column(Text, nullable=True, comment="错误信息")

    # 关系：属于某个作业流
    workflow = relationship("JobWorkflow", back_populates="instances")

    # 关系：一个作业流实例包含多个作业实例
    work_instances = relationship("JobWorkInstance", back_populates="workflow_instance", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<JobWorkflowInstance(id={self.workflow_instance_id}, status={self.status})>"


class JobWorkInstance(BaseModel):
    """作业实例模型"""
    __tablename__ = "job_work_instances"

    # 实例ID（用于前端展示）
    instance_id = Column(String(100), nullable=False, unique=True, index=True, comment="作业实例ID")

    # 关联的作业流实例
    workflow_instance_id = Column(String(100),
                                  ForeignKey('job_workflow_instances.workflow_instance_id', ondelete='CASCADE'),
                                  nullable=False, index=True, comment="作业流实例ID")

    # 关联的作业
    work_id = Column(Integer, ForeignKey('job_works.id'), nullable=False, index=True, comment="作业ID")
    work_name = Column(String(200), nullable=False, comment="作业名称")
    work_type = Column(String(50), nullable=False, comment="作业类型")

    # 状态
    status = Column(
        SQLEnum(JobInstanceStatus),
        nullable=False,
        default=JobInstanceStatus.PENDING,
        index=True,
        comment="执行状态"
    )

    # 时间信息
    start_datetime = Column(DateTime(timezone=True), nullable=True, comment="开始时间")
    end_datetime = Column(DateTime(timezone=True), nullable=True, comment="结束时间")

    # 日志信息
    log_path = Column(String(500), nullable=True, comment="日志路径")
    submit_log = Column(Text, nullable=True, comment="提交日志")
    running_log = Column(Text, nullable=True, comment="运行日志")

    # 结果数据（JSON格式）
    result_data = Column(JSON, nullable=True, comment="结果数据")

    # 错误信息
    error_message = Column(Text, nullable=True, comment="错误信息")

    # 关系：属于某个作业流实例
    workflow_instance = relationship("JobWorkflowInstance", back_populates="work_instances")

    # 关系：属于某个作业
    work = relationship("JobWork", back_populates="instances")

    def __repr__(self):
        return f"<JobWorkInstance(id={self.instance_id}, work={self.work_name}, status={self.status})>"