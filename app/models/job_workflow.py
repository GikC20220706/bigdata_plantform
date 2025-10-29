"""
作业流(JobWorkflow)数据库模型
用于数据开发-作业流功能
与WorkflowDefinition(工作流编排)区分开
"""
from datetime import datetime
from sqlalchemy import Column, String, Integer, Text, DateTime, JSON, Enum as SQLEnum, Boolean
from sqlalchemy.orm import relationship
from enum import Enum
from app.models.base import BaseModel


class JobWorkflowStatus(str, Enum):
    """作业流状态枚举"""
    DRAFT = "DRAFT"  # 草稿
    ONLINE = "ONLINE"  # 已上线
    OFFLINE = "OFFLINE"  # 已下线


class JobWorkflow(BaseModel):
    """
    作业流模型 - 用于数据开发的作业流管理
    这是一个简化的作业流模型，主要用于前端的作业流功能
    """
    __tablename__ = "job_workflows"

    # 基本信息
    name = Column(String(200), nullable=False, index=True, comment="作业流名称")
    remark = Column(Text, nullable=True, comment="备注")

    # 状态
    status = Column(
        SQLEnum(JobWorkflowStatus),
        nullable=False,
        default=JobWorkflowStatus.DRAFT,
        index=True,
        comment="发布状态"
    )

    # 流程图配置（JSON格式存储节点和连线信息）
    web_config = Column(JSON, nullable=True, comment="流程图配置")

    # 定时配置
    cron_config = Column(JSON, nullable=True, comment="定时配置")

    # 告警配置
    alarm_list = Column(JSON, nullable=True, comment="告警配置")

    # 其他配置（超时、重试等）
    other_config = Column(JSON, nullable=True, comment="其他配置")

    # 下次执行时间
    next_date_time = Column(DateTime(timezone=True), nullable=True, comment="下次执行时间")

    # 创建人信息
    create_username = Column(String(100), nullable=True, comment="创建人")

    # 关系：一个作业流包含多个作业
    works = relationship("JobWork", back_populates="workflow", cascade="all, delete-orphan")

    # 关系：一个作业流有多个实例
    instances = relationship("JobWorkflowInstance", back_populates="workflow", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<JobWorkflow(id={self.id}, name={self.name}, status={self.status})>"