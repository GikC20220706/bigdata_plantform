"""
作业(JobWork)数据库模型
"""
from datetime import datetime
from sqlalchemy import Column, String, Integer, Text, DateTime, JSON, Enum as SQLEnum, ForeignKey
from sqlalchemy.orm import relationship
from enum import Enum
from app.models.base import BaseModel


class JobWorkType(str, Enum):
    """作业类型枚举"""
    # SQL类作业
    EXE_JDBC = "EXE_JDBC"  # JDBC执行作业
    QUERY_JDBC = "QUERY_JDBC"  # JDBC查询作业
    SPARK_SQL = "SPARK_SQL"  # SparkSql查询作业

    # 数据同步类
    DATA_SYNC_JDBC = "DATA_SYNC_JDBC"  # 数据同步作业
    EXCEL_SYNC_JDBC = "EXCEL_SYNC_JDBC"  # Excel导入作业
    DB_MIGRATE = "DB_MIGRATE"  # 整库迁移作业

    # 脚本类
    BASH = "BASH"  # Bash作业
    PYTHON = "PYTHON"  # Python作业

    # API类
    CURL = "CURL"  # Curl作业
    API = "API"  # 接口调用作业

    # Spark Jar类
    SPARK_JAR = "SPARK_JAR"  # Spark自定义作业

    # Flink类
    FLINK_SQL = "FLINK_SQL"  # FlinkSql作业
    FLINK_JAR = "FLINK_JAR"  # Flink自定义作业


class JobWorkStatus(str, Enum):
    """作业状态枚举"""
    DRAFT = "DRAFT"  # 草稿
    ONLINE = "ONLINE"  # 已上线
    OFFLINE = "OFFLINE"  # 已下线


class JobWork(BaseModel):
    """作业模型"""
    __tablename__ = "job_works"

    # 所属作业流
    workflow_id = Column(Integer, ForeignKey('job_workflows.id', ondelete='CASCADE'), nullable=False, index=True,
                         comment="所属作业流ID")

    # 基本信息
    name = Column(String(200), nullable=False, index=True, comment="作业名称")
    work_type = Column(SQLEnum(JobWorkType), nullable=False, index=True, comment="作业类型")
    remark = Column(Text, nullable=True, comment="备注")

    # 状态
    status = Column(
        SQLEnum(JobWorkStatus),
        nullable=False,
        default=JobWorkStatus.DRAFT,
        index=True,
        comment="发布状态"
    )

    # 作业配置（JSON格式，不同类型的作业存储不同的配置）
    config = Column(JSON, nullable=True, comment="作业配置")

    # 执行器类型（对应后端的executor类）
    executor = Column(String(50), nullable=True, comment="执行器类型")

    # 关系：属于某个作业流
    workflow = relationship("JobWorkflow", back_populates="works")

    # 关系：一个作业有多个实例
    instances = relationship("JobWorkInstance", back_populates="work", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<JobWork(id={self.id}, name={self.name}, type={self.work_type}, status={self.status})>"