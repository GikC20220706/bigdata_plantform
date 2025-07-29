"""
业务系统管理的数据库模型
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON
from sqlalchemy.orm import relationship

from .base import BaseModel


class BusinessSystem(BaseModel):
    """
    业务系统模型

    用于存储用户手动添加的业务系统信息
    """
    __tablename__ = "business_systems"

    # 基本信息
    system_name = Column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="业务系统名称（唯一标识）"
    )

    display_name = Column(
        String(255),
        nullable=False,
        comment="显示名称"
    )

    description = Column(
        Text,
        nullable=True,
        comment="系统描述"
    )

    # 联系信息
    contact_person = Column(
        String(100),
        nullable=True,
        comment="联系人姓名"
    )

    contact_email = Column(
        String(255),
        nullable=True,
        comment="联系人邮箱"
    )

    contact_phone = Column(
        String(50),
        nullable=True,
        comment="联系人电话"
    )

    # 系统分类和属性
    system_type = Column(
        String(50),
        nullable=True,
        default="business",
        comment="系统类型：business/technical/platform"
    )

    business_domain = Column(
        String(100),
        nullable=True,
        comment="业务领域：财务/人力/客服/销售等"
    )

    criticality_level = Column(
        String(20),
        nullable=False,
        default="medium",
        comment="重要程度：high/medium/low"
    )

    # 技术信息
    database_info = Column(
        JSON,
        nullable=True,
        comment="关联的数据库信息"
    )

    data_volume_estimate = Column(
        String(50),
        nullable=True,
        comment="数据量估计"
    )

    # 状态管理
    status = Column(
        String(20),
        nullable=False,
        default="active",
        index=True,
        comment="系统状态：active/inactive/maintenance/deprecated"
    )

    is_data_source = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="是否作为数据源"
    )

    # 上线和维护信息
    go_live_date = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="系统上线时间"
    )

    last_data_sync = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最后数据同步时间"
    )

    # 扩展信息
    tags = Column(
        JSON,
        nullable=True,
        comment="标签列表，用于分类和搜索"
    )

    metadata_info = Column(
        JSON,
        nullable=True,
        comment="额外的元数据信息"
    )

    # 统计信息（可以通过定时任务更新）
    table_count = Column(
        Integer,
        nullable=True,
        default=0,
        comment="关联的数据表数量"
    )

    data_quality_score = Column(
        Integer,
        nullable=True,
        comment="数据质量评分（0-100）"
    )

    def __repr__(self) -> str:
        return f"<BusinessSystem(name='{self.system_name}', status='{self.status}')>"

    def to_dict_summary(self) -> dict:
        """返回摘要信息"""
        return {
            "id": self.id,
            "system_name": self.system_name,
            "display_name": self.display_name,
            "status": self.status,
            "business_domain": self.business_domain,
            "criticality_level": self.criticality_level,
            "table_count": self.table_count or 0,
            "last_data_sync": self.last_data_sync,
            "created_at": self.created_at
        }

    def to_dict_detail(self) -> dict:
        """返回详细信息"""
        return {
            "id": self.id,
            "system_name": self.system_name,
            "display_name": self.display_name,
            "description": self.description,
            "contact_person": self.contact_person,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "system_type": self.system_type,
            "business_domain": self.business_domain,
            "criticality_level": self.criticality_level,
            "database_info": self.database_info,
            "data_volume_estimate": self.data_volume_estimate,
            "status": self.status,
            "is_data_source": self.is_data_source,
            "go_live_date": self.go_live_date,
            "last_data_sync": self.last_data_sync,
            "tags": self.tags,
            "metadata_info": self.metadata_info,
            "table_count": self.table_count,
            "data_quality_score": self.data_quality_score,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }


class BusinessSystemDataSource(BaseModel):
    """
    业务系统与数据源的关联关系表

    一个业务系统可能对应多个数据源
    """
    __tablename__ = "business_system_data_sources"

    business_system_id = Column(
        Integer,
        nullable=False,
        index=True,
        comment="业务系统ID"
    )

    data_source_name = Column(
        String(255),
        nullable=False,
        comment="数据源名称"
    )

    data_source_type = Column(
        String(50),
        nullable=False,
        comment="数据源类型：mysql/oracle/hive等"
    )

    database_name = Column(
        String(255),
        nullable=True,
        comment="数据库名称"
    )

    table_prefix = Column(
        String(100),
        nullable=True,
        comment="表名前缀"
    )

    sync_frequency = Column(
        String(50),
        nullable=True,
        default="daily",
        comment="同步频率：realtime/hourly/daily/weekly"
    )

    is_primary = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="是否为主要数据源"
    )

    status = Column(
        String(20),
        nullable=False,
        default="active",
        comment="关联状态：active/inactive"
    )

    def __repr__(self) -> str:
        return f"<BusinessSystemDataSource(system_id={self.business_system_id}, source='{self.data_source_name}')>"