# app/models/indicator_system.py
"""
指标体系建设模型
用于管理企业级指标体系的定义、分类和关联
"""
from sqlalchemy import Column, String, Integer, Text, Boolean, JSON, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from app.models.base import BaseModel


class IndicatorSystem(BaseModel):
    """
    指标体系主表

    存储指标的完整定义信息，包括：
    - 业务属性：来源系统、业务领域、业务主题等
    - 技术属性：数据类型、数据长度、数据格式等
    - 管理属性：权责部门、采集频率、共享类型等
    """
    __tablename__ = "indicator_systems"

    # ==================== 业务属性 ====================
    source_system = Column(
        String(255),
        comment="来源系统"
    )

    business_domain = Column(
        String(255),
        index=True,
        comment="业务领域"
    )

    business_theme = Column(
        String(255),
        index=True,
        comment="业务主题"
    )

    indicator_category = Column(
        String(255),
        index=True,
        comment="指标类别"
    )

    indicator_name = Column(
        String(500),
        nullable=False,
        index=True,
        comment="指标名称"
    )

    indicator_description = Column(
        Text,
        comment="指标说明"
    )

    remark = Column(
        Text,
        comment="备注"
    )

    # ==================== 技术属性 ====================
    indicator_type = Column(
        String(100),
        comment="指标类型"
    )

    tech_classification = Column(
        String(255),
        comment="数据标准技术分类"
    )

    data_type = Column(
        String(100),
        comment="数据类型"
    )

    data_length = Column(
        Integer,
        comment="数据长度"
    )

    data_format = Column(
        String(255),
        comment="数据格式"
    )

    # ==================== 管理属性 ====================
    responsible_dept = Column(
        String(255),
        comment="权责部门"
    )

    collection_frequency = Column(
        String(100),
        comment="采集频率"
    )

    collection_time = Column(
        String(255),
        comment="采集时点"
    )

    share_type = Column(
        String(100),
        comment="共享类型"
    )

    open_attribute = Column(
        String(100),
        comment="开放属性"
    )

    # ==================== 扩展字段 ====================
    tags = Column(
        JSON,
        comment="标签列表"
    )

    is_active = Column(
        Boolean,
        default=True,
        index=True,
        comment="是否启用"
    )

    # 创建人信息
    created_by = Column(
        String(100),
        comment="创建人"
    )

    updated_by = Column(
        String(100),
        comment="更新人"
    )

    # ==================== 关系定义 ====================
    # 与数据资产的多对多关联
    asset_relations = relationship(
        "IndicatorAssetRelation",
        back_populates="indicator",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<IndicatorSystem(id={self.id}, name={self.indicator_name})>"


class IndicatorAssetRelation(BaseModel):
    """
    指标与数据资产关联表（中间表）

    用于建立指标与数据资产的多对多关系：
    - 一个指标可以关联多个数据资产（数据来源）
    - 一个数据资产可以被多个指标引用
    """
    __tablename__ = "indicator_asset_relations"

    # 指标ID
    indicator_id = Column(
        Integer,
        ForeignKey('indicator_systems.id'),
        nullable=False,
        index=True,
        comment="指标ID"
    )

    # 数据资产ID
    asset_id = Column(
        Integer,
        ForeignKey('data_assets.id'),
        nullable=False,
        index=True,
        comment="数据资产ID"
    )

    # 关联说明
    relation_description = Column(
        Text,
        comment="关联说明（该资产如何用于该指标）"
    )

    # 关联类型
    relation_type = Column(
        String(50),
        comment="关联类型: source/reference/derived"
    )

    # 排序
    sort_order = Column(
        Integer,
        default=0,
        comment="排序序号"
    )

    # 创建人
    created_by = Column(
        String(100),
        comment="创建人"
    )

    # ==================== 关系定义 ====================
    indicator = relationship(
        "IndicatorSystem",
        back_populates="asset_relations"
    )

    asset = relationship(
        "DataAsset"
    )

    def __repr__(self):
        return f"<IndicatorAssetRelation(indicator_id={self.indicator_id}, asset_id={self.asset_id})>"