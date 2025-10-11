# app/models/data_catalog.py
"""
数据资源目录模型
用于管理数据资产的分类目录结构
"""
from sqlalchemy import Column, String, Integer, Text, Boolean, JSON, ForeignKey
from sqlalchemy.orm import relationship
from app.models.base import BaseModel


class DataCatalog(BaseModel):
    """
    数据资源目录表

    用于构建树形的数据资源分类结构：
    - Level 1: 业务域（如：财务域、营销域、供应链域）
    - Level 2: 主题域（如：客户主题、订单主题）
    - Level 3: 数据集（具体的业务分类）
    """
    __tablename__ = "data_catalogs"

    # 基本信息
    catalog_name = Column(
        String(255),
        nullable=False,
        comment="目录名称"
    )

    catalog_code = Column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="目录编码（唯一标识）"
    )

    catalog_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="目录类型: domain/subject/dataset"
    )

    # 层级关系
    parent_id = Column(
        Integer,
        ForeignKey('data_catalogs.id'),
        nullable=True,
        index=True,
        comment="父级目录ID"
    )

    level = Column(
        Integer,
        nullable=False,
        default=1,
        comment="目录层级: 1-业务域 2-主题域 3-数据集"
    )

    path = Column(
        String(500),
        comment="目录路径，如: /财务域/客户主题"
    )

    # 扩展信息
    description = Column(
        Text,
        comment="目录描述"
    )

    sort_order = Column(
        Integer,
        default=0,
        comment="排序序号"
    )

    icon = Column(
        String(100),
        comment="图标标识"
    )

    tags = Column(
        JSON,
        comment="标签列表"
    )

    # 状态
    is_active = Column(
        Boolean,
        default=True,
        index=True,
        comment="是否启用"
    )

    # 统计信息
    asset_count = Column(
        Integer,
        default=0,
        comment="下属资产数量（包含子目录）"
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

    # 关系定义
    # 自关联：父子目录关系
    children = relationship(
        "DataCatalog",
        foreign_keys="DataCatalog.parent_id",
        backref="parent",
        remote_side="DataCatalog.id",  # ← 改成 lambda
        cascade="all"
    )

    # 关联的数据资产
    data_assets = relationship(
        "DataAsset",
        back_populates="catalog",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<DataCatalog(id={self.id}, name={self.catalog_name}, level={self.level})>"