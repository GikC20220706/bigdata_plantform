# app/models/data_asset.py
"""
数据资产模型
用于管理实际的数据表资产及其字段信息
"""
from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, ForeignKey, JSON, Float
from sqlalchemy.orm import relationship
from app.models.base import BaseModel


class DataAsset(BaseModel):
    """
    数据资产表

    代表实际的数据表资产，包含：
    - 基本信息（名称、描述）
    - 物理位置（数据源、数据库、表名）
    - 质量信息（质量等级、评分）
    - 使用信息（访问次数、重要程度）
    - API发布信息
    """
    __tablename__ = "data_assets"

    # 基本信息
    asset_name = Column(
        String(255),
        nullable=False,
        index=True,
        comment="资产名称"
    )

    asset_code = Column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="资产编码（唯一标识）"
    )

    asset_type = Column(
        String(50),
        nullable=False,
        default="table",
        index=True,
        comment="资产类型: table/view/external/temp"
    )

    # 关联关系
    catalog_id = Column(
        Integer,
        ForeignKey('data_catalogs.id'),
        nullable=False,
        index=True,
        comment="所属目录ID"
    )

    data_source_id = Column(
        Integer,
        ForeignKey('data_sources.id'),
        nullable=False,
        index=True,
        comment="数据源ID"
    )

    # 物理信息
    database_name = Column(
        String(255),
        comment="数据库名"
    )

    schema_name = Column(
        String(255),
        comment="模式名（Schema）"
    )

    table_name = Column(
        String(255),
        nullable=False,
        index=True,
        comment="表名"
    )

    # 业务信息
    business_description = Column(
        Text,
        comment="业务描述"
    )

    technical_description = Column(
        Text,
        comment="技术说明"
    )

    # 质量信息
    quality_level = Column(
        String(10),
        default="C",
        index=True,
        comment="质量等级: A/B/C/D"
    )

    quality_score = Column(
        Float,
        comment="质量评分（0-100）"
    )

    # 使用信息
    usage_frequency = Column(
        String(50),
        comment="使用频率: high/medium/low"
    )

    importance_level = Column(
        String(50),
        comment="重要程度: critical/important/normal"
    )

    # 状态信息
    status = Column(
        String(50),
        default="normal",
        index=True,
        comment="资产状态: normal/offline/maintenance/deprecated"
    )

    is_public = Column(
        Boolean,
        default=True,
        comment="是否公开"
    )

    # 统计信息
    row_count = Column(
        Integer,
        comment="表行数"
    )

    data_size = Column(
        String(50),
        comment="数据大小（格式化，如: 1.5GB）"
    )

    column_count = Column(
        Integer,
        comment="字段数量"
    )

    # API发布相关
    is_api_published = Column(
        Boolean,
        default=False,
        index=True,
        comment="是否已发布为API"
    )

    published_api_id = Column(
        Integer,
        ForeignKey('custom_apis.id'),
        nullable=True,
        comment="关联的API ID"
    )

    api_publish_time = Column(
        DateTime(timezone=True),
        comment="API发布时间"
    )

    # 访问统计
    preview_count = Column(
        Integer,
        default=0,
        comment="预览次数"
    )

    download_count = Column(
        Integer,
        default=0,
        comment="下载次数"
    )

    api_call_count = Column(
        Integer,
        default=0,
        comment="API调用次数"
    )

    # 下载配置
    allow_download = Column(
        Boolean,
        default=True,
        comment="是否允许下载"
    )

    max_download_rows = Column(
        Integer,
        default=10000,
        comment="最大下载行数"
    )

    # 元数据
    tags = Column(
        JSON,
        comment="标签列表"
    )

    labels = Column(
        JSON,
        comment="标签（键值对）"
    )

    business_owner = Column(
        String(100),
        comment="业务负责人"
    )

    technical_owner = Column(
        String(100),
        comment="技术负责人"
    )

    # 时间信息
    last_updated_at = Column(
        DateTime(timezone=True),
        comment="数据最后更新时间"
    )

    last_accessed_at = Column(
        DateTime(timezone=True),
        comment="最后访问时间"
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
    catalog = relationship(
        "DataCatalog",
        back_populates="data_assets"
    )

    data_source = relationship(
        "DataSource"
    )

    published_api = relationship(
        "CustomAPI",
        foreign_keys=[published_api_id]
    )

    columns = relationship(
        "AssetColumn",
        back_populates="asset",
        cascade="all, delete-orphan"
    )

    access_logs = relationship(
        "AssetAccessLog",
        back_populates="asset",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<DataAsset(id={self.id}, name={self.asset_name}, table={self.table_name})>"


class AssetColumn(BaseModel):
    """
    资产字段表

    存储数据资产的字段信息，包括：
    - 字段定义（名称、类型、长度）
    - 约束信息（主键、可空、唯一）
    - 业务信息（中文名、描述）
    - 统计信息（唯一值数、空值数）
    """
    __tablename__ = "asset_columns"

    # 基本信息
    asset_id = Column(
        Integer,
        ForeignKey('data_assets.id'),
        nullable=False,
        index=True,
        comment="资产ID"
    )

    column_name = Column(
        String(255),
        nullable=False,
        comment="字段名"
    )

    column_name_cn = Column(
        String(255),
        comment="字段中文名"
    )

    # 类型信息
    data_type = Column(
        String(100),
        comment="数据类型"
    )

    length = Column(
        Integer,
        comment="长度"
    )

    precision = Column(
        Integer,
        comment="精度"
    )

    scale = Column(
        Integer,
        comment="标度（小数位数）"
    )

    # 约束信息
    is_primary_key = Column(
        Boolean,
        default=False,
        comment="是否主键"
    )

    is_nullable = Column(
        Boolean,
        default=True,
        comment="是否可空"
    )

    is_unique = Column(
        Boolean,
        default=False,
        comment="是否唯一"
    )

    default_value = Column(
        String(500),
        comment="默认值"
    )

    # 业务信息
    business_description = Column(
        Text,
        comment="业务描述"
    )

    example_value = Column(
        String(500),
        comment="示例值"
    )

    # 关联字段标准
    field_standard_id = Column(
        Integer,
        ForeignKey('field_standards.id'),
        comment="关联的字段标准ID"
    )

    # 统计信息
    distinct_count = Column(
        Integer,
        comment="唯一值数量"
    )

    null_count = Column(
        Integer,
        comment="空值数量"
    )

    # 排序
    sort_order = Column(
        Integer,
        default=0,
        comment="字段排序"
    )

    # 关系定义
    asset = relationship(
        "DataAsset",
        back_populates="columns"
    )

    field_standard = relationship(
        "FieldStandard"
    )

    def __repr__(self):
        return f"<AssetColumn(id={self.id}, name={self.column_name}, type={self.data_type})>"


class AssetAccessLog(BaseModel):
    """
    资产访问日志表

    记录数据资产的所有访问记录：
    - 预览记录
    - 下载记录
    - API调用记录
    """
    __tablename__ = "asset_access_logs"

    # 资产信息
    asset_id = Column(
        Integer,
        ForeignKey('data_assets.id'),
        nullable=False,
        index=True,
        comment="资产ID"
    )

    # 访问信息
    access_type = Column(
        String(50),
        nullable=False,
        index=True,
        comment="访问类型: preview/download/api"
    )

    access_user = Column(
        String(100),
        comment="访问用户"
    )

    access_ip = Column(
        String(50),
        comment="访问IP地址"
    )

    # 操作详情
    operation_details = Column(
        JSON,
        comment="操作详情（JSON格式）"
    )

    record_count = Column(
        Integer,
        comment="返回记录数"
    )

    file_size = Column(
        Integer,
        comment="文件大小（字节）"
    )

    # 状态
    status = Column(
        String(20),
        index=True,
        comment="状态: success/failed"
    )

    error_message = Column(
        Text,
        comment="错误信息"
    )

    # 性能
    response_time_ms = Column(
        Integer,
        comment="响应时间（毫秒）"
    )

    # 时间
    access_time = Column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        comment="访问时间"
    )

    # 关系定义
    asset = relationship(
        "DataAsset",
        back_populates="access_logs"
    )

    def __repr__(self):
        return f"<AssetAccessLog(id={self.id}, type={self.access_type}, user={self.access_user})>"