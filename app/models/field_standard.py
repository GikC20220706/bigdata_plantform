# app/models/field_standard.py
"""
字段标准模型
用于管理企业级的字段标准规范
"""
from sqlalchemy import Column, String, Integer, Text, Boolean, JSON
from app.models.base import BaseModel


class FieldStandard(BaseModel):
    """
    字段标准表

    定义企业级的字段标准，包括：
    - 标准命名规范
    - 数据类型规范
    - 值域规范
    - 业务定义
    """
    __tablename__ = "field_standards"

    # 基本信息
    standard_code = Column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="标准编码（唯一标识）"
    )

    standard_name = Column(
        String(255),
        nullable=False,
        comment="标准名称"
    )

    standard_name_en = Column(
        String(255),
        comment="英文名称"
    )

    # 类型定义
    data_type = Column(
        String(100),
        comment="标准数据类型"
    )

    length = Column(
        Integer,
        comment="标准长度"
    )

    precision = Column(
        Integer,
        comment="标准精度"
    )

    scale = Column(
        Integer,
        comment="标准标度（小数位）"
    )

    # 约束规则
    is_nullable = Column(
        Boolean,
        default=True,
        comment="是否可空"
    )

    default_value = Column(
        String(500),
        comment="标准默认值"
    )

    value_range = Column(
        JSON,
        comment="值域范围（JSON格式）"
    )

    regex_pattern = Column(
        String(500),
        comment="正则表达式验证规则"
    )

    # 业务信息
    business_definition = Column(
        Text,
        comment="业务定义"
    )

    usage_guidelines = Column(
        Text,
        comment="使用指南"
    )

    example_values = Column(
        JSON,
        comment="示例值列表"
    )

    # 分类
    category = Column(
        String(100),
        index=True,
        comment="字段分类"
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

    version = Column(
        String(50),
        comment="版本号"
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

    def __repr__(self):
        return f"<FieldStandard(id={self.id}, code={self.standard_code}, name={self.standard_name})>"