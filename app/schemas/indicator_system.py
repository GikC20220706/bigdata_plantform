# app/schemas/indicator_system.py
"""
指标体系建设相关的Pydantic Schema
用于API请求和响应的数据验证
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


# ==================== 枚举类型 ====================

class RelationType(str, Enum):
    """指标与资产关联类型枚举"""
    SOURCE = "source"  # 数据来源
    REFERENCE = "reference"  # 参考引用
    DERIVED = "derived"  # 派生计算


# ==================== 指标体系相关 Schema ====================

class IndicatorSystemBase(BaseModel):
    """指标体系基础模型"""
    # 业务属性
    source_system: Optional[str] = Field(None, max_length=255, description="来源系统")
    business_domain: Optional[str] = Field(None, max_length=255, description="业务领域")
    business_theme: Optional[str] = Field(None, max_length=255, description="业务主题")
    indicator_category: Optional[str] = Field(None, max_length=255, description="指标类别")
    indicator_name: str = Field(..., min_length=1, max_length=500, description="指标名称")
    indicator_description: Optional[str] = Field(None, description="指标说明")
    remark: Optional[str] = Field(None, description="备注")

    # 技术属性
    indicator_type: Optional[str] = Field(None, max_length=100, description="指标类型")
    tech_classification: Optional[str] = Field(None, max_length=255, description="数据标准技术分类")
    data_type: Optional[str] = Field(None, max_length=100, description="数据类型")
    data_length: Optional[int] = Field(None, description="数据长度")
    data_format: Optional[str] = Field(None, max_length=255, description="数据格式")

    # 管理属性
    responsible_dept: Optional[str] = Field(None, max_length=255, description="权责部门")
    collection_frequency: Optional[str] = Field(None, max_length=100, description="采集频率")
    collection_time: Optional[str] = Field(None, max_length=255, description="采集时点")
    share_type: Optional[str] = Field(None, max_length=100, description="共享类型")
    open_attribute: Optional[str] = Field(None, max_length=100, description="开放属性")

    # 扩展属性
    tags: Optional[List[str]] = Field(None, description="标签列表")


class IndicatorSystemCreate(IndicatorSystemBase):
    """创建指标请求"""

    class Config:
        json_schema_extra = {
            "example": {
                "indicator_name": "客户总数",
                "source_system": "CRM系统",
                "business_domain": "客户管理",
                "business_theme": "客户分析",
                "indicator_category": "基础指标",
                "indicator_description": "统计所有注册客户的总数量",
                "indicator_type": "累计值",
                "data_type": "INTEGER",
                "data_length": 10,
                "responsible_dept": "市场部",
                "collection_frequency": "每日",
                "tags": ["客户", "核心指标"]
            }
        }


class IndicatorSystemUpdate(BaseModel):
    """更新指标请求"""
    # 业务属性
    source_system: Optional[str] = Field(None, max_length=255, description="来源系统")
    business_domain: Optional[str] = Field(None, max_length=255, description="业务领域")
    business_theme: Optional[str] = Field(None, max_length=255, description="业务主题")
    indicator_category: Optional[str] = Field(None, max_length=255, description="指标类别")
    indicator_name: Optional[str] = Field(None, min_length=1, max_length=500, description="指标名称")
    indicator_description: Optional[str] = Field(None, description="指标说明")
    remark: Optional[str] = Field(None, description="备注")

    # 技术属性
    indicator_type: Optional[str] = Field(None, max_length=100, description="指标类型")
    tech_classification: Optional[str] = Field(None, max_length=255, description="数据标准技术分类")
    data_type: Optional[str] = Field(None, max_length=100, description="数据类型")
    data_length: Optional[int] = Field(None, description="数据长度")
    data_format: Optional[str] = Field(None, max_length=255, description="数据格式")

    # 管理属性
    responsible_dept: Optional[str] = Field(None, max_length=255, description="权责部门")
    collection_frequency: Optional[str] = Field(None, max_length=100, description="采集频率")
    collection_time: Optional[str] = Field(None, max_length=255, description="采集时点")
    share_type: Optional[str] = Field(None, max_length=100, description="共享类型")
    open_attribute: Optional[str] = Field(None, max_length=100, description="开放属性")

    # 扩展属性
    tags: Optional[List[str]] = Field(None, description="标签列表")
    is_active: Optional[bool] = Field(None, description="是否启用")


class IndicatorSystemResponse(IndicatorSystemBase):
    """指标响应"""
    id: int
    is_active: bool
    created_by: Optional[str]
    updated_by: Optional[str]
    created_at: datetime
    updated_at: datetime

    # 关联的资产数量
    asset_count: int = Field(0, description="关联的数据资产数量")

    class Config:
        from_attributes = True


class IndicatorSystemListResponse(BaseModel):
    """指标列表响应"""
    total: int = Field(..., description="总数")
    page: int = Field(..., description="当前页")
    page_size: int = Field(..., description="每页数量")
    items: List[IndicatorSystemResponse] = Field(..., description="指标列表")


# ==================== 指标与资产关联相关 Schema ====================

class IndicatorAssetRelationCreate(BaseModel):
    """创建指标资产关联请求"""
    asset_id: int = Field(..., description="数据资产ID")
    relation_type: RelationType = Field(RelationType.SOURCE, description="关联类型")
    relation_description: Optional[str] = Field(None, description="关联说明")
    sort_order: int = Field(0, description="排序序号")


class IndicatorAssetRelationResponse(BaseModel):
    """指标资产关联响应"""
    id: int
    indicator_id: int
    asset_id: int
    relation_type: str
    relation_description: Optional[str]
    sort_order: int
    created_by: Optional[str]
    created_at: datetime

    # 关联的资产信息
    asset_name: Optional[str] = Field(None, description="资产名称")
    asset_code: Optional[str] = Field(None, description="资产编码")
    table_name: Optional[str] = Field(None, description="表名")

    class Config:
        from_attributes = True


class LinkAssetsRequest(BaseModel):
    """关联数据资产请求"""
    asset_ids: List[int] = Field(..., description="数据资产ID列表")
    relation_type: RelationType = Field(RelationType.SOURCE, description="关联类型")
    relation_description: Optional[str] = Field(None, description="关联说明")


# ==================== 批量操作相关 Schema ====================

class BatchCreateIndicatorsRequest(BaseModel):
    """批量创建指标请求"""
    indicators: List[IndicatorSystemCreate] = Field(..., description="指标列表")


class BatchOperationResult(BaseModel):
    """批量操作结果"""
    total: int = Field(..., description="总数")
    success_count: int = Field(..., description="成功数量")
    failed_count: int = Field(..., description="失败数量")
    success_ids: List[int] = Field(default_factory=list, description="成功的ID列表")
    failed_items: List[Dict[str, Any]] = Field(default_factory=list, description="失败的项目")


# ==================== Excel导入相关 Schema ====================

class ExcelImportResult(BaseModel):
    """Excel导入结果"""
    total_rows: int = Field(..., description="总行数")
    success_count: int = Field(..., description="成功导入数量")
    failed_count: int = Field(..., description="失败数量")
    failed_rows: List[Dict[str, Any]] = Field(default_factory=list, description="失败的行数据")
    message: str = Field(..., description="导入结果消息")


# ==================== 统计相关 Schema ====================

class IndicatorStatistics(BaseModel):
    """指标统计信息"""
    total_count: int = Field(..., description="总指标数")
    active_count: int = Field(..., description="启用的指标数")
    inactive_count: int = Field(..., description="未启用的指标数")
    by_domain: Dict[str, int] = Field(default_factory=dict, description="按业务领域统计")
    by_category: Dict[str, int] = Field(default_factory=dict, description="按指标类别统计")
    by_frequency: Dict[str, int] = Field(default_factory=dict, description="按采集频率统计")