"""
业务系统管理相关的Pydantic模式定义
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
from enum import Enum


class SystemType(str, Enum):
    """系统类型枚举"""
    BUSINESS = "business"
    TECHNICAL = "technical"
    PLATFORM = "platform"


class CriticalityLevel(str, Enum):
    """重要程度枚举"""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SystemStatus(str, Enum):
    """系统状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"
    DEPRECATED = "deprecated"


class SyncFrequency(str, Enum):
    """同步频率枚举"""
    REALTIME = "realtime"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


# === 基础模型 ===

class BusinessSystemBase(BaseModel):
    """业务系统基础模型"""
    system_name: str = Field(..., min_length=1, max_length=255, description="业务系统名称")
    display_name: str = Field(..., min_length=1, max_length=255, description="显示名称")
    description: Optional[str] = Field(None, max_length=1000, description="系统描述")
    contact_person: Optional[str] = Field(None, max_length=100, description="联系人姓名")
    contact_email: Optional[str] = Field(None, max_length=255, description="联系人邮箱")
    contact_phone: Optional[str] = Field(None, max_length=50, description="联系人电话")
    system_type: SystemType = Field(default=SystemType.BUSINESS, description="系统类型")
    business_domain: Optional[str] = Field(None, max_length=100, description="业务领域")
    criticality_level: CriticalityLevel = Field(default=CriticalityLevel.MEDIUM, description="重要程度")
    data_volume_estimate: Optional[str] = Field(None, max_length=50, description="数据量估计")
    is_data_source: bool = Field(default=True, description="是否作为数据源")
    go_live_date: Optional[datetime] = Field(None, description="系统上线时间")
    tags: Optional[List[str]] = Field(default_factory=list, description="标签列表")

    @validator('contact_email')
    def validate_email(cls, v):
        if v and '@' not in v:
            raise ValueError('联系人邮箱格式不正确')
        return v

    @validator('system_name')
    def validate_system_name(cls, v):
        if not v or not v.strip():
            raise ValueError('业务系统名称不能为空')
        # 检查是否包含特殊字符
        if any(char in v for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|']):
            raise ValueError('业务系统名称不能包含特殊字符')
        return v.strip()


class DatabaseInfo(BaseModel):
    """数据库信息模型"""
    database_type: str = Field(..., description="数据库类型")
    host: Optional[str] = Field(None, description="主机地址")
    port: Optional[int] = Field(None, description="端口")
    database_name: Optional[str] = Field(None, description="数据库名称")
    schema_name: Optional[str] = Field(None, description="模式名称")
    table_count: Optional[int] = Field(None, description="表数量")
    connection_status: Optional[str] = Field(None, description="连接状态")


# === 请求模型 ===

class BusinessSystemCreate(BusinessSystemBase):
    """创建业务系统请求模型"""
    database_info: Optional[DatabaseInfo] = Field(None, description="数据库信息")
    metadata_info: Optional[Dict[str, Any]] = Field(default_factory=dict, description="元数据信息")

    class Config:
        schema_extra = {
            "example": {
                "system_name": "finance_system",
                "display_name": "财务管理系统",
                "description": "集团财务核算和报表系统",
                "contact_person": "张三",
                "contact_email": "zhangsan@company.com",
                "contact_phone": "13800138000",
                "system_type": "business",
                "business_domain": "财务",
                "criticality_level": "high",
                "data_volume_estimate": "100GB",
                "is_data_source": True,
                "tags": ["财务", "核心系统", "Oracle"],
                "database_info": {
                    "database_type": "oracle",
                    "host": "192.168.1.100",
                    "port": 1521,
                    "database_name": "FINANCE",
                    "schema_name": "FINANCE_PROD"
                }
            }
        }


class BusinessSystemUpdate(BaseModel):
    """更新业务系统请求模型"""
    display_name: Optional[str] = Field(None, min_length=1, max_length=255, description="显示名称")
    description: Optional[str] = Field(None, max_length=1000, description="系统描述")
    contact_person: Optional[str] = Field(None, max_length=100, description="联系人姓名")
    contact_email: Optional[str] = Field(None, max_length=255, description="联系人邮箱")
    contact_phone: Optional[str] = Field(None, max_length=50, description="联系人电话")
    system_type: Optional[SystemType] = Field(None, description="系统类型")
    business_domain: Optional[str] = Field(None, max_length=100, description="业务领域")
    criticality_level: Optional[CriticalityLevel] = Field(None, description="重要程度")
    data_volume_estimate: Optional[str] = Field(None, max_length=50, description="数据量估计")
    is_data_source: Optional[bool] = Field(None, description="是否作为数据源")
    go_live_date: Optional[datetime] = Field(None, description="系统上线时间")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    database_info: Optional[DatabaseInfo] = Field(None, description="数据库信息")
    metadata_info: Optional[Dict[str, Any]] = Field(None, description="元数据信息")

    @validator('contact_email')
    def validate_email(cls, v):
        if v and '@' not in v:
            raise ValueError('联系人邮箱格式不正确')
        return v


class BusinessSystemStatusUpdate(BaseModel):
    """业务系统状态更新模型"""
    status: SystemStatus = Field(..., description="新状态")
    reason: Optional[str] = Field(None, max_length=500, description="状态变更原因")


# === 响应模型 ===

class BusinessSystemResponse(BusinessSystemBase):
    """业务系统响应模型"""
    id: int = Field(..., description="系统ID")
    status: SystemStatus = Field(..., description="系统状态")
    database_info: Optional[DatabaseInfo] = Field(None, description="数据库信息")
    table_count: Optional[int] = Field(None, description="关联表数量")
    data_quality_score: Optional[int] = Field(None, description="数据质量评分")
    last_data_sync: Optional[datetime] = Field(None, description="最后数据同步时间")
    metadata_info: Optional[Dict[str, Any]] = Field(None, description="元数据信息")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    class Config:
        from_attributes = True


class BusinessSystemSummary(BaseModel):
    """业务系统摘要模型"""
    id: int = Field(..., description="系统ID")
    system_name: str = Field(..., description="系统名称")
    display_name: str = Field(..., description="显示名称")
    status: SystemStatus = Field(..., description="系统状态")
    business_domain: Optional[str] = Field(None, description="业务领域")
    criticality_level: CriticalityLevel = Field(..., description="重要程度")
    table_count: Optional[int] = Field(None, description="关联表数量")
    last_data_sync: Optional[datetime] = Field(None, description="最后数据同步时间")
    created_at: datetime = Field(..., description="创建时间")

    class Config:
        from_attributes = True


class BusinessSystemListResponse(BaseModel):
    """业务系统列表响应模型"""
    items: List[BusinessSystemSummary] = Field(..., description="业务系统列表")
    total: int = Field(..., ge=0, description="总数量")
    page: int = Field(..., ge=1, description="当前页码")
    page_size: int = Field(..., ge=1, description="每页大小")
    total_pages: int = Field(..., ge=1, description="总页数")


# === 数据源关联模型 ===

class DataSourceAssociation(BaseModel):
    """数据源关联模型"""
    data_source_name: str = Field(..., description="数据源名称")
    data_source_type: str = Field(..., description="数据源类型")
    database_name: Optional[str] = Field(None, description="数据库名称")
    table_prefix: Optional[str] = Field(None, description="表名前缀")
    sync_frequency: SyncFrequency = Field(default=SyncFrequency.DAILY, description="同步频率")
    is_primary: bool = Field(default=False, description="是否为主要数据源")


class DataSourceAssociationCreate(DataSourceAssociation):
    """创建数据源关联请求模型"""
    pass


class DataSourceAssociationResponse(DataSourceAssociation):
    """数据源关联响应模型"""
    id: int = Field(..., description="关联ID")
    business_system_id: int = Field(..., description="业务系统ID")
    status: str = Field(..., description="关联状态")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    class Config:
        from_attributes = True


# === 统计和查询模型 ===

class BusinessSystemSearchParams(BaseModel):
    """业务系统搜索参数模型"""
    keyword: Optional[str] = Field(None, description="搜索关键词")
    status: Optional[SystemStatus] = Field(None, description="状态过滤")
    system_type: Optional[SystemType] = Field(None, description="系统类型过滤")
    business_domain: Optional[str] = Field(None, description="业务领域过滤")
    criticality_level: Optional[CriticalityLevel] = Field(None, description="重要程度过滤")
    tags: Optional[List[str]] = Field(None, description="标签过滤")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页大小")
    order_by: Optional[str] = Field("created_at", description="排序字段")
    order_desc: bool = Field(True, description="是否降序排列")


class BusinessSystemStatistics(BaseModel):
    """业务系统统计信息模型"""
    total_systems: int = Field(..., description="系统总数")
    active_systems: int = Field(..., description="活跃系统数")
    inactive_systems: int = Field(..., description="非活跃系统数")
    by_type: Dict[str, int] = Field(..., description="按类型分组统计")
    by_domain: Dict[str, int] = Field(..., description="按业务领域分组统计")
    by_criticality: Dict[str, int] = Field(..., description="按重要程度分组统计")
    by_status: Dict[str, int] = Field(..., description="按状态分组统计")
    total_tables: int = Field(..., description="总表数量")
    avg_data_quality: Optional[float] = Field(None, description="平均数据质量分数")
    last_updated: datetime = Field(..., description="统计更新时间")


class BusinessSystemHealth(BaseModel):
    """业务系统健康状态模型"""
    system_id: int = Field(..., description="系统ID")
    system_name: str = Field(..., description="系统名称")
    overall_status: str = Field(..., description="整体状态")
    database_connection: bool = Field(..., description="数据库连接状态")
    data_freshness: Optional[str] = Field(None, description="数据新鲜度")
    data_quality_score: Optional[int] = Field(None, description="数据质量分数")
    last_sync_time: Optional[datetime] = Field(None, description="最后同步时间")
    sync_status: Optional[str] = Field(None, description="同步状态")
    issues: List[str] = Field(default_factory=list, description="发现的问题")
    checked_at: datetime = Field(..., description="检查时间")


# === 批量操作模型 ===

class BusinessSystemBatchImport(BaseModel):
    """批量导入业务系统模型"""
    systems: List[BusinessSystemCreate] = Field(..., description="待导入的业务系统列表")
    overwrite_existing: bool = Field(False, description="是否覆盖已存在的系统")


class BusinessSystemBatchStatusUpdate(BaseModel):
    """批量状态更新模型"""
    system_ids: List[int] = Field(..., description="系统ID列表")
    status: SystemStatus = Field(..., description="新状态")
    reason: Optional[str] = Field(None, description="变更原因")


class BatchOperationResult(BaseModel):
    """批量操作结果模型"""
    total_count: int = Field(..., description="总处理数量")
    success_count: int = Field(..., description="成功数量")
    failed_count: int = Field(..., description="失败数量")
    failed_items: List[Dict[str, Any]] = Field(default_factory=list, description="失败项详情")
    operation_time: datetime = Field(..., description="操作时间")