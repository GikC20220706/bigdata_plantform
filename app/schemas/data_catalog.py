# app/schemas/data_catalog.py
"""
数据资源目录相关的Pydantic Schema
用于API请求和响应的数据验证
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum


# ==================== 枚举类型 ====================

class CatalogType(str, Enum):
    """目录类型枚举"""
    DOMAIN = "domain"  # 业务域
    SUBJECT = "subject"  # 主题域
    DATASET = "dataset"  # 数据集


class AssetType(str, Enum):
    """资产类型枚举"""
    TABLE = "table"  # 实体表
    VIEW = "view"  # 视图
    EXTERNAL = "external"  # 外部表
    TEMP = "temp"  # 临时表


class AssetStatus(str, Enum):
    """资产状态枚举"""
    NORMAL = "normal"  # 正常
    OFFLINE = "offline"  # 已下线
    MAINTENANCE = "maintenance"  # 维护中
    DEPRECATED = "deprecated"  # 已废弃


class QualityLevel(str, Enum):
    """质量等级枚举"""
    A = "A"  # 优秀
    B = "B"  # 良好
    C = "C"  # 一般
    D = "D"  # 较差


class AccessType(str, Enum):
    """访问类型枚举"""
    PREVIEW = "preview"  # 预览
    DOWNLOAD = "download"  # 下载
    API = "api"  # API调用


class PreviewFormat(str, Enum):
    """预览格式枚举"""
    JSON = "json"
    TABLE = "table"


class DownloadFormat(str, Enum):
    """下载格式枚举"""
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"


# ==================== 数据目录相关 Schema ====================

class DataCatalogBase(BaseModel):
    """目录基础模型"""
    catalog_name: str = Field(..., min_length=1, max_length=255, description="目录名称")
    catalog_code: str = Field(..., min_length=1, max_length=100, description="目录编码")
    catalog_type: CatalogType = Field(..., description="目录类型")
    description: Optional[str] = Field(None, description="目录描述")
    sort_order: int = Field(0, description="排序序号")
    icon: Optional[str] = Field(None, max_length=100, description="图标")
    tags: Optional[List[str]] = Field(None, description="标签列表")


class DataCatalogCreate(DataCatalogBase):
    """创建目录请求"""
    parent_id: Optional[int] = Field(None, description="父级目录ID，为空则创建顶级目录")
    level: int = Field(..., ge=1, le=3, description="层级: 1-业务域 2-主题域 3-数据集")

    class Config:
        json_schema_extra = {
            "example": {
                "catalog_name": "财务域",
                "catalog_code": "FIN_DOMAIN",
                "catalog_type": "domain",
                "description": "财务相关的数据资产",
                "parent_id": None,
                "level": 1,
                "sort_order": 1,
                "icon": "icon-finance",
                "tags": ["财务", "核心"]
            }
        }


class DataCatalogUpdate(BaseModel):
    """更新目录请求"""
    catalog_name: Optional[str] = Field(None, min_length=1, max_length=255, description="目录名称")
    description: Optional[str] = Field(None, description="目录描述")
    sort_order: Optional[int] = Field(None, description="排序序号")
    icon: Optional[str] = Field(None, max_length=100, description="图标")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    is_active: Optional[bool] = Field(None, description="是否启用")


class DataCatalogResponse(DataCatalogBase):
    """目录响应"""
    id: int
    parent_id: Optional[int]
    level: int
    path: Optional[str]
    is_active: bool
    asset_count: int
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class DataCatalogTreeNode(DataCatalogResponse):
    """目录树节点（递归结构）"""
    children: List['DataCatalogTreeNode'] = Field(default_factory=list, description="子节点列表")

    class Config:
        from_attributes = True


class DataCatalogListResponse(BaseModel):
    """目录列表响应"""
    total: int = Field(..., description="总数")
    page: int = Field(..., description="当前页")
    page_size: int = Field(..., description="每页数量")
    items: List[DataCatalogResponse] = Field(..., description="目录列表")


# ==================== 数据资产相关 Schema ====================

class DataAssetBase(BaseModel):
    """资产基础模型"""
    asset_name: str = Field(..., min_length=1, max_length=255, description="资产名称")
    asset_code: str = Field(..., min_length=1, max_length=100, description="资产编码")
    asset_type: AssetType = Field(AssetType.TABLE, description="资产类型")
    business_description: Optional[str] = Field(None, description="业务描述")
    technical_description: Optional[str] = Field(None, description="技术说明")


class DataAssetCreate(DataAssetBase):
    """创建资产请求"""
    catalog_id: int = Field(..., description="所属目录ID")
    data_source_id: int = Field(..., description="数据源ID")
    database_name: Optional[str] = Field(None, max_length=255, description="数据库名")
    schema_name: Optional[str] = Field(None, max_length=255, description="模式名")
    table_name: str = Field(..., min_length=1, max_length=255, description="表名")
    quality_level: QualityLevel = Field(QualityLevel.C, description="质量等级")
    usage_frequency: Optional[str] = Field(None, description="使用频率: high/medium/low")
    importance_level: Optional[str] = Field(None, description="重要程度: critical/important/normal")
    business_owner: Optional[str] = Field(None, max_length=100, description="业务负责人")
    technical_owner: Optional[str] = Field(None, max_length=100, description="技术负责人")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    allow_download: bool = Field(True, description="是否允许下载")
    max_download_rows: int = Field(10000, ge=1, le=100000, description="最大下载行数")

    class Config:
        json_schema_extra = {
            "example": {
                "asset_name": "客户基本信息表",
                "asset_code": "CUS_BASIC_INFO",
                "asset_type": "table",
                "catalog_id": 1,
                "data_source_id": 1,
                "database_name": "crm_db",
                "table_name": "customer_basic_info",
                "business_description": "存储客户的基本信息，包括姓名、联系方式等",
                "quality_level": "B",
                "usage_frequency": "high",
                "importance_level": "critical",
                "business_owner": "张三",
                "technical_owner": "李四",
                "tags": ["客户", "核心表"]
            }
        }


class DataAssetUpdate(BaseModel):
    """更新资产请求"""
    asset_name: Optional[str] = Field(None, description="资产名称")
    catalog_id: Optional[int] = Field(None, description="所属目录ID")
    business_description: Optional[str] = Field(None, description="业务描述")
    technical_description: Optional[str] = Field(None, description="技术说明")
    quality_level: Optional[QualityLevel] = Field(None, description="质量等级")
    usage_frequency: Optional[str] = Field(None, description="使用频率")
    importance_level: Optional[str] = Field(None, description="重要程度")
    status: Optional[AssetStatus] = Field(None, description="资产状态")
    business_owner: Optional[str] = Field(None, description="业务负责人")
    technical_owner: Optional[str] = Field(None, description="技术负责人")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    allow_download: Optional[bool] = Field(None, description="是否允许下载")
    max_download_rows: Optional[int] = Field(None, description="最大下载行数")


class DataAssetResponse(DataAssetBase):
    """资产响应"""
    id: int
    catalog_id: int
    catalog_name: Optional[str] = None
    data_source_id: int
    data_source_name: Optional[str] = None
    database_name: Optional[str]
    schema_name: Optional[str]
    table_name: str
    quality_level: str
    quality_score: Optional[float]
    usage_frequency: Optional[str]
    importance_level: Optional[str]
    status: str
    is_public: bool
    row_count: Optional[int]
    data_size: Optional[str]
    column_count: Optional[int]
    is_api_published: bool
    published_api_id: Optional[int]
    preview_count: int
    download_count: int
    api_call_count: int
    allow_download: bool
    max_download_rows: int
    business_owner: Optional[str]
    technical_owner: Optional[str]
    tags: Optional[List[str]]
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime
    last_updated_at: Optional[datetime]
    last_accessed_at: Optional[datetime]

    class Config:
        from_attributes = True


class DataAssetListResponse(BaseModel):
    """资产列表响应"""
    total: int = Field(..., description="总数")
    page: int = Field(..., description="当前页")
    page_size: int = Field(..., description="每页数量")
    items: List[DataAssetResponse] = Field(..., description="资产列表")


# ==================== 资产字段相关 Schema ====================

class AssetColumnDetail(BaseModel):
    """资产字段详情"""
    id: int
    column_name: str = Field(..., description="字段名")
    column_name_cn: Optional[str] = Field(None, description="字段中文名")
    data_type: str = Field(..., description="数据类型")
    length: Optional[int] = Field(None, description="长度")
    precision: Optional[int] = Field(None, description="精度")
    scale: Optional[int] = Field(None, description="标度")
    is_primary_key: bool = Field(False, description="是否主键")
    is_nullable: bool = Field(True, description="是否可空")
    is_unique: bool = Field(False, description="是否唯一")
    default_value: Optional[str] = Field(None, description="默认值")
    business_description: Optional[str] = Field(None, description="业务描述")
    example_value: Optional[str] = Field(None, description="示例值")
    distinct_count: Optional[int] = Field(None, description="唯一值数量")
    null_count: Optional[int] = Field(None, description="空值数量")
    sort_order: int = Field(0, description="排序")
    field_standard_id: Optional[int] = Field(None, description="关联字段标准ID")
    field_standard_name: Optional[str] = Field(None, description="字段标准名称")

    class Config:
        from_attributes = True


class AssetColumnsResponse(BaseModel):
    """资产字段列表响应"""
    asset_id: int
    asset_name: str
    total_columns: int
    columns: List[AssetColumnDetail]


# ==================== 数据预览相关 Schema ====================

class DataPreviewRequest(BaseModel):
    """数据预览请求"""
    limit: int = Field(100, ge=1, le=1000, description="预览行数")
    offset: int = Field(0, ge=0, description="偏移量")
    format: PreviewFormat = Field(PreviewFormat.JSON, description="返回格式")
    columns: Optional[List[str]] = Field(None, description="指定列，不传则返回全部")
    filter_conditions: Optional[Dict[str, Any]] = Field(None, description="过滤条件")


class DataPreviewResponse(BaseModel):
    """数据预览响应"""
    asset_id: int
    asset_name: str
    columns: List[Dict[str, Any]] = Field(..., description="列信息")
    data: List[Dict[str, Any]] = Field(..., description="数据行")
    total_rows: Optional[int] = Field(None, description="总行数（如果能获取）")
    preview_rows: int = Field(..., description="预览行数")
    has_more: bool = Field(..., description="是否有更多数据")
    preview_time: datetime = Field(..., description="预览时间")


# ==================== 数据下载相关 Schema ====================

class DataDownloadRequest(BaseModel):
    """数据下载请求"""
    format: DownloadFormat = Field(DownloadFormat.EXCEL, description="下载格式")
    columns: Optional[List[str]] = Field(None, description="指定列")
    filter_conditions: Optional[Dict[str, Any]] = Field(None, description="过滤条件")
    max_rows: Optional[int] = Field(None, ge=1, le=100000, description="最大行数")
    filename: Optional[str] = Field(None, description="文件名")


class DataDownloadResponse(BaseModel):
    """数据下载响应"""
    download_id: str = Field(..., description="下载任务ID")
    filename: str = Field(..., description="文件名")
    file_size: int = Field(..., description="文件大小（字节）")
    row_count: int = Field(..., description="行数")
    format: str = Field(..., description="格式")
    download_url: Optional[str] = Field(None, description="下载链接")
    expires_at: datetime = Field(..., description="过期时间")


# ==================== API发布相关 Schema ====================

class PublishAPIRequest(BaseModel):
    """发布API请求"""
    api_name: str = Field(..., min_length=1, max_length=255, description="API名称")
    api_path: str = Field(..., description="API路径，如: /api/custom/customer_info")
    description: Optional[str] = Field(None, description="API描述")
    http_method: str = Field("GET", description="HTTP方法: GET/POST")
    response_format: str = Field("json", description="响应格式: json/csv/excel")

    # 查询配置
    select_columns: Optional[List[str]] = Field(None, description="SELECT的字段，不传则查询全部")
    where_conditions: Optional[str] = Field(None, description="WHERE条件，如: status='active'")
    order_by: Optional[str] = Field(None, description="排序字段，如: created_at DESC")
    limit: Optional[int] = Field(1000, ge=1, le=10000, description="默认返回行数")

    # 参数配置
    parameters: Optional[List[Dict[str, Any]]] = Field(None, description="API参数配置")

    # 访问控制
    is_public: bool = Field(True, description="是否公开")
    rate_limit: int = Field(100, ge=1, le=1000, description="频率限制（次/分钟）")
    cache_ttl: int = Field(300, ge=0, le=3600, description="缓存时间（秒）")

    class Config:
        json_schema_extra = {
            "example": {
                "api_name": "客户信息查询API",
                "api_path": "/api/custom/customer_info",
                "description": "查询客户基本信息",
                "http_method": "GET",
                "response_format": "json",
                "select_columns": ["id", "name", "phone", "email"],
                "where_conditions": "status='active'",
                "order_by": "created_at DESC",
                "limit": 100,
                "is_public": True,
                "rate_limit": 100,
                "cache_ttl": 300
            }
        }


class PublishAPIResponse(BaseModel):
    """发布API响应"""
    api_id: int = Field(..., description="API ID")
    api_name: str = Field(..., description="API名称")
    api_path: str = Field(..., description="API路径")
    api_url: str = Field(..., description="完整的访问URL")
    status: str = Field(..., description="状态")
    published_at: datetime = Field(..., description="发布时间")
    test_url: str = Field(..., description="测试URL")


# ==================== 资产统计相关 Schema ====================

class AssetStatistics(BaseModel):
    """资产统计信息"""
    asset_id: int
    asset_name: str

    # 基础统计
    total_rows: Optional[int] = Field(None, description="总行数")
    total_size: Optional[str] = Field(None, description="数据大小")
    column_count: int = Field(..., description="字段数量")

    # 访问统计
    total_preview_count: int = Field(0, description="总预览次数")
    total_download_count: int = Field(0, description="总下载次数")
    total_api_call_count: int = Field(0, description="总API调用次数")

    # 最近访问
    last_preview_time: Optional[datetime] = Field(None, description="最后预览时间")
    last_download_time: Optional[datetime] = Field(None, description="最后下载时间")
    last_api_call_time: Optional[datetime] = Field(None, description="最后API调用时间")

    # 热度评分
    popularity_score: float = Field(0.0, ge=0, le=100, description="热度评分（0-100）")


class AssetAccessLogResponse(BaseModel):
    """资产访问日志响应"""
    id: int
    asset_id: int
    access_type: str = Field(..., description="访问类型")
    access_user: Optional[str] = Field(None, description="访问用户")
    access_ip: Optional[str] = Field(None, description="访问IP")
    record_count: Optional[int] = Field(None, description="返回记录数")
    file_size: Optional[int] = Field(None, description="文件大小")
    status: str = Field(..., description="状态")
    response_time_ms: Optional[int] = Field(None, description="响应时间（毫秒）")
    access_time: datetime = Field(..., description="访问时间")

    class Config:
        from_attributes = True


class AssetAccessLogListResponse(BaseModel):
    """访问日志列表响应"""
    total: int
    page: int
    page_size: int
    items: List[AssetAccessLogResponse]


# ==================== 字段标准相关 Schema ====================

class FieldStandardBase(BaseModel):
    """字段标准基础模型"""
    standard_code: str = Field(..., min_length=1, max_length=100, description="标准编码")
    standard_name: str = Field(..., min_length=1, max_length=255, description="标准名称")
    standard_name_en: Optional[str] = Field(None, max_length=255, description="英文名称")
    data_type: str = Field(..., description="标准数据类型")
    business_definition: Optional[str] = Field(None, description="业务定义")
    category: Optional[str] = Field(None, max_length=100, description="分类")


class FieldStandardCreate(FieldStandardBase):
    """创建字段标准请求"""
    length: Optional[int] = Field(None, description="标准长度")
    precision: Optional[int] = Field(None, description="标准精度")
    scale: Optional[int] = Field(None, description="标准标度")
    is_nullable: bool = Field(True, description="是否可空")
    default_value: Optional[str] = Field(None, description="默认值")
    value_range: Optional[Dict[str, Any]] = Field(None, description="值域范围")
    regex_pattern: Optional[str] = Field(None, description="正则表达式")
    usage_guidelines: Optional[str] = Field(None, description="使用指南")
    example_values: Optional[List[str]] = Field(None, description="示例值")
    tags: Optional[List[str]] = Field(None, description="标签列表")

    class Config:
        json_schema_extra = {
            "example": {
                "standard_code": "STD_CUSTOMER_ID",
                "standard_name": "客户ID",
                "standard_name_en": "customer_id",
                "data_type": "VARCHAR",
                "length": 32,
                "is_nullable": False,
                "business_definition": "客户的唯一标识",
                "category": "标识类",
                "tags": ["客户", "主键"]
            }
        }


class FieldStandardUpdate(BaseModel):
    """更新字段标准请求"""
    standard_name: Optional[str] = Field(None, description="标准名称")
    business_definition: Optional[str] = Field(None, description="业务定义")
    usage_guidelines: Optional[str] = Field(None, description="使用指南")
    is_active: Optional[bool] = Field(None, description="是否启用")
    tags: Optional[List[str]] = Field(None, description="标签列表")


class FieldStandardResponse(FieldStandardBase):
    """字段标准响应"""
    id: int
    length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    is_nullable: bool
    default_value: Optional[str]
    value_range: Optional[Dict[str, Any]]
    regex_pattern: Optional[str]
    usage_guidelines: Optional[str]
    example_values: Optional[List[str]]
    tags: Optional[List[str]]
    is_active: bool
    version: Optional[str]
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class FieldStandardListResponse(BaseModel):
    """字段标准列表响应"""
    total: int
    page: int
    page_size: int
    items: List[FieldStandardResponse]


# ==================== 批量操作相关 Schema ====================

class BatchCreateAssetsRequest(BaseModel):
    """批量创建资产请求"""
    assets: List[DataAssetCreate] = Field(..., description="资产列表")


class BatchOperationResult(BaseModel):
    """批量操作结果"""
    total: int = Field(..., description="总数")
    success_count: int = Field(..., description="成功数量")
    failed_count: int = Field(..., description="失败数量")
    success_ids: List[int] = Field(default_factory=list, description="成功的ID列表")
    failed_items: List[Dict[str, Any]] = Field(default_factory=list, description="失败的项目")


class ImportTablesRequest(BaseModel):
    """从数据源导入表请求"""
    data_source_id: int = Field(..., description="数据源ID")
    catalog_id: int = Field(..., description="目标目录ID")
    database_name: Optional[str] = Field(None, description="数据库名")
    table_patterns: Optional[List[str]] = Field(None, description="表名匹配模式，如: ['cus_%', 'order_%']")
    include_columns: bool = Field(True, description="是否导入字段信息")