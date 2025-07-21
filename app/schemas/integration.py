"""
Data Integration Schemas
数据集成模块的Pydantic模式定义
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field
from enum import Enum


class DatabaseType(str, Enum):
    """支持的数据库类型枚举"""
    MYSQL = "mysql"
    KINGBASE = "kingbase"
    HIVE = "hive"
    DORIS = "doris"
    DAMENG = "dameng"
    GBASE = "gbase"
    TIDB = "tidb"


class ConnectionStatus(str, Enum):
    """连接状态枚举"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    TESTING = "testing"
    ERROR = "error"


class TableType(str, Enum):
    """表类型枚举"""
    TABLE = "TABLE"
    VIEW = "VIEW"
    EXTERNAL_TABLE = "EXTERNAL_TABLE"
    MANAGED_TABLE = "MANAGED_TABLE"
    SYSTEM_TABLE = "SYSTEM_TABLE"


# === 基础模型 ===

class DatabaseColumn(BaseModel):
    """数据库列信息"""
    name: str = Field(..., description="列名")
    type: str = Field(..., description="数据类型")
    nullable: bool = Field(True, description="是否允许NULL")
    default: Optional[str] = Field(None, description="默认值")
    comment: Optional[str] = Field(None, description="列注释")
    max_length: Optional[int] = Field(None, description="最大长度")
    precision: Optional[int] = Field(None, description="数值精度")
    scale: Optional[int] = Field(None, description="数值标度")
    key: Optional[str] = Field(None, description="键类型(PRI/UNI/MUL)")
    extra: Optional[str] = Field(None, description="额外信息")


class DatabaseIndex(BaseModel):
    """数据库索引信息"""
    name: str = Field(..., description="索引名称")
    columns: List[str] = Field(..., description="索引列")
    unique: bool = Field(False, description="是否唯一索引")
    primary: bool = Field(False, description="是否主键")
    type: Optional[str] = Field(None, description="索引类型")


class TableSchema(BaseModel):
    """表结构信息"""
    columns: List[DatabaseColumn] = Field(..., description="列信息")
    indexes: List[DatabaseIndex] = Field(default_factory=list, description="索引信息")
    primary_key: List[DatabaseIndex] = Field(default_factory=list, description="主键信息")
    foreign_keys: List[Dict[str, Any]] = Field(default_factory=list, description="外键信息")
    partitions: Optional[List[Dict[str, Any]]] = Field(None, description="分区信息")
    table_properties: Optional[Dict[str, Any]] = Field(None, description="表属性")


class TableStatistics(BaseModel):
    """表统计信息"""
    row_count: int = Field(0, description="行数")
    estimated_size: str = Field("未知", description="估计大小")
    last_updated: str = Field("未知", description="最后更新时间")
    data_size: Optional[int] = Field(None, description="数据大小(字节)")


class TableInfo(BaseModel):
    """表信息"""
    table_name: str = Field(..., description="表名")
    table_type: Optional[str] = Field(None, description="表类型")
    database: Optional[str] = Field(None, description="所属数据库")
    engine: Optional[str] = Field(None, description="存储引擎")
    estimated_rows: int = Field(0, description="估计行数")
    data_size: int = Field(0, description="数据大小")
    created_at: Optional[datetime] = Field(None, description="创建时间")
    updated_at: Optional[datetime] = Field(None, description="更新时间")
    comment: Optional[str] = Field(None, description="表注释")
    location: Optional[str] = Field(None, description="存储位置")
    owner: Optional[str] = Field(None, description="所有者")


class TableMetadata(BaseModel):
    """表元数据"""
    table_name: str = Field(..., description="表名")
    database: Optional[str] = Field(None, description="数据库名")
    schema: TableSchema = Field(..., description="表结构")
    statistics: TableStatistics = Field(..., description="统计信息")
    collected_at: datetime = Field(..., description="收集时间")
    error: Optional[str] = Field(None, description="错误信息")


# === 数据源相关模型 ===

class DataSourceConfig(BaseModel):
    """数据源配置"""
    host: str = Field(..., description="主机地址")
    port: int = Field(..., description="端口")
    username: str = Field(..., description="用户名")
    password: Optional[str] = Field(None, description="密码")
    database: Optional[str] = Field(None, description="默认数据库")
    charset: Optional[str] = Field("utf8mb4", description="字符集")
    timeout: int = Field(30, description="连接超时(秒)")
    pool_size: int = Field(10, description="连接池大小")
    extra_params: Optional[Dict[str, Any]] = Field(None, description="额外参数")


class DataSourceInfo(BaseModel):
    """数据源信息"""
    name: str = Field(..., description="数据源名称")
    type: DatabaseType = Field(..., description="数据库类型")
    status: ConnectionStatus = Field(..., description="连接状态")
    host: str = Field(..., description="主机地址")
    port: int = Field(..., description="端口")
    database: Optional[str] = Field(None, description="数据库名")
    description: Optional[str] = Field(None, description="描述")
    version: Optional[str] = Field(None, description="数据库版本")
    tables_count: int = Field(0, description="表数量")
    last_test: Optional[datetime] = Field(None, description="最后测试时间")
    created_at: Optional[datetime] = Field(None, description="创建时间")
    error: Optional[str] = Field(None, description="错误信息")


class ConnectionTestResult(BaseModel):
    """连接测试结果"""
    success: bool = Field(..., description="测试是否成功")
    database_type: Optional[str] = Field(None, description="数据库类型")
    version: Optional[str] = Field(None, description="版本信息")
    test_time: datetime = Field(..., description="测试时间")
    response_time_ms: Optional[int] = Field(None, description="响应时间(毫秒)")
    error: Optional[str] = Field(None, description="错误信息")


# === 请求和响应模型 ===

class CreateDataSourceRequest(BaseModel):
    """创建数据源请求"""
    name: str = Field(..., description="数据源名称", min_length=1, max_length=100)
    type: DatabaseType = Field(..., description="数据库类型")
    config: DataSourceConfig = Field(..., description="连接配置")
    description: Optional[str] = Field(None, description="描述信息", max_length=500)

    class Config:
        schema_extra = {
            "example": {
                "name": "mysql_demo",
                "type": "mysql",
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "username": "root",
                    "password": "password",
                    "database": "demo"
                },
                "description": "MySQL演示数据库"
            }
        }


class UpdateDataSourceRequest(BaseModel):
    """更新数据源请求"""
    config: Optional[DataSourceConfig] = Field(None, description="连接配置")
    description: Optional[str] = Field(None, description="描述信息", max_length=500)


class QueryRequest(BaseModel):
    """查询请求"""
    query: str = Field(..., description="SQL查询语句", min_length=1)
    database: Optional[str] = Field(None, description="数据库名称")
    limit: int = Field(100, description="结果限制条数", ge=1, le=1000)

    class Config:
        schema_extra = {
            "example": {
                "query": "SELECT * FROM users",
                "database": "demo",
                "limit": 50
            }
        }


class QueryResult(BaseModel):
    """查询结果"""
    success: bool = Field(..., description="查询是否成功")
    source_name: str = Field(..., description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    query: str = Field(..., description="执行的查询语句")
    results: List[Dict[str, Any]] = Field(..., description="查询结果")
    row_count: int = Field(..., description="结果行数")
    execution_time_ms: int = Field(..., description="执行时间(毫秒)")
    executed_at: datetime = Field(..., description="执行时间")
    error: Optional[str] = Field(None, description="错误信息")


class BatchTestRequest(BaseModel):
    """批量测试请求"""
    source_names: List[str] = Field(..., description="数据源名称列表", min_items=1)


class BatchTestResult(BaseModel):
    """批量测试结果"""
    total_tested: int = Field(..., description="测试总数")
    successful: int = Field(..., description="成功数量")
    failed: int = Field(..., description="失败数量")
    success_rate: float = Field(..., description="成功率(%)")
    test_results: Dict[str, ConnectionTestResult] = Field(..., description="详细测试结果")
    tested_at: datetime = Field(..., description="测试时间")


# === 概览和统计模型 ===

class DataSourceOverview(BaseModel):
    """数据源概览"""
    total_sources: int = Field(..., description="数据源总数")
    active_connections: int = Field(..., description="活跃连接数")
    failed_connections: int = Field(..., description="失败连接数")
    supported_types: List[str] = Field(..., description="支持的数据库类型")
    sources_by_type: Dict[str, int] = Field(..., description="按类型分组统计")
    data_volume_estimate: str = Field(..., description="数据量估计")
    last_sync: datetime = Field(..., description="最后同步时间")
    health_status: str = Field(..., description="健康状态")


class DataSourceStatistics(BaseModel):
    """数据源统计信息"""
    source_name: str = Field(..., description="数据源名称")
    connection_status: ConnectionStatus = Field(..., description="连接状态")
    database_type: str = Field(..., description="数据库类型")
    version: str = Field(..., description="版本信息")
    total_databases: int = Field(..., description="数据库总数")
    total_tables: int = Field(..., description="表总数")
    tables_by_database: Dict[str, int] = Field(..., description="各数据库表数量")
    last_test: datetime = Field(..., description="最后测试时间")
    response_time_ms: int = Field(..., description="响应时间(毫秒)")
    collected_at: datetime = Field(..., description="收集时间")
    error: Optional[str] = Field(None, description="错误信息")


class DatabaseTypeInfo(BaseModel):
    """数据库类型信息"""
    type: str = Field(..., description="类型标识")
    name: str = Field(..., description="显示名称")
    description: str = Field(..., description="描述")
    default_port: int = Field(..., description="默认端口")
    features: List[str] = Field(..., description="特性列表")
    connection_example: Optional[Dict[str, Any]] = Field(None, description="连接示例")


class HealthStatus(BaseModel):
    """健康状态"""
    status: str = Field(..., description="状态(healthy/warning/critical)")
    health_score: float = Field(..., description="健康分数(0-100)")
    total_sources: int = Field(..., description="数据源总数")
    active_connections: int = Field(..., description="活跃连接数")
    failed_connections: int = Field(..., description="失败连接数")
    uptime_percentage: float = Field(..., description="正常运行时间百分比")
    last_check: datetime = Field(..., description="最后检查时间")
    issues: List[str] = Field(default_factory=list, description="问题列表")


# === 搜索和过滤模型 ===

class TableSearchRequest(BaseModel):
    """表搜索请求"""
    keyword: Optional[str] = Field(None, description="搜索关键词")
    source_name: Optional[str] = Field(None, description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    table_type: Optional[TableType] = Field(None, description="表类型")
    page: int = Field(1, description="页码", ge=1)
    page_size: int = Field(20, description="每页大小", ge=1, le=100)


class TableSearchResult(BaseModel):
    """表搜索结果"""
    success: bool = Field(..., description="搜索是否成功")
    tables: List[TableInfo] = Field(..., description="表列表")
    total_count: int = Field(..., description="总数量")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页大小")
    total_pages: int = Field(..., description="总页数")
    search_criteria: Dict[str, Any] = Field(..., description="搜索条件")
    searched_at: datetime = Field(..., description="搜索时间")


class DataPreview(BaseModel):
    """数据预览"""
    source_name: str = Field(..., description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    table_name: str = Field(..., description="表名")
    preview_data: List[Dict[str, Any]] = Field(..., description="预览数据")
    row_count: int = Field(..., description="行数")
    schema: TableSchema = Field(..., description="表结构")
    execution_time_ms: int = Field(..., description="执行时间(毫秒)")
    previewed_at: datetime = Field(..., description="预览时间")


class DataSample(BaseModel):
    """数据样本"""
    source_name: str = Field(..., description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    table_name: str = Field(..., description="表名")
    sample_data: List[Dict[str, Any]] = Field(..., description="样本数据")
    sample_size: int = Field(..., description="样本大小")
    schema: TableSchema = Field(..., description="表结构")
    sampling_method: str = Field(..., description="采样方法(random/sequential)")
    sampled_at: datetime = Field(..., description="采样时间")


# === 响应模型 ===

class DataSourceListResponse(BaseModel):
    """数据源列表响应"""
    sources: List[DataSourceInfo] = Field(..., description="数据源列表")
    total: int = Field(..., description="总数")
    connected: int = Field(..., description="已连接数量")
    disconnected: int = Field(..., description="断开连接数量")


class DatabaseListResponse(BaseModel):
    """数据库列表响应"""
    success: bool = Field(..., description="是否成功")
    source_name: str = Field(..., description="数据源名称")
    databases: List[str] = Field(..., description="数据库列表")
    count: int = Field(..., description="数据库数量")
    retrieved_at: datetime = Field(..., description="获取时间")
    error: Optional[str] = Field(None, description="错误信息")


class TableListResponse(BaseModel):
    """表列表响应"""
    success: bool = Field(..., description="是否成功")
    source_name: str = Field(..., description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    tables: List[TableInfo] = Field(..., description="表列表")
    count: int = Field(..., description="表数量")
    retrieved_at: datetime = Field(..., description="获取时间")
    error: Optional[str] = Field(None, description="错误信息")


class TableSchemaResponse(BaseModel):
    """表结构响应"""
    success: bool = Field(..., description="是否成功")
    source_name: str = Field(..., description="数据源名称")
    database: Optional[str] = Field(None, description="数据库名称")
    table_name: str = Field(..., description="表名")
    schema: TableSchema = Field(..., description="表结构")
    retrieved_at: datetime = Field(..., description="获取时间")
    error: Optional[str] = Field(None, description="错误信息")


class SupportedTypesResponse(BaseModel):
    """支持的数据库类型响应"""
    supported_types: List[DatabaseTypeInfo] = Field(..., description="支持的类型列表")
    total_count: int = Field(..., description="总数量")