"""
自定义API相关的请求和响应模式
"""

from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class HTTPMethod(str, Enum):
    GET = "GET"
    POST = "POST"


class ResponseFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"


class ParameterType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class APIParameterCreate(BaseModel):
    """创建API参数的请求模式"""
    param_name: str = Field(..., pattern="^[a-zA-Z_][a-zA-Z0-9_]*$", min_length=1, max_length=50, description="参数名称")
    param_type: ParameterType = Field(..., description="参数类型")
    is_required: bool = Field(False, description="是否必填参数")
    default_value: Optional[str] = Field(None, max_length=200, description="默认值")
    description: Optional[str] = Field(None, max_length=500, description="参数描述")
    validation_rule: Optional[Dict[str, Any]] = Field(None, description="参数验证规则")

    @field_validator('default_value')
    @classmethod
    def validate_default_value(cls, v, info):
        """验证默认值与参数类型匹配"""
        if v is None:
            return v

        # 从 info.data 获取其他字段值
        param_type = info.data.get('param_type') if info.data else None

        if param_type == ParameterType.INTEGER:
            try:
                int(v)
            except ValueError:
                raise ValueError('整数类型参数的默认值必须是有效整数')
        elif param_type == ParameterType.FLOAT:
            try:
                float(v)
            except ValueError:
                raise ValueError('浮点类型参数的默认值必须是有效数字')
        elif param_type == ParameterType.BOOLEAN:
            if v.lower() not in ['true', 'false', '1', '0']:
                raise ValueError('布尔类型参数的默认值必须是 true/false 或 1/0')

        return v


class CreateAPIRequest(BaseModel):
    """创建自定义API的请求模式"""
    api_name: str = Field(..., min_length=1, max_length=100, pattern="^[a-zA-Z_][a-zA-Z0-9_]*$", description="API名称")
    api_path: str = Field(..., pattern="^/api/custom/[a-zA-Z0-9_/-]+$", max_length=200, description="API路径")
    description: Optional[str] = Field(None, max_length=1000, description="API描述")
    data_source_id: int = Field(..., gt=0, description="数据源ID")
    sql_template: str = Field(..., min_length=10, description="SQL查询模板")
    http_method: HTTPMethod = Field(HTTPMethod.GET, description="HTTP请求方法")
    response_format: ResponseFormat = Field(ResponseFormat.JSON, description="响应格式")
    cache_ttl: int = Field(300, ge=0, le=3600, description="缓存时间(秒)")
    rate_limit: int = Field(100, ge=1, le=10000, description="频率限制(次/分钟)")
    parameters: List[APIParameterCreate] = Field([], description="API参数列表")

    @field_validator('sql_template')
    def validate_sql_template(cls, v):
        """验证SQL模板安全性"""
        if not v or not v.strip():
            raise ValueError('SQL模板不能为空')

        # 转换为小写进行检查
        sql_lower = v.lower().strip()

        # 必须以SELECT开头
        if not sql_lower.startswith('select'):
            raise ValueError('SQL模板必须以SELECT开头，仅支持查询操作')

        # 危险关键词检查
        dangerous_keywords = [
            'drop', 'delete', 'truncate', 'alter', 'create', 'insert',
            'update', 'replace', 'merge', 'grant', 'revoke', 'exec',
            'execute', 'sp_', 'xp_', '--', '/*', '*/', ';drop', ';delete'
        ]

        for keyword in dangerous_keywords:
            if keyword in sql_lower:
                raise ValueError(f'SQL模板不能包含危险关键词: {keyword}')

        return v.strip()

    @field_validator('api_path')
    def validate_api_path(cls, v):
        """验证API路径格式"""
        if not v.startswith('/api/custom/'):
            raise ValueError('API路径必须以 /api/custom/ 开头')

        # 检查路径是否包含特殊字符
        import re
        if not re.match(r'^/api/custom/[a-zA-Z0-9_/-]+$', v):
            raise ValueError('API路径只能包含字母、数字、下划线、连字符和斜线')

        return v


class UpdateAPIRequest(BaseModel):
    """更新自定义API的请求模式"""
    description: Optional[str] = Field(None, max_length=1000, description="API描述")
    sql_template: Optional[str] = Field(None, min_length=10, description="SQL查询模板")
    response_format: Optional[ResponseFormat] = Field(None, description="响应格式")
    cache_ttl: Optional[int] = Field(None, ge=0, le=3600, description="缓存时间")
    rate_limit: Optional[int] = Field(None, ge=1, le=10000, description="频率限制")
    is_active: Optional[bool] = Field(None, description="是否激活")
    parameters: Optional[List[APIParameterCreate]] = Field(None, description="API参数列表")

    @field_validator('sql_template')
    def validate_sql_template(cls, v):
        """验证SQL模板"""
        if v is not None:
            return CreateAPIRequest.validate_sql_template(v)
        return v


class APIParameterResponse(BaseModel):
    """API参数响应模式"""
    id: int
    param_name: str
    param_type: ParameterType
    is_required: bool
    default_value: Optional[str]
    description: Optional[str]
    validation_rule: Optional[Dict[str, Any]]


class CustomAPIResponse(BaseModel):
    """自定义API响应模式"""
    id: int
    api_name: str
    api_path: str
    description: Optional[str]
    data_source_id: int
    data_source_name: Optional[str]
    data_source_type: Optional[str]
    sql_template: str
    http_method: HTTPMethod
    response_format: ResponseFormat
    is_active: bool
    cache_ttl: int
    rate_limit: int
    total_calls: int
    success_calls: int
    last_call_time: Optional[datetime]
    created_by: Optional[str]
    created_at: datetime
    updated_at: datetime
    parameters: List[APIParameterResponse]

    class Config:
        from_attributes = True


class SQLValidationResult(BaseModel):
    """SQL验证结果"""
    is_valid: bool
    error_message: Optional[str] = None
    extracted_parameters: List[str] = []
    rendered_sql_example: Optional[str] = None
    parameter_count: int = 0


class APIExecutionResult(BaseModel):
    """API执行结果"""
    success: bool
    data: List[Dict[str, Any]] = []
    total_count: int = 0
    response_time_ms: int
    executed_sql: Optional[str] = None
    error_message: Optional[str] = None
    test_note: Optional[str] = None
    pagination: Optional[Dict[str, Any]] = None