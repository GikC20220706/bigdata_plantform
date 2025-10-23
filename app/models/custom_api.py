"""
自定义API生成器的数据库模型
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import LONGTEXT
from enum import Enum
from .base import BaseModel


class HTTPMethod(str, Enum):
    """HTTP方法枚举"""
    GET = "GET"
    POST = "POST"


class ResponseFormat(str, Enum):
    """响应格式枚举"""
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"


class ParameterType(str, Enum):
    """参数类型枚举"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class CustomAPI(BaseModel):
    """自定义API定义表"""
    __tablename__ = "custom_apis"

    # 基本信息
    api_name = Column(String(100), unique=True, nullable=False, index=True, comment="API名称")
    api_path = Column(String(200), unique=True, nullable=False, index=True, comment="API路径")
    description = Column(Text, nullable=True, comment="API描述")

    # 数据源关联
    data_source_id = Column(Integer, ForeignKey('data_sources.id'), nullable=False, comment="关联数据源ID")

    # SQL配置
    sql_template = Column(LONGTEXT, nullable=False, comment="SQL查询模板")

    # API配置
    http_method = Column(SQLEnum(HTTPMethod), nullable=False, default=HTTPMethod.GET, comment="HTTP方法")
    response_format = Column(SQLEnum(ResponseFormat), nullable=False, default=ResponseFormat.JSON, comment="响应格式")

    # 状态和配置
    is_active = Column(Boolean, nullable=False, default=True, index=True, comment="是否激活")
    access_level = Column(
        String(20),
        nullable=False,
        default='authenticated',
        index=True,
        comment="访问级别: public=公开, authenticated=需认证, restricted=限定用户"
    )
    cache_ttl = Column(Integer, nullable=False, default=300, comment="缓存时间(秒)")
    rate_limit = Column(Integer, nullable=False, default=100, comment="频率限制(次/分钟)")

    # 统计信息
    total_calls = Column(Integer, nullable=False, default=0, comment="总调用次数")
    success_calls = Column(Integer, nullable=False, default=0, comment="成功调用次数")
    last_call_time = Column(DateTime, nullable=True, comment="最后调用时间")

    # 创建者信息
    created_by = Column(String(100), nullable=True, comment="创建者")

    # 关系映射
    data_source = relationship("DataSource", back_populates="custom_apis")
    parameters = relationship("APIParameter", back_populates="api", cascade="all, delete-orphan")
    access_logs = relationship("APIAccessLog", back_populates="api", cascade="all, delete-orphan")
    user_permissions = relationship("APIUserPermission", back_populates="api", cascade="all, delete-orphan")


class APIParameter(BaseModel):
    """API参数定义表"""
    __tablename__ = "api_parameters"

    # 关联API
    api_id = Column(Integer, ForeignKey('custom_apis.id'), nullable=False, comment="关联API ID")

    # 参数配置
    param_name = Column(String(50), nullable=False, comment="参数名称")
    param_type = Column(SQLEnum(ParameterType), nullable=False, comment="参数类型")
    is_required = Column(Boolean, nullable=False, default=False, comment="是否必填")
    default_value = Column(String(200), nullable=True, comment="默认值")
    description = Column(String(500), nullable=True, comment="参数描述")

    # 验证规则(JSON格式)
    validation_rule = Column(JSON, nullable=True, comment="参数验证规则")

    # 关系映射
    api = relationship("CustomAPI", back_populates="parameters")


class APIAccessLog(BaseModel):
    """API访问日志表"""
    __tablename__ = "api_access_logs"

    # 关联API
    api_id = Column(Integer, ForeignKey('custom_apis.id'), nullable=False, comment="关联API ID")

    # 请求信息
    client_ip = Column(String(45), nullable=True, comment="客户端IP地址")
    user_agent = Column(String(500), nullable=True, comment="用户代理")
    # 添加认证相关字段
    auth_type = Column(String(20), nullable=True, comment="认证方式: api_key, public, none")
    api_key_id = Column(Integer, nullable=True, comment="使用的API Key ID")
    user_id = Column(Integer, nullable=True, comment="用户ID")

    request_params = Column(JSON, nullable=True, comment="请求参数(JSON)")

    # 响应信息
    response_time_ms = Column(Integer, nullable=True, comment="响应时间(毫秒)")
    status_code = Column(Integer, nullable=True, comment="HTTP状态码")
    response_size = Column(Integer, nullable=True, comment="响应大小(字节)")

    # 错误信息
    error_message = Column(Text, nullable=True, comment="错误消息")
    error_type = Column(String(100), nullable=True, comment="错误类型")

    # 执行信息
    executed_sql = Column(LONGTEXT, nullable=True, comment="实际执行的SQL")
    result_count = Column(Integer, nullable=True, comment="返回记录数")

    # 时间信息
    access_time = Column(DateTime, nullable=False, default=datetime.now, comment="访问时间")

    # 关系映射
    api = relationship("CustomAPI", back_populates="access_logs")