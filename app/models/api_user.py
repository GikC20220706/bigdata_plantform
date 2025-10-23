"""
API用户和密钥管理模型
app/models/api_user.py
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, ForeignKey, Enum as SQLEnum, func
from sqlalchemy.orm import relationship
from enum import Enum

from .base import BaseModel


class UserType(str, Enum):
    """用户类型枚举"""
    SYSTEM = "system"  # 系统账号
    APPLICATION = "application"  # 应用
    DEVELOPER = "developer"  # 开发者


class APIUser(BaseModel):
    """API用户表 - 用于管理API调用者"""
    __tablename__ = "api_users"
    __table_args__ = {'extend_existing': True}

    # 基本信息
    username = Column(String(50), unique=True, nullable=False, index=True, comment="用户名")
    display_name = Column(String(100), nullable=False, comment="显示名称")
    description = Column(Text, nullable=True, comment="用户描述")

    # 用户类型
    user_type = Column(
        SQLEnum(UserType),
        nullable=False,
        default=UserType.APPLICATION,
        index=True,
        comment="用户类型"
    )

    # 状态
    is_active = Column(Boolean, nullable=False, default=True, index=True, comment="是否启用")

    # 关系映射
    api_keys = relationship("APIKey", back_populates="user", cascade="all, delete-orphan")
    permissions = relationship("APIUserPermission", back_populates="user", cascade="all, delete-orphan")


class APIKey(BaseModel):
    """API密钥表"""
    __tablename__ = "api_keys"
    __table_args__ = {'extend_existing': True}

    # 关联用户
    user_id = Column(Integer, ForeignKey('api_users.id'), nullable=False, index=True, comment="关联用户ID")

    # 密钥信息
    key_name = Column(String(100), nullable=False, comment="密钥名称")
    api_key = Column(String(128), unique=True, nullable=False, index=True, comment="API密钥")
    key_prefix = Column(String(20), nullable=False, comment="密钥前缀")

    # 状态和配置
    is_active = Column(Boolean, nullable=False, default=True, index=True, comment="是否启用")
    expires_at = Column(DateTime, nullable=True, comment="过期时间")

    # 限制配置
    rate_limit = Column(Integer, nullable=False, default=1000, comment="速率限制(次/小时)")
    allowed_ips = Column(Text, nullable=True, comment="IP白名单，逗号分隔")

    # 使用统计
    last_used_at = Column(DateTime, nullable=True, comment="最后使用时间")
    total_calls = Column(Integer, nullable=False, default=0, comment="总调用次数")

    # 关系映射
    user = relationship("APIUser", back_populates="api_keys")


class APIUserPermission(BaseModel):
    """API用户权限关联表"""
    __tablename__ = "api_user_permissions"
    __table_args__ = {'extend_existing': True}

    # 关联关系
    api_id = Column(Integer, ForeignKey('custom_apis.id'), nullable=False, index=True, comment="接口ID")
    user_id = Column(Integer, ForeignKey('api_users.id'), nullable=False, index=True, comment="用户ID")

    # 授权信息
    granted_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), comment="授权时间")
    granted_by = Column(String(100), nullable=True, comment="授权人")

    # 关系映射
    api = relationship("CustomAPI", back_populates="user_permissions")
    user = relationship("APIUser", back_populates="permissions")