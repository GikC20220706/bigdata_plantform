"""
API用户和密钥管理的Schema
app/schemas/api_user.py
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, validator
from enum import Enum


class UserType(str, Enum):
    """用户类型"""
    SYSTEM = "system"
    APPLICATION = "application"
    DEVELOPER = "developer"


class AccessLevel(str, Enum):
    """API访问级别"""
    PUBLIC = "public"  # 公开
    AUTHENTICATED = "authenticated"  # 需要认证
    RESTRICTED = "restricted"  # 限定用户


# ==================== API用户相关 ====================

class CreateAPIUserRequest(BaseModel):
    """创建API用户请求"""
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    display_name: str = Field(..., min_length=1, max_length=100, description="显示名称")
    description: Optional[str] = Field(None, max_length=1000, description="用户描述")
    user_type: UserType = Field(UserType.APPLICATION, description="用户类型")

    @validator('username')
    def validate_username(cls, v):
        """验证用户名格式"""
        import re
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError('用户名只能包含字母、数字、下划线，且必须以字母开头')
        return v


class UpdateAPIUserRequest(BaseModel):
    """更新API用户请求"""
    display_name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    user_type: Optional[UserType] = None
    is_active: Optional[bool] = None


class APIUserResponse(BaseModel):
    """API用户响应"""
    id: int
    username: str
    display_name: str
    description: Optional[str]
    user_type: str
    is_active: bool
    total_keys: int = 0  # 密钥数量
    total_permissions: int = 0  # 权限数量
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ==================== API密钥相关 ====================

class CreateAPIKeyRequest(BaseModel):
    """创建API密钥请求"""
    user_id: int = Field(..., description="用户ID")
    key_name: str = Field(..., min_length=1, max_length=100, description="密钥名称")
    expires_days: Optional[int] = Field(None, ge=1, le=3650, description="有效天数，不填表示永久")
    rate_limit: int = Field(1000, ge=1, le=100000, description="速率限制(次/小时)")
    allowed_ips: Optional[str] = Field(None, description="IP白名单，逗号分隔")


class APIKeyResponse(BaseModel):
    """API密钥响应"""
    id: int
    user_id: int
    key_name: str
    api_key: str
    key_prefix: str
    is_active: bool
    expires_at: Optional[datetime]
    rate_limit: int
    allowed_ips: Optional[str]
    last_used_at: Optional[datetime]
    total_calls: int
    created_at: datetime

    class Config:
        from_attributes = True


class APIKeyListResponse(BaseModel):
    """API密钥列表响应（隐藏完整key）"""
    id: int
    user_id: int
    key_name: str
    key_preview: str  # 只显示前缀+后4位
    key_prefix: str
    is_active: bool
    expires_at: Optional[datetime]
    rate_limit: int
    last_used_at: Optional[datetime]
    total_calls: int
    created_at: datetime

    class Config:
        from_attributes = True


# ==================== API权限相关 ====================

class GrantAPIPermissionRequest(BaseModel):
    """授权API权限请求"""
    api_id: int = Field(..., description="接口ID")
    user_ids: List[int] = Field(..., min_items=1, description="用户ID列表")
    granted_by: Optional[str] = Field(None, description="授权人")


class RevokeAPIPermissionRequest(BaseModel):
    """撤销API权限请求"""
    api_id: int
    user_ids: List[int]


class APIPermissionResponse(BaseModel):
    """API权限响应"""
    id: int
    api_id: int
    user_id: int
    api_name: Optional[str]
    user_name: Optional[str]
    granted_at: datetime
    granted_by: Optional[str]

    class Config:
        from_attributes = True


# ==================== 验证相关 ====================

class ValidateAPIKeyRequest(BaseModel):
    """验证API密钥请求"""
    api_key: str
    api_path: str
    client_ip: Optional[str] = None


class ValidateAPIKeyResponse(BaseModel):
    """验证API密钥响应"""
    valid: bool
    user_id: Optional[int] = None
    username: Optional[str] = None
    error_message: Optional[str] = None