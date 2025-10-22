"""
API用户和密钥管理服务
app/services/api_user_service.py
"""

import secrets
import hashlib
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, delete
from sqlalchemy.orm import selectinload
from loguru import logger

from app.models.api_user import APIUser, APIKey, APIUserPermission
from app.models.custom_api import CustomAPI
from app.schemas.api_user import (
    CreateAPIUserRequest, UpdateAPIUserRequest,
    CreateAPIKeyRequest, GrantAPIPermissionRequest
)


class APIUserService:
    """API用户管理服务"""

    # ==================== API用户管理 ====================

    async def create_user(
            self,
            db: AsyncSession,
            request: CreateAPIUserRequest
    ) -> APIUser:
        """创建API用户"""
        # 检查用户名是否已存在
        query = select(APIUser).where(APIUser.username == request.username)
        result = await db.execute(query)
        existing_user = result.scalar_one_or_none()

        if existing_user:
            raise ValueError(f"用户名 '{request.username}' 已存在")

        # 创建用户
        user = APIUser(
            username=request.username,
            display_name=request.display_name,
            description=request.description,
            user_type=request.user_type
        )

        db.add(user)
        await db.commit()
        await db.refresh(user)

        logger.info(f"创建API用户成功: {user.username} (ID: {user.id})")
        return user

    async def get_user(
            self,
            db: AsyncSession,
            user_id: int
    ) -> Optional[APIUser]:
        """获取用户详情"""
        query = select(APIUser).where(APIUser.id == user_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_user_by_username(
            self,
            db: AsyncSession,
            username: str
    ) -> Optional[APIUser]:
        """根据用户名获取用户"""
        query = select(APIUser).where(APIUser.username == username)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_users_list(
            self,
            db: AsyncSession,
            skip: int = 0,
            limit: int = 20,
            search: Optional[str] = None,
            user_type: Optional[str] = None,
            is_active: Optional[bool] = None
    ) -> Tuple[List[APIUser], int]:
        """获取用户列表"""
        # 构建查询
        query = select(APIUser)

        # 添加筛选条件
        conditions = []
        if search:
            search_pattern = f"%{search}%"
            conditions.append(
                (APIUser.username.like(search_pattern)) |
                (APIUser.display_name.like(search_pattern))
            )
        if user_type:
            conditions.append(APIUser.user_type == user_type)
        if is_active is not None:
            conditions.append(APIUser.is_active == is_active)

        if conditions:
            query = query.where(and_(*conditions))

        # 获取总数
        count_query = select(func.count(APIUser.id))
        if conditions:
            count_query = count_query.where(and_(*conditions))

        total_result = await db.execute(count_query)
        total_count = total_result.scalar()

        # 分页查询
        query = query.order_by(APIUser.created_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query)
        users = result.scalars().all()

        return users, total_count

    async def update_user(
            self,
            db: AsyncSession,
            user_id: int,
            request: UpdateAPIUserRequest
    ) -> Optional[APIUser]:
        """更新用户信息"""
        user = await self.get_user(db, user_id)
        if not user:
            return None

        # 更新字段
        update_data = request.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(user, field, value)

        await db.commit()
        await db.refresh(user)

        logger.info(f"更新API用户: {user.username} (ID: {user.id})")
        return user

    async def delete_user(
            self,
            db: AsyncSession,
            user_id: int
    ) -> bool:
        """删除用户"""
        user = await self.get_user(db, user_id)
        if not user:
            return False

        await db.delete(user)
        await db.commit()

        logger.info(f"删除API用户: {user.username} (ID: {user.id})")
        return True

    # ==================== API密钥管理 ====================

    def _generate_api_key(self, prefix: str = "sk_live") -> str:
        """生成API密钥"""
        # 生成32字节随机数据
        random_bytes = secrets.token_bytes(32)
        # 转换为十六进制
        key_hex = random_bytes.hex()
        # 添加前缀
        return f"{prefix}_{key_hex}"

    async def create_api_key(
            self,
            db: AsyncSession,
            request: CreateAPIKeyRequest
    ) -> APIKey:
        """创建API密钥"""
        # 检查用户是否存在
        user = await self.get_user(db, request.user_id)
        if not user:
            raise ValueError("用户不存在")

        # 生成密钥
        key_prefix = "sk_live"
        api_key = self._generate_api_key(key_prefix)

        # 计算过期时间
        expires_at = None
        if request.expires_days:
            expires_at = datetime.now() + timedelta(days=request.expires_days)

        # 创建密钥记录
        key = APIKey(
            user_id=request.user_id,
            key_name=request.key_name,
            api_key=api_key,
            key_prefix=key_prefix,
            expires_at=expires_at,
            rate_limit=request.rate_limit,
            allowed_ips=request.allowed_ips
        )

        db.add(key)
        await db.commit()
        await db.refresh(key)

        logger.info(f"为用户 {user.username} 创建API密钥: {key.key_name}")
        return key

    async def get_api_key(
            self,
            db: AsyncSession,
            key_id: int
    ) -> Optional[APIKey]:
        """获取密钥详情"""
        query = select(APIKey).where(APIKey.id == key_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_api_key_by_key(
            self,
            db: AsyncSession,
            api_key: str
    ) -> Optional[APIKey]:
        """根据密钥字符串获取密钥记录"""
        query = select(APIKey).where(APIKey.api_key == api_key)
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_user_api_keys(
            self,
            db: AsyncSession,
            user_id: int,
            include_inactive: bool = False
    ) -> List[APIKey]:
        """获取用户的所有密钥"""
        query = select(APIKey).where(APIKey.user_id == user_id)

        if not include_inactive:
            query = query.where(APIKey.is_active == True)

        query = query.order_by(APIKey.created_at.desc())
        result = await db.execute(query)
        return result.scalars().all()

    async def delete_api_key(
            self,
            db: AsyncSession,
            key_id: int
    ) -> bool:
        """删除API密钥"""
        key = await self.get_api_key(db, key_id)
        if not key:
            return False

        await db.delete(key)
        await db.commit()

        logger.info(f"删除API密钥: {key.key_name} (ID: {key.id})")
        return True

    async def toggle_api_key_status(
            self,
            db: AsyncSession,
            key_id: int
    ) -> Optional[APIKey]:
        """切换密钥状态（启用/禁用）"""
        key = await self.get_api_key(db, key_id)
        if not key:
            return None

        key.is_active = not key.is_active
        await db.commit()
        await db.refresh(key)

        logger.info(f"切换API密钥状态: {key.key_name} -> {key.is_active}")
        return key

    # ==================== 权限管理 ====================

    async def grant_permissions(
            self,
            db: AsyncSession,
            request: GrantAPIPermissionRequest
    ) -> List[APIUserPermission]:
        """授权API访问权限"""
        # 检查API是否存在
        api_query = select(CustomAPI).where(CustomAPI.id == request.api_id)
        api_result = await db.execute(api_query)
        api = api_result.scalar_one_or_none()

        if not api:
            raise ValueError("API不存在")

        # 批量创建权限
        permissions = []
        for user_id in request.user_ids:
            # 检查是否已存在
            existing_query = select(APIUserPermission).where(
                and_(
                    APIUserPermission.api_id == request.api_id,
                    APIUserPermission.user_id == user_id
                )
            )
            existing_result = await db.execute(existing_query)
            existing = existing_result.scalar_one_or_none()

            if existing:
                continue  # 跳过已存在的权限

            # 创建新权限
            permission = APIUserPermission(
                api_id=request.api_id,
                user_id=user_id,
                granted_by=request.granted_by
            )
            db.add(permission)
            permissions.append(permission)

        await db.commit()

        logger.info(f"为API {api.api_name} 授权给 {len(permissions)} 个用户")
        return permissions

    async def revoke_permissions(
            self,
            db: AsyncSession,
            api_id: int,
            user_ids: List[int]
    ) -> int:
        """撤销API访问权限"""
        # 批量删除
        delete_query = delete(APIUserPermission).where(
            and_(
                APIUserPermission.api_id == api_id,
                APIUserPermission.user_id.in_(user_ids)
            )
        )

        result = await db.execute(delete_query)
        await db.commit()

        deleted_count = result.rowcount
        logger.info(f"撤销API权限: {deleted_count} 条")
        return deleted_count

    async def get_api_permissions(
            self,
            db: AsyncSession,
            api_id: int
    ) -> List[APIUserPermission]:
        """获取API的所有授权用户"""
        query = select(APIUserPermission).where(
            APIUserPermission.api_id == api_id
        ).options(
            selectinload(APIUserPermission.user)
        )

        result = await db.execute(query)
        return result.scalars().all()

    async def get_user_permissions(
            self,
            db: AsyncSession,
            user_id: int
    ) -> List[APIUserPermission]:
        """获取用户可访问的所有API"""
        query = select(APIUserPermission).where(
            APIUserPermission.user_id == user_id
        ).options(
            selectinload(APIUserPermission.api)
        )

        result = await db.execute(query)
        return result.scalars().all()

    # ==================== 验证相关 ====================

    async def validate_api_key(
            self,
            db: AsyncSession,
            api_key: str,
            api_id: int,
            client_ip: Optional[str] = None
    ) -> Tuple[bool, Optional[str], Optional[APIKey]]:
        """
        验证API密钥
        返回: (是否有效, 错误消息, 密钥对象)
        """
        # 1. 查找密钥
        key = await self.get_api_key_by_key(db, api_key)
        if not key:
            return False, "Invalid API Key", None

        # 2. 检查是否启用
        if not key.is_active:
            return False, "API Key is disabled", None

        # 3. 检查是否过期
        if key.expires_at and key.expires_at < datetime.now():
            return False, "API Key has expired", None

        # 4. 检查IP白名单
        if key.allowed_ips and client_ip:
            allowed_ips = [ip.strip() for ip in key.allowed_ips.split(',')]
            if client_ip not in allowed_ips:
                return False, f"IP {client_ip} not allowed", None

        # 5. 检查用户是否启用
        user = await self.get_user(db, key.user_id)
        if not user or not user.is_active:
            return False, "User is disabled", None

        # 6. 检查API访问权限
        api_query = select(CustomAPI).where(CustomAPI.id == api_id)
        api_result = await db.execute(api_query)
        api = api_result.scalar_one_or_none()

        if not api:
            return False, "API not found", None

        # 根据API的访问级别判断
        if api.access_level == 'public':
            # 公开API，任何人都可以访问
            return True, None, key

        elif api.access_level == 'authenticated':
            # 需要认证，有效Key即可
            return True, None, key

        elif api.access_level == 'restricted':
            # 限定用户，检查权限表
            permission_query = select(APIUserPermission).where(
                and_(
                    APIUserPermission.api_id == api_id,
                    APIUserPermission.user_id == key.user_id
                )
            )
            permission_result = await db.execute(permission_query)
            permission = permission_result.scalar_one_or_none()

            if not permission:
                return False, f"User does not have permission to access this API", None

            return True, None, key

        return False, "Unknown access level", None

    async def update_key_usage(
            self,
            db: AsyncSession,
            key_id: int
    ):
        """更新密钥使用统计"""
        key = await self.get_api_key(db, key_id)
        if key:
            key.last_used_at = datetime.now()
            key.total_calls += 1
            await db.commit()


# 创建服务实例
api_user_service = APIUserService()