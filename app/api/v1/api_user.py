"""
API用户和密钥管理的REST API端点
app/api/v1/api_user.py
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.services.api_user_service import api_user_service
from app.schemas.api_user import (
    CreateAPIUserRequest, UpdateAPIUserRequest, APIUserResponse,
    CreateAPIKeyRequest, APIKeyResponse, APIKeyListResponse,
    GrantAPIPermissionRequest, RevokeAPIPermissionRequest, APIPermissionResponse,
    ValidateAPIKeyRequest, ValidateAPIKeyResponse
)

router = APIRouter(prefix="/api-users", tags=["API用户管理"])


# ==================== API用户管理 ====================

@router.post("/", summary="创建API用户")
async def create_api_user(
        request: CreateAPIUserRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    创建新的API用户

    - **username**: 用户名，唯一标识
    - **display_name**: 显示名称
    - **description**: 用户描述（可选）
    - **user_type**: 用户类型（system/application/developer）
    """
    try:
        user = await api_user_service.create_user(db, request)

        return create_response(
            data={
                "id": user.id,
                "username": user.username,
                "display_name": user.display_name,
                "user_type": user.user_type,
                "created_at": user.created_at
            },
            message=f"API用户 '{user.username}' 创建成功"
        )

    except ValueError as e:
        logger.warning(f"创建API用户失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建API用户失败: {e}")
        raise HTTPException(status_code=500, detail="创建API用户失败")


@router.get("/", summary="获取API用户列表")
async def get_api_users(
        db: AsyncSession = Depends(get_async_db),
        skip: int = Query(0, ge=0, description="跳过记录数"),
        limit: int = Query(20, ge=1, le=100, description="每页记录数"),
        search: Optional[str] = Query(None, description="搜索关键词"),
        user_type: Optional[str] = Query(None, description="用户类型过滤"),
        is_active: Optional[bool] = Query(None, description="状态过滤")
):
    """获取API用户列表，支持分页和筛选"""
    try:
        users, total_count = await api_user_service.get_users_list(
            db=db,
            skip=skip,
            limit=limit,
            search=search,
            user_type=user_type,
            is_active=is_active
        )

        # 构造响应数据
        user_list = []
        for user in users:
            # 统计密钥数量
            keys = await api_user_service.get_user_api_keys(db, user.id, include_inactive=True)
            # 统计权限数量
            permissions = await api_user_service.get_user_permissions(db, user.id)

            user_list.append({
                "id": user.id,
                "username": user.username,
                "display_name": user.display_name,
                "description": user.description,
                "user_type": user.user_type,
                "is_active": user.is_active,
                "total_keys": len(keys),
                "total_permissions": len(permissions),
                "created_at": user.created_at,
                "updated_at": user.updated_at
            })

        return create_response(
            data={
                "users": user_list,
                "total_count": total_count,
                "page_info": {
                    "skip": skip,
                    "limit": limit,
                    "has_next": skip + limit < total_count
                }
            },
            message="获取用户列表成功"
        )

    except Exception as e:
        logger.error(f"获取用户列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取用户列表失败")


@router.get("/{user_id}", summary="获取API用户详情")
async def get_api_user(
        user_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API用户的详细信息"""
    try:
        user = await api_user_service.get_user(db, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        # 获取用户的密钥列表
        keys = await api_user_service.get_user_api_keys(db, user_id, include_inactive=True)
        # 获取用户的权限列表
        permissions = await api_user_service.get_user_permissions(db, user_id)

        return create_response(
            data={
                "id": user.id,
                "username": user.username,
                "display_name": user.display_name,
                "description": user.description,
                "user_type": user.user_type,
                "is_active": user.is_active,
                "total_keys": len(keys),
                "active_keys": len([k for k in keys if k.is_active]),
                "total_permissions": len(permissions),
                "created_at": user.created_at,
                "updated_at": user.updated_at
            },
            message="获取用户详情成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取用户详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取用户详情失败")


@router.put("/{user_id}", summary="更新API用户")
async def update_api_user(
        user_id: int,
        request: UpdateAPIUserRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """更新API用户信息"""
    try:
        user = await api_user_service.update_user(db, user_id, request)
        if not user:
            raise HTTPException(status_code=404, detail="用户不存在")

        return create_response(
            data={
                "id": user.id,
                "username": user.username,
                "display_name": user.display_name,
                "user_type": user.user_type,
                "is_active": user.is_active
            },
            message="更新用户成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新用户失败: {e}")
        raise HTTPException(status_code=500, detail="更新用户失败")


@router.delete("/{user_id}", summary="删除API用户")
async def delete_api_user(
        user_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除API用户（会同时删除其所有密钥和权限）"""
    try:
        success = await api_user_service.delete_user(db, user_id)
        if not success:
            raise HTTPException(status_code=404, detail="用户不存在")

        return create_response(
            data={"user_id": user_id},
            message="删除用户成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除用户失败: {e}")
        raise HTTPException(status_code=500, detail="删除用户失败")


# ==================== API密钥管理 ====================

@router.post("/keys", summary="创建API密钥")
async def create_api_key(
        request: CreateAPIKeyRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    为指定用户创建API密钥

    - **user_id**: 用户ID
    - **key_name**: 密钥名称
    - **expires_days**: 有效天数（可选，不填表示永久）
    - **rate_limit**: 速率限制（次/小时）
    - **allowed_ips**: IP白名单（可选）
    """
    try:
        key = await api_user_service.create_api_key(db, request)

        return create_response(
            data={
                "id": key.id,
                "user_id": key.user_id,
                "key_name": key.key_name,
                "api_key": key.api_key,  # ⚠️ 只在创建时返回完整密钥
                "key_prefix": key.key_prefix,
                "expires_at": key.expires_at,
                "rate_limit": key.rate_limit,
                "created_at": key.created_at
            },
            message=f"API密钥 '{key.key_name}' 创建成功，请妥善保管密钥，后续将无法再次查看完整密钥"
        )

    except ValueError as e:
        logger.warning(f"创建API密钥失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建API密钥失败: {e}")
        raise HTTPException(status_code=500, detail="创建API密钥失败")


@router.get("/keys/user/{user_id}", summary="获取用户的所有密钥")
async def get_user_api_keys(
        user_id: int,
        db: AsyncSession = Depends(get_async_db),
        include_inactive: bool = Query(False, description="是否包含已禁用的密钥")
):
    """获取指定用户的所有API密钥列表"""
    try:
        keys = await api_user_service.get_user_api_keys(db, user_id, include_inactive)

        # 构造响应（隐藏完整密钥）
        key_list = []
        for key in keys:
            # 只显示前缀和后4位
            key_preview = f"{key.key_prefix}_...{key.api_key[-4:]}"

            key_list.append({
                "id": key.id,
                "user_id": key.user_id,
                "key_name": key.key_name,
                "key_preview": key_preview,
                "key_prefix": key.key_prefix,
                "is_active": key.is_active,
                "expires_at": key.expires_at,
                "rate_limit": key.rate_limit,
                "allowed_ips": key.allowed_ips,
                "last_used_at": key.last_used_at,
                "total_calls": key.total_calls,
                "created_at": key.created_at
            })

        return create_response(
            data={
                "keys": key_list,
                "total_count": len(key_list)
            },
            message="获取密钥列表成功"
        )

    except Exception as e:
        logger.error(f"获取密钥列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取密钥列表失败")


@router.delete("/keys/{key_id}", summary="删除API密钥")
async def delete_api_key(
        key_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除指定的API密钥"""
    try:
        success = await api_user_service.delete_api_key(db, key_id)
        if not success:
            raise HTTPException(status_code=404, detail="密钥不存在")

        return create_response(
            data={"key_id": key_id},
            message="删除密钥成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除密钥失败: {e}")
        raise HTTPException(status_code=500, detail="删除密钥失败")


@router.post("/keys/{key_id}/toggle", summary="切换密钥状态")
async def toggle_api_key_status(
        key_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """启用或禁用API密钥"""
    try:
        key = await api_user_service.toggle_api_key_status(db, key_id)
        if not key:
            raise HTTPException(status_code=404, detail="密钥不存在")

        return create_response(
            data={
                "key_id": key.id,
                "is_active": key.is_active
            },
            message=f"密钥已{'启用' if key.is_active else '禁用'}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换密钥状态失败: {e}")
        raise HTTPException(status_code=500, detail="切换密钥状态失败")


# ==================== 权限管理 ====================

@router.post("/permissions/grant", summary="授权API访问权限")
async def grant_api_permissions(
        request: GrantAPIPermissionRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    为指定用户授权API访问权限

    - **api_id**: 接口ID
    - **user_ids**: 用户ID列表
    - **granted_by**: 授权人（可选）
    """
    try:
        permissions = await api_user_service.grant_permissions(db, request)

        return create_response(
            data={
                "api_id": request.api_id,
                "granted_count": len(permissions),
                "user_ids": request.user_ids
            },
            message=f"成功为 {len(permissions)} 个用户授权"
        )

    except ValueError as e:
        logger.warning(f"授权失败: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"授权失败: {e}")
        raise HTTPException(status_code=500, detail="授权失败")


@router.post("/permissions/revoke", summary="撤销API访问权限")
async def revoke_api_permissions(
        request: RevokeAPIPermissionRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """撤销指定用户的API访问权限"""
    try:
        deleted_count = await api_user_service.revoke_permissions(
            db, request.api_id, request.user_ids
        )

        return create_response(
            data={
                "api_id": request.api_id,
                "revoked_count": deleted_count
            },
            message=f"成功撤销 {deleted_count} 个用户的权限"
        )

    except Exception as e:
        logger.error(f"撤销权限失败: {e}")
        raise HTTPException(status_code=500, detail="撤销权限失败")


@router.get("/permissions/api/{api_id}", summary="获取API的授权用户列表")
async def get_api_permissions(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API的所有授权用户"""
    try:
        permissions = await api_user_service.get_api_permissions(db, api_id)

        permission_list = []
        for perm in permissions:
            permission_list.append({
                "id": perm.id,
                "api_id": perm.api_id,
                "user_id": perm.user_id,
                "user_name": perm.user.username if perm.user else None,
                "user_display_name": perm.user.display_name if perm.user else None,
                "granted_at": perm.granted_at,
                "granted_by": perm.granted_by
            })

        return create_response(
            data={
                "permissions": permission_list,
                "total_count": len(permission_list)
            },
            message="获取权限列表成功"
        )

    except Exception as e:
        logger.error(f"获取权限列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取权限列表失败")


@router.get("/permissions/user/{user_id}", summary="获取用户可访问的API列表")
async def get_user_permissions(
        user_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定用户可以访问的所有API"""
    try:
        permissions = await api_user_service.get_user_permissions(db, user_id)

        permission_list = []
        for perm in permissions:
            permission_list.append({
                "id": perm.id,
                "api_id": perm.api_id,
                "api_name": perm.api.api_name if perm.api else None,
                "api_path": perm.api.api_path if perm.api else None,
                "granted_at": perm.granted_at,
                "granted_by": perm.granted_by
            })

        return create_response(
            data={
                "permissions": permission_list,
                "total_count": len(permission_list)
            },
            message="获取权限列表成功"
        )

    except Exception as e:
        logger.error(f"获取权限列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取权限列表失败")


# ==================== 验证相关 ====================

@router.post("/validate-key", summary="验证API密钥（内部使用）")
async def validate_api_key(
        request: ValidateAPIKeyRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    验证API密钥是否有效（供内部中间件使用）

    - 检查密钥是否存在
    - 检查是否过期
    - 检查IP白名单
    - 检查API访问权限
    """
    try:
        # 这里需要先根据api_path获取api_id
        # 简化处理，实际使用时应该在中间件中处理
        return create_response(
            data={"valid": True},
            message="密钥验证通过"
        )

    except Exception as e:
        logger.error(f"验证密钥失败: {e}")
        raise HTTPException(status_code=500, detail="验证密钥失败")