"""
自定义API认证中间件
用于验证API Key并控制访问权限
app/middleware/api_auth_middleware.py
"""

from typing import Optional, Callable
from fastapi import Request, Response, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
import time

from app.utils.database import get_async_db, get_async_db_context
from app.services.api_user_service import api_user_service
from app.services.custom_api_service import custom_api_service


class APIAuthMiddleware(BaseHTTPMiddleware):
    """
    API认证中间件

    功能：
    1. 拦截所有 /api/custom/* 路径的请求
    2. 提取并验证 API Key
    3. 检查API访问权限
    4. 记录认证信息到访问日志
    """

    async def dispatch(self, request: Request, call_next):
        """中间件主逻辑"""

        # 检查是否是自定义API路径
        if not request.url.path.startswith("/api/custom/"):
            return await call_next(request)

        logger.info(f"拦截自定义API请求: {request.method} {request.url.path}")

        # 🔧 改用 async with 方式获取数据库会话
        try:
            async with get_async_db_context() as db:
                # 根据路径获取API配置
                api_path = request.url.path
                api = await self._get_api_by_path(db, api_path)

                if not api:
                    return JSONResponse(
                        status_code=404,
                        content={
                            "code": 404,
                            "message": f"API not found: {api_path}",
                            "data": None
                        }
                    )

                # 检查API是否启用
                if not api.is_active:
                    return JSONResponse(
                        status_code=403,
                        content={
                            "code": 403,
                            "message": "API is disabled",
                            "data": None
                        }
                    )

                # 进行认证
                auth_result = await self._authenticate_request(db, request, api)

                if not auth_result["valid"]:
                    return JSONResponse(
                        status_code=401,
                        content={
                            "code": 401,
                            "message": auth_result.get("error", "Authentication failed"),
                            "data": None
                        }
                    )

                # 将认证信息存入request.state，供后续使用
                request.state.auth_info = auth_result
                request.state.api_id = api.id

                # 调用下一个中间件
                response = await call_next(request)

                return response

        except Exception as e:
            logger.error(f"认证中间件处理异常: {e}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "code": 500,
                    "message": "Internal server error",
                    "data": None
                }
            )

    async def _get_api_by_path(self, db: AsyncSession, api_path: str):
        """根据路径查找API配置"""
        try:
            from sqlalchemy import select
            from app.models.custom_api import CustomAPI

            query = select(CustomAPI).where(CustomAPI.api_path == api_path)
            result = await db.execute(query)
            return result.scalar_one_or_none()

        except Exception as e:
            logger.error(f"查找API配置失败: {e}")
            return None

    async def _authenticate_request(
            self,
            db: AsyncSession,
            request: Request,
            api
    ) -> dict:
        """认证请求"""

        # 1. 如果是公开API，直接通过
        if api.access_level == "public":
            logger.info(f"公开API，无需认证: {api.api_path}")
            return {
                "valid": True,
                "auth_type": "public"
            }

        # 2. 需要认证或限定用户，提取API Key
        api_key = self._extract_api_key(request)

        if not api_key:
            return {
                "valid": False,
                "error": "Missing API Key. Please provide API Key in 'X-API-Key' header or 'api_key' query parameter"
            }

        # 3. 获取客户端IP
        client_ip = self._get_client_ip(request)

        # 4. 验证API Key
        valid, error_message, key_record = await api_user_service.validate_api_key(
            db=db,
            api_key=api_key,
            api_id=api.id,
            client_ip=client_ip
        )

        if not valid:
            return {
                "valid": False,
                "error": error_message or "Invalid API Key"
            }

        # 🔧 5. 如果是限定用户模式，检查用户是否有权限
        if api.access_level == "restricted":
            has_permission = await self._check_user_permission(db, api.id, key_record.user_id)

            if not has_permission:
                logger.warning(f"用户无权限访问: API={api.api_path}, User={key_record.user_id}")
                return {
                    "valid": False,
                    "error": "Access denied. You don't have permission to access this API"
                }

        # 6. 认证成功
        logger.info(f"认证成功: API={api.api_path}, User={key_record.user_id}, Key={key_record.id}")

        return {
            "valid": True,
            "auth_type": "api_key",
            "api_key_id": key_record.id,
            "user_id": key_record.user_id
        }

    async def _check_user_permission(self, db: AsyncSession, api_id: int, user_id: int) -> bool:
        """
        检查用户是否有权限访问API

        Args:
            db: 数据库会话
            api_id: API ID
            user_id: 用户ID

        Returns:
            bool: 是否有权限
        """
        try:
            from sqlalchemy import select, and_
            from app.models.api_user import APIUserPermission

            # 查询权限表
            query = select(APIUserPermission).where(
                and_(
                    APIUserPermission.api_id == api_id,
                    APIUserPermission.user_id == user_id
                )
            )

            result = await db.execute(query)
            permission = result.scalar_one_or_none()

            return permission is not None

        except Exception as e:
            logger.error(f"检查用户权限失败: {e}")
            return False

    def _extract_api_key(self, request: Request) -> Optional[str]:
        """
        提取API Key

        支持三种方式：
        1. Header: X-API-Key
        2. Header: Authorization: Bearer <key>
        3. Query Parameter: api_key
        """

        # 方式1: X-API-Key header
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return api_key.strip()

        # 方式2: Authorization Bearer
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            return auth_header[7:].strip()

        # 方式3: Query parameter
        api_key = request.query_params.get("api_key")
        if api_key:
            return api_key.strip()

        return None

    def _get_client_ip(self, request: Request) -> str:
        """获取客户端IP"""
        # 优先从 X-Forwarded-For 获取（如果有反向代理）
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        # 从 X-Real-IP 获取
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip.strip()

        # 直接从请求获取
        if request.client:
            return request.client.host

        return "unknown"