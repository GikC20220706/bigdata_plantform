"""
更新主API路由以包含数据集成模块
"""

# 更新 app/api/v1/__init__.py 文件：

"""
API v1 package for the Big Data Platform.

This package contains all API endpoint modules organized by functional area.
"""

from fastapi import APIRouter

# Import all routers
from .optimized_overview import router as overview_router
from .cluster import router as cluster_router
from .development import router as development_router
from .governance import router as governance_router
from .sync import router as sync_router
from .integration import router as integration_router  # 新增数据集成路由

# Create main API router
api_router = APIRouter(prefix="/api/v1")

# Include all sub-routers with proper configuration
api_router.include_router(
    overview_router,
    prefix="/overview",
    tags=["overview"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    cluster_router,
    prefix="/cluster",
    tags=["cluster"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    development_router,
    prefix="/development",
    tags=["development"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    governance_router,
    prefix="/governance",
    tags=["governance"],
    responses={404: {"description": "Not found"}}
)


api_router.include_router(
    sync_router,
    prefix="/sync",
    tags=["sync"],
    responses={404: {"description": "Not found"}}
)

# 新增数据集成路由
api_router.include_router(
    integration_router,
    prefix="/integration",
    tags=["data-integration"],
    responses={404: {"description": "Not found"}}
)

__all__ = ["api_router"]