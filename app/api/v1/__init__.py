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
#from .integration import router as integration_router  # 新增数据集成路由
from .optimized_integration import router as optimized_integration_router
from .business_system import router as business_system_router #业务系统接入路由
from .smart_sync import router as smart_sync_router
from .scheduler import router as scheduler_router
from .executor import router as executor_router
from .monitoring import router as monitoring_router
from .workflow import router as workflow_router
from .custom_api import router as custom_api_router
from .api_docs import router as api_docs_router

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


# api_router.include_router(
#     sync_router,
#     prefix="/sync",
#     tags=["sync"],
#     responses={404: {"description": "Not found"}}
# )

# 新增数据集成路由
# api_router.include_router(
#     integration_router,
#     prefix="/integration",
#     tags=["data-integration"],
#     responses={404: {"description": "Not found"}}
# )

api_router.include_router(
    optimized_integration_router,
    prefix="/integration",
    tags=["data-integration-optimized"],
    responses={404: {"description": "Not found"}}
)
api_router.include_router(
    business_system_router,
    prefix="/business-systems",
    tags=["business-systems"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    smart_sync_router,
    prefix="/smart-sync",
    tags=["smart-sync"],
    responses={404: {"description": "Not found"}}
)

# 新增调度管理路由
api_router.include_router(
    scheduler_router,
    prefix="/scheduler",
    tags=["scheduler", "airflow", "dag-management"],
    responses={404: {"description": "Not found"}}
)

# 新增执行器管理路由
api_router.include_router(
    executor_router,
    prefix="/executor",
    tags=["executor", "task-execution", "sql", "shell", "datax"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    monitoring_router,
    prefix="/monitoring",
    tags=["monitoring", "alerts", "performance"],
    responses={404: {"description": "Not found"}}
)

# 在路由注册部分添加
api_router.include_router(
    workflow_router,
    prefix="/workflows",
    tags=["workflows"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    custom_api_router,
    prefix="/custom-api",
    tags=["custom-api", "api-generator", "sql-api"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    api_docs_router,
    prefix="/api-docs",
    tags=["api-docs", "documentation", "openapi"],
    responses={404: {"description": "Not found"}}
)

__all__ = ["api_router"]