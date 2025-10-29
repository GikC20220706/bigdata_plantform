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
from .user_cluster import router as user_cluster_router
from .resource_files import router as resource_files_router
from .data_catalog import router as data_catalog_router
from .data_asset import router as data_asset_router
from .field_standard import router as field_standard_router
from app.api.v1.indicator_system import router as indicator_system_router
from app.api.v1 import api_user
from .job_workflow import router as job_workflow_router
from .job_work import router as job_work_router
from .job_instance import router as job_instance_router


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
    user_cluster_router,
    prefix="/user-cluster",
    tags=["计算集群"],
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
    tags=["调度管理"],
    responses={404: {"description": "Not found"}}
)

# 新增执行器管理路由
api_router.include_router(
    executor_router,
    prefix="/executor",
    tags=["执行期管理"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    monitoring_router,
    prefix="/monitoring",
    tags=["监控告警"],
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
    tags=["自定义API"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    api_docs_router,
    prefix="/api-docs",
    tags=["API文档生成"],
    responses={404: {"description": "Not found"}}
)

api_router.include_router(
    resource_files_router
)

# 🆕 注册数据资源目录路由
api_router.include_router(
    data_catalog_router,
    tags=["数据资源目录"]
)

# 🆕 注册数据资产路由
api_router.include_router(
    data_asset_router,
    tags=["数据资产"]
)

api_router.include_router(
    field_standard_router,
    tags=["字段标准"]
)

# 🆕 注册指标体系建设路由
api_router.include_router(
    indicator_system_router,
    tags=["指标体系建设"]
)

api_router.include_router(
    job_workflow_router,
    tags=["数据开发-作业流"],
    responses={404: {"description": "Not found"}}
)

# 注册作业路由
api_router.include_router(
    job_work_router,
    tags=["数据开发-作业"],
    responses={404: {"description": "Not found"}}
)

# 注册实例查询路由
api_router.include_router(
    job_instance_router,
    tags=["数据开发-作业实例"],
    responses={404: {"description": "Not found"}}
)
api_router.include_router(api_user.router)
__all__ = ["api_router"]