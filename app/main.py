# =============================================================================
# app/main.py - Main FastAPI Application
# =============================================================================
"""
Main FastAPI application for the Big Data Platform.

This is the core application module that sets up FastAPI with all necessary
middleware, routers, and configuration for production deployment.
"""
import asyncio
import os
import sys
import time
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Any

import app
from app.api.v1 import api_router
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger
import uvicorn
from app.services.monitoring_integration import (
    monitoring_startup_event,
    monitoring_shutdown_event,
    monitoring_integration,
    validate_monitoring_config
)

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from app.api.v1 import api_router
from app.utils.response import create_error_response
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Handles startup and shutdown events for the FastAPI application.
    """
    # Startup
    logger.info("Starting Big Data Platform API...")
    logger.info(f"Environment: {'Development' if settings.DEBUG else 'Production'}")
    logger.info(f"Data Mode: {'Mock Data' if settings.MOCK_DATA_MODE else 'Real Data'}")
    logger.info(f"Cluster Integration: {'Enabled' if settings.use_real_clusters else 'Disabled'}")

    # Initialize database if needed
    try:
        from app.utils.database import create_tables_sync, test_connection
        try:
            # 使用同步版本
            if test_connection():
                create_tables_sync()
                logger.info("MySQL database initialized")
            else:
                logger.warning(" Database connection test failed")
        except Exception as e:
            logger.warning(f"Database initialization failed: {e}")
            create_tables_sync()
            logger.info("MySQL database initialized")
        else:
            logger.warning("Database connection test failed")
    except Exception as e:
        logger.warning(f"Database initialization failed: {e}")

        # 初始化数据集成缓存
    try:
        from app.utils.integration_cache import integration_cache
        logger.info("Integration cache manager initialized")
        logger.info(f"Cache levels: Memory(60s) → Redis(5m) → DB(1h)")
    except Exception as e:
        logger.warning(f"Integration cache initialization failed: {e}")

    # 🆕 验证监控配置
    try:
        config_validation = validate_monitoring_config()
        if config_validation["valid"]:
            logger.info("✅ 监控配置验证通过")
        else:
            logger.warning(f"⚠️ 监控配置验证有问题: {config_validation['errors']}")
    except Exception as e:
        logger.warning(f"监控配置验证失败: {e}")

    # 🆕 初始化监控告警系统
    try:
        await monitoring_startup_event()
        logger.info("✅ 监控告警系统初始化完成")
    except Exception as e:
        logger.error(f"❌ 监控告警系统初始化失败: {e}")

        # 初始化优化的数据集成服务
    try:
        from app.services.optimized_data_integration_service import get_optimized_data_integration_service
        logger.info("Optimized data integration service initialized")

        # 预热关键缓存（后台执行）
        import asyncio
        asyncio.create_task(warm_critical_cache())

    except Exception as e:
        logger.warning(f"Data integration service initialization failed: {e}")

    # Initialize metrics collector
    try:
        from app.utils.metrics_collector import metrics_collector
        logger.info("Metrics collector initialized")
        logger.info(f"Cache TTL: {metrics_collector.cache_ttl} seconds")
    except Exception as e:
        logger.warning(f"Metrics collector initialization failed: {e}")

    # Test cluster connections if enabled
    if settings.use_real_clusters:
        try:
            from app.utils.hadoop_client import HDFSClient, HiveClient

            # Test HDFS connection
            hdfs_client = HDFSClient()
            storage_info = hdfs_client.get_storage_info()
            if storage_info.get('total_size', 0) > 0:
                logger.info("HDFS connection successful")
            else:
                logger.warning("HDFS connection test failed")

            # Test Hive connection
            hive_client = HiveClient()
            databases = hive_client.get_databases()
            if databases:
                logger.info(f"Hive connection successful ({len(databases)} databases)")
            else:
                logger.warning("Hive connection test failed")

        except Exception as e:
            logger.warning(f"Cluster connection test failed: {e}")

    logger.info("Application startup completed successfully!")

    yield

    # Shutdown
    logger.info("Shutting down Big Data Platform API...")

    # Clear metrics cache
    try:
        await monitoring_shutdown_event()
        logger.info("✅ 监控告警系统已关闭")
    except Exception as e:
        logger.error(f"❌ 监控告警系统关闭异常: {e}")
    try:
        from app.utils.metrics_collector import metrics_collector
        metrics_collector.clear_cache()
        logger.info("Metrics cache cleared")
    except Exception as e:
        logger.warning(f"Failed to clear metrics cache: {e}")

    logger.info("Application shutdown completed")



async def warm_critical_cache():
    """预热关键缓存"""
    try:
        await asyncio.sleep(5)  # 等待服务完全启动

        from app.services.optimized_data_integration_service import get_optimized_data_integration_service

        # 预热概览数据
        service = get_optimized_data_integration_service()
        await service.get_data_sources_overview()
        logger.info("Integration overview cache warmed")

        # 预热数据源列表
        #await optimized_data_integration_service.get_data_sources_list()
        #logger.info("Data sources list cache warmed")

    except Exception as e:
        logger.warning(f"Cache warming failed: {e}")


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        FastAPI: Configured FastAPI application instance
    """

    # Create FastAPI app with lifespan manager
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.VERSION,
        description="大数据平台API - 提供集群监控、数据治理、任务管理等功能",
        docs_url=None,  # 禁用默认docs
        redoc_url=None,  # 禁用默认redoc
        openapi_url="/openapi.json",
        lifespan=lifespan
    )

    # Add middleware
    setup_middleware(app)

    # Include routers
    setup_routers(app)

    # Setup static files and templates
    setup_static_files(app)

    # Setup exception handlers
    setup_exception_handlers(app)

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema

        from fastapi.openapi.utils import get_openapi
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        # 强制修改版本号
        openapi_schema["openapi"] = "3.0.3"
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi

    return app


def setup_middleware(app: FastAPI) -> None:
    """Setup middleware for the FastAPI application."""

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.DEBUG else [
            "http://localhost:3000",
            "http://localhost:8080",
            "https://yourdomain.com"
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Gzip compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Add request logging middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        """Log all HTTP requests."""
        start_time = time.time()

        # Skip logging for health checks and static files
        if request.url.path in ["/health", "/favicon.ico"] or request.url.path.startswith("/static"):
            response = await call_next(request)
            return response

        logger.info(f"{request.method} {request.url.path} - Client: {request.client.host}")

        try:
            response = await call_next(request)
            process_time = time.time() - start_time

            # Log slow requests
            if process_time > 1.0:
                logger.warning(f"Slow request: {request.url.path} took {process_time * 1000:.1f}ms")

                # 🆕 如果监控系统已初始化，触发性能告警
                try:
                    if monitoring_integration.initialized and process_time > 5.0:
                        from app.services.monitoring_service import monitoring_service, AlertType, AlertLevel
                        await monitoring_service._trigger_alert(
                            AlertType.PERFORMANCE_DEGRADATION,
                            AlertLevel.MEDIUM,
                            f"API响应缓慢: {request.url.path}",
                            f"路径: {request.method} {request.url.path}\n响应时间: {process_time:.2f}秒",
                            {
                                "method": request.method,
                                "path": str(request.url.path),
                                "response_time": process_time,
                                "client_ip": request.client.host if request.client else "unknown"
                            }
                        )
                except Exception as monitoring_error:
                    logger.debug(f"监控告警触发失败: {monitoring_error}")

            logger.info(
                f"{request.method} {request.url.path} - "
                f"Status: {response.status_code} - "
                f"Time: {process_time:.3f}s"
            )

            # Add response time header
            response.headers["X-Process-Time"] = str(process_time)
            if "/api/v1/overview" in request.url.path:
                response.headers["X-Cache-Version"] = "enhanced_v2"

            return response

        except Exception as e:
            process_time = time.time() - start_time
            logger.error(
                f"{request.method} {request.url.path} - "
                f"Error: {str(e)} - "
                f"Time: {process_time:.3f}s"
            )

            # 🆕 记录API错误到监控系统
            try:
                if monitoring_integration.initialized:
                    from app.services.monitoring_service import monitoring_service, AlertType, AlertLevel
                    await monitoring_service._trigger_alert(
                        AlertType.SYSTEM_ERROR,
                        AlertLevel.HIGH,
                        f"API错误: {request.url.path}",
                        f"路径: {request.method} {request.url.path}\n错误: {str(e)}",
                        {
                            "method": request.method,
                            "path": str(request.url.path),
                            "error_message": str(e),
                            "process_time": process_time,
                            "client_ip": request.client.host if request.client else "unknown"
                        }
                    )
            except Exception as monitoring_error:
                logger.debug(f"监控告警触发失败: {monitoring_error}")

            raise


def setup_routers(app: FastAPI) -> None:
    """Setup API routers."""

    # Include main API router
    app.include_router(api_router)

    # 🆕 监控告警路由
    try:
        from app.api.v1.monitoring import router as monitoring_router
        app.include_router(
            monitoring_router,
            prefix="/api/v1/monitoring",
            tags=["monitoring", "alerts", "performance"]
        )
        logger.info("监控告警路由已加载")
    except ImportError as e:
        logger.warning(f"监控告警路由加载失败: {e}")

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui_html():
        return get_swagger_ui_html(
            openapi_url="/openapi.json",
            title="Big Data Platform API Documentation",
            swagger_js_url="/static/swagger-ui/swagger-ui-bundle.js",
            swagger_css_url="/static/swagger-ui/swagger-ui.css"
        )

    # Health check endpoint
    @app.get("/health", tags=["health"])
    async def health_check():
        """Enhanced health check including monitoring system status."""
        try:
            health_status = {
                "status": "healthy",
                "timestamp": datetime.now(),
                "version": settings.VERSION,
                "environment": "development" if settings.DEBUG else "production",
                "cluster_mode": "real" if settings.use_real_clusters else "mock",
                "components": {
                    "api": "healthy",
                    "monitoring": "unknown"
                }
            }

            #检查监控系统健康状态
            try:
                if monitoring_integration.initialized:
                    monitoring_health = await monitoring_integration.monitoring_service.check_system_health()
                    health_status["components"]["monitoring"] = monitoring_health.get("overall_status", "unknown")
                    health_status["components"]["monitoring_details"] = monitoring_health.get("components", {})

                    # 获取监控统计
                    monitoring_stats = monitoring_integration.monitoring_service.get_monitoring_stats()
                    health_status["monitoring_stats"] = monitoring_stats
                else:
                    health_status["components"]["monitoring"] = "not_initialized"
            except Exception as e:
                health_status["components"]["monitoring"] = f"error: {str(e)}"
                logger.warning(f"监控健康检查失败: {e}")

            # 如果监控系统不健康，整体状态也标记为警告
            if health_status["components"]["monitoring"] not in ["healthy", "not_initialized"]:
                health_status["status"] = "warning"

            return health_status

        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now()
            }

    # 🆕 监控系统专用状态端点
    @app.get("/monitoring/status", tags=["monitoring"])
    async def monitoring_system_status():
        """Monitoring system dedicated status endpoint."""
        try:
            if not monitoring_integration.initialized:
                return {
                    "initialized": False,
                    "message": "监控系统未初始化"
                }

            # 获取集成状态
            integration_status = monitoring_integration.get_integration_status()

            # 获取监控统计
            monitoring_stats = monitoring_integration.monitoring_service.get_monitoring_stats()

            # 获取配置验证状态
            config_validation = validate_monitoring_config()

            return {
                "initialized": True,
                "integration_status": integration_status,
                "monitoring_stats": monitoring_stats,
                "config_validation": config_validation,
                "active_alerts_count": len(monitoring_integration.monitoring_service.active_alerts),
                "alert_rules_count": len(monitoring_integration.monitoring_service.alert_rules),
                "timestamp": datetime.now()
            }

        except Exception as e:
            logger.error(f"获取监控状态失败: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now()
            }
    # Root endpoint
    @app.get("/", tags=["frontend"])
    async def root(request: Request):
        """Serve the main dashboard page at root."""
        from fastapi.templating import Jinja2Templates
        import os
        templates_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
        templates = Jinja2Templates(directory=templates_dir)
        return templates.TemplateResponse("index.html", {"request": request})


def setup_static_files(app: FastAPI) -> None:
    """Setup static file serving and templates."""

    # Static files
    static_dir = Path(__file__).parent.parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
        logger.info(f"Static files mounted from {static_dir}")

    # Templates
    templates_dir = Path(__file__).parent.parent / "templates"
    if templates_dir.exists():
        templates = Jinja2Templates(directory=str(templates_dir))

        @app.get("/dashboard", tags=["frontend"])
        async def dashboard(request: Request):
            """Serve the main dashboard page."""
            return templates.TemplateResponse("index.html", {"request": request})

        logger.info(f"Templates loaded from {templates_dir}")


def setup_exception_handlers(app: FastAPI) -> None:
    """Setup global exception handlers."""

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """Handle HTTP exceptions."""
        return JSONResponse(
            status_code=exc.status_code,
            content=create_error_response(
                message=exc.detail,
                code=exc.status_code
            )
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """Handle general exceptions with monitoring integration."""
        logger.error(f"Unhandled exception: {exc}")

        # 🆕 触发系统异常告警
        try:
            if monitoring_integration.initialized:
                from app.services.monitoring_service import monitoring_service, AlertType, AlertLevel
                await monitoring_service._trigger_alert(
                    AlertType.SYSTEM_ERROR,
                    AlertLevel.CRITICAL,
                    "系统异常",
                    f"未捕获的系统异常\n路径: {request.method} {request.url.path}\n异常: {str(exc)}",
                    {
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                        "request_path": str(request.url.path),
                        "request_method": request.method
                    }
                )
        except Exception as monitoring_error:
            logger.debug(f"监控告警触发失败: {monitoring_error}")

        return JSONResponse(
            status_code=500,
            content=create_error_response(
                message="内部服务器错误" if not settings.DEBUG else str(exc),
                code=500
            )
        )

# 在 app/main.py 的 setup_middleware 函数中添加性能监控中间件

# Create the FastAPI app instance
app = create_app()

# Development server runner
if __name__ == "__main__":
    logger.info("🚀 Starting development server...")

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG,
        reload_dirs=["app", "config"] if settings.DEBUG else None,
        log_level="info" if settings.DEBUG else "warning",
        access_log=settings.DEBUG,
        server_header=False,
        date_header=False
    )