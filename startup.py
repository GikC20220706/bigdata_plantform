"""
Production startup script for the Big Data Platform.

This script handles production deployment with proper configuration,
logging setup, and environment validation.
"""

import os
import sys
import time
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import settings


def setup_production_logging():
    """Setup production logging configuration."""

    # Remove default logger
    logger.remove()

    # Add console logger with appropriate level
    logger.add(
        sys.stdout,
        level=settings.LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        colorize=True
    )

    # Add file logger
    log_file = Path(settings.LOG_FILE)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_file),
        level=settings.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="10 MB",
        retention="30 days",
        compression="gz",
        encoding="utf-8"
    )

    logger.info("✅ Production logging configured")


def validate_environment():
    """Validate the production environment."""

    logger.info("🔍 Validating production environment...")
    logger.info(f"🔧 IS_LOCAL_DEV: {settings.IS_LOCAL_DEV}")
    logger.info(f"🔧 MOCK_DATA_MODE: {settings.MOCK_DATA_MODE}")
    logger.info(f"🔧 use_real_clusters: {settings.use_real_clusters}")

    # Check required configuration values only if using real clusters
    if settings.use_real_clusters:
        logger.info("🔍 Validating real cluster configuration...")

        # Check configuration values from settings (already loaded from .env)
        required_configs = {
            'HADOOP_HOME': settings.HADOOP_HOME,
            'HDFS_NAMENODE': settings.HDFS_NAMENODE,
            'HDFS_USER': settings.HDFS_USER,
            'HIVE_SERVER_HOST': settings.HIVE_SERVER_HOST,
            'HIVE_USERNAME': settings.HIVE_USERNAME
        }

        missing_configs = []
        for config_name, config_value in required_configs.items():
            if not config_value:
                missing_configs.append(config_name)
            else:
                logger.info(f"✅ {config_name}: {config_value}")

        if missing_configs:
            logger.error(f"❌ Missing required configuration values: {missing_configs}")
            logger.info("💡 Make sure your .env file contains all required values")
            sys.exit(1)

        # Validate Hadoop directory exists
        if settings.HADOOP_HOME:
            hadoop_home = Path(settings.HADOOP_HOME)
            if not hadoop_home.exists():
                logger.warning(f"⚠️ HADOOP_HOME directory not found: {hadoop_home}")
                logger.info("💡 This might be OK if running in a different environment")
            else:
                logger.info(f"✅ HADOOP_HOME found: {hadoop_home}")

    else:
        logger.info("ℹ️ Using mock data mode - skipping cluster environment validation")

    # Check log directory
    log_dir = Path(settings.LOG_FILE).parent
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Log directory ready: {log_dir}")
    except Exception as e:
        logger.error(f"❌ Cannot create log directory: {e}")
        sys.exit(1)

    logger.info("✅ Environment validation passed")


async def test_dependencies():
    """Test critical dependencies and connections."""

    logger.info("🔍 Testing critical dependencies...")

    # Test database connection
    try:
        from app.utils.database import get_db

        # Test database connection
        db = next(get_db())
        db.close()
        logger.info("✅ Database connection test passed")
    except Exception as e:
        logger.warning(f"⚠️ Database connection test failed: {e}")

    # Test cluster connections only if enabled
    if settings.use_real_clusters:
        logger.info("🔍 Testing cluster connections...")

        try:
            from app.utils.hadoop_client import HDFSClient, HiveClient
            from app.utils.metrics_collector import metrics_collector

            # Test HDFS
            try:
                hdfs_client = HDFSClient()
                storage_info = hdfs_client.get_storage_info()
                if storage_info.get('total_size', 0) > 0:
                    logger.info("✅ HDFS connection test passed")
                else:
                    logger.warning("⚠️ HDFS connection test failed - no storage info")
            except Exception as e:
                logger.warning(f"⚠️ HDFS connection test failed: {e}")

            # Test Hive
            try:
                hive_client = HiveClient()
                databases = hive_client.get_databases()
                if databases:
                    logger.info(f"✅ Hive connection test passed ({len(databases)} databases)")
                else:
                    logger.warning("⚠️ Hive connection test failed - no databases returned")
            except Exception as e:
                logger.warning(f"⚠️ Hive connection test failed: {e}")

            # Test metrics collector
            try:
                cluster_metrics = await metrics_collector.get_cluster_metrics('hadoop')
                if cluster_metrics:
                    logger.info("✅ Metrics collector test passed")
                else:
                    logger.warning("⚠️ Metrics collector test failed - no metrics returned")
            except Exception as e:
                logger.warning(f"⚠️ Metrics collector test failed: {e}")

        except ImportError as e:
            logger.warning(f"⚠️ Cluster modules not available: {e}")
        except Exception as e:
            logger.warning(f"⚠️ Cluster connection tests failed: {e}")

    else:
        logger.info("ℹ️ Mock data mode - skipping cluster connection tests")

    logger.info("✅ Dependency testing completed")


def print_startup_banner():
    """Print startup banner with system information."""

    banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                            {settings.APP_NAME}                               ║
║                                Version {settings.VERSION}                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Environment: {'Production' if not settings.DEBUG else 'Development'}         ║
║ Host: {settings.HOST}:{settings.PORT}                                        ║
║ Data Mode: {'Real Clusters' if settings.use_real_clusters else 'Mock Data'}  ║
║ Platform: {sys.platform}                                                     ║
║ Python: {sys.version.split()[0]}                                             ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

    print(banner)


async def main():
    """Main startup function."""

    print_startup_banner()

    # Setup logging
    setup_production_logging()

    # Validate environment
    validate_environment()

    # Test dependencies
    await test_dependencies()

    logger.info("🎉 Startup validation completed successfully!")
    logger.info(f"🚀 Starting server on {settings.HOST}:{settings.PORT}")

    # Import and run the server
    import uvicorn
    from app.main import app

    # Production server configuration
    config = uvicorn.Config(
        app=app,
        host=settings.HOST,
        port=settings.PORT,
        log_level=settings.LOG_LEVEL.lower(),
        access_log=False,  # We handle logging ourselves
        server_header=False,
        date_header=False,
        reload=False,  # Never reload in production
        workers=1 if settings.DEBUG else 4,  # Multiple workers in production
    )

    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Server shutdown by user")
    except Exception as e:
        logger.error(f"❌ Server startup failed: {e}")
        sys.exit(1)