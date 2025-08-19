"""
Cross-platform production startup script for the Big Data Platform.

This script handles production deployment with proper configuration,
logging setup, and environment validation across Windows and Linux.
"""

import os
import sys
import time
import asyncio
import platform
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from loguru import logger
from config.settings import settings


def check_datax_installation():
    """检查DataX是否正确安装"""
    import os
    import subprocess

    datax_home = os.environ.get('DATAX_HOME', '/opt/datax')
    datax_script = os.path.join(datax_home, 'bin', 'datax.py')

    logger.info("Checking DataX installation...")

    if not os.path.exists(datax_script):
        logger.warning(f"DataX not found at {datax_script}")
        logger.info("Please ensure DataX is properly installed in ./datax directory")
        return False

    # 检查DataX脚本权限
    if not os.access(datax_script, os.X_OK):
        logger.warning(f"DataX script not executable: {datax_script}")
        try:
            os.chmod(datax_script, 0o755)
            logger.info("Fixed DataX script permissions")
        except Exception as e:
            logger.error(f"Failed to fix DataX permissions: {e}")
            return False

    datax_jvm_args = [
        "-Xss2m",
        "-Dfastjson2.parser.safeMode=true",
        "-Dfastjson.parser.autoTypeSupport=false",
        "-XX:+UseG1GC"
    ]

    os.environ['DATAX_JVM_ARGS'] = " ".join(datax_jvm_args)
    logger.info(f"设置 DataX JVM 参数: {os.environ['DATAX_JVM_ARGS']}")

    # 检查Java环境
    try:
        result = subprocess.run(['java', '-version'],
                                capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            java_version = result.stderr.split('\n')[0] if result.stderr else "Unknown"
            logger.info(f"ava version check passed: {java_version}")
        else:
            logger.error("Java version check failed")
            return False
    except Exception as e:
        logger.error(f"Java check failed: {e}")
        return False

    logger.info("DataX installation check passed")
    return True
def setup_production_logging():
    """Setup cross-platform production logging configuration."""

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

    # Add file logger with cross-platform path
    log_file = Path(settings.get_resolved_log_file())
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

    logger.info("Cross-platform production logging configured")
    logger.info(f"Log file: {log_file}")


def validate_environment():
    """Validate the production environment across platforms."""

    logger.info("Validating production environment...")
    logger.info(f"Platform: {platform.system()} {platform.release()}")
    logger.info(f"Python: {sys.version}")
    logger.info(f"IS_LOCAL_DEV: {settings.IS_LOCAL_DEV}")
    logger.info(f"MOCK_DATA_MODE: {settings.MOCK_DATA_MODE}")
    logger.info(f"use_real_clusters: {settings.use_real_clusters}")

    # Check if running in Docker
    in_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
    if in_docker:
        logger.info("Running in Docker container")

    # Check required configuration values only if using real clusters
    if settings.use_real_clusters:
        logger.info("Validating real cluster configuration...")

        # Check configuration values from settings (already loaded from .env)
        required_configs = {
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
                logger.info(f"{config_name}: {config_value}")

        if missing_configs:
            logger.error(f"Missing required configuration values: {missing_configs}")
            logger.info("Make sure your .env file contains all required values")
            sys.exit(1)

        # Validate Hadoop directory exists (if specified and not in Docker)
        if settings.HADOOP_HOME and not in_docker:
            hadoop_home = Path(settings.HADOOP_HOME)
            if not hadoop_home.exists():
                logger.warning(f"HADOOP_HOME directory not found: {hadoop_home}")
                logger.info("This might be OK if running in a different environment")
            else:
                logger.info(f"DOOP_HOME found: {hadoop_home}")

        # Validate SSH key for cluster access
        ssh_key_path = settings.get_resolved_ssh_key_path()
        if ssh_key_path and os.path.exists(ssh_key_path):
            logger.info(f"SSH key found: {ssh_key_path}")

            # Check permissions on Unix-like systems
            if platform.system() != "Windows":
                import stat
                file_stat = os.stat(ssh_key_path)
                if stat.filemode(file_stat.st_mode) != '-rw-------':
                    logger.warning(f"SSH key permissions may be too open: {ssh_key_path}")
                    logger.info("Consider running: chmod 600 {ssh_key_path}")
        else:
            logger.warning(f"SSH key not found: {ssh_key_path}")
            logger.info("Cluster monitoring may not work without SSH access")

        # Validate nodes configuration
        nodes_config = settings.cluster_nodes_config
        nodes_count = len(nodes_config.get('nodes', []))
        if nodes_count > 0:
            logger.info(f"Cluster nodes configured: {nodes_count}")

            # Show summary by cluster type
            cluster_summary = {}
            for node in nodes_config.get('nodes', []):
                for cluster_type in node.get('cluster_types', ['unknown']):
                    cluster_summary[cluster_type] = cluster_summary.get(cluster_type, 0) + 1

            for cluster_type, count in cluster_summary.items():
                logger.info(f" - {cluster_type}: {count} nodes")
        else:
            logger.warning("No cluster nodes configured")
            logger.info("Add node configuration to config/nodes.json")

    else:
        logger.info("Using mock data mode - skipping cluster environment validation")

    # Check required directories
    required_dirs = [
        settings.get_resolved_log_file(),
        settings.get_resolved_upload_dir(),
    ]

    for dir_path in required_dirs:
        if str(dir_path).endswith('.log'):
            # For log files, check parent directory
            directory = Path(dir_path).parent
        else:
            directory = Path(dir_path)

        try:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"rectory ready: {directory}")
        except Exception as e:
            logger.error(f"Cannot create directory {directory}: {e}")
            sys.exit(1)

    logger.info("Environment validation passed")


async def test_dependencies():
    """Test critical dependencies and connections."""

    logger.info("Testing critical dependencies...")

    # Test database connection
    try:
        from app.utils.database import get_async_db

        # Test database connection
        db = next(get_async_db())
        db.close()
        logger.info("Database connection test passed")
    except Exception as e:
        logger.warning(f"Database connection test failed: {e}")

    # Test Redis connection if configured
    if not settings.IS_LOCAL_DEV:
        try:
            from app.utils.cache_service import cache_service
            await cache_service.initialize()

            if await cache_service.health_check():
                logger.info("Redis connection test passed")
            else:
                logger.warning("Redis connection test failed")
        except Exception as e:
            logger.warning(f"Redis connection test failed: {e}")

    # Test cluster connections only if enabled
    if settings.use_real_clusters:
        logger.info("Testing cluster connections...")

        try:
            from app.utils.hadoop_client import HDFSClient, HiveClient
            from app.utils.metrics_collector import metrics_collector

            # Test HDFS
            try:
                hdfs_client = HDFSClient()
                storage_info = hdfs_client.get_storage_info()
                if storage_info.get('total_size', 0) > 0:
                    logger.info("HDFS connection test passed")
                else:
                    logger.warning("HDFS connection test failed - no storage info")
            except Exception as e:
                logger.warning(f"HDFS connection test failed: {e}")

            # Test Hive
            try:
                hive_client = HiveClient()
                databases = hive_client.get_databases()
                if databases:
                    logger.info(f"Hive connection test passed ({len(databases)} databases)")
                else:
                    logger.warning("Hive connection test failed - no databases returned")
            except Exception as e:
                logger.warning(f"Hive connection test failed: {e}")

            # Test metrics collector
            try:
                cluster_metrics = await metrics_collector.get_cluster_metrics('hadoop')
                if cluster_metrics:
                    active_nodes = cluster_metrics.get('active_nodes', 0)
                    total_nodes = cluster_metrics.get('total_nodes', 0)
                    logger.info(f"Metrics collector test passed ({active_nodes}/{total_nodes} nodes)")
                else:
                    logger.warning("Metrics collector test failed - no metrics returned")
            except Exception as e:
                logger.warning(f"Metrics collector test failed: {e}")

        except ImportError as e:
            logger.warning(f"Cluster modules not available: {e}")
        except Exception as e:
            logger.warning(f"Cluster connection tests failed: {e}")

    else:
        logger.info("Mock data mode - skipping cluster connection tests")

    logger.info("Dependency testing completed")


def print_startup_banner():
    """Print startup banner with system information."""

    # Get system information
    system_info = {
        'platform': platform.system(),
        'release': platform.release(),
        'machine': platform.machine(),
        'python': sys.version.split()[0],
        'working_dir': os.getcwd(),
    }

    # Check if running in Docker
    in_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
    container_info = " (Docker)" if in_docker else ""

    banner = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                            {settings.APP_NAME}                               ║
║                                Version {settings.VERSION}                                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Environment: {'Production' if not settings.DEBUG else 'Development'}{container_info:<20} ║
║ Platform: {system_info['platform']} {system_info['release']} ({system_info['machine']})                     ║
║ Python: {system_info['python']} on {system_info['platform']:<50} ║
║ Host: {settings.HOST}:{settings.PORT}                                                ║
║ Data Mode: {'Real Clusters' if settings.use_real_clusters else 'Mock Data':<15}                        ║
║ Working Dir: {system_info['working_dir']:<56} ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

    print(banner)


def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 8):
        logger.error("Python 3.8 or higher is required")
        sys.exit(1)
    logger.info(f"Python version check passed: {sys.version}")


async def initialize_cache():
    """Initialize cache service if not in local development mode."""
    if not settings.IS_LOCAL_DEV:
        try:
            from app.utils.cache_service import cache_service
            await cache_service.initialize()
            logger.info("Cache service initialized")
        except Exception as e:
            logger.warning(f"Cache service initialization failed: {e}")


async def main():
    """Main startup function."""

    print_startup_banner()

    # Check Python version
    check_python_version()

    # Setup logging
    setup_production_logging()

    # Validate environment
    validate_environment()

    # Initialize cache
    await initialize_cache()

    # Test dependencies
    await test_dependencies()

    logger.info("Startup validation completed successfully!")
    logger.info(f"Starting server on {settings.HOST}:{settings.PORT}")

    # Import and run the server
    import uvicorn
    from app.main import app

    # Determine number of workers based on environment
    if settings.DEBUG or settings.IS_LOCAL_DEV:
        workers = 1
    else:
        # In production, use multiple workers but limit based on CPU count
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        workers = min(4, max(1, cpu_count))

    logger.info(f"Using {workers} worker{'s' if workers > 1 else ''}")

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
        workers=workers,
    )

    server = uvicorn.Server(config)

    try:
        await server.serve()
    except KeyboardInterrupt:
        logger.info("Server shutdown by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise


if __name__ == "__main__":
    try:
        if platform.system() == "Windows":
            # On Windows, use ProactorEventLoop for better compatibility
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutdown by user")
    except Exception as e:
        logger.error(f"Server startup failed: {e}")
        sys.exit(1)