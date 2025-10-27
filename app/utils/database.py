"""
Database utilities and connection management for the Big Data Platform.
支持MySQL和SQLite数据库
"""

from typing import Generator

from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from config.settings import settings
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from contextlib import asynccontextmanager


# 根据数据库类型配置连接参数
def get_engine_config():
    """根据数据库类型获取引擎配置"""
    config = {
        "echo": settings.DATABASE_ECHO,
        "pool_pre_ping": settings.DATABASE_POOL_PRE_PING,
    }

    if settings.is_mysql:
        # MySQL配置
        config.update({
            "poolclass": QueuePool,
            "pool_size": settings.DATABASE_POOL_SIZE,
            "max_overflow": settings.DATABASE_POOL_SIZE * 2,
            "pool_recycle": settings.DATABASE_POOL_RECYCLE,
            "pool_timeout": 30,
            "connect_args": {
                "charset": "utf8mb4",
                "autocommit": False,
            }
        })
    elif settings.is_sqlite:
        # SQLite配置
        config.update({
            "connect_args": {"check_same_thread": False}
        })

    return config


def get_sync_db_session():
    """获取同步数据库会话 - 用于非异步上下文"""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from config.settings import settings

    # 创建同步引擎
    sync_engine = create_engine(
        settings.DATABASE_URL.replace('+aiomysql', '+pymysql'),  # 使用pymysql替代aiomysql
        pool_pre_ping=True,
        pool_recycle=300,
        echo=False
    )

    # 创建会话
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=sync_engine)
    return SessionLocal()

# 创建数据库引擎
engine_config = get_engine_config()
#engine = create_engine(settings.DATABASE_URL, **engine_config)
async_engine = create_async_engine(
    settings.DATABASE_URL,  # 确保URL包含 +aiomysql
    pool_size=20,
    max_overflow=40,
    pool_timeout=60,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=False
)
async_session_maker = async_sessionmaker(
    bind=async_engine,  # 使用现有的异步引擎
    class_=AsyncSession,
    expire_on_commit=False
)
#SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# def get_db() -> Generator[Session, None, None]:
#     """
#     Get database session.
#
#     Yields:
#         Session: Database session
#     """
#     db = SessionLocal()
#     try:
#         yield db
#         db.commit()
#     except Exception:
#         db.rollback()
#         raise
#     finally:
#         db.close()

async def get_async_db() -> AsyncSession:
    """异步数据库会话依赖"""
    async with async_session_maker() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

@asynccontextmanager
async def get_async_db_context():
    """异步数据库会话上下文管理器"""
    session = async_session_maker()
    try:
        yield session
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise e
    finally:
        await session.close()

def create_tables_sync():
    """Create all database tables - 完全同步版本"""
    try:
        # 导入所有模型 - 确保模型被注册
        from app.models import (
            Base,
            Cluster, ClusterNode, ClusterMetric,
            DataSource, DataSourceConnection,
            TaskDefinition, TaskExecution, TaskSchedule,
            BusinessSystem, BusinessSystemDataSource,
            # 添加新的同步任务模型
            SyncTask, SyncTableMapping, SyncExecution, SyncTableResult,
            DataSourceMetadata, SyncTemplate,SyncHistory, SyncTableHistory,
            DataCatalog, DataAsset, AssetColumn, AssetAccessLog, FieldStandard,
            CustomAPI, APIParameter, APIAccessLog,
            APIUser, APIKey, APIUserPermission,
            UserCluster,
            WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition,
            WorkflowExecution, WorkflowNodeExecution, WorkflowTemplate,
            WorkflowVariable, WorkflowAlert,
            ResourceFile,
            IndicatorSystem, IndicatorAssetRelation
        )
        from sqlalchemy import text

        logger.info("正在创建数据库表...")
        logger.info(f"发现 {len(Base.metadata.tables)} 个表需要创建")

        # 打印所有将要创建的表名
        table_names = list(Base.metadata.tables.keys())
        logger.info(f"准备创建的表: {table_names}")

        # 使用同步引擎创建表
        sync_url = settings.DATABASE_URL.replace('+aiomysql', '+pymysql')
        from sqlalchemy import create_engine
        sync_engine = create_engine(sync_url, pool_pre_ping=True)

        Base.metadata.create_all(bind=sync_engine)

        # 验证表创建
        with sync_engine.connect() as conn:
            if settings.is_mysql:
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result.fetchall()]
                logger.info(f"成功创建 {len(tables)} 个表: {tables}")
            else:
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                tables = [row[0] for row in result.fetchall()]
                logger.info(f"成功创建 {len(tables)} 个表: {tables}")

        if settings.is_mysql and len(tables) > 0:
            create_mysql_indexes_sync(sync_engine)
        else:
            logger.info("跳过索引创建，因为没有表被创建")

    except Exception as e:
        logger.error(f"❌ 创建数据库表失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def create_mysql_indexes_sync(engine):
    """为MySQL创建额外的索引 - 同步版本"""
    try:
        from sqlalchemy import text

        with engine.connect() as conn:
            indexes_to_create = [
                {
                    'name': 'idx_data_source_connections_source_time',
                    'table': 'data_source_connections',
                    'sql': 'CREATE INDEX idx_data_source_connections_source_time ON data_source_connections(data_source_id, connection_timestamp)'
                },
                {
                    'name': 'idx_cluster_metrics_time',
                    'table': 'cluster_metrics',
                    'sql': 'CREATE INDEX idx_cluster_metrics_time ON cluster_metrics(metric_timestamp DESC)'
                },
                {
                    'name': 'idx_task_executions_def_time',
                    'table': 'task_executions',
                    'sql': 'CREATE INDEX idx_task_executions_def_time ON task_executions(task_definition_id, started_at DESC)'
                }
            ]

            for index_info in indexes_to_create:
                try:
                    # 先检查表是否存在
                    table_check = text("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.tables 
                        WHERE table_schema = DATABASE() 
                        AND table_name = :table_name
                    """)

                    table_result = conn.execute(table_check, {'table_name': index_info['table']})

                    if table_result.fetchone()[0] == 0:
                        logger.warning(f"Table {index_info['table']} does not exist, skipping index creation")
                        continue

                    # 检查索引是否已存在
                    check_sql = text("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.statistics 
                        WHERE table_schema = DATABASE() 
                        AND table_name = :table_name 
                        AND index_name = :index_name
                    """)

                    result = conn.execute(check_sql, {
                        'table_name': index_info['table'],
                        'index_name': index_info['name']
                    })

                    if result.fetchone()[0] == 0:
                        conn.execute(text(index_info['sql']))
                        logger.info(f"Created index: {index_info['name']}")
                    else:
                        logger.info(f"Index already exists: {index_info['name']}")

                except Exception as e:
                    logger.warning(f"Failed to create index {index_info['name']}: {e}")

            conn.commit()

        logger.info("✅ MySQL indexes processing completed")
    except Exception as e:
        logger.warning(f"Failed to create MySQL indexes: {e}")


def drop_tables():
    """Drop all database tables."""
    Base.metadata.drop_all(bind=async_engine)


def test_connection():
    """测试数据库连接 - 同步版本"""
    try:
        # 使用同步连接字符串
        sync_url = settings.DATABASE_URL.replace('+aiomysql', '+pymysql')

        # 创建同步引擎
        from sqlalchemy import create_engine
        sync_engine = create_engine(
            sync_url,
            pool_pre_ping=True,
            pool_recycle=300
        )

        with sync_engine.connect() as conn:
            if settings.is_mysql:
                result = conn.execute(text("SELECT VERSION() as version"))
                version = result.fetchone()[0]
                logger.info(f"MySQL connection successful - Version: {version}")
            else:
                result = conn.execute(text("SELECT sqlite_version() as version"))
                version = result.fetchone()[0]
                logger.info(f"SQLite connection successful - Version: {version}")

        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False


# 异步数据库支持（可选）
def create_async_engine():
    """创建异步数据库引擎（如果需要）"""
    try:
        from sqlalchemy.ext.asyncio import create_async_engine as create_async_engine_sqlalchemy

        # 将同步URL转换为异步URL
        async_url = settings.DATABASE_URL.replace("mysql+pymysql://", "mysql+aiomysql://")

        return create_async_engine_sqlalchemy(
            async_url,
            echo=settings.DATABASE_ECHO,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_POOL_SIZE * 2,
            pool_recycle=settings.DATABASE_POOL_RECYCLE,
        )
    except ImportError:
        print("Async database dependencies not installed")
        return None