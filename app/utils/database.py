"""
Database utilities and connection management for the Big Data Platform.
支持MySQL和SQLite数据库
"""

from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from config.settings import settings


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


# 创建数据库引擎
engine_config = get_engine_config()
engine = create_engine(settings.DATABASE_URL, **engine_config)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db() -> Generator[Session, None, None]:
    """
    Get database session.

    Yields:
        Session: Database session
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def create_tables():
    """Create all database tables."""
    try:
        # 导入所有模型以确保它们被注册
        from app.models import (
            Cluster, ClusterNode, ClusterMetric,
            DataSource, DataSourceConnection,
            TaskDefinition, TaskExecution, TaskSchedule
        )

        Base.metadata.create_all(bind=engine)
        print("✅ Database tables created successfully")

        # 如果是MySQL，创建索引
        if settings.is_mysql:
            create_mysql_indexes()

    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        raise


def create_mysql_indexes():
    """为MySQL创建额外的索引"""
    try:
        with engine.connect() as conn:
            # 数据源连接表的复合索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_source_connections_source_time 
                ON data_source_connections(data_source_id, connection_timestamp)
            """)

            # 集群指标表的时间索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cluster_metrics_time 
                ON cluster_metrics(metric_timestamp DESC)
            """)

            # 任务执行表的复合索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_task_executions_def_time 
                ON task_executions(task_definition_id, started_at DESC)
            """)

        print("✅ MySQL indexes created successfully")
    except Exception as e:
        print(f"⚠️ Failed to create MySQL indexes: {e}")


def drop_tables():
    """Drop all database tables."""
    Base.metadata.drop_all(bind=engine)


def test_connection():
    """测试数据库连接"""
    try:
        with engine.connect() as conn:
            if settings.is_mysql:
                result = conn.execute("SELECT VERSION() as version")
                version = result.fetchone()[0]
                print(f"✅ MySQL connection successful - Version: {version}")
            else:
                result = conn.execute("SELECT sqlite_version() as version")
                version = result.fetchone()[0]
                print(f"✅ SQLite connection successful - Version: {version}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
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
        print("⚠️ Async database dependencies not installed")
        return None