"""
Utilities package initialization.

This package contains utility modules for the Big Data Platform including
Hadoop clients, metrics collection, database operations, and response formatting.
"""

from .hadoop_client import (
    HadoopClient,
    HDFSClient,
    HiveClient,
    FlinkClient,
    DorisClient,
    format_size
)
from .metrics_collector import metrics_collector
from .response import (
    ApiResponse,
    create_response,
    create_success_response,
    create_error_response,
    create_validation_error_response,
    create_not_found_response,
    create_unauthorized_response
)

__all__ = [
    # Hadoop clients
    "HadoopClient",
    "HDFSClient",
    "HiveClient",
    "FlinkClient",
    "DorisClient",

    # Utilities
    "format_size",
    "metrics_collector",

    # Response utilities
    "ApiResponse",
    "create_response",
    "create_success_response",
    "create_error_response",
    "create_validation_error_response",
    "create_not_found_response",
    "create_unauthorized_response"
]

# app/utils/database.py - Should contain database utilities
"""
Database utilities and connection management for the Big Data Platform.

This module provides database connection helpers, session management,
and common database operations.
"""

from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

from config.settings import settings

# Database setup
engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in settings.DATABASE_URL else {}
)

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
    finally:
        db.close()


def create_tables():
    """Create all database tables."""
    Base.metadata.create_all(bind=engine)


def drop_tables():
    """Drop all database tables."""
    Base.metadata.drop_all(bind=engine)


# app/utils/hdfs_client.py - Can be empty since HDFS functionality is in hadoop_client.py
"""
HDFS client utilities.

Note: HDFS client functionality has been moved to hadoop_client.py
This file is kept for backward compatibility.
"""

from .hadoop_client import HDFSClient

# Re-export for backward compatibility
__all__ = ["HDFSClient"]
