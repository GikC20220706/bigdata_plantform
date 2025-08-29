# app/models/sync_history.py
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Float
from datetime import datetime
from app.models.base import BaseModel


class SyncHistory(BaseModel):
    """同步历史记录表"""
    __tablename__ = "sync_history"

    sync_id = Column(String(100), unique=True, nullable=False, index=True)
    source_name = Column(String(100), nullable=False, index=True)
    target_name = Column(String(100), nullable=False, index=True)
    status = Column(String(20), nullable=False, index=True)  # running, completed, failed, error

    # 时间信息
    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    # 统计信息
    total_tables = Column(Integer, default=0)
    success_tables = Column(Integer, default=0)
    failed_tables = Column(Integer, default=0)
    total_records = Column(Integer, default=0)

    # 执行信息
    sync_mode = Column(String(50), nullable=True)  # single, multiple, database
    parallel_jobs = Column(Integer, default=1)
    created_by = Column(String(100), nullable=True)

    # 结果和错误信息
    error_message = Column(Text, nullable=True)
    sync_result = Column(JSON, nullable=True)  # 详细的同步结果

    # 进度信息
    current_step = Column(String(200), nullable=True)
    progress = Column(Integer, default=0)  # 0-100


class SyncTableHistory(BaseModel):
    """单表同步历史详情表"""
    __tablename__ = "sync_table_history"

    sync_id = Column(String(100), nullable=False, index=True)
    source_table = Column(String(200), nullable=False)
    target_table = Column(String(200), nullable=False)
    status = Column(String(20), nullable=False)  # success, failed

    start_time = Column(DateTime, nullable=True)
    end_time = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    source_records = Column(Integer, default=0)
    target_records = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    datax_result = Column(JSON, nullable=True)