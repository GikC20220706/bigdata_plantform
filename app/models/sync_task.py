# app/models/sync_task.py
"""
智能数据同步相关的数据库模型
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON, ForeignKey, Float, Index
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import LONGTEXT
from .base import BaseModel




class SyncTask(BaseModel):
    """数据同步任务表"""
    __tablename__ = "sync_tasks"

    # 基本信息
    task_name = Column(String(255), nullable=False, comment="任务名称")
    task_description = Column(Text, nullable=True, comment="任务描述")
    task_type = Column(String(50), nullable=False, index=True, comment="任务类型(single/multiple/database)")

    # 源和目标
    source_data_source_id = Column(Integer, ForeignKey('data_sources.id'), nullable=False, comment="源数据源ID")
    target_data_source_id = Column(Integer, ForeignKey('data_sources.id'), nullable=False, comment="目标数据源ID")

    # 同步配置
    sync_config = Column(JSON, nullable=False, comment="同步配置JSON")
    datax_config = Column(LONGTEXT, nullable=True, comment="DataX配置文件内容")

    # 调度信息
    schedule_type = Column(String(20), nullable=False, default="manual", comment="调度类型(manual/cron/trigger)")
    cron_expression = Column(String(100), nullable=True, comment="定时表达式")

    # 状态信息
    status = Column(String(20), nullable=False, default="created", index=True, comment="任务状态")
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")

    # 执行统计
    total_executions = Column(Integer, nullable=False, default=0, comment="总执行次数")
    successful_executions = Column(Integer, nullable=False, default=0, comment="成功执行次数")
    failed_executions = Column(Integer, nullable=False, default=0, comment="失败执行次数")
    last_execution_time = Column(DateTime(timezone=True), nullable=True, comment="最后执行时间")

    # 创建者信息
    created_by = Column(String(100), nullable=True, comment="创建者")

    # 关系
    source_data_source = relationship("DataSource", foreign_keys=[source_data_source_id])
    target_data_source = relationship("DataSource", foreign_keys=[target_data_source_id])
    executions = relationship("SyncExecution", back_populates="sync_task", cascade="all, delete-orphan")
    table_mappings = relationship("SyncTableMapping", back_populates="sync_task", cascade="all, delete-orphan")


class SyncTableMapping(BaseModel):
    """同步表映射表"""
    __tablename__ = "sync_table_mappings"

    # 关联任务
    sync_task_id = Column(Integer, ForeignKey('sync_tasks.id'), nullable=False, comment="同步任务ID")

    # 源表信息
    source_database = Column(String(255), nullable=True, comment="源数据库名")
    source_table = Column(String(255), nullable=False, comment="源表名")
    source_query = Column(Text, nullable=True, comment="源查询SQL")

    # 目标表信息
    target_database = Column(String(255), nullable=True, comment="目标数据库名")
    target_table = Column(String(255), nullable=False, comment="目标表名")

    # 映射配置
    column_mapping = Column(JSON, nullable=True, comment="列映射配置")
    where_condition = Column(Text, nullable=True, comment="过滤条件")

    # 同步设置
    sync_mode = Column(String(20), nullable=False, default="full", comment="同步模式(full/incremental)")
    incremental_column = Column(String(100), nullable=True, comment="增量同步列")
    watermark_value = Column(String(255), nullable=True, comment="水位线值")

    # 状态信息
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")

    # 关系
    sync_task = relationship("SyncTask", back_populates="table_mappings")


class SyncExecution(BaseModel):
    """同步执行记录表"""
    __tablename__ = "sync_executions"

    # 关联任务
    sync_task_id = Column(Integer, ForeignKey('sync_tasks.id'), nullable=False, comment="同步任务ID")

    # 执行信息
    execution_id = Column(String(100), nullable=False, unique=True, index=True, comment="执行ID")
    execution_type = Column(String(20), nullable=False, comment="执行类型(manual/scheduled/triggered)")

    # 执行状态
    status = Column(String(20), nullable=False, index=True, comment="执行状态")
    start_time = Column(DateTime(timezone=True), nullable=False, comment="开始时间")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="结束时间")
    duration_seconds = Column(Integer, nullable=True, comment="执行时长(秒)")

    # 执行结果
    total_tables = Column(Integer, nullable=False, default=0, comment="总表数")
    successful_tables = Column(Integer, nullable=False, default=0, comment="成功表数")
    failed_tables = Column(Integer, nullable=False, default=0, comment="失败表数")
    total_records = Column(Integer, nullable=False, default=0, comment="总记录数")

    # 性能指标
    avg_throughput = Column(Float, nullable=True, comment="平均吞吐量(记录/秒)")
    peak_throughput = Column(Float, nullable=True, comment="峰值吞吐量(记录/秒)")
    data_volume_mb = Column(Float, nullable=True, comment="数据量(MB)")

    # 错误信息
    error_message = Column(Text, nullable=True, comment="错误信息")
    error_details = Column(LONGTEXT, nullable=True, comment="详细错误信息")

    # 执行日志
    log_file_path = Column(String(500), nullable=True, comment="日志文件路径")
    datax_log = Column(LONGTEXT, nullable=True, comment="DataX执行日志")

    # 触发信息
    triggered_by = Column(String(100), nullable=True, comment="触发者")
    trigger_source = Column(String(50), nullable=True, comment="触发源")

    # 关系
    sync_task = relationship("SyncTask", back_populates="executions")
    table_results = relationship("SyncTableResult", back_populates="sync_execution", cascade="all, delete-orphan")


class SyncTableResult(BaseModel):
    """同步表结果表"""
    __tablename__ = "sync_table_results"

    # 关联执行
    sync_execution_id = Column(Integer, ForeignKey('sync_executions.id'), nullable=False, comment="同步执行ID")

    # 表信息
    source_table = Column(String(255), nullable=False, comment="源表名")
    target_table = Column(String(255), nullable=False, comment="目标表名")

    # 执行结果
    status = Column(String(20), nullable=False, comment="表同步状态")
    start_time = Column(DateTime(timezone=True), nullable=False, comment="开始时间")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="结束时间")
    duration_seconds = Column(Integer, nullable=True, comment="执行时长(秒)")

    # 数据统计
    source_records = Column(Integer, nullable=True, comment="源记录数")
    target_records = Column(Integer, nullable=True, comment="目标记录数")
    inserted_records = Column(Integer, nullable=True, comment="插入记录数")
    updated_records = Column(Integer, nullable=True, comment="更新记录数")
    deleted_records = Column(Integer, nullable=True, comment="删除记录数")

    # 数据完整性
    data_integrity_check = Column(Boolean, nullable=True, comment="数据完整性检查")
    data_loss_count = Column(Integer, nullable=True, default=0, comment="数据丢失数量")

    # 性能指标
    throughput = Column(Float, nullable=True, comment="吞吐量(记录/秒)")
    data_size_mb = Column(Float, nullable=True, comment="数据大小(MB)")

    # 错误信息
    error_message = Column(Text, nullable=True, comment="错误信息")
    warning_message = Column(Text, nullable=True, comment="警告信息")

    # 关系
    sync_execution = relationship("SyncExecution", back_populates="table_results")


class DataSourceMetadata(BaseModel):
    """数据源元数据缓存表"""
    __tablename__ = "data_source_metadata"

    # 数据源信息
    data_source_id = Column(Integer, ForeignKey('data_sources.id'), nullable=False, comment="数据源ID")
    database_name = Column(String(255), nullable=True, comment="数据库名")
    table_name = Column(String(255), nullable=False, comment="表名")

    # 表结构信息
    table_schema = Column(JSON, nullable=True, comment="表结构JSON")
    column_count = Column(Integer, nullable=True, comment="列数量")

    # 统计信息
    row_count = Column(Integer, nullable=True, comment="行数")
    data_size_bytes = Column(Integer, nullable=True, comment="数据大小(字节)")
    avg_row_size = Column(Float, nullable=True, comment="平均行大小")

    # 索引信息
    indexes_info = Column(JSON, nullable=True, comment="索引信息")
    primary_key_columns = Column(JSON, nullable=True, comment="主键列")

    # 采样数据
    sample_data = Column(JSON, nullable=True, comment="样本数据")

    # 缓存信息
    last_collected = Column(DateTime(timezone=True), nullable=False, comment="最后收集时间")
    collection_duration_ms = Column(Integer, nullable=True, comment="收集耗时(毫秒)")
    is_stale = Column(Boolean, nullable=False, default=False, comment="是否过期")

    # 关系
    data_source = relationship("DataSource")

    # 添加唯一约束
    __table_args__ = (
        Index('idx_metadata_unique', 'data_source_id', 'database_name', 'table_name', unique=True),
        Index('idx_metadata_collected', 'last_collected'),
        Index('idx_metadata_stale', 'is_stale'),
    )


class SyncTemplate(BaseModel):
    """同步模板表"""
    __tablename__ = "sync_templates"

    # 模板信息
    template_name = Column(String(255), nullable=False, unique=True, comment="模板名称")
    template_description = Column(Text, nullable=True, comment="模板描述")
    template_category = Column(String(100), nullable=True, comment="模板分类")

    # 适用场景
    source_type = Column(String(50), nullable=False, comment="源数据库类型")
    target_type = Column(String(50), nullable=False, comment="目标数据库类型")

    # 模板配置
    default_config = Column(JSON, nullable=False, comment="默认配置")
    recommended_settings = Column(JSON, nullable=True, comment="推荐设置")

    # 模板标签
    tags = Column(JSON, nullable=True, comment="标签")

    # 使用统计
    usage_count = Column(Integer, nullable=False, default=0, comment="使用次数")

    # 状态
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")
    is_builtin = Column(Boolean, nullable=False, default=False, comment="是否内置模板")

    # 创建者
    created_by = Column(String(100), nullable=True, comment="创建者")


# # 需要在 app/models/__init__.py 中添加新模型的导入
# from sqlalchemy import Index