# app/models/workflow.py
"""
工作流编排相关的数据库模型
支持复杂依赖关系的可视化工作流编排功能
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import Column, String, Integer, Text, Boolean, DateTime, JSON, ForeignKey, Float, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.mysql import LONGTEXT
from enum import Enum
from .base import BaseModel


class WorkflowStatus(Enum):
    """工作流状态枚举"""
    DRAFT = "draft"  # 草稿
    PUBLISHED = "published"  # 已发布
    RUNNING = "running"  # 运行中
    PAUSED = "paused"  # 暂停
    SUCCESS = "success"  # 成功
    FAILED = "failed"  # 失败
    CANCELLED = "cancelled"  # 已取消
    ARCHIVED = "archived"  # 已归档


class NodeType(Enum):
    """节点类型枚举"""
    START = "start"  # 开始节点
    END = "end"  # 结束节点
    SQL = "sql"  # SQL任务节点
    SHELL = "shell"  # Shell任务节点
    DATAX = "datax"  # DataX同步节点
    PYTHON = "python"  # Python脚本节点
    SPARK = "spark"  # Spark任务节点
    FLINK = "flink"  # Flink任务节点
    CONDITION = "condition"  # 条件判断节点
    PARALLEL = "parallel"  # 并行分支节点
    JOIN = "join"  # 汇聚节点
    SUBWORKFLOW = "subworkflow"  # 子工作流节点
    TIMER = "timer"  # 定时器节点
    NOTIFICATION = "notification"  # 通知节点
    DATA_QUALITY = "data_quality"  # 数据质量检查节点


class NodeStatus(Enum):
    """节点状态枚举"""
    PENDING = "pending"  # 等待中
    READY = "ready"  # 就绪
    RUNNING = "running"  # 运行中
    SUCCESS = "success"  # 成功
    FAILED = "failed"  # 失败
    SKIPPED = "skipped"  # 跳过
    TIMEOUT = "timeout"  # 超时
    CANCELLED = "cancelled"  # 已取消


class TriggerType(Enum):
    """触发方式枚举"""
    MANUAL = "manual"  # 手动触发
    SCHEDULE = "schedule"  # 定时调度
    EVENT = "event"  # 事件触发
    API = "api"  # API触发
    DEPENDENCY = "dependency"  # 依赖触发


class WorkflowDefinition(BaseModel):
    """工作流定义表"""
    __tablename__ = "workflow_definitions"

    # 基本信息
    workflow_id = Column(String(100), nullable=False, unique=True, index=True, comment="工作流唯一标识")
    workflow_name = Column(String(255), nullable=False, comment="工作流名称")
    display_name = Column(String(255), nullable=False, comment="显示名称")
    description = Column(Text, nullable=True, comment="工作流描述")

    # 分类信息
    category = Column(String(100), nullable=True, index=True, comment="工作流分类")
    business_domain = Column(String(100), nullable=True, index=True, comment="业务领域")
    tags = Column(JSON, nullable=True, comment="标签列表")

    # 状态信息
    status = Column(SQLEnum(WorkflowStatus), nullable=False, default=WorkflowStatus.DRAFT, index=True,
                    comment="工作流状态")
    version = Column(String(50), nullable=False, default="1.0.0", comment="版本号")

    # 触发配置
    trigger_type = Column(SQLEnum(TriggerType), nullable=False, default=TriggerType.MANUAL, comment="触发方式")
    schedule_config = Column(JSON, nullable=True, comment="调度配置")

    # 工作流配置
    workflow_config = Column(JSON, nullable=True, comment="工作流全局配置")
    default_timeout = Column(Integer, nullable=True, default=3600, comment="默认超时时间(秒)")
    max_parallel_tasks = Column(Integer, nullable=True, default=10, comment="最大并行任务数")
    retry_policy = Column(JSON, nullable=True, comment="重试策略配置")

    # 可视化配置
    canvas_config = Column(JSON, nullable=True, comment="画布配置信息")
    layout_config = Column(JSON, nullable=True, comment="布局配置信息")

    # 权限控制
    owner_id = Column(String(100), nullable=True, comment="拥有者ID")
    owner_name = Column(String(255), nullable=True, comment="拥有者姓名")
    visibility = Column(String(20), nullable=False, default="private", comment="可见性(private/public/shared)")

    # 统计信息
    total_executions = Column(Integer, nullable=False, default=0, comment="总执行次数")
    successful_executions = Column(Integer, nullable=False, default=0, comment="成功执行次数")
    failed_executions = Column(Integer, nullable=False, default=0, comment="失败执行次数")
    last_execution_time = Column(DateTime(timezone=True), nullable=True, comment="最后执行时间")
    avg_execution_duration = Column(Integer, nullable=True, comment="平均执行时长(秒)")

    # 状态标识
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")
    is_template = Column(Boolean, nullable=False, default=False, comment="是否为模板")

    # 关系映射
    nodes = relationship("WorkflowNodeDefinition", back_populates="workflow", cascade="all, delete-orphan")
    edges = relationship("WorkflowEdgeDefinition", back_populates="workflow", cascade="all, delete-orphan")
    executions = relationship("WorkflowExecution", back_populates="workflow", cascade="all, delete-orphan")
    alerts = relationship("WorkflowAlert", back_populates="workflow", cascade="all, delete-orphan")


class WorkflowNodeDefinition(BaseModel):
    """工作流节点定义表"""
    __tablename__ = "workflow_node_definitions"

    # 基本信息
    node_id = Column(String(100), nullable=False, index=True, comment="节点唯一标识")
    node_name = Column(String(255), nullable=False, comment="节点名称")
    display_name = Column(String(255), nullable=False, comment="显示名称")
    description = Column(Text, nullable=True, comment="节点描述")

    # 关联工作流
    workflow_id = Column(Integer, ForeignKey('workflow_definitions.id'), nullable=False, comment="关联工作流ID")

    # 节点类型和配置
    node_type = Column(SQLEnum(NodeType), nullable=False, comment="节点类型")
    task_config = Column(JSON, nullable=True, comment="任务配置信息")

    # 执行配置
    timeout = Column(Integer, nullable=True, comment="超时时间(秒)")
    retry_times = Column(Integer, nullable=False, default=0, comment="重试次数")
    retry_interval = Column(Integer, nullable=False, default=60, comment="重试间隔(秒)")

    # 条件配置(用于条件节点)
    condition_expression = Column(Text, nullable=True, comment="条件表达式")

    # 资源配置
    required_cpu = Column(Float, nullable=True, comment="需要CPU核数")
    required_memory_mb = Column(Integer, nullable=True, comment="需要内存(MB)")

    # 可视化配置
    position_x = Column(Float, nullable=True, comment="画布X坐标")
    position_y = Column(Float, nullable=True, comment="画布Y坐标")
    width = Column(Float, nullable=True, default=120, comment="节点宽度")
    height = Column(Float, nullable=True, default=60, comment="节点高度")
    style_config = Column(JSON, nullable=True, comment="样式配置")

    # 状态标识
    is_start_node = Column(Boolean, nullable=False, default=False, comment="是否为开始节点")
    is_end_node = Column(Boolean, nullable=False, default=False, comment="是否为结束节点")
    is_critical = Column(Boolean, nullable=False, default=False, comment="是否为关键节点")

    # 关系映射
    workflow = relationship("WorkflowDefinition", back_populates="nodes")
    outgoing_edges = relationship("WorkflowEdgeDefinition", foreign_keys="WorkflowEdgeDefinition.source_node_id",
                                  back_populates="source_node")
    incoming_edges = relationship("WorkflowEdgeDefinition", foreign_keys="WorkflowEdgeDefinition.target_node_id",
                                  back_populates="target_node")
    node_executions = relationship("WorkflowNodeExecution", back_populates="node_definition",
                                   cascade="all, delete-orphan")


class WorkflowEdgeDefinition(BaseModel):
    """工作流边定义表(节点间依赖关系)"""
    __tablename__ = "workflow_edge_definitions"

    # 基本信息
    edge_id = Column(String(100), nullable=False, index=True, comment="边唯一标识")
    edge_name = Column(String(255), nullable=True, comment="边名称")

    # 关联工作流
    workflow_id = Column(Integer, ForeignKey('workflow_definitions.id'), nullable=False, comment="关联工作流ID")

    # 源节点和目标节点
    source_node_id = Column(Integer, ForeignKey('workflow_node_definitions.id'), nullable=False, comment="源节点ID")
    target_node_id = Column(Integer, ForeignKey('workflow_node_definitions.id'), nullable=False, comment="目标节点ID")

    # 依赖条件
    condition_type = Column(String(50), nullable=False, default="success",
                            comment="依赖条件类型(success/failure/always)")
    condition_expression = Column(Text, nullable=True, comment="自定义条件表达式")

    # 可视化配置
    path_config = Column(JSON, nullable=True, comment="路径配置")
    style_config = Column(JSON, nullable=True, comment="样式配置")

    # 关系映射
    workflow = relationship("WorkflowDefinition", back_populates="edges")
    source_node = relationship("WorkflowNodeDefinition", foreign_keys=[source_node_id], back_populates="outgoing_edges")
    target_node = relationship("WorkflowNodeDefinition", foreign_keys=[target_node_id], back_populates="incoming_edges")


class WorkflowExecution(BaseModel):
    """工作流执行记录表"""
    __tablename__ = "workflow_executions"

    # 基本信息
    execution_id = Column(String(100), nullable=False, unique=True, index=True, comment="执行唯一标识")
    execution_name = Column(String(255), nullable=True, comment="执行名称")

    # 关联工作流
    workflow_id = Column(Integer, ForeignKey('workflow_definitions.id'), nullable=False, comment="关联工作流ID")
    workflow_version = Column(String(50), nullable=True, comment="工作流版本")

    # 执行信息
    status = Column(SQLEnum(WorkflowStatus), nullable=False, default=WorkflowStatus.RUNNING, index=True,
                    comment="执行状态")
    trigger_type = Column(SQLEnum(TriggerType), nullable=False, comment="触发方式")
    trigger_user = Column(String(100), nullable=True, comment="触发用户")

    # 时间信息
    start_time = Column(DateTime(timezone=True), nullable=False, comment="开始时间")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="结束时间")
    execution_date = Column(DateTime(timezone=True), nullable=False, comment="执行日期")

    # 执行统计
    total_nodes = Column(Integer, nullable=False, default=0, comment="总节点数")
    completed_nodes = Column(Integer, nullable=False, default=0, comment="已完成节点数")
    failed_nodes = Column(Integer, nullable=False, default=0, comment="失败节点数")
    skipped_nodes = Column(Integer, nullable=False, default=0, comment="跳过节点数")

    # 执行结果
    result_summary = Column(JSON, nullable=True, comment="执行结果摘要")
    error_message = Column(Text, nullable=True, comment="错误消息")

    # 执行配置
    execution_config = Column(JSON, nullable=True, comment="执行时配置")
    runtime_variables = Column(JSON, nullable=True, comment="运行时变量")

    # 日志和监控
    log_file_path = Column(String(500), nullable=True, comment="日志文件路径")
    metrics_data = Column(JSON, nullable=True, comment="监控指标数据")

    # 关系映射
    workflow = relationship("WorkflowDefinition", back_populates="executions")
    node_executions = relationship("WorkflowNodeExecution", back_populates="workflow_execution",
                                   cascade="all, delete-orphan")


class WorkflowNodeExecution(BaseModel):
    """工作流节点执行记录表"""
    __tablename__ = "workflow_node_executions"

    # 基本信息
    node_execution_id = Column(String(100), nullable=False, unique=True, index=True, comment="节点执行唯一标识")

    # 关联信息
    workflow_execution_id = Column(Integer, ForeignKey('workflow_executions.id'), nullable=False,
                                   comment="工作流执行ID")
    node_definition_id = Column(Integer, ForeignKey('workflow_node_definitions.id'), nullable=False,
                                comment="节点定义ID")

    # 执行信息
    status = Column(SQLEnum(NodeStatus), nullable=False, default=NodeStatus.PENDING, index=True, comment="节点执行状态")
    start_time = Column(DateTime(timezone=True), nullable=True, comment="开始时间")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="结束时间")
    duration_seconds = Column(Integer, nullable=True, comment="执行时长(秒)")

    # 重试信息
    retry_count = Column(Integer, nullable=False, default=0, comment="重试次数")
    max_retry_count = Column(Integer, nullable=False, default=0, comment="最大重试次数")

    # 执行结果
    result_data = Column(JSON, nullable=True, comment="执行结果数据")
    output_variables = Column(JSON, nullable=True, comment="输出变量")
    error_message = Column(Text, nullable=True, comment="错误消息")
    error_stack_trace = Column(LONGTEXT, nullable=True, comment="错误堆栈信息")

    # 资源使用情况
    cpu_usage_percent = Column(Float, nullable=True, comment="CPU使用率")
    memory_usage_mb = Column(Float, nullable=True, comment="内存使用量(MB)")

    # 外部任务信息(如Airflow TaskInstance)
    external_task_id = Column(String(255), nullable=True, comment="外部任务ID")
    external_execution_id = Column(String(255), nullable=True, comment="外部执行ID")

    # 日志信息
    log_file_path = Column(String(500), nullable=True, comment="日志文件路径")
    log_url = Column(String(500), nullable=True, comment="日志访问URL")

    # 关系映射
    workflow_execution = relationship("WorkflowExecution", back_populates="node_executions")
    node_definition = relationship("WorkflowNodeDefinition", back_populates="node_executions")


class WorkflowTemplate(BaseModel):
    """工作流模板表"""
    __tablename__ = "workflow_templates"

    # 基本信息
    template_name = Column(String(255), nullable=False, unique=True, comment="模板名称")
    display_name = Column(String(255), nullable=False, comment="显示名称")
    description = Column(Text, nullable=True, comment="模板描述")

    # 分类信息
    category = Column(String(100), nullable=True, index=True, comment="模板分类")
    business_scenario = Column(String(200), nullable=True, comment="业务场景")
    tags = Column(JSON, nullable=True, comment="标签列表")

    # 模板配置
    template_config = Column(JSON, nullable=False, comment="模板配置JSON")
    default_variables = Column(JSON, nullable=True, comment="默认变量配置")
    parameter_schema = Column(JSON, nullable=True, comment="参数Schema定义")

    # 使用统计
    usage_count = Column(Integer, nullable=False, default=0, comment="使用次数")

    # 状态标识
    is_active = Column(Boolean, nullable=False, default=True, comment="是否激活")
    is_builtin = Column(Boolean, nullable=False, default=False, comment="是否内置模板")
    is_public = Column(Boolean, nullable=False, default=True, comment="是否公开模板")

    # 权限控制
    required_permissions = Column(JSON, nullable=True, comment="所需权限列表")
    max_instances = Column(Integer, nullable=True, comment="最大实例数限制")
    usage_limit = Column(Integer, nullable=True, comment="使用次数限制")

    # 版本控制
    version = Column(String(20), nullable=False, default="1.0.0", comment="模板版本")
    changelog = Column(Text, nullable=True, comment="版本变更日志")

    # 创建者信息
    created_by = Column(String(100), nullable=True, comment="创建者")
    created_by_name = Column(String(200), nullable=True, comment="创建者姓名")


class WorkflowVariable(BaseModel):
    """工作流变量表"""
    __tablename__ = "workflow_variables"

    # 基本信息
    variable_key = Column(String(255), nullable=False, index=True, comment="变量键")
    variable_name = Column(String(255), nullable=False, comment="变量名称")
    description = Column(Text, nullable=True, comment="变量描述")

    # 关联工作流
    workflow_id = Column(Integer, ForeignKey('workflow_definitions.id'), nullable=False, comment="关联工作流ID")

    # 变量配置
    variable_type = Column(String(50), nullable=False, default="string", comment="变量类型")
    default_value = Column(Text, nullable=True, comment="默认值")
    is_required = Column(Boolean, nullable=False, default=False, comment="是否必填")
    is_sensitive = Column(Boolean, nullable=False, default=False, comment="是否敏感信息")

    # 验证规则
    validation_rules = Column(JSON, nullable=True, comment="验证规则")

    # 关系映射
    workflow = relationship("WorkflowDefinition")


class WorkflowAlert(BaseModel):
    """工作流告警规则表"""
    __tablename__ = "workflow_alerts"

    # 基本信息
    alert_name = Column(String(255), nullable=False, comment="告警名称")
    description = Column(Text, nullable=True, comment="告警描述")
    workflow_id = Column(Integer, ForeignKey("workflow_definitions.id"), nullable=False, comment="关联工作流ID")

    # 告警类型和级别
    alert_type = Column(String(50), nullable=False, comment="告警类型")
    severity = Column(String(20), nullable=False, default="medium", comment="严重级别")

    # 触发条件
    conditions = Column(JSON, nullable=False, comment="触发条件列表")
    condition_logic = Column(String(10), nullable=False, default="AND", comment="条件逻辑")

    # 通知设置
    notification_channels = Column(JSON, nullable=False, comment="通知渠道列表")
    recipients = Column(JSON, nullable=False, comment="接收人列表")

    # 告警配置
    is_enabled = Column(Boolean, nullable=False, default=True, comment="是否启用")
    suppress_duration_minutes = Column(Integer, nullable=False, default=60, comment="告警抑制时长(分钟)")
    max_alerts_per_hour = Column(Integer, nullable=False, default=10, comment="每小时最大告警次数")

    # 自定义消息
    custom_message_template = Column(Text, nullable=True, comment="自定义消息模板")
    include_execution_context = Column(Boolean, nullable=False, default=True, comment="是否包含执行上下文")

    # 自动恢复
    auto_resolve = Column(Boolean, nullable=False, default=False, comment="是否自动恢复")
    resolve_conditions = Column(JSON, nullable=True, comment="恢复条件")

    # 创建者信息
    created_by = Column(String(100), nullable=True, comment="创建者")

    # 统计信息
    trigger_count = Column(Integer, nullable=False, default=0, comment="触发次数")
    last_trigger_time = Column(DateTime(timezone=True), nullable=True, comment="最后触发时间")

    # 关系映射
    workflow = relationship("WorkflowDefinition", back_populates="alerts")