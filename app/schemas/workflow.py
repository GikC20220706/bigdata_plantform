# app/schemas/workflow.py
"""
工作流编排相关的Pydantic模式定义
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, field_validator, root_field_validator, field_field_validator
from enum import Enum


# ==================== 枚举定义 ====================

class WorkflowStatus(str, Enum):
    """工作流状态枚举"""
    DRAFT = "draft"
    PUBLISHED = "published"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ARCHIVED = "archived"


class NodeType(str, Enum):
    """节点类型枚举"""
    START = "start"
    END = "end"
    SQL = "sql"
    SHELL = "shell"
    DATAX = "datax"
    PYTHON = "python"
    SPARK = "spark"
    FLINK = "flink"
    CONDITION = "condition"
    PARALLEL = "parallel"
    JOIN = "join"
    SUBWORKFLOW = "subworkflow"
    TIMER = "timer"
    NOTIFICATION = "notification"
    DATA_QUALITY = "data_quality"


class NodeStatus(str, Enum):
    """节点状态枚举"""
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class TriggerType(str, Enum):
    """触发方式枚举"""
    MANUAL = "manual"
    SCHEDULE = "schedule"
    EVENT = "event"
    API = "api"
    DEPENDENCY = "dependency"


class ConditionType(str, Enum):
    """依赖条件类型枚举"""
    SUCCESS = "success"
    FAILED = "failed"
    ALWAYS = "always"
    CUSTOM = "custom"


class SeverityLevel(str, Enum):
    """严重级别枚举"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# ==================== 基础模型 ====================

class PositionConfig(BaseModel):
    """位置配置模型"""
    x: float = Field(..., description="X坐标")
    y: float = Field(..., description="Y坐标")
    width: Optional[float] = Field(120, description="宽度")
    height: Optional[float] = Field(60, description="高度")


class StyleConfig(BaseModel):
    """样式配置模型"""
    color: Optional[str] = Field(None, description="颜色")
    background_color: Optional[str] = Field(None, description="背景色")
    border_color: Optional[str] = Field(None, description="边框色")
    border_width: Optional[int] = Field(1, description="边框宽度")
    border_radius: Optional[int] = Field(4, description="圆角大小")
    font_size: Optional[int] = Field(12, description="字体大小")
    font_weight: Optional[str] = Field("normal", description="字体粗细")


class PathConfig(BaseModel):
    """路径配置模型"""
    points: Optional[List[Dict[str, float]]] = Field(None, description="路径点坐标")
    curve_type: Optional[str] = Field("straight", description="曲线类型")
    style: Optional[StyleConfig] = Field(None, description="路径样式")


class ScheduleConfig(BaseModel):
    """调度配置模型"""
    schedule_interval: Optional[str] = Field(
        None,
        description="调度表达式(Cron格式)",
        example="0 2 * * *"
    )
    start_date: Optional[datetime] = Field(None, description="开始时间")
    end_date: Optional[datetime] = Field(None, description="结束时间")
    timezone: Optional[str] = Field("Asia/Shanghai", description="时区")
    catchup: bool = Field(False, description="是否补跑历史任务")
    max_active_runs: int = Field(1, description="最大并发运行数", ge=1, le=10)
    depends_on_past: bool = Field(False, description="是否依赖前一次执行")


class RetryPolicy(BaseModel):
    """重试策略模型"""
    retry_times: int = Field(0, description="重试次数", ge=0, le=10)
    retry_interval: int = Field(60, description="重试间隔(秒)", ge=1)
    exponential_backoff: bool = Field(False, description="是否指数退避")
    max_retry_interval: Optional[int] = Field(None, description="最大重试间隔(秒)")


class NotificationConfig(BaseModel):
    """通知配置模型"""
    channels: List[str] = Field(..., description="通知渠道")
    recipients: List[str] = Field(..., description="接收人列表")
    on_success: bool = Field(False, description="成功时通知")
    on_failure: bool = Field(True, description="失败时通知")
    on_start: bool = Field(False, description="开始时通知")
    template: Optional[str] = Field(None, description="消息模板")


class ValidationRule(BaseModel):
    """验证规则模型"""
    rule_type: str = Field(..., description="规则类型")
    rule_expression: str = Field(..., description="规则表达式")
    error_message: Optional[str] = Field(None, description="错误消息")


class VariableDefinition(BaseModel):
    """变量定义模型"""
    key: str = Field(..., description="变量键")
    name: str = Field(..., description="变量名称")
    description: Optional[str] = Field(None, description="变量描述")
    type: str = Field("string", description="变量类型")
    default_value: Optional[str] = Field(None, description="默认值")
    is_required: bool = Field(False, description="是否必填")
    is_sensitive: bool = Field(False, description="是否敏感")
    validation_rules: Optional[List[ValidationRule]] = Field(None, description="验证规则")

    @field_validator('key')
    def validate_key(cls, v):
        if not v.replace('_', '').isalnum():
            raise ValueError('变量键只能包含字母、数字和下划线')
        return v


# ==================== 节点相关模型 ====================

class NodeTaskConfig(BaseModel):
    """节点任务配置模型"""
    # SQL任务配置
    sql: Optional[str] = Field(None, description="SQL语句")
    sql_file: Optional[str] = Field(None, description="SQL文件路径")
    connection_id: Optional[str] = Field(None, description="数据库连接ID")

    # Shell任务配置
    command: Optional[str] = Field(None, description="Shell命令")
    script_file: Optional[str] = Field(None, description="脚本文件路径")
    working_directory: Optional[str] = Field(None, description="工作目录")
    environment: Optional[Dict[str, str]] = Field(None, description="环境变量")

    # DataX任务配置
    datax_config: Optional[Dict[str, Any]] = Field(None, description="DataX配置")
    datax_config_file: Optional[str] = Field(None, description="DataX配置文件路径")

    # Python任务配置
    python_code: Optional[str] = Field(None, description="Python代码")
    python_file: Optional[str] = Field(None, description="Python文件路径")
    python_dependencies: Optional[List[str]] = Field(None, description="Python依赖包")

    # Spark任务配置
    spark_application: Optional[str] = Field(None, description="Spark应用路径")
    spark_config: Optional[Dict[str, Any]] = Field(None, description="Spark配置")
    driver_memory: Optional[str] = Field(None, description="Driver内存")
    executor_memory: Optional[str] = Field(None, description="Executor内存")
    executor_cores: Optional[int] = Field(None, description="Executor核数")

    # Flink任务配置
    flink_jar: Optional[str] = Field(None, description="Flink JAR包路径")
    flink_config: Optional[Dict[str, Any]] = Field(None, description="Flink配置")
    parallelism: Optional[int] = Field(None, description="并行度")

    # 条件节点配置
    condition_expression: Optional[str] = Field(None, description="条件表达式")
    true_branch: Optional[str] = Field(None, description="条件为真时的分支")
    false_branch: Optional[str] = Field(None, description="条件为假时的分支")

    # 定时器配置
    delay_seconds: Optional[int] = Field(None, description="延迟秒数")

    # 通知配置
    notification: Optional[NotificationConfig] = Field(None, description="通知配置")

    # 子工作流配置
    subworkflow_id: Optional[str] = Field(None, description="子工作流ID")
    subworkflow_params: Optional[Dict[str, Any]] = Field(None, description="子工作流参数")

    # 数据质量检查配置
    quality_rules: Optional[List[Dict[str, Any]]] = Field(None, description="质量规则")
    quality_threshold: Optional[float] = Field(None, description="质量阈值")


class WorkflowNodeRequest(BaseModel):
    """工作流节点请求模型"""
    node_id: str = Field(..., description="节点ID", min_length=1, max_length=100)
    node_name: str = Field(..., description="节点名称", min_length=1, max_length=255)
    display_name: str = Field(..., description="显示名称", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="节点描述", max_length=1000)
    node_type: NodeType = Field(..., description="节点类型")
    task_config: Optional[NodeTaskConfig] = Field(None, description="任务配置")
    timeout: Optional[int] = Field(None, description="超时时间(秒)", ge=1)
    retry_policy: Optional[RetryPolicy] = Field(None, description="重试策略")
    condition_expression: Optional[str] = Field(None, description="条件表达式")
    required_cpu: Optional[float] = Field(None, description="所需CPU核数", ge=0)
    required_memory_mb: Optional[int] = Field(None, description="所需内存(MB)", ge=0)
    position: Optional[PositionConfig] = Field(None, description="位置配置")
    style: Optional[StyleConfig] = Field(None, description="样式配置")
    is_start_node: bool = Field(False, description="是否为开始节点")
    is_end_node: bool = Field(False, description="是否为结束节点")
    is_critical: bool = Field(False, description="是否为关键节点")

    @field_validator('node_id')
    def validate_node_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('节点ID只能包含字母、数字、下划线和短横线')
        return v

    @root_field_validator
    def validate_node_config(cls, values):
        node_type = values.get('node_type')
        task_config = values.get('task_config')

        if node_type in [NodeType.START, NodeType.END]:
            # 开始和结束节点不需要任务配置
            pass
        elif node_type == NodeType.SQL:
            if task_config and not (task_config.sql or task_config.sql_file):
                raise ValueError('SQL节点必须配置SQL语句或SQL文件')
        elif node_type == NodeType.SHELL:
            if task_config and not (task_config.command or task_config.script_file):
                raise ValueError('Shell节点必须配置命令或脚本文件')
        elif node_type == NodeType.CONDITION:
            if not values.get('condition_expression'):
                raise ValueError('条件节点必须配置条件表达式')

        return values


class WorkflowEdgeRequest(BaseModel):
    """工作流边请求模型"""
    edge_id: str = Field(..., description="边ID", min_length=1, max_length=100)
    edge_name: Optional[str] = Field(None, description="边名称", max_length=255)
    source_node_id: str = Field(..., description="源节点ID")
    target_node_id: str = Field(..., description="目标节点ID")
    condition_type: ConditionType = Field(ConditionType.SUCCESS, description="依赖条件类型")
    condition_expression: Optional[str] = Field(None, description="自定义条件表达式")
    path_config: Optional[PathConfig] = Field(None, description="路径配置")
    style_config: Optional[StyleConfig] = Field(None, description="样式配置")

    @field_validator('edge_id')
    def validate_edge_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('边ID只能包含字母、数字、下划线和短横线')
        return v

    @field_field_validator('condition_expression')
    def validate_condition_expression(cls, v, values):
        condition_type = values.get('condition_type')
        if condition_type == ConditionType.CUSTOM and not v:
            raise ValueError('自定义条件类型必须提供条件表达式')
        return v


# ==================== 工作流请求模型 ====================

class CreateWorkflowRequest(BaseModel):
    """创建工作流请求模型"""
    workflow_id: str = Field(..., description="工作流ID", min_length=1, max_length=100)
    workflow_name: str = Field(..., description="工作流名称", min_length=1, max_length=255)
    display_name: str = Field(..., description="显示名称", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="工作流描述", max_length=1000)
    category: Optional[str] = Field(None, description="工作流分类", max_length=100)
    business_domain: Optional[str] = Field(None, description="业务领域", max_length=100)
    tags: Optional[List[str]] = Field(None, description="标签列表")
    trigger_type: TriggerType = Field(TriggerType.MANUAL, description="触发方式")
    schedule_config: Optional[ScheduleConfig] = Field(None, description="调度配置")
    workflow_config: Optional[Dict[str, Any]] = Field(None, description="工作流全局配置")
    default_timeout: Optional[int] = Field(3600, description="默认超时时间(秒)", ge=1)
    max_parallel_tasks: Optional[int] = Field(10, description="最大并行任务数", ge=1, le=100)
    retry_policy: Optional[RetryPolicy] = Field(None, description="全局重试策略")
    visibility: Optional[str] = Field("private", description="可见性")
    nodes: List[WorkflowNodeRequest] = Field(..., description="节点列表", min_items=1)
    edges: Optional[List[WorkflowEdgeRequest]] = Field(None, description="边列表")
    variables: Optional[List[VariableDefinition]] = Field(None, description="工作流变量")
    canvas_config: Optional[Dict[str, Any]] = Field(None, description="画布配置")
    layout_config: Optional[Dict[str, Any]] = Field(None, description="布局配置")

    @field_validator('workflow_id')
    def validate_workflow_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('工作流ID只能包含字母、数字、下划线和短横线')
        return v

    @field_validator('nodes')
    def validate_nodes(cls, v):
        if not v:
            raise ValueError('工作流至少需要一个节点')

        # 检查节点ID唯一性
        node_ids = [node.node_id for node in v]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError('节点ID不能重复')

        # 检查是否有开始节点和结束节点
        start_nodes = [node for node in v if node.is_start_node]
        end_nodes = [node for node in v if node.is_end_node]

        if not start_nodes:
            raise ValueError('工作流必须有至少一个开始节点')
        if not end_nodes:
            raise ValueError('工作流必须有至少一个结束节点')

        return v

    @field_validator('edges')
    def validate_edges(cls, v, values):
        if not v:
            return v

        nodes = values.get('nodes', [])
        node_ids = {node.node_id for node in nodes}

        # 检查边ID唯一性
        edge_ids = [edge.edge_id for edge in v]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError('边ID不能重复')

        # 检查源节点和目标节点是否存在
        for edge in v:
            if edge.source_node_id not in node_ids:
                raise ValueError(f'源节点 {edge.source_node_id} 不存在')
            if edge.target_node_id not in node_ids:
                raise ValueError(f'目标节点 {edge.target_node_id} 不存在')
            if edge.source_node_id == edge.target_node_id:
                raise ValueError('源节点和目标节点不能相同')

        return v

    class Config:
        schema_extra = {
            "example": {
                "workflow_id": "data_processing_workflow",
                "workflow_name": "数据处理工作流",
                "display_name": "每日数据处理工作流",
                "description": "处理每日业务数据的完整工作流",
                "category": "数据处理",
                "business_domain": "业务数据",
                "tags": ["数据处理", "每日任务"],
                "trigger_type": "schedule",
                "schedule_config": {
                    "schedule_interval": "0 2 * * *",
                    "start_date": "2024-01-01T00:00:00",
                    "catchup": False,
                    "max_active_runs": 1
                },
                "nodes": [
                    {
                        "node_id": "start",
                        "node_name": "开始",
                        "display_name": "开始",
                        "node_type": "start",
                        "is_start_node": True,
                        "position": {"x": 100, "y": 100}
                    },
                    {
                        "node_id": "extract_data",
                        "node_name": "数据抽取",
                        "display_name": "抽取业务数据",
                        "node_type": "sql",
                        "task_config": {
                            "sql": "SELECT * FROM business_data WHERE date = '{{ ds }}'",
                            "connection_id": "mysql_business"
                        },
                        "position": {"x": 300, "y": 100}
                    }
                ],
                "edges": [
                    {
                        "edge_id": "start_to_extract",
                        "source_node_id": "start",
                        "target_node_id": "extract_data",
                        "condition_type": "always"
                    }
                ]
            }
        }


class UpdateWorkflowRequest(BaseModel):
    """更新工作流请求模型"""
    workflow_name: Optional[str] = Field(None, description="工作流名称", min_length=1, max_length=255)
    display_name: Optional[str] = Field(None, description="显示名称", min_length=1, max_length=255)
    description: Optional[str] = Field(None, description="工作流描述", max_length=1000)
    category: Optional[str] = Field(None, description="工作流分类", max_length=100)
    business_domain: Optional[str] = Field(None, description="业务领域", max_length=100)
    tags: Optional[List[str]] = Field(None, description="标签列表")
    trigger_type: Optional[TriggerType] = Field(None, description="触发方式")
    schedule_config: Optional[ScheduleConfig] = Field(None, description="调度配置")
    workflow_config: Optional[Dict[str, Any]] = Field(None, description="工作流全局配置")
    default_timeout: Optional[int] = Field(None, description="默认超时时间(秒)", ge=1)
    max_parallel_tasks: Optional[int] = Field(None, description="最大并行任务数", ge=1, le=100)
    retry_policy: Optional[RetryPolicy] = Field(None, description="全局重试策略")
    visibility: Optional[str] = Field(None, description="可见性")
    nodes: Optional[List[WorkflowNodeRequest]] = Field(None, description="节点列表")
    edges: Optional[List[WorkflowEdgeRequest]] = Field(None, description="边列表")
    variables: Optional[List[VariableDefinition]] = Field(None, description="工作流变量")
    canvas_config: Optional[Dict[str, Any]] = Field(None, description="画布配置")
    layout_config: Optional[Dict[str, Any]] = Field(None, description="布局配置")


class TriggerWorkflowRequest(BaseModel):
    """触发工作流请求模型"""
    execution_name: Optional[str] = Field(None, description="执行名称", max_length=255)
    execution_date: Optional[datetime] = Field(None, description="执行日期")
    runtime_variables: Optional[Dict[str, Any]] = Field(None, description="运行时变量")
    execution_config: Optional[Dict[str, Any]] = Field(None, description="执行配置")


class WorkflowStatusUpdateRequest(BaseModel):
    """工作流状态更新请求模型"""
    status: WorkflowStatus = Field(..., description="新状态")
    reason: Optional[str] = Field(None, description="状态变更原因", max_length=500)


# ==================== 响应模型 ====================

class WorkflowNodeResponse(BaseModel):
    """工作流节点响应模型"""
    id: int = Field(..., description="节点数据库ID")
    node_id: str = Field(..., description="节点ID")
    node_name: str = Field(..., description="节点名称")
    display_name: str = Field(..., description="显示名称")
    description: Optional[str] = Field(None, description="节点描述")
    node_type: NodeType = Field(..., description="节点类型")
    task_config: Optional[Dict[str, Any]] = Field(None, description="任务配置")
    timeout: Optional[int] = Field(None, description="超时时间(秒)")
    retry_times: int = Field(..., description="重试次数")
    retry_interval: int = Field(..., description="重试间隔(秒)")
    condition_expression: Optional[str] = Field(None, description="条件表达式")
    required_cpu: Optional[float] = Field(None, description="所需CPU核数")
    required_memory_mb: Optional[int] = Field(None, description="所需内存(MB)")
    position_x: Optional[float] = Field(None, description="X坐标")
    position_y: Optional[float] = Field(None, description="Y坐标")
    width: Optional[float] = Field(None, description="宽度")
    height: Optional[float] = Field(None, description="高度")
    style_config: Optional[Dict[str, Any]] = Field(None, description="样式配置")
    is_start_node: bool = Field(..., description="是否为开始节点")
    is_end_node: bool = Field(..., description="是否为结束节点")
    is_critical: bool = Field(..., description="是否为关键节点")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowEdgeResponse(BaseModel):
    """工作流边响应模型"""
    id: int = Field(..., description="边数据库ID")
    edge_id: str = Field(..., description="边ID")
    edge_name: Optional[str] = Field(None, description="边名称")
    source_node_id: int = Field(..., description="源节点数据库ID")
    target_node_id: int = Field(..., description="目标节点数据库ID")
    condition_type: ConditionType = Field(..., description="依赖条件类型")
    condition_expression: Optional[str] = Field(None, description="自定义条件表达式")
    path_config: Optional[Dict[str, Any]] = Field(None, description="路径配置")
    style_config: Optional[Dict[str, Any]] = Field(None, description="样式配置")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowResponse(BaseModel):
    """工作流响应模型"""
    id: int = Field(..., description="工作流数据库ID")
    workflow_id: str = Field(..., description="工作流ID")
    workflow_name: str = Field(..., description="工作流名称")
    display_name: str = Field(..., description="显示名称")
    description: Optional[str] = Field(None, description="工作流描述")
    category: Optional[str] = Field(None, description="工作流分类")
    business_domain: Optional[str] = Field(None, description="业务领域")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    status: WorkflowStatus = Field(..., description="工作流状态")
    version: str = Field(..., description="版本号")
    trigger_type: TriggerType = Field(..., description="触发方式")
    schedule_config: Optional[Dict[str, Any]] = Field(None, description="调度配置")
    workflow_config: Optional[Dict[str, Any]] = Field(None, description="工作流全局配置")
    default_timeout: Optional[int] = Field(None, description="默认超时时间(秒)")
    max_parallel_tasks: Optional[int] = Field(None, description="最大并行任务数")
    retry_policy: Optional[Dict[str, Any]] = Field(None, description="重试策略配置")
    canvas_config: Optional[Dict[str, Any]] = Field(None, description="画布配置信息")
    layout_config: Optional[Dict[str, Any]] = Field(None, description="布局配置信息")
    owner_id: Optional[str] = Field(None, description="拥有者ID")
    owner_name: Optional[str] = Field(None, description="拥有者姓名")
    visibility: str = Field(..., description="可见性")
    total_executions: int = Field(..., description="总执行次数")
    successful_executions: int = Field(..., description="成功执行次数")
    failed_executions: int = Field(..., description="失败执行次数")
    last_execution_time: Optional[datetime] = Field(None, description="最后执行时间")
    avg_execution_duration: Optional[int] = Field(None, description="平均执行时长(秒)")
    is_active: bool = Field(..., description="是否激活")
    is_template: bool = Field(..., description="是否为模板")
    nodes: Optional[List[WorkflowNodeResponse]] = Field(None, description="节点列表")
    edges: Optional[List[WorkflowEdgeResponse]] = Field(None, description="边列表")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowExecutionResponse(BaseModel):
    """工作流执行响应模型"""
    id: int = Field(..., description="执行记录数据库ID")
    execution_id: str = Field(..., description="执行唯一标识")
    execution_name: Optional[str] = Field(None, description="执行名称")
    workflow_id: int = Field(..., description="关联工作流ID")
    workflow_version: Optional[str] = Field(None, description="工作流版本")
    status: WorkflowStatus = Field(..., description="执行状态")
    trigger_type: TriggerType = Field(..., description="触发方式")
    trigger_user: Optional[str] = Field(None, description="触发用户")
    start_time: datetime = Field(..., description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    execution_date: datetime = Field(..., description="执行日期")
    total_nodes: int = Field(..., description="总节点数")
    completed_nodes: int = Field(..., description="已完成节点数")
    failed_nodes: int = Field(..., description="失败节点数")
    skipped_nodes: int = Field(..., description="跳过节点数")
    result_summary: Optional[Dict[str, Any]] = Field(None, description="执行结果摘要")
    error_message: Optional[str] = Field(None, description="错误消息")
    execution_config: Optional[Dict[str, Any]] = Field(None, description="执行时配置")
    runtime_variables: Optional[Dict[str, Any]] = Field(None, description="运行时变量")
    log_file_path: Optional[str] = Field(None, description="日志文件路径")
    metrics_data: Optional[Dict[str, Any]] = Field(None, description="监控指标数据")
    duration_seconds: Optional[int] = Field(None, description="执行总时长(秒)")
    progress_percent: Optional[float] = Field(None, description="执行进度百分比")
    estimated_remaining_time: Optional[int] = Field(None, description="预估剩余时间(秒)")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")

    @field_validator('progress_percent')
    def validate_progress_percent(cls, v):
        if v is not None and not 0 <= v <= 100:
            raise ValueError('执行进度百分比必须在0-100之间')
        return v


class WorkflowNodeExecutionResponse(BaseModel):
    """工作流节点执行响应模型"""
    id: int = Field(..., description="节点执行记录数据库ID")
    node_execution_id: str = Field(..., description="节点执行唯一标识")
    workflow_execution_id: int = Field(..., description="工作流执行ID")
    node_definition_id: int = Field(..., description="节点定义ID")
    status: NodeStatus = Field(..., description="节点执行状态")
    start_time: Optional[datetime] = Field(None, description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    duration_seconds: Optional[int] = Field(None, description="执行时长(秒)")
    retry_count: int = Field(..., description="重试次数")
    max_retry_count: int = Field(..., description="最大重试次数")
    result_data: Optional[Dict[str, Any]] = Field(None, description="执行结果数据")
    output_variables: Optional[Dict[str, Any]] = Field(None, description="输出变量")
    error_message: Optional[str] = Field(None, description="错误消息")
    error_stack_trace: Optional[str] = Field(None, description="错误堆栈信息")
    cpu_usage_percent: Optional[float] = Field(None, description="CPU使用率")
    memory_usage_mb: Optional[float] = Field(None, description="内存使用量(MB)")
    external_task_id: Optional[str] = Field(None, description="外部任务ID")
    external_execution_id: Optional[str] = Field(None, description="外部执行ID")
    log_file_path: Optional[str] = Field(None, description="日志文件路径")
    log_url: Optional[str] = Field(None, description="日志访问URL")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowTemplateResponse(BaseModel):
    """工作流模板响应模型"""
    id: int = Field(..., description="模板数据库ID")
    template_name: str = Field(..., description="模板名称")
    display_name: str = Field(..., description="显示名称")
    description: Optional[str] = Field(None, description="模板描述")
    category: Optional[str] = Field(None, description="模板分类")
    business_scenario: Optional[str] = Field(None, description="业务场景")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    template_config: Dict[str, Any] = Field(..., description="模板配置JSON")
    default_variables: Optional[Dict[str, Any]] = Field(None, description="默认变量配置")
    parameter_schema: Optional[Dict[str, Any]] = Field(None, description="参数Schema定义")
    usage_count: int = Field(..., description="使用次数")
    is_active: bool = Field(..., description="是否激活")
    is_builtin: bool = Field(..., description="是否内置模板")
    is_public: bool = Field(..., description="是否公开模板")
    created_by: Optional[str] = Field(None, description="创建者")
    created_by_name: Optional[str] = Field(None, description="创建者姓名")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowVariableResponse(BaseModel):
    """工作流变量响应模型"""
    id: int = Field(..., description="变量数据库ID")
    variable_key: str = Field(..., description="变量键")
    variable_name: str = Field(..., description="变量名称")
    description: Optional[str] = Field(None, description="变量描述")
    workflow_id: int = Field(..., description="关联工作流ID")
    variable_type: str = Field(..., description="变量类型")
    default_value: Optional[str] = Field(None, description="默认值")
    is_required: bool = Field(..., description="是否必填")
    is_sensitive: bool = Field(..., description="是否敏感信息")
    validation_rules: Optional[Dict[str, Any]] = Field(None, description="验证规则")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class WorkflowAlertResponse(BaseModel):
    """工作流告警规则响应模型"""
    id: int = Field(..., description="告警规则数据库ID")
    alert_name: str = Field(..., description="告警规则名称")
    description: Optional[str] = Field(None, description="告警描述")
    workflow_id: int = Field(..., description="关联工作流ID")
    alert_type: str = Field(..., description="告警类型")
    condition_expression: str = Field(..., description="告警条件表达式")
    severity_level: SeverityLevel = Field(..., description="严重级别")
    notification_channels: Optional[Dict[str, Any]] = Field(None, description="通知渠道配置")
    notification_recipients: Optional[Dict[str, Any]] = Field(None, description="通知接收人")
    is_active: bool = Field(..., description="是否激活")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


# ==================== 列表和搜索模型 ====================

class WorkflowSearchParams(BaseModel):
    """工作流搜索参数模型"""
    keyword: Optional[str] = Field(None, description="搜索关键词")
    category: Optional[str] = Field(None, description="工作流分类")
    business_domain: Optional[str] = Field(None, description="业务领域")
    status: Optional[WorkflowStatus] = Field(None, description="工作流状态")
    trigger_type: Optional[TriggerType] = Field(None, description="触发方式")
    owner_id: Optional[str] = Field(None, description="拥有者ID")
    visibility: Optional[str] = Field(None, description="可见性")
    is_template: Optional[bool] = Field(None, description="是否为模板")
    tags: Optional[List[str]] = Field(None, description="标签过滤")
    created_start: Optional[datetime] = Field(None, description="创建开始时间")
    created_end: Optional[datetime] = Field(None, description="创建结束时间")
    page: int = Field(1, description="页码", ge=1)
    page_size: int = Field(20, description="每页大小", ge=1, le=100)
    sort_by: Optional[str] = Field("created_at", description="排序字段")
    sort_order: Optional[str] = Field("desc", description="排序顺序(asc/desc)")


class WorkflowListResponse(BaseModel):
    """工作流列表响应模型"""
    workflows: List[WorkflowResponse] = Field(..., description="工作流列表")
    total: int = Field(..., description="总数量")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页大小")
    total_pages: int = Field(..., description="总页数")
    has_next: bool = Field(..., description="是否有下一页")
    has_prev: bool = Field(..., description="是否有上一页")


class WorkflowExecutionSearchParams(BaseModel):
    """工作流执行搜索参数模型"""
    workflow_id: Optional[str] = Field(None, description="工作流ID")
    status: Optional[WorkflowStatus] = Field(None, description="执行状态")
    trigger_type: Optional[TriggerType] = Field(None, description="触发方式")
    trigger_user: Optional[str] = Field(None, description="触发用户")
    start_time_begin: Optional[datetime] = Field(None, description="开始时间范围开始")
    start_time_end: Optional[datetime] = Field(None, description="开始时间范围结束")
    execution_date_begin: Optional[datetime] = Field(None, description="执行日期范围开始")
    execution_date_end: Optional[datetime] = Field(None, description="执行日期范围结束")
    page: int = Field(1, description="页码", ge=1)
    page_size: int = Field(20, description="每页大小", ge=1, le=100)
    sort_by: Optional[str] = Field("start_time", description="排序字段")
    sort_order: Optional[str] = Field("desc", description="排序顺序(asc/desc)")


class WorkflowExecutionListResponse(BaseModel):
    """工作流执行列表响应模型"""
    executions: List[WorkflowExecutionResponse] = Field(..., description="执行记录列表")
    total: int = Field(..., description="总数量")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页大小")
    total_pages: int = Field(..., description="总页数")
    has_next: bool = Field(..., description="是否有下一页")
    has_prev: bool = Field(..., description="是否有上一页")


class WorkflowStatistics(BaseModel):
    """工作流统计模型"""
    total_workflows: int = Field(..., description="工作流总数")
    active_workflows: int = Field(..., description="活跃工作流数")
    draft_workflows: int = Field(..., description="草稿工作流数")
    template_workflows: int = Field(..., description="模板工作流数")
    total_executions_today: int = Field(..., description="今日总执行数")
    successful_executions_today: int = Field(..., description="今日成功执行数")
    failed_executions_today: int = Field(..., description="今日失败执行数")
    running_executions: int = Field(..., description="当前运行中执行数")
    avg_execution_duration: Optional[float] = Field(None, description="平均执行时长(秒)")
    success_rate: float = Field(..., description="成功率(%)")
    most_used_node_types: List[Dict[str, Any]] = Field(..., description="最常用节点类型统计")
    workflow_categories: List[Dict[str, Any]] = Field(..., description="工作流分类统计")


class WorkflowHealthCheck(BaseModel):
    """工作流健康检查模型"""
    workflow_id: str = Field(..., description="工作流ID")
    workflow_name: str = Field(..., description="工作流名称")
    health_score: float = Field(..., description="健康分数(0-100)")
    issues: List[Dict[str, Any]] = Field(..., description="问题列表")
    recommendations: List[str] = Field(..., description="建议列表")
    last_check_time: datetime = Field(..., description="最后检查时间")

# 添加验证相关模型
class WorkflowValidationResult(BaseModel):
    """工作流验证结果"""
    is_valid: bool = Field(..., description="是否验证通过")
    errors: List[str] = Field(default_factory=list, description="验证错误列表")
    warnings: List[str] = Field(default_factory=list, description="验证警告列表")
    validation_time: datetime = Field(default_factory=datetime.now, description="验证时间")


class WorkflowDependencyGraph(BaseModel):
    """工作流依赖关系图"""
    workflow_id: int = Field(..., description="工作流ID")
    nodes: List[Dict[str, Any]] = Field(..., description="节点列表")
    edges: List[Dict[str, Any]] = Field(..., description="边列表")
    layers: List[List[str]] = Field(..., description="拓扑分层")
    max_depth: int = Field(..., description="最大深度")
    has_cycles: bool = Field(..., description="是否存在循环依赖")

class BatchWorkflowOperationRequest(BaseModel):
    """批量工作流操作请求"""
    workflow_ids: List[int] = Field(..., description="工作流ID列表")
    operation: str = Field(..., description="操作类型")
    operation_params: Optional[Dict[str, Any]] = Field(None, description="操作参数")

class BatchOperationResult(BaseModel):
    """批量操作结果"""
    total: int = Field(..., description="总数")
    successful: int = Field(..., description="成功数量")
    failed: int = Field(..., description="失败数量")
    success_ids: List[int] = Field(..., description="成功的工作流ID")
    failed_results: List[Dict[str, Any]] = Field(..., description="失败结果详情")
    operation_time: datetime = Field(..., description="操作时间")


class WorkflowTemplateCategory(str, Enum):
    """工作流模板分类"""
    DATA_PROCESSING = "data_processing"
    ETL = "etl"
    API_INTEGRATION = "api_integration"
    NOTIFICATION = "notification"
    APPROVAL = "approval"
    MONITORING = "monitoring"
    CUSTOM = "custom"


class CreateWorkflowTemplateRequest(BaseModel):
    """创建工作流模板请求"""
    template_name: str = Field(..., min_length=1, max_length=100, description="模板名称")
    display_name: str = Field(..., min_length=1, max_length=200, description="显示名称")
    description: Optional[str] = Field(None, max_length=1000, description="模板描述")
    category: WorkflowTemplateCategory = Field(..., description="模板分类")
    business_scenario: Optional[str] = Field(None, max_length=200, description="业务场景")
    tags: Optional[List[str]] = Field(default_factory=list, description="标签列表")

    # 模板配置
    template_config: Dict[str, Any] = Field(..., description="模板配置JSON")
    default_variables: Optional[Dict[str, Any]] = Field(None, description="默认变量配置")
    parameter_schema: Optional[Dict[str, Any]] = Field(None, description="参数Schema定义")

    # 模板属性
    is_public: bool = Field(True, description="是否公开模板")
    is_builtin: bool = Field(False, description="是否内置模板")
    required_permissions: Optional[List[str]] = Field(None, description="所需权限列表")

    # 使用限制
    max_instances: Optional[int] = Field(None, description="最大实例数限制")
    usage_limit: Optional[int] = Field(None, description="使用次数限制")

    # 版本控制
    version: str = Field("1.0.0", description="模板版本")
    changelog: Optional[str] = Field(None, description="版本变更日志")


class CreateWorkflowFromTemplateRequest(BaseModel):
    """从模板创建工作流请求"""
    template_id: int = Field(..., description="模板ID")
    workflow_name: str = Field(..., min_length=1, max_length=100, description="新工作流名称")
    display_name: Optional[str] = Field(None, max_length=200, description="显示名称")
    description: Optional[str] = Field(None, max_length=1000, description="工作流描述")

    # 参数替换
    parameter_values: Optional[Dict[str, Any]] = Field(None, description="模板参数值")
    variable_overrides: Optional[Dict[str, Any]] = Field(None, description="变量覆盖值")

    # 自定义配置
    custom_config: Optional[Dict[str, Any]] = Field(None, description="自定义配置")
    category: Optional[str] = Field(None, description="工作流分类")
    business_domain: Optional[str] = Field(None, description="业务域")
    tags: Optional[List[str]] = Field(default_factory=list, description="标签列表")

    # 执行设置
    trigger_type: Optional[str] = Field(None, description="触发方式")
    schedule_config: Optional[Dict[str, Any]] = Field(None, description="调度配置")

    # 权限设置
    visibility: str = Field("private", description="可见性")
    owner_permissions: Optional[List[str]] = Field(None, description="所有者权限")


class ExportFormat(str, Enum):
    """导出格式"""
    JSON = "json"
    YAML = "yaml"
    XML = "xml"


class WorkflowExportRequest(BaseModel):
    """工作流导出请求"""
    workflow_ids: List[int] = Field(..., min_items=1, description="要导出的工作流ID列表")
    export_format: ExportFormat = Field(ExportFormat.JSON, description="导出格式")

    # 导出选项
    include_execution_history: bool = Field(False, description="是否包含执行历史")
    include_statistics: bool = Field(False, description="是否包含统计信息")
    include_variables: bool = Field(True, description="是否包含变量定义")
    include_templates: bool = Field(False, description="是否包含关联模板")

    # 过滤选项
    date_range_start: Optional[datetime] = Field(None, description="导出数据开始时间")
    date_range_end: Optional[datetime] = Field(None, description="导出数据结束时间")
    execution_status_filter: Optional[List[str]] = Field(None, description="执行状态过滤")

    # 导出设置
    compress: bool = Field(True, description="是否压缩导出文件")
    password_protect: Optional[str] = Field(None, description="密码保护")
    split_large_files: bool = Field(False, description="是否拆分大文件")
    max_file_size_mb: int = Field(100, description="最大文件大小(MB)")


class WorkflowImportRequest(BaseModel):
    """工作流导入请求"""
    import_data: str = Field(..., description="导入数据内容")
    import_format: ExportFormat = Field(ExportFormat.JSON, description="导入格式")

    # 导入选项
    import_mode: str = Field("create_new", description="导入模式: create_new, update_existing, merge")
    conflict_resolution: str = Field("skip", description="冲突解决策略: skip, overwrite, rename")

    # 数据选项
    import_execution_history: bool = Field(False, description="是否导入执行历史")
    import_variables: bool = Field(True, description="是否导入变量")
    import_templates: bool = Field(False, description="是否导入模板")

    # 验证选项
    validate_before_import: bool = Field(True, description="导入前是否验证")
    dry_run: bool = Field(False, description="是否仅模拟导入")

    # 目标设置
    target_category: Optional[str] = Field(None, description="目标分类")
    target_owner: Optional[str] = Field(None, description="目标所有者")
    name_prefix: Optional[str] = Field(None, description="名称前缀")
    name_suffix: Optional[str] = Field(None, description="名称后缀")


class ImportResultItem(BaseModel):
    """单个导入结果项"""
    original_name: str = Field(..., description="原始名称")
    imported_name: str = Field(..., description="导入后名称")
    workflow_id: Optional[int] = Field(None, description="导入后的工作流ID")
    status: str = Field(..., description="导入状态")
    message: Optional[str] = Field(None, description="状态消息")
    errors: Optional[List[str]] = Field(None, description="错误信息")
    warnings: Optional[List[str]] = Field(None, description="警告信息")


class WorkflowImportResult(BaseModel):
    """工作流导入结果"""
    import_id: str = Field(..., description="导入任务ID")
    total_count: int = Field(..., description="总数量")
    success_count: int = Field(..., description="成功数量")
    failed_count: int = Field(..., description="失败数量")
    skipped_count: int = Field(..., description="跳过数量")

    # 详细结果
    success_results: List[ImportResultItem] = Field(default_factory=list, description="成功结果")
    failed_results: List[ImportResultItem] = Field(default_factory=list, description="失败结果")
    skipped_results: List[ImportResultItem] = Field(default_factory=list, description="跳过结果")

    # 统计信息
    import_time: datetime = Field(default_factory=datetime.now, description="导入时间")
    duration_seconds: Optional[float] = Field(None, description="导入耗时")

    # 附加信息
    validation_errors: Optional[List[str]] = Field(None, description="验证错误")
    import_warnings: Optional[List[str]] = Field(None, description="导入警告")
    summary_message: Optional[str] = Field(None, description="摘要消息")


class AlertType(str, Enum):
    """告警类型"""
    EXECUTION_FAILURE = "execution_failure"
    EXECUTION_TIMEOUT = "execution_timeout"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    DEPENDENCY_FAILURE = "dependency_failure"
    SCHEDULE_MISS = "schedule_miss"
    CUSTOM = "custom"


class AlertChannel(str, Enum):
    """告警渠道"""
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"
    SLACK = "slack"


class AlertCondition(BaseModel):
    """告警条件"""
    metric: str = Field(..., description="监控指标")
    operator: str = Field(..., description="比较操作符: gt, lt, eq, ne, gte, lte")
    threshold: float = Field(..., description="阈值")
    duration_minutes: Optional[int] = Field(None, description="持续时间(分钟)")


class CreateWorkflowAlertRequest(BaseModel):
    """创建工作流告警请求"""
    alert_name: str = Field(..., min_length=1, max_length=100, description="告警名称")
    description: Optional[str] = Field(None, max_length=500, description="告警描述")
    workflow_id: int = Field(..., description="关联工作流ID")

    # 告警类型和级别
    alert_type: AlertType = Field(..., description="告警类型")
    severity: str = Field("medium", description="严重级别: low, medium, high, critical")

    # 触发条件
    conditions: List[AlertCondition] = Field(..., min_items=1, description="触发条件列表")
    condition_logic: str = Field("AND", description="条件逻辑: AND, OR")

    # 通知设置
    notification_channels: List[AlertChannel] = Field(..., min_items=1, description="通知渠道")
    recipients: List[str] = Field(..., min_items=1, description="接收人列表")

    # 告警配置
    is_enabled: bool = Field(True, description="是否启用")
    suppress_duration_minutes: int = Field(60, description="告警抑制时长(分钟)")
    max_alerts_per_hour: int = Field(10, description="每小时最大告警次数")

    # 自定义消息
    custom_message_template: Optional[str] = Field(None, description="自定义消息模板")
    include_execution_context: bool = Field(True, description="是否包含执行上下文")

    # 自动恢复
    auto_resolve: bool = Field(False, description="是否自动恢复")
    resolve_conditions: Optional[List[AlertCondition]] = Field(None, description="恢复条件")