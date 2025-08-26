"""
调度管理相关的Pydantic模式定义
"""

from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class TaskType(str, Enum):
    """任务类型枚举"""
    SQL = "sql"
    SHELL = "shell"
    DATAX = "datax"
    PYTHON = "python"
    MULTI = "multi"


class TaskStatus(str, Enum):
    """任务状态枚举"""
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    QUEUED = "queued"
    RETRY = "retry"
    SKIPPED = "skipped"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    REMOVED = "removed"


class DAGState(str, Enum):
    """DAG状态枚举"""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    QUEUED = "queued"


# ==================== 基础模型 ====================

class ScheduleConfigRequest(BaseModel):
    """调度配置请求模型"""
    schedule_interval: Optional[str] = Field(
        None,
        description="调度表达式(Cron格式)，None为手动触发",
        example="0 2 * * *"
    )
    start_date: str = Field(
        ...,
        description="开始日期(YYYY-MM-DD格式)",
        example="2024-01-01"
    )
    catchup: bool = Field(
        False,
        description="是否执行历史补偿"
    )
    max_active_runs: int = Field(
        1,
        description="最大并发运行数",
        ge=1,
        le=10
    )
    max_active_tasks: int = Field(
        16,
        description="最大并发任务数",
        ge=1,
        le=50
    )
    retries: int = Field(
        3,
        description="重试次数",
        ge=0,
        le=10
    )
    retry_delay_seconds: int = Field(
        300,
        description="重试延迟(秒)",
        ge=0
    )

    @validator('schedule_interval')
    def validate_schedule_interval(cls, v):
        if v is not None and v.strip() == "":
            return None
        return v


class TaskConfigRequest(BaseModel):
    """任务配置请求模型"""
    task_id: str = Field(
        ...,
        description="任务ID",
        min_length=1,
        max_length=100
    )
    type: TaskType = Field(..., description="任务类型")
    description: Optional[str] = Field(
        None,
        description="任务描述",
        max_length=500
    )

    # SQL任务专用字段
    sql: Optional[str] = Field(None, description="SQL语句")
    sql_file: Optional[str] = Field(None, description="SQL文件路径")
    connection: Optional[Dict[str, Any]] = Field(None, description="数据库连接配置")

    # Shell任务专用字段
    command: Optional[str] = Field(None, description="Shell命令")
    script_file: Optional[str] = Field(None, description="脚本文件路径")

    # DataX任务专用字段
    config: Optional[Dict[str, Any]] = Field(None, description="DataX配置")
    config_file: Optional[str] = Field(None, description="DataX配置文件路径")

    # Python任务专用字段
    python_code: Optional[str] = Field(None, description="Python代码")

    # 通用字段
    timeout: Optional[int] = Field(
        None,
        description="任务超时时间(秒)",
        ge=1
    )
    depends_on: Optional[List[str]] = Field(
        None,
        description="依赖的任务ID列表"
    )
    priority_weight: int = Field(
        1,
        description="任务优先级权重",
        ge=1,
        le=10
    )
    pool: Optional[str] = Field(
        None,
        description="任务池名称"
    )

    @validator('task_id')
    def validate_task_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('任务ID只能包含字母、数字、下划线和短横线')
        return v


class TaskDependency(BaseModel):
    """任务依赖关系模型"""
    upstream: str = Field(..., description="上游任务ID")
    downstream: str = Field(..., description="下游任务ID")


# ==================== 请求模型 ====================

class CreateDAGRequest(BaseModel):
    """创建DAG请求模型"""
    dag_id: str = Field(
        ...,
        description="DAG唯一标识",
        min_length=1,
        max_length=100
    )
    display_name: str = Field(
        ...,
        description="显示名称",
        min_length=1,
        max_length=200
    )
    description: Optional[str] = Field(
        None,
        description="DAG描述",
        max_length=1000
    )
    task_type: TaskType = Field(..., description="主要任务类型")
    tasks: List[TaskConfigRequest] = Field(
        ...,
        description="任务列表",
        min_items=1
    )
    dependencies: Optional[List[TaskDependency]] = Field(
        None,
        description="任务依赖关系"
    )
    schedule: Optional[ScheduleConfigRequest] = Field(
        None,
        description="调度配置"
    )
    owner: str = Field(
        "bigdata-platform",
        description="DAG所有者"
    )
    tags: Optional[List[str]] = Field(
        None,
        description="标签列表"
    )

    @validator('dag_id')
    def validate_dag_id(cls, v):
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('DAG ID只能包含字母、数字、下划线和短横线')
        return v

    @validator('tasks')
    def validate_tasks(cls, v):
        task_ids = [task.task_id for task in v]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError('任务ID不能重复')
        return v

    class Config:
        schema_extra = {
            "example": {
                "dag_id": "daily_data_sync",
                "display_name": "每日数据同步",
                "description": "每日凌晨执行数据同步任务",
                "task_type": "datax",
                "tasks": [
                    {
                        "task_id": "sync_user_data",
                        "type": "datax",
                        "description": "同步用户数据",
                        "config_file": "/opt/datax/jobs/sync_users.json"
                    }
                ],
                "schedule": {
                    "schedule_interval": "0 2 * * *",
                    "start_date": "2024-01-01",
                    "catchup": False
                },
                "owner": "bigdata-platform",
                "tags": ["sync", "daily"]
            }
        }


class UpdateDAGRequest(BaseModel):
    """更新DAG请求模型"""
    display_name: Optional[str] = Field(
        None,
        description="显示名称",
        min_length=1,
        max_length=200
    )
    description: Optional[str] = Field(
        None,
        description="DAG描述",
        max_length=1000
    )
    task_type: Optional[TaskType] = Field(None, description="主要任务类型")
    tasks: Optional[List[TaskConfigRequest]] = Field(
        None,
        description="任务列表"
    )
    dependencies: Optional[List[TaskDependency]] = Field(
        None,
        description="任务依赖关系"
    )
    schedule: Optional[ScheduleConfigRequest] = Field(
        None,
        description="调度配置"
    )
    owner: Optional[str] = Field(None, description="DAG所有者")
    tags: Optional[List[str]] = Field(None, description="标签列表")

    @validator('tasks')
    def validate_tasks(cls, v):
        if v is not None:
            task_ids = [task.task_id for task in v]
            if len(task_ids) != len(set(task_ids)):
                raise ValueError('任务ID不能重复')
        return v


class TriggerDAGRequest(BaseModel):
    """触发DAG请求模型"""
    conf: Optional[Dict[str, Any]] = Field(
        None,
        description="DAG运行配置参数"
    )
    execution_date: Optional[str] = Field(
        None,
        description="执行日期时间(ISO格式)"
    )
    note: Optional[str] = Field(
        None,
        description="触发说明",
        max_length=500
    )


# ==================== 响应模型 ====================

class DAGInfo(BaseModel):
    """DAG信息模型"""
    dag_id: str = Field(..., description="DAG ID")
    description: Optional[str] = Field(None, description="描述")
    schedule_interval: Optional[str] = Field(None, description="调度间隔")
    is_paused: bool = Field(..., description="是否暂停")
    is_active: bool = Field(..., description="是否激活")
    last_parsed_time: Optional[datetime] = Field(None, description="最后解析时间")
    max_active_tasks: int = Field(..., description="最大活跃任务数")
    max_active_runs: int = Field(..., description="最大活跃运行数")
    has_task_concurrency_limits: bool = Field(..., description="是否有任务并发限制")
    has_import_errors: bool = Field(..., description="是否有导入错误")
    next_dagrun: Optional[datetime] = Field(None, description="下次运行时间")
    tags: List[str] = Field(default_factory=list, description="标签列表")
    owners: List[str] = Field(default_factory=list, description="所有者列表")
    is_platform_generated: bool = Field(
        False,
        description="是否为平台生成"
    )


class DAGStatusSummary(BaseModel):
    """DAG状态摘要模型"""
    dag_id: str = Field(..., description="DAG ID")
    total_runs: int = Field(..., description="总运行次数")
    period_days: int = Field(..., description="统计周期天数")
    status_summary: Dict[str, int] = Field(
        default_factory=dict,
        description="状态统计"
    )
    success_rate: float = Field(..., description="成功率百分比")
    error: Optional[str] = Field(None, description="错误信息")


class DAGRunInfo(BaseModel):
    """DAG运行信息模型"""
    dag_run_id: str = Field(..., description="DAG运行ID")
    dag_id: str = Field(..., description="DAG ID")
    execution_date: datetime = Field(..., description="执行日期")
    start_date: Optional[datetime] = Field(None, description="开始时间")
    end_date: Optional[datetime] = Field(None, description="结束时间")
    state: Optional[str] = Field(None, description="运行状态")
    run_type: str = Field(..., description="运行类型")
    conf: Optional[Dict[str, Any]] = Field(None, description="运行配置")
    external_trigger: bool = Field(..., description="是否外部触发")
    note: Optional[str] = Field(None, description="运行说明")


class TaskInstanceInfo(BaseModel):
    """任务实例信息模型"""
    task_id: str = Field(..., description="任务ID")
    dag_id: str = Field(..., description="DAG ID")
    execution_date: datetime = Field(..., description="执行日期")
    start_date: Optional[datetime] = Field(None, description="开始时间")
    end_date: Optional[datetime] = Field(None, description="结束时间")
    duration: Optional[float] = Field(None, description="持续时间(秒)")
    state: Optional[TaskStatus] = Field(None, description="任务状态")
    try_number: int = Field(..., description="尝试次数")
    max_tries: int = Field(..., description="最大尝试次数")
    hostname: Optional[str] = Field(None, description="执行主机")
    unixname: Optional[str] = Field(None, description="执行用户")
    job_id: Optional[int] = Field(None, description="作业ID")
    pool: str = Field(..., description="任务池")
    pool_slots: int = Field(..., description="任务池槽位")
    queue: str = Field(..., description="队列名称")
    priority_weight: int = Field(..., description="优先级权重")
    operator: str = Field(..., description="操作器类型")
    queued_dttm: Optional[datetime] = Field(None, description="入队时间")
    pid: Optional[int] = Field(None, description="进程ID")
    executor_config: Optional[Dict[str, Any]] = Field(
        None,
        description="执行器配置"
    )


class DAGListResponse(BaseModel):
    """DAG列表响应模型"""
    dags: List[Dict[str, Any]] = Field(..., description="DAG列表")
    total_entries: int = Field(..., description="总数")
    limit: int = Field(..., description="每页限制")
    offset: int = Field(..., description="偏移量")


class CreateDAGResponse(BaseModel):
    """创建DAG响应模型"""
    success: bool = Field(..., description="创建是否成功")
    dag_id: str = Field(..., description="DAG ID")
    dag_file_path: str = Field(..., description="DAG文件路径")
    template_used: str = Field(..., description="使用的模板")
    validation_result: Dict[str, Any] = Field(
        ...,
        description="验证结果"
    )
    generated_at: str = Field(..., description="生成时间")
    error: Optional[str] = Field(None, description="错误信息")


class SchedulerOverview(BaseModel):
    """调度器概览模型"""
    airflow_status: Dict[str, Any] = Field(..., description="Airflow状态")
    dag_statistics: Dict[str, int] = Field(..., description="DAG统计信息")
    system_info: Dict[str, Any] = Field(..., description="系统信息")


# ==================== 回调模型 ====================

class TaskStatusUpdate(BaseModel):
    """任务状态更新模型"""
    dag_id: str = Field(..., description="DAG ID")
    task_id: str = Field(..., description="任务ID")
    status: TaskStatus = Field(..., description="任务状态")
    execution_date: str = Field(..., description="执行日期")
    log_url: Optional[str] = Field(None, description="日志URL")
    start_date: Optional[datetime] = Field(None, description="开始时间")
    end_date: Optional[datetime] = Field(None, description="结束时间")
    duration: Optional[float] = Field(None, description="执行时长(秒)")
    try_number: int = Field(1, description="重试次数")
    error_message: Optional[str] = Field(None, description="错误消息")


# ==================== 便捷创建模型 ====================

class CreateSQLDAGRequest(BaseModel):
    """创建SQL DAG便捷请求模型"""
    dag_id: str = Field(..., description="DAG ID")
    sql_tasks: List[Dict[str, Any]] = Field(..., description="SQL任务配置")
    schedule_interval: Optional[str] = Field(None, description="调度表达式")
    display_name: Optional[str] = Field(None, description="显示名称")
    description: Optional[str] = Field(None, description="描述")

    class Config:
        schema_extra = {
            "example": {
                "dag_id": "daily_sql_tasks",
                "sql_tasks": [
                    {
                        "task_id": "update_user_stats",
                        "sql": "UPDATE user_stats SET last_login = NOW() WHERE active = 1",
                        "connection": {
                            "host": "localhost",
                            "port": 3306,
                            "username": "root",
                            "password": "password",
                            "database": "analytics"
                        }
                    }
                ],
                "schedule_interval": "0 1 * * *",
                "display_name": "每日SQL统计任务"
            }
        }


class CreateDataXDAGRequest(BaseModel):
    """创建DataX DAG便捷请求模型"""
    dag_id: str = Field(..., description="DAG ID")
    sync_tasks: List[Dict[str, Any]] = Field(..., description="同步任务配置")
    schedule_interval: Optional[str] = Field(None, description="调度表达式")
    display_name: Optional[str] = Field(None, description="显示名称")
    description: Optional[str] = Field(None, description="描述")

    class Config:
        schema_extra = {
            "example": {
                "dag_id": "daily_data_sync",
                "sync_tasks": [
                    {
                        "task_id": "sync_orders",
                        "config_file": "/opt/datax/jobs/orders_sync.json"
                    }
                ],
                "schedule_interval": "0 2 * * *",
                "display_name": "每日数据同步"
            }
        }


class CreateShellDAGRequest(BaseModel):
    """创建Shell DAG便捷请求模型"""
    dag_id: str = Field(..., description="DAG ID")
    shell_tasks: List[Dict[str, Any]] = Field(..., description="Shell任务配置")
    schedule_interval: Optional[str] = Field(None, description="调度表达式")
    display_name: Optional[str] = Field(None, description="显示名称")
    description: Optional[str] = Field(None, description="描述")

    class Config:
        schema_extra = {
            "example": {
                "dag_id": "daily_backup",
                "shell_tasks": [
                    {
                        "task_id": "backup_database",
                        "script_file": "/opt/scripts/backup_db.sh"
                    }
                ],
                "schedule_interval": "0 3 * * *",
                "display_name": "每日数据备份"
            }
        }


# ==================== 验证函数 ====================

def validate_cron_expression(cron_expr: str) -> bool:
    """验证Cron表达式格式"""
    import re

    if not cron_expr:
        return True  # None值表示手动触发

    # 基础的Cron表达式验证（5个字段）
    pattern = r'^(\*|([0-5]?\d))(\s+(\*|([01]?\d|2[0-3]))(\s+(\*|([0-2]?\d|3[01]))(\s+(\*|([0]?\d|1[0-2]))(\s+(\*|[0-6]))?)?)?)?'
    return bool(re.match(pattern, cron_expr.strip()))


def validate_task_dependencies(
        tasks: List[TaskConfigRequest],
        dependencies: List[TaskDependency]
) -> bool:
    """验证任务依赖关系的合法性"""
    task_ids = {task.task_id for task in tasks}

    for dep in dependencies:
        if dep.upstream not in task_ids or dep.downstream not in task_ids:
            return False

        # 简单的循环依赖检查（可以进一步完善）
        if dep.upstream == dep.downstream:
            return False

    return True