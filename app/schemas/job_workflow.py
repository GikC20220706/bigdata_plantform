"""
作业流和作业的Pydantic Schema定义
用于API请求验证和响应序列化
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ==================== 作业流 Schemas ====================

class JobWorkflowCreate(BaseModel):
    """创建作业流请求"""
    name: str = Field(..., min_length=1, max_length=200, description="作业流名称")
    remark: Optional[str] = Field(None, description="备注")

    class Config:
        schema_extra = {
            "example": {
                "name": "用户数据ETL流程",
                "remark": "每日凌晨执行的用户数据处理流程"
            }
        }


class JobWorkflowUpdate(BaseModel):
    """更新作业流请求"""
    name: Optional[str] = Field(None, min_length=1, max_length=200, description="作业流名称")
    remark: Optional[str] = Field(None, description="备注")


class JobWorkflowConfigSave(BaseModel):
    """保存作业流流程图配置"""
    workflowId: int = Field(..., description="作业流ID")
    webConfig: List[Dict[str, Any]] = Field(..., description="流程图配置")


class JobWorkflowSettingSave(BaseModel):
    """保存作业流设置"""
    workflowId: int = Field(..., description="作业流ID")
    cronConfig: Optional[Dict[str, Any]] = Field(None, description="定时配置")
    alarmList: Optional[List[Dict[str, Any]]] = Field(None, description="告警配置")
    otherConfig: Optional[Dict[str, Any]] = Field(None, description="其他配置")


class JobWorkflowPageQuery(BaseModel):
    """分页查询作业流"""
    page: int = Field(0, ge=0, description="页码（从0开始）")
    pageSize: int = Field(10, ge=1, le=100, description="每页大小")
    searchKeyWord: Optional[str] = Field(None, description="搜索关键词")


class JobWorkflowResponse(BaseModel):
    """作业流响应"""
    id: int
    name: str
    remark: Optional[str]
    status: str
    nextDateTime: Optional[datetime]
    createUsername: Optional[str]
    createDateTime: datetime
    updateDateTime: datetime

    # 包含流程图配置
    webConfig: Optional[List[Dict[str, Any]]]
    cronConfig: Optional[Dict[str, Any]]
    alarmList: Optional[List[Dict[str, Any]]]
    otherConfig: Optional[Dict[str, Any]]

    class Config:
        from_attributes = True


# ==================== 作业 Schemas ====================

class JobWorkCreate(BaseModel):
    """创建作业请求"""
    workflowId: int = Field(..., description="所属作业流ID")
    name: str = Field(..., min_length=1, max_length=200, description="作业名称")
    workType: str = Field(..., description="作业类型")
    remark: Optional[str] = Field(None, description="备注")

    datasourceId: Optional[int] = Field(None, description="数据源ID（JDBC类作业）")
    clusterId: Optional[int] = Field(None, description="计算集群ID（Spark/Flink类作业）")
    clusterNodeId: Optional[int] = Field(None, description="集群节点ID")
    containerId: Optional[int] = Field(None, description="Spark容器ID")

    class Config:
        # 添加这个配置，允许通过属性名访问字段
        populate_by_name = True  # Pydantic v2


class JobWorkUpdate(BaseModel):
    """更新作业请求"""
    workId: int = Field(..., description="作业ID")
    name: Optional[str] = Field(None, min_length=1, max_length=200, description="作业名称")
    remark: Optional[str] = Field(None, description="备注")


class JobWorkCopy(BaseModel):
    """复制作业请求"""
    workId: int = Field(..., description="源作业ID")
    name: str = Field(..., min_length=1, max_length=200, description="新作业名称")


class JobWorkConfigSave(BaseModel):
    """保存作业配置"""
    workId: int = Field(..., description="作业ID")
    config: Dict[str, Any] = Field(..., description="作业配置（根据workType不同而不同）")


class JobWorkPageQuery(BaseModel):
    """分页查询作业"""
    workflowId: int = Field(..., description="作业流ID")
    page: int = Field(0, ge=0, description="页码（从0开始）")
    pageSize: int = Field(10, ge=1, le=100, description="每页大小")
    searchKeyWord: Optional[str] = Field(None, description="搜索关键词")


class JobWorkResponse(BaseModel):
    """作业响应"""
    id: int
    workflowId: int
    name: str
    workType: str
    remark: Optional[str]
    status: str
    config: Optional[Dict[str, Any]]
    executor: Optional[str]
    createDateTime: datetime
    updateDateTime: datetime

    class Config:
        from_attributes = True


# ==================== 运行控制 Schemas ====================

class JobWorkflowRun(BaseModel):
    """运行作业流"""
    workflowId: int = Field(..., description="作业流ID")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文（变量、参数等）")


class JobWorkflowAbort(BaseModel):
    """中止作业流"""
    workflowInstanceId: str = Field(..., description="作业流实例ID")


class JobWorkRun(BaseModel):
    """运行作业"""
    workId: int = Field(..., description="作业ID")
    context: Optional[Dict[str, Any]] = Field(None, description="执行上下文（变量、参数等）")


class JobWorkStop(BaseModel):
    """停止作业"""
    workInstanceId: str = Field(..., description="作业实例ID")


# ==================== 实例查询 Schemas ====================

class JobWorkflowInstanceQuery(BaseModel):
    """查询作业流实例"""
    workflowId: int = Field(..., description="作业流ID")


class JobWorkflowInstanceResponse(BaseModel):
    """作业流实例响应"""
    workflowInstanceId: str
    workflowId: int
    workflowName: str
    status: str
    triggerType: str
    startDatetime: Optional[datetime]
    endDatetime: Optional[datetime]
    lastModifiedBy: Optional[str]

    class Config:
        from_attributes = True


class JobWorkInstanceResponse(BaseModel):
    """作业实例响应"""
    instanceId: str
    workflowInstanceId: str
    workId: int
    workName: str
    workType: str
    status: str
    startDatetime: Optional[datetime]
    endDatetime: Optional[datetime]
    submitLog: Optional[str]
    runningLog: Optional[str]
    resultData: Optional[Dict[str, Any]]
    errorMessage: Optional[str]

    class Config:
        from_attributes = True


# ==================== 分页响应 ====================

class PageResponse(BaseModel):
    """统一分页响应格式"""
    content: List[Any] = Field(..., description="数据列表")
    total: int = Field(..., description="总数")
    page: int = Field(..., description="当前页")
    pageSize: int = Field(..., description="每页大小")