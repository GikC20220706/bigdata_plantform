# app/models/user_cluster.py
"""
用户管理的计算集群模型 - 对应前端计算集群管理页面
"""

from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, JSON, Enum as SQLEnum
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any

from app.models.base import BaseModel


class ClusterType(str, Enum):
    """集群类型枚举"""
    KUBERNETES = "kubernetes"
    YARN = "yarn"
    STANDALONE = "standalone"


class ClusterStatus(str, Enum):
    """集群状态枚举"""
    ACTIVE = "可用"
    INACTIVE = "不可用"
    CHECKING = "检测中"
    ERROR = "异常"


class UserCluster(BaseModel):
    """
    用户管理的计算集群表
    对应前端"资源管理 -> 计算集群"页面的数据
    """
    __tablename__ = "user_clusters"

    # 基础信息
    name = Column(String(200), nullable=False, comment="集群名称")
    cluster_type = Column(SQLEnum(ClusterType), nullable=False, comment="集群类型")
    remark = Column(Text, comment="备注信息")

    # 状态信息
    status = Column(SQLEnum(ClusterStatus), default=ClusterStatus.CHECKING, comment="集群状态")
    is_default = Column(Boolean, default=False, comment="是否为默认集群")
    check_datetime = Column(DateTime(timezone=True), comment="最后检测时间")

    # 集群配置
    config = Column(JSON, comment="集群配置信息")

    # 资源信息（从检测中获取）
    node_count = Column(String(50), comment="节点数量，如'2/3'")
    memory_info = Column(String(100), comment="内存信息")
    storage_info = Column(String(100), comment="存储信息")

    # 连接信息
    master_url = Column(String(500), comment="集群主节点地址")
    web_ui_url = Column(String(500), comment="Web UI地址")

    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "id": self.id,
            "name": self.name,
            "clusterType": self.cluster_type.value,
            "remark": self.remark,
            "status": self.status.value,
            "defaultCluster": self.is_default,
            "checkDateTime": self.check_datetime.isoformat() if self.check_datetime else None,
            "node": self.node_count or "0/0",
            "memory": self.memory_info or "0GB",
            "storage": self.storage_info or "0GB",
            "config": self.config or {},
            "masterUrl": self.master_url,
            "webUiUrl": self.web_ui_url,
            "createdAt": self.created_at.isoformat(),
            "updatedAt": self.updated_at.isoformat()
        }

    def update_status_info(self, status: ClusterStatus, node_count: str = None,
                           memory_info: str = None, storage_info: str = None):
        """更新状态信息"""
        self.status = status
        self.check_datetime = datetime.now()
        if node_count:
            self.node_count = node_count
        if memory_info:
            self.memory_info = memory_info
        if storage_info:
            self.storage_info = storage_info