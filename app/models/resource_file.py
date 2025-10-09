# app/models/resource_file.py
"""
资源文件管理模型
用于资源中心的文件元数据存储
"""

from sqlalchemy import Column, Integer, String, BigInteger, Text, DateTime, Boolean, Enum as SQLEnum
from sqlalchemy.sql import func
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any

from app.models.base import BaseModel


class ResourceType(str, Enum):
    """资源类型枚举"""
    JOB = "JOB"  # 作业脚本（Shell、Python等）
    FUNC = "FUNC"  # 函数（UDF JAR包）
    LIB = "LIB"  # 依赖库（第三方JAR包）
    EXCEL = "EXCEL"  # Excel数据文件


class ResourceFile(BaseModel):
    """
    资源文件表
    存储上传到资源中心的所有文件元数据
    """
    __tablename__ = "resource_files"

    # 文件基本信息
    file_name = Column(String(255), nullable=False, comment="文件名称")
    original_filename = Column(String(255), nullable=False, comment="原始文件名")
    file_path = Column(String(500), nullable=False, comment="文件存储路径")
    file_size = Column(BigInteger, nullable=False, comment="文件大小（字节）")
    file_type = Column(SQLEnum(ResourceType), nullable=False, index=True, comment="资源类型")
    file_extension = Column(String(20), nullable=True, comment="文件扩展名")

    # 文件唯一标识
    md5_hash = Column(String(32), nullable=True, index=True, comment="文件MD5值")

    # 描述信息
    remark = Column(Text, nullable=True, comment="备注信息")

    # 使用统计
    reference_count = Column(Integer, default=0, comment="被引用次数")
    download_count = Column(Integer, default=0, comment="下载次数")

    # 创建者信息
    create_username = Column(String(100), nullable=True, comment="创建人")

    # 时间戳
    create_datetime = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间"
    )
    update_datetime = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="更新时间"
    )

    # 软删除标记
    is_deleted = Column(Boolean, default=False, index=True, comment="是否删除")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，适配前端需求"""
        return {
            "id": self.id,
            "fileName": self.file_name,
            "originalFilename": self.original_filename,
            "filePath": self.file_path,
            "fileSize": self._format_file_size(self.file_size),
            "fileSizeBytes": self.file_size,
            "fileType": self.file_type.value if self.file_type else None,
            "fileExtension": self.file_extension,
            "md5Hash": self.md5_hash,
            "remark": self.remark,
            "referenceCount": self.reference_count,
            "downloadCount": self.download_count,
            "createUsername": self.create_username,
            "createDateTime": self.create_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.create_datetime else None,
            "updateDateTime": self.update_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.update_datetime else None,
            "isDeleted": self.is_deleted
        }

    @staticmethod
    def _format_file_size(size_bytes: int) -> str:
        """
        格式化文件大小

        Args:
            size_bytes: 文件大小（字节）

        Returns:
            格式化后的文件大小字符串，如 "1.5 MB"
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def __repr__(self):
        return f"<ResourceFile(id={self.id}, name={self.file_name}, type={self.file_type}, size={self._format_file_size(self.file_size)})>"