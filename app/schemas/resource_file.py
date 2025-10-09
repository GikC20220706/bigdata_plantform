# app/schemas/resource_file.py
"""
资源文件相关的Pydantic模式定义
用于API请求和响应的数据验证
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from enum import Enum


class ResourceType(str, Enum):
    """资源类型枚举"""
    JOB = "JOB"          # 作业脚本
    FUNC = "FUNC"        # 函数
    LIB = "LIB"          # 依赖库
    EXCEL = "EXCEL"      # Excel文件


class ResourceFileBase(BaseModel):
    """资源文件基础模型"""
    file_type: ResourceType = Field(..., description="资源类型")
    remark: Optional[str] = Field(None, max_length=500, description="备注信息")


class ResourceFileCreate(ResourceFileBase):
    """创建资源文件时的请求模型"""
    # 文件通过 UploadFile 上传，不在这里定义
    pass


class ResourceFileUpdate(BaseModel):
    """更新资源文件时的请求模型"""
    remark: Optional[str] = Field(None, max_length=500, description="备注信息")


class ResourceFileResponse(BaseModel):
    """资源文件响应模型"""
    id: int = Field(..., description="资源ID")
    fileName: str = Field(..., description="文件名称")  # 🆕 改为驼峰
    originalFilename: str = Field(..., description="原始文件名")  # 🆕 改为驼峰
    filePath: str = Field(..., description="文件存储路径")  # 🆕 改为驼峰
    fileSize: str = Field(..., description="文件大小（格式化）")  # 🆕 改为驼峰
    fileSizeBytes: int = Field(..., description="文件大小（字节）")  # 🆕 改为驼峰
    fileType: str = Field(..., description="资源类型")  # 🆕 改为驼峰
    fileExtension: Optional[str] = Field(None, description="文件扩展名")  # 🆕 改为驼峰
    md5Hash: Optional[str] = Field(None, description="文件MD5值")  # 🆕 改为驼峰
    remark: Optional[str] = Field(None, description="备注信息")
    referenceCount: int = Field(0, description="被引用次数")  # 🆕 改为驼峰
    downloadCount: int = Field(0, description="下载次数")  # 🆕 改为驼峰
    createUsername: Optional[str] = Field(None, description="创建人")  # 🆕 改为驼峰
    createDatetime: Optional[str] = Field(None, description="创建时间")  # 🆕 改为驼峰
    updateDatetime: Optional[str] = Field(None, description="更新时间")  # 🆕 改为驼峰
    isDeleted: bool = Field(False, description="是否删除")  # 🆕 改为驼峰

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": 1,
                "fileName": "data_etl_20240101.sh",
                "originalFilename": "data_etl.sh",
                "filePath": "/app/uploads/resources/scripts/data_etl_20240101.sh",
                "fileSize": "2.5 KB",
                "fileSizeBytes": 2560,
                "fileType": "JOB",
                "fileExtension": ".sh",
                "md5Hash": "5d41402abc4b2a76b9719d911017c592",
                "remark": "每日数据ETL脚本",
                "referenceCount": 3,
                "downloadCount": 10,
                "createUsername": "admin",
                "createDatetime": "2024-01-01 10:30:00",
                "updateDatetime": "2024-01-01 10:30:00",
                "isDeleted": False
            }
        }


class ResourceFileListResponse(BaseModel):
    """资源文件列表响应模型"""
    total: int = Field(..., description="总记录数")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页记录数")
    items: List[ResourceFileResponse] = Field(..., description="资源文件列表")

    class Config:
        json_schema_extra = {
            "example": {
                "total": 100,
                "page": 1,
                "page_size": 10,
                "items": [
                    {
                        "id": 1,
                        "file_name": "data_etl_20240101.sh",
                        "original_filename": "data_etl.sh",
                        "file_path": "/app/uploads/resources/scripts/data_etl_20240101.sh",
                        "file_size": "2.5 KB",
                        "file_size_bytes": 2560,
                        "file_type": "JOB",
                        "file_extension": ".sh",
                        "md5_hash": "5d41402abc4b2a76b9719d911017c592",
                        "remark": "每日数据ETL脚本",
                        "reference_count": 3,
                        "download_count": 10,
                        "create_username": "admin",
                        "create_datetime": "2024-01-01 10:30:00",
                        "update_datetime": "2024-01-01 10:30:00",
                        "is_deleted": False
                    }
                ]
            }
        }


class ResourceFileUploadResponse(BaseModel):
    """资源文件上传响应模型"""
    id: int = Field(..., description="资源ID")
    fileName: str = Field(..., description="文件名称")  # 🆕 改为驼峰
    fileSize: str = Field(..., description="文件大小")  # 🆕 改为驼峰
    fileType: str = Field(..., description="资源类型")  # 🆕 改为驼峰
    message: str = Field(..., description="上传结果消息")

    class Config:
        json_schema_extra = {
            "example": {
                "id": 1,
                "fileName": "data_etl_20240101.sh",
                "fileSize": "2.5 KB",
                "fileType": "JOB",
                "message": "文件上传成功"
            }
        }


class ResourceReferenceInfo(BaseModel):
    """资源引用信息模型"""
    workflowId: str = Field(..., description="工作流ID")  # 🆕 改为驼峰
    workflowName: str = Field(..., description="工作流名称")  # 🆕 改为驼峰
    nodeId: str = Field(..., description="节点ID")  # 🆕 改为驼峰
    nodeName: str = Field(..., description="节点名称")  # 🆕 改为驼峰
    referenceType: str = Field(..., description="引用类型：script/function/library")  # 🆕 改为驼峰

    class Config:
        json_schema_extra = {
            "example": {
                "workflowId": "wf_20240101_001",
                "workflowName": "每日数据处理流程",
                "nodeId": "node_001",
                "nodeName": "数据清洗任务",
                "referenceType": "script"
            }
        }


class ResourceReferencesResponse(BaseModel):
    """资源引用查询响应模型"""
    resourceId: int = Field(..., description="资源ID")  # 🆕 改为驼峰
    fileName: str = Field(..., description="文件名称")  # 🆕 改为驼峰
    referenceCount: int = Field(..., description="引用次数")  # 🆕 改为驼峰
    references: List[ResourceReferenceInfo] = Field(..., description="引用详情列表")

    class Config:
        json_schema_extra = {
            "example": {
                "resourceId": 1,
                "fileName": "data_etl_20240101.sh",
                "referenceCount": 3,
                "references": [
                    {
                        "workflowId": "wf_20240101_001",
                        "workflowName": "每日数据处理流程",
                        "nodeId": "node_001",
                        "nodeName": "数据清洗任务",
                        "referenceType": "script"
                    }
                ]
            }
        }


class FileTypeValidation(BaseModel):
    """文件类型验证配置"""
    resourceType: ResourceType  # 🆕 改为驼峰
    allowedExtensions: List[str]  # 🆕 改为驼峰
    maxSizeMb: int  # 🆕 改为驼峰

    class Config:
        json_schema_extra = {
            "example": {
                "resourceType": "JOB",
                "allowedExtensions": [".sh", ".py", ".sql"],
                "maxSizeMb": 10
            }
        }