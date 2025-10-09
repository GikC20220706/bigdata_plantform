# app/api/v1/resource_files.py
"""
资源文件管理的REST API端点
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
import os

from app.utils.database import get_async_db
from app.utils.response import create_response
from app.services.resource_file_service import resource_file_service
from app.schemas.resource_file import (
    ResourceFileResponse,
    ResourceFileListResponse,
    ResourceFileUploadResponse,
    ResourceFileUpdate
)

router = APIRouter(prefix="/resources", tags=["资源中心"])


@router.post("/upload", summary="上传资源文件")
async def upload_resource_file(
        file: UploadFile = File(..., description="上传的文件"),
        type: str = Form(..., description="资源类型: JOB/FUNC/LIB/EXCEL"),
        remark: Optional[str] = Form(None, description="备注信息"),
        db: AsyncSession = Depends(get_async_db),
        username: Optional[str] = Query(None, description="创建人用户名")
):
    """
    上传资源文件

    - **file**: 上传的文件
    - **type**: 资源类型（JOB-作业脚本、FUNC-函数、LIB-依赖库、EXCEL-Excel文件）
    - **remark**: 备注信息（可选）
    - **username**: 创建人用户名（可选）

    **文件类型限制**:
    - JOB: .sh, .py, .sql, .bash (最大10MB)
    - FUNC: .jar, .zip (最大100MB)
    - LIB: .jar, .zip, .tar.gz, .whl (最大500MB)
    - EXCEL: .xlsx, .xls, .csv (最大50MB)
    """

    try:
        # 🆕 添加：FastAPI层面的文件大小预检查
        # 读取文件前先检查 content-length header
        from fastapi import Request

        # 获取文件类型对应的最大大小（转换为字节）
        from app.utils.file_handler import file_handler
        max_size_bytes = file_handler.get_max_file_size(type) * 1024 * 1024

        # 如果文件对象有 size 属性，先检查
        if hasattr(file, 'size') and file.size and file.size > max_size_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"文件大小超过限制 {file_handler.get_max_file_size(type)}MB"
            )

        result = await resource_file_service.upload_resource(
            db=db,
            file=file,
            resource_type=type,
            remark=remark,
            username=username
        )

        resource = result["resource"]

        return create_response(
            data={
                "id": resource["id"],
                "fileName": resource["fileName"],
                "fileSize": resource["fileSize"],
                "fileType": resource["fileType"],
                "message": result["message"]
            },
            message="资源文件上传成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"上传资源文件API异常: {e}")
        raise HTTPException(status_code=500, detail=f"上传失败: {str(e)}")


@router.get("", summary="获取资源文件列表")
async def get_resource_files(
        page: int = Query(1, ge=1, description="页码，从1开始"),
        page_size: int = Query(10, ge=1, le=100, description="每页记录数"),
        type: Optional[str] = Query(None, description="资源类型筛选: JOB/FUNC/LIB/EXCEL"),
        keyword: Optional[str] = Query(None, description="关键字搜索（文件名、备注）"),
        username: Optional[str] = Query(None, description="创建人筛选"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资源文件列表（分页）

    - **page**: 页码，从1开始
    - **page_size**: 每页记录数，最大100
    - **type**: 资源类型筛选（可选）
    - **keyword**: 关键字搜索，在文件名和备注中搜索（可选）
    - **username**: 创建人筛选（可选）

    **返回格式**:
```json
    {
        "code": 200,
        "data": {
            "total": 100,
            "page": 1,
            "page_size": 10,
            "items": [...]
        },
        "message": "查询成功"
    }
"""


    try:
        result = await resource_file_service.get_resource_list(
            db=db,
            page=page,
            page_size=page_size,
            resource_type=type,
            keyword=keyword,
            username=username
        )

        return create_response(
            data=result.model_dump(),
            message="查询成功"
        )

    except Exception as e:
        logger.error(f"获取资源列表API异常: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/{resource_id}", summary="获取资源文件详情")
async def get_resource_file(
        resource_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取资源文件详情
    - **resource_id**: 资源ID
    """


    try:
        resource = await resource_file_service.get_resource_by_id(db, resource_id)

        if not resource:
            raise HTTPException(
                status_code=404,
                detail=f"资源文件不存在: ID={resource_id}"
            )

        return create_response(
            data=resource.to_dict(),
            message="查询成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取资源详情API异常: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/{resource_id}/download", summary="下载资源文件")
async def download_resource_file(
        resource_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """
    下载资源文件

    - **resource_id**: 资源ID

    **返回**: 文件流
    """
    try:
        # 1. 获取资源信息
        resource = await resource_file_service.get_resource_by_id(db, resource_id)

        if not resource:
            raise HTTPException(
                status_code=404,
                detail=f"资源文件不存在: ID={resource_id}"
            )

        # 2. 检查文件是否存在
        if not os.path.exists(resource.file_path):
            logger.error(f"物理文件不存在: {resource.file_path}")
            raise HTTPException(
                status_code=404,
                detail="文件不存在或已被删除"
            )

        # 3. 增加下载次数（异步执行，不阻塞下载）
        try:
            await resource_file_service.increment_download_count(db, resource_id)
        except Exception as e:
            logger.warning(f"更新下载次数失败: {e}")

        # 4. 返回文件
        # 🆕 修改：正确处理中文文件名
        from urllib.parse import quote

        # 对文件名进行URL编码
        encoded_filename = quote(resource.original_filename.encode('utf-8'))

        # 🆕 方案：使用StreamingResponse代替FileResponse
        def file_iterator():
            with open(resource.file_path, mode="rb") as file_like:
                yield from file_like

        from fastapi.responses import StreamingResponse

        return StreamingResponse(
            file_iterator(),
            media_type='application/octet-stream',
            headers={
                # 🆕 使用RFC 5987标准的文件名编码
                'Content-Disposition': f"attachment; filename*=UTF-8''{encoded_filename}",
                'Content-Type': 'application/octet-stream',
                'Cache-Control': 'no-cache'
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"下载资源文件API异常: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"下载失败: {str(e)}")

@router.delete("/{resource_id}", summary="删除资源文件")
async def delete_resource_file(
        resource_id: int,
        force: bool = Query(False, description="是否强制删除（即使有引用）"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除资源文件
    - **resource_id**: 资源ID
    - **force**: 是否强制删除，默认false。如果资源被作业流引用，需要设置为true才能删除
    """


    try:
        result = await resource_file_service.delete_resource(
            db=db,
            resource_id=resource_id,
            force_delete=force
        )

        return create_response(
            data=result,
            message="删除成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除资源文件API异常: {e}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@router.put("/{resource_id}/remark", summary="更新资源文件备注")
async def update_resource_remark(
        resource_id: int,
        remark: str = Query(..., description="新的备注信息"),
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新资源文件备注
    - **resource_id**: 资源ID
    - **remark**: 新的备注信息
    """


    try:
        result = await resource_file_service.update_resource_remark(
            db=db,
            resource_id=resource_id,
            remark=remark
        )

        return create_response(
            data=result["resource"],
            message="备注更新成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新资源备注API异常: {e}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.get("/{resource_id}/references", summary="查看资源引用情况")
async def get_resource_references(
        resource_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """
    查看资源引用情况
    返回哪些作业流正在使用该资源

    - **resource_id**: 资源ID

    **注意**: 此功能需要与作业流模块集成后才能返回详细信息，目前仅返回引用次数
    """


    try:
        resource = await resource_file_service.get_resource_by_id(db, resource_id)

        if not resource:
            raise HTTPException(
                status_code=404,
                detail=f"资源文件不存在: ID={resource_id}"
            )

        # TODO: 查询具体的引用详情（需要与workflow模块集成）
        # 目前仅返回引用次数
        return create_response(
            data={
                "resource_id": resource.id,
                "file_name": resource.file_name,
                "reference_count": resource.reference_count,
                "references": [],  # 待实现
                "message": "资源引用查询功能待与作业流模块集成"
            },
            message="查询成功"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询资源引用API异常: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")


@router.get("/types/info", summary="获取资源类型配置信息")
async def get_resource_types_info():
    """
    获取资源类型配置信息
    返回各资源类型的允许扩展名和最大文件大小
    """


    try:
        from app.utils.file_handler import file_handler

        types_info = []
        for resource_type in ["JOB", "FUNC", "LIB", "EXCEL"]:
            types_info.append({
                "type": resource_type,
                "allowed_extensions": file_handler.get_allowed_extensions(resource_type),
                "max_size_mb": file_handler.get_max_file_size(resource_type)
            })

        return create_response(
            data=types_info,
            message="查询成功"
        )

    except Exception as e:
        logger.error(f"获取资源类型配置API异常: {e}")
        raise HTTPException(status_code=500, detail=f"查询失败: {str(e)}")