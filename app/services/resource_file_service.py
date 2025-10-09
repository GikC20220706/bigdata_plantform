# app/services/resource_file_service.py
"""
资源文件服务层
处理资源文件的业务逻辑和数据库操作
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, desc
from sqlalchemy.orm import selectinload
from fastapi import UploadFile, HTTPException
from loguru import logger

from app.models.resource_file import ResourceFile, ResourceType
from app.schemas.resource_file import (
    ResourceFileCreate,
    ResourceFileUpdate,
    ResourceFileResponse,
    ResourceFileListResponse
)
from app.utils.file_handler import file_handler


class ResourceFileService:
    """资源文件服务类"""

    async def upload_resource(
            self,
            db: AsyncSession,
            file: UploadFile,
            resource_type: str,
            remark: Optional[str] = None,
            username: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        上传资源文件

        Args:
            db: 数据库会话
            file: 上传的文件
            resource_type: 资源类型
            remark: 备注信息
            username: 创建人用户名

        Returns:
            包含资源信息的字典
        """
        try:
            # 1. 验证资源类型
            if resource_type not in [rt.value for rt in ResourceType]:
                raise HTTPException(
                    status_code=400,
                    detail=f"不支持的资源类型: {resource_type}"
                )

            # 2. 保存文件到磁盘
            save_result = await file_handler.save_upload_file(file, resource_type)

            if not save_result.get("success"):
                raise HTTPException(
                    status_code=400,
                    detail=save_result.get("error", "文件保存失败")
                )

            # 3. 检查是否存在相同MD5的文件（可选去重）
            md5_hash = save_result["md5_hash"]
            existing_file = await self._check_duplicate_by_md5(db, md5_hash)

            if existing_file:
                logger.warning(
                    f"发现重复文件: {existing_file.file_name}, "
                    f"MD5: {md5_hash}, 继续创建新记录"
                )

            # 4. 创建数据库记录
            resource_file = ResourceFile(
                file_name=save_result["file_name"],
                original_filename=save_result["original_filename"],
                file_path=save_result["file_path"],
                file_size=save_result["file_size"],
                file_type=ResourceType(resource_type),
                file_extension=save_result["file_extension"],
                md5_hash=md5_hash,
                remark=remark or "",
                create_username=username or "system",
                reference_count=0,
                download_count=0,
                is_deleted=False
            )

            db.add(resource_file)
            await db.commit()
            await db.refresh(resource_file)

            logger.info(
                f"资源文件上传成功: ID={resource_file.id}, "
                f"文件名={resource_file.file_name}, "
                f"类型={resource_type}, "
                f"创建人={username}"
            )

            return {
                "success": True,
                "resource": resource_file.to_dict(),
                "message": "资源文件上传成功"
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"上传资源文件失败: {e}")
            # 如果数据库操作失败，尝试删除已保存的文件
            if save_result.get("success"):
                file_handler.delete_file(save_result["file_path"])
            raise HTTPException(
                status_code=500,
                detail=f"上传资源文件失败: {str(e)}"
            )

    async def get_resource_list(
            self,
            db: AsyncSession,
            page: int = 1,
            page_size: int = 10,
            resource_type: Optional[str] = None,
            keyword: Optional[str] = None,
            username: Optional[str] = None
    ) -> ResourceFileListResponse:
        """
        获取资源文件列表（分页）

        Args:
            db: 数据库会话
            page: 页码（从1开始）
            page_size: 每页记录数
            resource_type: 资源类型筛选
            keyword: 关键字搜索（文件名、备注）
            username: 创建人筛选

        Returns:
            资源文件列表响应
        """
        try:
            # 构建查询条件
            conditions = [ResourceFile.is_deleted == False]

            # 资源类型筛选
            if resource_type:
                conditions.append(ResourceFile.file_type == ResourceType(resource_type))

            # 创建人筛选
            if username:
                conditions.append(ResourceFile.create_username == username)

            # 关键字搜索
            if keyword:
                keyword_pattern = f"%{keyword}%"
                conditions.append(
                    or_(
                        ResourceFile.file_name.like(keyword_pattern),
                        ResourceFile.original_filename.like(keyword_pattern),
                        ResourceFile.remark.like(keyword_pattern)
                    )
                )

            # 查询总数
            count_query = select(func.count(ResourceFile.id)).where(and_(*conditions))
            total_result = await db.execute(count_query)
            total = total_result.scalar() or 0

            # 分页查询
            offset = (page - 1) * page_size
            query = (
                select(ResourceFile)
                .where(and_(*conditions))
                .order_by(desc(ResourceFile.create_datetime))
                .offset(offset)
                .limit(page_size)
            )

            result = await db.execute(query)
            resources = result.scalars().all()

            # 转换为响应格式
            items = [
                ResourceFileResponse(**resource.to_dict())
                for resource in resources
            ]

            return ResourceFileListResponse(
                total=total,
                page=page,
                page_size=page_size,
                items=items
            )

        except Exception as e:
            logger.error(f"获取资源列表失败: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"获取资源列表失败: {str(e)}"
            )

    async def get_resource_by_id(
            self,
            db: AsyncSession,
            resource_id: int
    ) -> Optional[ResourceFile]:
        """
        根据ID获取资源文件

        Args:
            db: 数据库会话
            resource_id: 资源ID

        Returns:
            资源文件对象，如果不存在返回None
        """
        try:
            query = select(ResourceFile).where(
                and_(
                    ResourceFile.id == resource_id,
                    ResourceFile.is_deleted == False
                )
            )
            result = await db.execute(query)
            resource = result.scalar_one_or_none()

            return resource

        except Exception as e:
            logger.error(f"获取资源文件失败: {e}, ID={resource_id}")
            raise HTTPException(
                status_code=500,
                detail=f"获取资源文件失败: {str(e)}"
            )

    async def delete_resource(
            self,
            db: AsyncSession,
            resource_id: int,
            force_delete: bool = False
    ) -> Dict[str, Any]:
        """
        删除资源文件

        Args:
            db: 数据库会话
            resource_id: 资源ID
            force_delete: 是否强制删除（即使有引用）

        Returns:
            删除结果
        """
        try:
            # 1. 查询资源
            resource = await self.get_resource_by_id(db, resource_id)

            if not resource:
                raise HTTPException(
                    status_code=404,
                    detail=f"资源文件不存在: ID={resource_id}"
                )

            # 2. 检查是否被引用
            if resource.reference_count > 0 and not force_delete:
                raise HTTPException(
                    status_code=400,
                    detail=f"资源文件正在被 {resource.reference_count} 个作业流使用，无法删除"
                )

            # 3. 删除物理文件
            file_deleted, error_msg = file_handler.delete_file(resource.file_path)

            if not file_deleted:
                logger.warning(f"删除物理文件失败: {error_msg}, 继续删除数据库记录")

            # 4. 软删除数据库记录
            resource.is_deleted = True
            await db.commit()

            logger.info(f"资源文件删除成功: ID={resource_id}, 文件名={resource.file_name}")

            return {
                "success": True,
                "message": "资源文件删除成功",
                "resource_id": resource_id
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"删除资源文件失败: {e}, ID={resource_id}")
            raise HTTPException(
                status_code=500,
                detail=f"删除资源文件失败: {str(e)}"
            )

    async def update_resource_remark(
            self,
            db: AsyncSession,
            resource_id: int,
            remark: str
    ) -> Dict[str, Any]:
        """
        更新资源文件备注

        Args:
            db: 数据库会话
            resource_id: 资源ID
            remark: 新的备注信息

        Returns:
            更新结果
        """
        try:
            # 1. 查询资源
            resource = await self.get_resource_by_id(db, resource_id)

            if not resource:
                raise HTTPException(
                    status_code=404,
                    detail=f"资源文件不存在: ID={resource_id}"
                )

            # 2. 更新备注
            resource.remark = remark
            await db.commit()
            await db.refresh(resource)

            logger.info(f"资源文件备注更新成功: ID={resource_id}")

            return {
                "success": True,
                "message": "备注更新成功",
                "resource": resource.to_dict()
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"更新资源备注失败: {e}, ID={resource_id}")
            raise HTTPException(
                status_code=500,
                detail=f"更新资源备注失败: {str(e)}"
            )

    async def increment_download_count(
            self,
            db: AsyncSession,
            resource_id: int
    ) -> None:
        """
        增加资源下载次数

        Args:
            db: 数据库会话
            resource_id: 资源ID
        """
        try:
            resource = await self.get_resource_by_id(db, resource_id)
            if resource:
                resource.download_count += 1
                await db.commit()
                logger.debug(f"资源下载次数+1: ID={resource_id}")

        except Exception as e:
            logger.error(f"更新下载次数失败: {e}, ID={resource_id}")
            # 不抛出异常，下载次数统计失败不影响下载功能

    async def increment_reference_count(
            self,
            db: AsyncSession,
            resource_id: int,
            increment: int = 1
    ) -> None:
        """
        增加或减少资源引用次数

        Args:
            db: 数据库会话
            resource_id: 资源ID
            increment: 增量（正数增加，负数减少）
        """
        try:
            resource = await self.get_resource_by_id(db, resource_id)
            if resource:
                resource.reference_count = max(0, resource.reference_count + increment)
                await db.commit()
                logger.info(
                    f"资源引用次数更新: ID={resource_id}, "
                    f"变化={increment:+d}, "
                    f"当前={resource.reference_count}"
                )

        except Exception as e:
            logger.error(f"更新引用次数失败: {e}, ID={resource_id}")
            raise

    async def get_resources_by_ids(
            self,
            db: AsyncSession,
            resource_ids: List[int]
    ) -> List[ResourceFile]:
        """
        批量获取资源文件

        Args:
            db: 数据库会话
            resource_ids: 资源ID列表

        Returns:
            资源文件列表
        """
        try:
            if not resource_ids:
                return []

            query = select(ResourceFile).where(
                and_(
                    ResourceFile.id.in_(resource_ids),
                    ResourceFile.is_deleted == False
                )
            )
            result = await db.execute(query)
            resources = result.scalars().all()

            return list(resources)

        except Exception as e:
            logger.error(f"批量获取资源失败: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"批量获取资源失败: {str(e)}"
            )

    async def get_resources_by_type(
            self,
            db: AsyncSession,
            resource_type: str
    ) -> List[ResourceFile]:
        """
        获取指定类型的所有资源

        Args:
            db: 数据库会话
            resource_type: 资源类型

        Returns:
            资源文件列表
        """
        try:
            query = select(ResourceFile).where(
                and_(
                    ResourceFile.file_type == ResourceType(resource_type),
                    ResourceFile.is_deleted == False
                )
            ).order_by(desc(ResourceFile.create_datetime))

            result = await db.execute(query)
            resources = result.scalars().all()

            return list(resources)

        except Exception as e:
            logger.error(f"获取指定类型资源失败: {e}, 类型={resource_type}")
            raise HTTPException(
                status_code=500,
                detail=f"获取指定类型资源失败: {str(e)}"
            )

    async def _check_duplicate_by_md5(
            self,
            db: AsyncSession,
            md5_hash: str
    ) -> Optional[ResourceFile]:
        """
        检查是否存在相同MD5的文件

        Args:
            db: 数据库会话
            md5_hash: MD5值

        Returns:
            如果存在返回资源文件对象，否则返回None
        """
        try:
            query = select(ResourceFile).where(
                and_(
                    ResourceFile.md5_hash == md5_hash,
                    ResourceFile.is_deleted == False
                )
            ).limit(1)

            result = await db.execute(query)
            resource = result.scalar_one_or_none()

            return resource

        except Exception as e:
            logger.error(f"检查MD5重复失败: {e}")
            return None


# 创建全局服务实例
resource_file_service = ResourceFileService()