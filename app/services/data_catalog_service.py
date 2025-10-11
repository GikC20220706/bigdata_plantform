# app/services/data_catalog_service.py
"""
数据资源目录服务层
处理数据目录的业务逻辑
"""
from typing import List, Optional, Dict, Any
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from loguru import logger
from datetime import datetime

from app.models.data_catalog import DataCatalog
from app.schemas.data_catalog import (
    DataCatalogCreate, DataCatalogUpdate,
    DataCatalogResponse, DataCatalogTreeNode
)


class DataCatalogService:
    """数据资源目录服务类"""

    async def create_catalog(
            self,
            db: AsyncSession,
            catalog_data: DataCatalogCreate,
            creator: Optional[str] = None
    ) -> DataCatalog:
        """
        创建数据目录

        Args:
            db: 数据库会话
            catalog_data: 目录创建数据
            creator: 创建人

        Returns:
            创建的目录对象

        Raises:
            ValueError: 编码重复或父目录不存在
        """
        try:
            # 1. 检查编码是否重复
            existing = await self._get_by_code(db, catalog_data.catalog_code)
            if existing:
                raise ValueError(f"目录编码 {catalog_data.catalog_code} 已存在")

            # 2. 如果有父目录，验证父目录是否存在
            parent_catalog = None
            if catalog_data.parent_id:
                parent_catalog = await self.get_catalog_by_id(db, catalog_data.parent_id)
                if not parent_catalog:
                    raise ValueError(f"父目录 ID {catalog_data.parent_id} 不存在")

                # 验证层级关系（父目录层级必须比当前小1）
                if parent_catalog.level != catalog_data.level - 1:
                    raise ValueError(
                        f"层级关系错误：父目录层级为 {parent_catalog.level}，"
                        f"当前目录层级应为 {parent_catalog.level + 1}"
                    )

            # 3. 构建路径
            path = self._build_path(parent_catalog, catalog_data.catalog_name)

            # 4. 创建目录对象
            catalog = DataCatalog(
                catalog_name=catalog_data.catalog_name,
                catalog_code=catalog_data.catalog_code,
                catalog_type=catalog_data.catalog_type.value,
                parent_id=catalog_data.parent_id,
                level=catalog_data.level,
                path=path,
                description=catalog_data.description,
                sort_order=catalog_data.sort_order,
                icon=catalog_data.icon,
                tags=catalog_data.tags,
                is_active=True,
                asset_count=0,
                created_by=creator,
                updated_by=creator
            )

            db.add(catalog)
            await db.commit()
            await db.refresh(catalog)

            logger.info(f"创建数据目录成功: {catalog.catalog_name} (ID: {catalog.id})")
            return catalog

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"创建数据目录失败: {e}")
            raise ValueError(f"创建目录失败: {str(e)}")

    async def get_catalog_by_id(
            self,
            db: AsyncSession,
            catalog_id: int
    ) -> Optional[DataCatalog]:
        """
        根据ID获取目录

        Args:
            db: 数据库会话
            catalog_id: 目录ID

        Returns:
            目录对象，不存在返回None
        """
        try:
            result = await db.execute(
                select(DataCatalog).where(DataCatalog.id == catalog_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"获取目录失败: {e}")
            return None

    async def update_catalog(
            self,
            db: AsyncSession,
            catalog_id: int,
            catalog_data: DataCatalogUpdate,
            updater: Optional[str] = None
    ) -> DataCatalog:
        """
        更新数据目录

        Args:
            db: 数据库会话
            catalog_id: 目录ID
            catalog_data: 更新数据
            updater: 更新人

        Returns:
            更新后的目录对象

        Raises:
            ValueError: 目录不存在
        """
        try:
            # 1. 获取目录
            catalog = await self.get_catalog_by_id(db, catalog_id)
            if not catalog:
                raise ValueError(f"目录 ID {catalog_id} 不存在")

            # 2. 更新字段
            update_data = catalog_data.model_dump(exclude_unset=True)
            for field, value in update_data.items():
                setattr(catalog, field, value)

            # 3. 如果名称变了，需要更新路径
            if catalog_data.catalog_name and catalog_data.catalog_name != catalog.catalog_name:
                # 获取父目录
                parent_catalog = None
                if catalog.parent_id:
                    parent_catalog = await self.get_catalog_by_id(db, catalog.parent_id)

                # 重新构建路径
                catalog.path = self._build_path(parent_catalog, catalog_data.catalog_name)

                # TODO: 如果有子目录，需要递归更新子目录的路径
                await self._update_children_paths(db, catalog)

            catalog.updated_by = updater

            await db.commit()
            await db.refresh(catalog)

            logger.info(f"更新数据目录成功: {catalog.catalog_name} (ID: {catalog.id})")
            return catalog

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"更新数据目录失败: {e}")
            raise ValueError(f"更新目录失败: {str(e)}")

    async def delete_catalog(
            self,
            db: AsyncSession,
            catalog_id: int,
            force: bool = False
    ) -> bool:
        """
        删除数据目录

        Args:
            db: 数据库会话
            catalog_id: 目录ID
            force: 是否强制删除（忽略子节点和资产检查）

        Returns:
            是否删除成功

        Raises:
            ValueError: 目录不存在或有子节点/资产
        """
        try:
            # 1. 获取目录
            catalog = await self.get_catalog_by_id(db, catalog_id)
            if not catalog:
                raise ValueError(f"目录 ID {catalog_id} 不存在")

            # 2. 检查是否有子目录
            if not force:
                children_count = await self._count_children(db, catalog_id)
                if children_count > 0:
                    raise ValueError(f"目录下还有 {children_count} 个子目录，无法删除")

                # 3. 检查是否有资产
                if catalog.asset_count > 0:
                    raise ValueError(f"目录下还有 {catalog.asset_count} 个数据资产，无法删除")

            # 4. 删除目录
            await db.delete(catalog)
            await db.commit()

            logger.info(f"删除数据目录成功: {catalog.catalog_name} (ID: {catalog.id})")
            return True

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"删除数据目录失败: {e}")
            raise ValueError(f"删除目录失败: {str(e)}")

    async def get_catalog_tree(
            self,
            db: AsyncSession,
            parent_id: Optional[int] = None,
            include_inactive: bool = False
    ) -> List[DataCatalogTreeNode]:
        """
        获取目录树形结构

        Args:
            db: 数据库会话
            parent_id: 父目录ID，None表示从根节点开始
            include_inactive: 是否包含未启用的目录

        Returns:
            目录树节点列表
        """
        try:
            # 1. 构建查询条件
            conditions = [DataCatalog.parent_id == parent_id]
            if not include_inactive:
                conditions.append(DataCatalog.is_active == True)

            # 2. 查询当前层级的目录
            result = await db.execute(
                select(DataCatalog)
                .where(and_(*conditions))
                .order_by(DataCatalog.sort_order, DataCatalog.id)
            )
            catalogs = result.scalars().all()

            # 3. 递归构建树形结构
            tree_nodes = []
            for catalog in catalogs:
                # 先转换为字典
                catalog_dict = {
                    'id': catalog.id,
                    'catalog_name': catalog.catalog_name,
                    'catalog_code': catalog.catalog_code,
                    'catalog_type': catalog.catalog_type,
                    'parent_id': catalog.parent_id,
                    'level': catalog.level,
                    'path': catalog.path,
                    'description': catalog.description,
                    'sort_order': catalog.sort_order,
                    'icon': catalog.icon,
                    'tags': catalog.tags,
                    'is_active': catalog.is_active,
                    'asset_count': catalog.asset_count,
                    'created_by': catalog.created_by,
                    'created_at': catalog.created_at,
                    'updated_at': catalog.updated_at,
                    'children': []  # ← 关键：先初始化为空列表
                }

                # 转换为响应模型
                node = DataCatalogTreeNode(**catalog_dict)

                # 递归获取子节点
                children = await self.get_catalog_tree(
                    db,
                    parent_id=catalog.id,
                    include_inactive=include_inactive
                )
                node.children = children  # 设置子节点

                tree_nodes.append(node)

            return tree_nodes

        except Exception as e:
            logger.error(f"获取目录树失败: {e}")
            raise ValueError(f"获取目录树失败: {str(e)}")
    async def search_catalogs(
            self,
            db: AsyncSession,
            page: int = 1,
            page_size: int = 20,
            keyword: Optional[str] = None,
            catalog_type: Optional[str] = None,
            parent_id: Optional[int] = None,
            is_active: Optional[bool] = None
    ) -> tuple[List[DataCatalog], int]:
        """
        搜索数据目录（分页）

        Args:
            db: 数据库会话
            page: 页码
            page_size: 每页数量
            keyword: 搜索关键词（名称、编码、描述）
            catalog_type: 目录类型
            parent_id: 父目录ID
            is_active: 是否启用

        Returns:
            (目录列表, 总数)
        """
        try:
            # 1. 构建查询条件
            conditions = []

            if keyword:
                keyword_pattern = f"%{keyword}%"
                conditions.append(
                    or_(
                        DataCatalog.catalog_name.like(keyword_pattern),
                        DataCatalog.catalog_code.like(keyword_pattern),
                        DataCatalog.description.like(keyword_pattern)
                    )
                )

            if catalog_type:
                conditions.append(DataCatalog.catalog_type == catalog_type)

            if parent_id is not None:
                conditions.append(DataCatalog.parent_id == parent_id)

            if is_active is not None:
                conditions.append(DataCatalog.is_active == is_active)

            # 2. 查询总数
            count_query = select(func.count(DataCatalog.id))
            if conditions:
                count_query = count_query.where(and_(*conditions))

            total_result = await db.execute(count_query)
            total = total_result.scalar_one()

            # 3. 分页查询
            query = select(DataCatalog)
            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(
                DataCatalog.level,
                DataCatalog.sort_order,
                DataCatalog.id
            ).limit(page_size).offset((page - 1) * page_size)

            result = await db.execute(query)
            catalogs = result.scalars().all()

            return list(catalogs), total

        except Exception as e:
            logger.error(f"搜索目录失败: {e}")
            raise ValueError(f"搜索目录失败: {str(e)}")

    async def get_catalog_path_list(
            self,
            db: AsyncSession,
            catalog_id: int
    ) -> List[DataCatalog]:
        """
        获取目录的路径列表（从根到当前节点）

        Args:
            db: 数据库会话
            catalog_id: 目录ID

        Returns:
            路径上的目录列表
        """
        try:
            path_list = []
            current_id = catalog_id

            while current_id:
                catalog = await self.get_catalog_by_id(db, current_id)
                if not catalog:
                    break

                path_list.insert(0, catalog)
                current_id = catalog.parent_id

            return path_list

        except Exception as e:
            logger.error(f"获取目录路径失败: {e}")
            return []

    async def update_asset_count(
            self,
            db: AsyncSession,
            catalog_id: int,
            increment: int = 1
    ) -> None:
        """
        更新目录的资产数量

        Args:
            db: 数据库会话
            catalog_id: 目录ID
            increment: 增量（正数增加，负数减少）
        """
        try:
            catalog = await self.get_catalog_by_id(db, catalog_id)
            if catalog:
                catalog.asset_count = max(0, catalog.asset_count + increment)
                await db.commit()

                # 递归更新父目录的数量
                if catalog.parent_id:
                    await self.update_asset_count(db, catalog.parent_id, increment)

        except Exception as e:
            logger.error(f"更新目录资产数量失败: {e}")

    # ==================== 私有方法 ====================

    async def _get_by_code(
            self,
            db: AsyncSession,
            catalog_code: str
    ) -> Optional[DataCatalog]:
        """根据编码获取目录"""
        try:
            result = await db.execute(
                select(DataCatalog).where(DataCatalog.catalog_code == catalog_code)
            )
            return result.scalar_one_or_none()
        except Exception:
            return None

    def _build_path(
            self,
            parent_catalog: Optional[DataCatalog],
            catalog_name: str
    ) -> str:
        """
        构建目录路径

        Args:
            parent_catalog: 父目录对象
            catalog_name: 当前目录名称

        Returns:
            完整路径，如: /财务域/客户主题/客户信息
        """
        if parent_catalog and parent_catalog.path:
            return f"{parent_catalog.path}/{catalog_name}"
        else:
            return f"/{catalog_name}"

    async def _update_children_paths(
            self,
            db: AsyncSession,
            parent_catalog: DataCatalog
    ) -> None:
        """
        递归更新子目录的路径

        Args:
            db: 数据库会话
            parent_catalog: 父目录对象
        """
        try:
            # 查询所有子目录
            result = await db.execute(
                select(DataCatalog).where(DataCatalog.parent_id == parent_catalog.id)
            )
            children = result.scalars().all()

            for child in children:
                # 更新子目录路径
                child.path = self._build_path(parent_catalog, child.catalog_name)

                # 递归更新孙子目录
                await self._update_children_paths(db, child)

            await db.commit()

        except Exception as e:
            logger.error(f"更新子目录路径失败: {e}")

    async def _count_children(
            self,
            db: AsyncSession,
            catalog_id: int
    ) -> int:
        """统计子目录数量"""
        try:
            result = await db.execute(
                select(func.count(DataCatalog.id))
                .where(DataCatalog.parent_id == catalog_id)
            )
            return result.scalar_one()
        except Exception:
            return 0


# 创建全局服务实例
data_catalog_service = DataCatalogService()