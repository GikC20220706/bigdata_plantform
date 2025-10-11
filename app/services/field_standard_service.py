# app/services/field_standard_service.py
"""
字段标准服务层
处理字段标准的业务逻辑
"""
from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
from datetime import datetime

from app.models.field_standard import FieldStandard
from app.schemas.data_catalog import (
    FieldStandardCreate, FieldStandardUpdate,
    FieldStandardResponse
)


class FieldStandardService:
    """字段标准服务类"""

    async def create_standard(
            self,
            db: AsyncSession,
            standard_data: FieldStandardCreate,
            creator: Optional[str] = None
    ) -> FieldStandard:
        """
        创建字段标准

        Args:
            db: 数据库会话
            standard_data: 标准创建数据
            creator: 创建人

        Returns:
            创建的字段标准对象

        Raises:
            ValueError: 编码重复
        """
        try:
            # 1. 检查编码是否重复
            existing = await self._get_by_code(db, standard_data.standard_code)
            if existing:
                raise ValueError(f"字段标准编码 {standard_data.standard_code} 已存在")

            # 2. 创建字段标准对象
            standard = FieldStandard(
                standard_code=standard_data.standard_code,
                standard_name=standard_data.standard_name,
                standard_name_en=standard_data.standard_name_en,
                data_type=standard_data.data_type,
                length=standard_data.length,
                precision=standard_data.precision,
                scale=standard_data.scale,
                is_nullable=standard_data.is_nullable,
                default_value=standard_data.default_value,
                value_range=standard_data.value_range,
                regex_pattern=standard_data.regex_pattern,
                business_definition=standard_data.business_definition,
                usage_guidelines=standard_data.usage_guidelines,
                example_values=standard_data.example_values,
                category=standard_data.category,
                tags=standard_data.tags,
                is_active=True,
                version="1.0",
                created_by=creator,
                updated_by=creator
            )

            db.add(standard)
            await db.commit()
            await db.refresh(standard)

            logger.info(f"创建字段标准成功: {standard.standard_name} (ID: {standard.id})")
            return standard

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"创建字段标准失败: {e}")
            raise ValueError(f"创建标准失败: {str(e)}")

    async def get_standard_by_id(
            self,
            db: AsyncSession,
            standard_id: int
    ) -> Optional[FieldStandard]:
        """
        根据ID获取字段标准

        Args:
            db: 数据库会话
            standard_id: 标准ID

        Returns:
            字段标准对象，不存在返回None
        """
        try:
            result = await db.execute(
                select(FieldStandard).where(FieldStandard.id == standard_id)
            )
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"获取字段标准失败: {e}")
            return None

    async def update_standard(
            self,
            db: AsyncSession,
            standard_id: int,
            standard_data: FieldStandardUpdate,
            updater: Optional[str] = None
    ) -> FieldStandard:
        """
        更新字段标准

        Args:
            db: 数据库会话
            standard_id: 标准ID
            standard_data: 更新数据
            updater: 更新人

        Returns:
            更新后的字段标准对象

        Raises:
            ValueError: 标准不存在
        """
        try:
            # 1. 获取字段标准
            standard = await self.get_standard_by_id(db, standard_id)
            if not standard:
                raise ValueError(f"字段标准 ID {standard_id} 不存在")

            # 2. 更新字段
            update_data = standard_data.model_dump(exclude_unset=True)
            for field, value in update_data.items():
                setattr(standard, field, value)

            standard.updated_by = updater

            await db.commit()
            await db.refresh(standard)

            logger.info(f"更新字段标准成功: {standard.standard_name} (ID: {standard.id})")
            return standard

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"更新字段标准失败: {e}")
            raise ValueError(f"更新标准失败: {str(e)}")

    async def delete_standard(
            self,
            db: AsyncSession,
            standard_id: int,
            force: bool = False
    ) -> bool:
        """
        删除字段标准

        Args:
            db: 数据库会话
            standard_id: 标准ID
            force: 是否强制删除

        Returns:
            是否删除成功

        Raises:
            ValueError: 标准不存在或正在使用
        """
        try:
            # 1. 获取字段标准
            standard = await self.get_standard_by_id(db, standard_id)
            if not standard:
                raise ValueError(f"字段标准 ID {standard_id} 不存在")

            # 2. 检查是否正在使用
            if not force:
                usage_count = await self._check_standard_usage(db, standard_id)
                if usage_count > 0:
                    raise ValueError(f"字段标准正在被 {usage_count} 个字段使用，无法删除")

            # 3. 删除字段标准
            if force:
                # 物理删除
                await db.delete(standard)
            else:
                # 软删除（标记为不启用）
                standard.is_active = False

            await db.commit()

            logger.info(f"删除字段标准成功: {standard.standard_name} (ID: {standard.id}, force={force})")
            return True

        except ValueError:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(f"删除字段标准失败: {e}")
            raise ValueError(f"删除标准失败: {str(e)}")

    async def search_standards(
            self,
            db: AsyncSession,
            page: int = 1,
            page_size: int = 20,
            keyword: Optional[str] = None,
            category: Optional[str] = None,
            data_type: Optional[str] = None,
            is_active: Optional[bool] = None
    ) -> Tuple[List[FieldStandard], int]:
        """
        搜索字段标准（分页）

        Args:
            db: 数据库会话
            page: 页码
            page_size: 每页数量
            keyword: 搜索关键词（名称、编码、定义）
            category: 分类
            data_type: 数据类型
            is_active: 是否启用

        Returns:
            (标准列表, 总数)
        """
        try:
            # 1. 构建查询条件
            conditions = []

            if keyword:
                keyword_pattern = f"%{keyword}%"
                conditions.append(
                    or_(
                        FieldStandard.standard_name.like(keyword_pattern),
                        FieldStandard.standard_code.like(keyword_pattern),
                        FieldStandard.standard_name_en.like(keyword_pattern),
                        FieldStandard.business_definition.like(keyword_pattern)
                    )
                )

            if category:
                conditions.append(FieldStandard.category == category)

            if data_type:
                conditions.append(FieldStandard.data_type == data_type)

            if is_active is not None:
                conditions.append(FieldStandard.is_active == is_active)

            # 2. 查询总数
            count_query = select(func.count(FieldStandard.id))
            if conditions:
                count_query = count_query.where(and_(*conditions))

            total_result = await db.execute(count_query)
            total = total_result.scalar_one()

            # 3. 分页查询
            query = select(FieldStandard)
            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(
                desc(FieldStandard.created_at)
            ).limit(page_size).offset((page - 1) * page_size)

            result = await db.execute(query)
            standards = result.scalars().all()

            return list(standards), total

        except Exception as e:
            logger.error(f"搜索字段标准失败: {e}")
            raise ValueError(f"搜索标准失败: {str(e)}")

    async def get_standards_by_category(
            self,
            db: AsyncSession,
            category: str
    ) -> List[FieldStandard]:
        """
        根据分类获取字段标准

        Args:
            db: 数据库会话
            category: 分类

        Returns:
            字段标准列表
        """
        try:
            result = await db.execute(
                select(FieldStandard)
                .where(
                    and_(
                        FieldStandard.category == category,
                        FieldStandard.is_active == True
                    )
                )
                .order_by(FieldStandard.standard_name)
            )
            return list(result.scalars().all())
        except Exception as e:
            logger.error(f"获取分类字段标准失败: {e}")
            return []

    async def get_all_categories(
            self,
            db: AsyncSession
    ) -> List[str]:
        """
        获取所有字段标准分类

        Args:
            db: 数据库会话

        Returns:
            分类列表
        """
        try:
            result = await db.execute(
                select(FieldStandard.category)
                .where(FieldStandard.category.isnot(None))
                .distinct()
            )
            categories = [row[0] for row in result.all()]
            return sorted(categories)
        except Exception as e:
            logger.error(f"获取分类列表失败: {e}")
            return []

    # ==================== 私有方法 ====================

    async def _get_by_code(
            self,
            db: AsyncSession,
            standard_code: str
    ) -> Optional[FieldStandard]:
        """根据编码获取字段标准"""
        try:
            result = await db.execute(
                select(FieldStandard).where(FieldStandard.standard_code == standard_code)
            )
            return result.scalar_one_or_none()
        except Exception:
            return None

    async def _check_standard_usage(
            self,
            db: AsyncSession,
            standard_id: int
    ) -> int:
        """检查字段标准的使用情况"""
        try:
            from app.models.data_asset import AssetColumn

            result = await db.execute(
                select(func.count(AssetColumn.id))
                .where(AssetColumn.field_standard_id == standard_id)
            )
            return result.scalar_one()
        except Exception:
            return 0


# 创建全局服务实例
field_standard_service = FieldStandardService()