"""
业务系统管理服务层
处理业务系统的核心业务逻辑
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc, asc
from loguru import logger

from app.models.business_system import BusinessSystem, BusinessSystemDataSource
from app.schemas.business_system import (
    BusinessSystemCreate,
    BusinessSystemUpdate,
    BusinessSystemSearchParams,
    BusinessSystemStatistics,
    BusinessSystemHealth,
    DataSourceAssociationCreate,
    SystemStatus,
    BatchOperationResult
)
from app.utils.database import get_async_db
from app.utils.cache_service import cache_service
from config.settings import settings
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, asc


class BusinessSystemService:
    """业务系统管理服务"""

    def __init__(self):
        self.cache_prefix = "business_system"
        self.cache_ttl = 300  # 5分钟缓存

    # === 基础CRUD操作 ===

    async def create_business_system(
            self,
            db: AsyncSession,
            system_data: BusinessSystemCreate
    ) -> BusinessSystem:
        """创建业务系统"""
        try:
            # 检查系统名称是否已存在
            result = await db.execute(
                select(BusinessSystem).where(BusinessSystem.system_name == system_data.system_name)
            )
            existing = result.scalar_one_or_none()

            if existing:
                raise ValueError(f"业务系统名称 '{system_data.system_name}' 已存在")

            # 创建新的业务系统
            db_system = BusinessSystem(
                system_name=system_data.system_name,
                display_name=system_data.display_name,
                description=system_data.description,
                contact_person=system_data.contact_person,
                contact_email=system_data.contact_email,
                contact_phone=system_data.contact_phone,
                system_type=system_data.system_type.value,
                business_domain=system_data.business_domain,
                criticality_level=system_data.criticality_level.value,
                database_info=system_data.database_info.dict() if system_data.database_info else None,
                data_volume_estimate=system_data.data_volume_estimate,
                is_data_source=system_data.is_data_source,
                go_live_date=system_data.go_live_date,
                tags=system_data.tags,
                metadata_info=system_data.metadata_info,
                status=SystemStatus.ACTIVE.value
            )

            db.add(db_system)
            await db.commit()
            await db.refresh(db_system)
            # 添加显式字段加载
            _ = db_system.created_at
            _ = db_system.updated_at
            _ = db_system.id
            # 清除相关缓存
            await self._invalidate_cache_patterns([
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics",
                f"{self.cache_prefix}:count"
            ])

            logger.info(f"创建业务系统成功: {system_data.system_name}")
            return db_system

        except Exception as e:
            await db.rollback()
            logger.error(f"创建业务系统失败: {e}")
            raise

    async def get_business_system_by_id(
            self,
            db: AsyncSession,
            system_id: int
    ) -> Optional[BusinessSystem]:
        """根据ID获取业务系统"""
        cache_key = f"{self.cache_prefix}:detail:{system_id}"

        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return self._deserialize_business_system(cached_data)

        # 从数据库查询
        result = await db.execute(select(BusinessSystem).where(BusinessSystem.id == system_id))
        system = result.scalar_one_or_none()

        if system:
            # 添加显式字段加载
            _ = system.created_at
            _ = system.updated_at
            _ = system.id
            _ = system.go_live_date
            _ = system.last_data_sync
            # 缓存结果
            await cache_service.set(cache_key, self._serialize_business_system(system), self.cache_ttl)

        return system

    async def get_business_system_by_name(
            self,
            db: AsyncSession,
            system_name: str
    ) -> Optional[BusinessSystem]:
        """根据名称获取业务系统"""
        cache_key = f"{self.cache_prefix}:name:{system_name}"

        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return self._deserialize_business_system(cached_data)

        result = await db.execute(select(BusinessSystem).where(BusinessSystem.system_name == system_name))
        system = result.scalar_one_or_none()

        if system:
            # 添加显式字段加载
            _ = system.created_at
            _ = system.updated_at
            _ = system.id
            _ = system.go_live_date
            _ = system.last_data_sync
            await cache_service.set(cache_key, self._serialize_business_system(system), self.cache_ttl)

        return system

    async def update_business_system(
            self,
            db: AsyncSession,
            system_id: int,
            update_data: BusinessSystemUpdate
    ) -> Optional[BusinessSystem]:
        """更新业务系统"""
        try:
            result = await db.execute(select(BusinessSystem).where(BusinessSystem.id == system_id))
            system = result.scalar_one_or_none()
            if not system:
                return None

            # 更新字段
            update_dict = update_data.dict(exclude_unset=True)

            for field, value in update_dict.items():
                if hasattr(system, field):
                    if field in ['system_type', 'criticality_level'] and value:
                        # 处理枚举类型
                        setattr(system, field, value.value if hasattr(value, 'value') else value)
                    elif field == 'database_info' and value:
                        setattr(system, field, value.dict() if hasattr(value, 'dict') else value)
                    else:
                        setattr(system, field, value)

            await db.commit()
            await db.refresh(system)

            # 清除相关缓存
            # await self._invalidate_cache_patterns([
            #     f"{self.cache_prefix}:detail:{system_id}",
            #     f"{self.cache_prefix}:name:{system.system_name}",
            #     f"{self.cache_prefix}:list:*",
            #     f"{self.cache_prefix}:statistics"
            # ])
            # 添加显式字段加载
            _ = system.created_at
            _ = system.updated_at
            _ = system.id

            logger.info(f"更新业务系统成功: {system.system_name}")
            return system

        except Exception as e:
            await db.rollback()
            logger.error(f"更新业务系统失败: {e}")
            raise

    async def delete_business_system(self, db: AsyncSession, system_id: int) -> bool:
        """删除业务系统（软删除）"""
        try:
            result = await db.execute(select(BusinessSystem).where(BusinessSystem.id == system_id))
            system = result.scalar_one_or_none()
            if not system:
                return False

            # 软删除：设置状态为deprecated
            system.status = SystemStatus.DEPRECATED.value
            await db.commit()

            # 清除相关缓存
            await self._invalidate_cache_patterns([
                f"{self.cache_prefix}:detail:{system_id}",
                f"{self.cache_prefix}:name:{system.system_name}",
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics",
                f"{self.cache_prefix}:count"
            ])

            logger.info(f"删除业务系统成功: {system.system_name}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除业务系统失败: {e}")
            raise

    # === 列表查询和搜索 ===

    async def get_business_systems_list(
            self,
            db: AsyncSession,
            search_params: BusinessSystemSearchParams
    ) -> Tuple[List[BusinessSystem], int]:
        """获取业务系统列表（支持搜索和分页）"""
        cache_key = f"{self.cache_prefix}:list:{hash(str(search_params.dict()))}"

        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return self._deserialize_list_result(cached_data)

        # 构建查询
        base_query = select(BusinessSystem)

        # 应用搜索条件
        filtered_query = self._apply_search_filters_async(base_query, search_params)

        # 获取总数
        count_result = await db.execute(select(func.count()).select_from(filtered_query.subquery()))
        total = count_result.scalar()

        # 应用排序
        final_query = self._apply_sorting_async(filtered_query, search_params)

        # 应用分页
        offset = (search_params.page - 1) * search_params.page_size
        final_query = final_query.offset(offset).limit(search_params.page_size)

        # 执行查询
        query_result = await db.execute(final_query)
        systems = query_result.scalars().all()

        for system in systems:
            _ = system.created_at
            _ = system.updated_at
            _ = system.id
            _ = system.go_live_date
            _ = system.last_data_sync

        # 将 systems 转换为列表（如果不是的话）
        systems_list = list(systems) if systems else []

        # 缓存结果
        cache_result = (systems_list, total)
        await cache_service.set(cache_key, self._serialize_list_result(cache_result), self.cache_ttl)

        # 返回结果
        return cache_result

    def _apply_search_filters(self, query, search_params: BusinessSystemSearchParams):
        """应用搜索过滤条件"""
        # 关键词搜索
        if search_params.keyword:
            keyword = f"%{search_params.keyword}%"
            query = query.filter(
                or_(
                    BusinessSystem.system_name.ilike(keyword),
                    BusinessSystem.display_name.ilike(keyword),
                    BusinessSystem.description.ilike(keyword),
                    BusinessSystem.business_domain.ilike(keyword)
                )
            )

        # 状态过滤
        if search_params.status:
            query = query.filter(BusinessSystem.status == search_params.status.value)

        # 系统类型过滤
        if search_params.system_type:
            query = query.filter(BusinessSystem.system_type == search_params.system_type.value)

        # 业务领域过滤
        if search_params.business_domain:
            query = query.filter(BusinessSystem.business_domain == search_params.business_domain)

        # 重要程度过滤
        if search_params.criticality_level:
            query = query.filter(BusinessSystem.criticality_level == search_params.criticality_level.value)

        # 标签过滤
        if search_params.tags:
            for tag in search_params.tags:
                query = query.filter(BusinessSystem.tags.contains([tag]))

        return query

    def _apply_sorting(self, query, search_params: BusinessSystemSearchParams):
        """应用排序"""
        order_field = getattr(BusinessSystem, search_params.order_by, BusinessSystem.created_at)

        if search_params.order_desc:
            query = query.order_by(desc(order_field))
        else:
            query = query.order_by(asc(order_field))

        return query

    # === 统计和监控 ===

    async def get_business_systems_statistics(self, db: AsyncSession) -> BusinessSystemStatistics:
        """获取业务系统统计信息"""
        cache_key = f"{self.cache_prefix}:statistics"

        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return BusinessSystemStatistics(**cached_data)

        # 计算统计信息
        total_result = await db.execute(select(func.count(BusinessSystem.id)))
        total_systems = total_result.scalar()


        # 活跃系统数
        active_result = await db.execute(
            select(func.count(BusinessSystem.id)).where(BusinessSystem.status == SystemStatus.ACTIVE.value)
        )
        active_systems = active_result.scalar()

        # 按类型分组统计
        type_result = await db.execute(
            select(BusinessSystem.system_type, func.count(BusinessSystem.id))
            .group_by(BusinessSystem.system_type)
        )
        by_type = dict(type_result.all())

        # 按业务领域分组统计
        domain_result = await db.execute(
            select(BusinessSystem.business_domain, func.count(BusinessSystem.id))
            .where(BusinessSystem.business_domain.isnot(None))
            .group_by(BusinessSystem.business_domain)
        )
        by_domain = dict(domain_result.all())

        # 按重要程度分组统计
        criticality_result = await db.execute(
            select(BusinessSystem.criticality_level, func.count(BusinessSystem.id))
            .group_by(BusinessSystem.criticality_level)
        )
        by_criticality = dict(criticality_result.all())

        # 按状态分组统计
        status_result = await db.execute(
            select(BusinessSystem.status, func.count(BusinessSystem.id))
            .group_by(BusinessSystem.status)
        )
        by_status = dict(status_result.all())

        # 总表数量
        tables_result = await db.execute(select(func.sum(BusinessSystem.table_count)))
        total_tables = tables_result.scalar() or 0

        # 平均数据质量分数
        quality_result = await db.execute(select(func.avg(BusinessSystem.data_quality_score)))
        avg_data_quality = quality_result.scalar()
        inactive_systems = total_systems - active_systems
        statistics = BusinessSystemStatistics(
            total_systems=total_systems,
            active_systems=active_systems,
            inactive_systems=inactive_systems,
            by_type=by_type,
            by_domain=by_domain,
            by_criticality=by_criticality,
            by_status=by_status,
            total_tables=total_tables,
            avg_data_quality=round(avg_data_quality, 2) if avg_data_quality else None,
            last_updated=datetime.now()
        )

        # 缓存结果
        await cache_service.set(cache_key, statistics.dict(), self.cache_ttl)

        return statistics

    async def get_business_systems_count(self, db: AsyncSession) -> int:
        """获取活跃业务系统数量（用于总览页面）"""
        cache_key = f"{self.cache_prefix}:count"

        # 尝试从缓存获取
        cached_count = await cache_service.get(cache_key)
        if cached_count is not None:
            return cached_count

        # 查询活跃系统数量
        result = await db.execute(
            select(func.count(BusinessSystem.id)).where(BusinessSystem.status == SystemStatus.ACTIVE.value)
        )
        count = result.scalar()

        # 缓存结果
        await cache_service.set(cache_key, count, self.cache_ttl)

        return count

    async def get_business_system_health(
            self,
            db: AsyncSession,
            system_id: int
    ) -> Optional[BusinessSystemHealth]:
        """获取业务系统健康状态"""
        system = await self.get_business_system_by_id(db, system_id)
        if not system:
            return None

        # 检查数据库连接状态
        database_connection = await self._check_database_connection(system)

        # 检查数据新鲜度
        data_freshness = self._check_data_freshness(system)

        # 检查同步状态
        sync_status = self._check_sync_status(system)

        # 收集问题
        issues = []
        if not database_connection:
            issues.append("数据库连接失败")
        if system.last_data_sync and (datetime.now() - system.last_data_sync).days > 7:
            issues.append("数据同步超过7天未更新")
        if system.data_quality_score and system.data_quality_score < 70:
            issues.append("数据质量分数偏低")

        # 确定整体状态
        if not database_connection:
            overall_status = "error"
        elif issues:
            overall_status = "warning"
        else:
            overall_status = "healthy"

        return BusinessSystemHealth(
            system_id=system.id,
            system_name=system.system_name,
            overall_status=overall_status,
            database_connection=database_connection,
            data_freshness=data_freshness,
            data_quality_score=system.data_quality_score,
            last_sync_time=system.last_data_sync,
            sync_status=sync_status,
            issues=issues,
            checked_at=datetime.now()
        )

    # === 状态管理 ===

    async def update_system_status(
            self,
            db: AsyncSession,
            system_id: int,
            new_status: SystemStatus,
            reason: Optional[str] = None
    ) -> Optional[BusinessSystem]:
        """更新业务系统状态"""
        try:
            result = await db.execute(select(BusinessSystem).where(BusinessSystem.id == system_id))
            system = result.scalar_one_or_none()
            if not system:
                return None

            old_status = system.status
            system.status = new_status.value

            # 记录状态变更日志
            if system.metadata_info is None:
                system.metadata_info = {}

            if 'status_history' not in system.metadata_info:
                system.metadata_info['status_history'] = []

            system.metadata_info['status_history'].append({
                'old_status': old_status,
                'new_status': new_status.value,
                'reason': reason,
                'changed_at': datetime.now().isoformat(),
                'changed_by': 'system'  # 实际应用中应该记录操作用户
            })

            await db.commit()
            await db.refresh(system)

            # 添加显式字段加载
            _ = system.created_at
            _ = system.updated_at
            _ = system.id

            # 清除相关缓存
            await self._invalidate_cache_patterns([
                f"{self.cache_prefix}:detail:{system_id}",
                f"{self.cache_prefix}:name:{system.system_name}",
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics",
                f"{self.cache_prefix}:count"
            ])

            logger.info(f"更新系统状态成功: {system.system_name} {old_status} -> {new_status.value}")
            return system

        except Exception as e:
            await db.rollback()
            logger.error(f"更新系统状态失败: {e}")
            raise

    # === 数据源关联管理 ===

    async def add_data_source_association(
            self,
            db: AsyncSession,
            system_id: int,
            association_data: DataSourceAssociationCreate
    ) -> BusinessSystemDataSource:
        """添加数据源关联"""
        try:
            # 检查业务系统是否存在
            system = await self.get_business_system_by_id(db, system_id)
            if not system:
                raise ValueError(f"业务系统 {system_id} 不存在")

            # 检查是否已存在相同的关联
            result = await db.execute(
                select(BusinessSystemDataSource).where(
                    and_(
                        BusinessSystemDataSource.business_system_id == system_id,
                        BusinessSystemDataSource.data_source_name == association_data.data_source_name
                    )
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                raise ValueError(f"数据源关联已存在: {association_data.data_source_name}")

            # 创建新关联
            db_association = BusinessSystemDataSource(
                business_system_id=system_id,
                data_source_name=association_data.data_source_name,
                data_source_type=association_data.data_source_type,
                database_name=association_data.database_name,
                table_prefix=association_data.table_prefix,
                sync_frequency=association_data.sync_frequency.value,
                is_primary=association_data.is_primary,
                status="active"
            )

            db.add(db_association)
            await db.commit()
            await db.refresh(db_association)

            _ = db_association.created_at
            _ = db_association.updated_at
            _ = db_association.id

            # 自动更新业务系统的表数量
            await self._update_system_table_count(db, system_id)

            logger.info(f"添加数据源关联成功: {system.system_name} -> {association_data.data_source_name}")
            return db_association

        except Exception as e:
            await db.rollback()
            logger.error(f"添加数据源关联失败: {e}")
            raise

    # === 批量操作 ===

    async def batch_import_systems(
            self,
            db: AsyncSession,
            systems_data: List[BusinessSystemCreate],
            overwrite_existing: bool = False
    ) -> BatchOperationResult:
        """批量导入业务系统"""
        total_count = len(systems_data)
        success_count = 0
        failed_count = 0
        failed_items = []

        for i, system_data in enumerate(systems_data):
            try:
                # 检查是否已存在
                result = await db.execute(
                    select(BusinessSystem).where(BusinessSystem.system_name == system_data.system_name)
                )
                existing = result.scalar_one_or_none()

                if existing and not overwrite_existing:
                    failed_items.append({
                        "index": i,
                        "system_name": system_data.system_name,
                        "error": "系统已存在且未设置覆盖"
                    })
                    failed_count += 1
                    continue

                if existing and overwrite_existing:
                    # 更新现有系统
                    update_data = BusinessSystemUpdate(**system_data.dict(exclude={'system_name'}))
                    await self.update_business_system(db, existing.id, update_data)
                else:
                    # 创建新系统
                    await self.create_business_system(db, system_data)

                success_count += 1

            except Exception as e:
                failed_items.append({
                    "index": i,
                    "system_name": system_data.system_name,
                    "error": str(e)
                })
                failed_count += 1

        return BatchOperationResult(
            total_count=total_count,
            success_count=success_count,
            failed_count=failed_count,
            failed_items=failed_items,
            operation_time=datetime.now()
        )

    async def get_system_data_sources(
            self,
            db: AsyncSession,
            system_id: int
    ) -> List[BusinessSystemDataSource]:
        """获取业务系统的数据源关联"""
        try:
            result = await db.execute(
                select(BusinessSystemDataSource).where(
                    BusinessSystemDataSource.business_system_id == system_id
                )
            )
            associations = result.scalars().all()

            # 确保字段已加载
            for assoc in associations:
                _ = assoc.created_at
                _ = assoc.updated_at
                _ = assoc.id

            return list(associations)

        except Exception as e:
            logger.error(f"获取数据源关联失败: {e}")
            raise

    async def _update_system_table_count(self, db: AsyncSession, system_id: int):
        """更新业务系统的表数量统计"""
        try:
            # 获取该业务系统关联的所有数据源
            result = await db.execute(
                select(BusinessSystemDataSource).where(
                    BusinessSystemDataSource.business_system_id == system_id
                )
            )
            associations = result.scalars().all()

            total_tables = 0
            for assoc in associations:
                # 从数据源表元数据中统计表数量
                # 注意：这里需要确保你之前创建的 DataSourceTable 模型可以使用
                try:
                    from app.models.data_source import DataSourceTable
                    table_result = await db.execute(
                        select(func.count(DataSourceTable.id)).where(
                            and_(
                                DataSourceTable.data_source_name == assoc.data_source_name,
                                DataSourceTable.is_active == True
                            )
                        )
                    )
                    count = table_result.scalar() or 0
                    total_tables += count
                    logger.info(f"数据源 {assoc.data_source_name} 包含 {count} 张表")
                except Exception as e:
                    logger.warning(f"无法统计数据源 {assoc.data_source_name} 的表数量: {e}")
                    # 如果没有表元数据，尝试直接从数据源获取
                    total_tables += await self._get_table_count_from_source(assoc.data_source_name)

            # 更新业务系统的表数量和同步时间
            system_result = await db.execute(
                select(BusinessSystem).where(BusinessSystem.id == system_id)
            )
            system = system_result.scalar_one_or_none()

            if system:
                system.table_count = total_tables
                system.last_data_sync = datetime.now()
                await db.commit()
                logger.info(f"更新业务系统 {system.system_name} 表数量: {total_tables}")

            return total_tables

        except Exception as e:
            logger.error(f"更新系统表数量失败: {e}")
            return 0

    async def _get_table_count_from_source(self, data_source_name: str) -> int:
        """从数据源直接获取表数量"""
        try:
            # 这里调用数据源管理模块的接口
            from app.services.optimized_data_integration_service import get_optimized_data_integration_service
            integration_service = get_optimized_data_integration_service()

            # 获取表列表并计数
            result = await integration_service.get_tables(data_source_name, limit=9999)
            if result.get('success'):
                return result.get('total_count', 0)
            return 0

        except Exception as e:
            logger.warning(f"从数据源 {data_source_name} 获取表数量失败: {e}")
            return 0

    # === 缓存管理 ===

    async def _invalidate_cache_patterns(self, patterns: List[str]):
        """批量清除缓存模式"""
        tasks = [cache_service.delete_pattern(pattern) for pattern in patterns]
        await asyncio.gather(*tasks, return_exceptions=True)

    def _serialize_business_system(self, system: BusinessSystem) -> Dict:
        """序列化业务系统对象"""
        return {
            "id": system.id,
            "system_name": system.system_name,
            "display_name": system.display_name,
            "description": system.description,
            "contact_person": system.contact_person,
            "contact_email": system.contact_email,
            "contact_phone": system.contact_phone,
            "system_type": system.system_type,
            "business_domain": system.business_domain,
            "criticality_level": system.criticality_level,
            "database_info": system.database_info,
            "data_volume_estimate": system.data_volume_estimate,
            "status": system.status,
            "is_data_source": system.is_data_source,
            "go_live_date": system.go_live_date.isoformat() if system.go_live_date else None,
            "last_data_sync": system.last_data_sync.isoformat() if system.last_data_sync else None,
            "tags": system.tags,
            "metadata_info": system.metadata_info,
            "table_count": system.table_count,
            "data_quality_score": system.data_quality_score,
            "created_at": system.created_at.isoformat(),
            "updated_at": system.updated_at.isoformat()
        }

    def _deserialize_business_system(self, data: Dict) -> BusinessSystem:
        """反序列化业务系统对象"""
        # 这里应该创建一个BusinessSystem对象
        # 为了简化，返回一个模拟对象
        system = BusinessSystem()
        for key, value in data.items():
            if key in ['go_live_date', 'last_data_sync', 'created_at', 'updated_at'] and value:
                setattr(system, key, datetime.fromisoformat(value))
            else:
                setattr(system, key, value)
        return system

    def _serialize_list_result(self, result: Tuple[List[BusinessSystem], int]) -> Dict:
        """序列化列表查询结果"""
        systems, total = result
        return {
            "systems": [self._serialize_business_system(system) for system in systems],
            "total": total
        }

    def _deserialize_list_result(self, data: Dict) -> Tuple[List[BusinessSystem], int]:
        """反序列化列表查询结果"""
        systems = [self._deserialize_business_system(system_data) for system_data in data["systems"]]
        return systems, data["total"]

    # === 健康检查辅助方法 ===

    async def _check_database_connection(self, system: BusinessSystem) -> bool:
        """检查数据库连接状态"""
        if not system.database_info:
            return True  # 如果没有数据库信息，认为正常

        try:
            # 这里应该实际检查数据库连接
            # 为了演示，返回模拟结果
            return True
        except Exception as e:
            logger.warning(f"数据库连接检查失败 {system.system_name}: {e}")
            return False

    def _check_data_freshness(self, system: BusinessSystem) -> str:
        """检查数据新鲜度"""
        if not system.last_data_sync:
            return "unknown"

        days_since_sync = (datetime.now() - system.last_data_sync).days

        if days_since_sync == 0:
            return "fresh"
        elif days_since_sync <= 1:
            return "recent"
        elif days_since_sync <= 7:
            return "stale"
        else:
            return "outdated"

    def _check_sync_status(self, system: BusinessSystem) -> str:
        """检查同步状态"""
        if not system.last_data_sync:
            return "never_synced"

        hours_since_sync = (datetime.now() - system.last_data_sync).total_seconds() / 3600

        if hours_since_sync <= 1:
            return "synced"
        elif hours_since_sync <= 24:
            return "pending"
        else:
            return "overdue"

    def _apply_search_filters_async(self, query, search_params: BusinessSystemSearchParams):
        """应用搜索过滤条件 - 异步版本"""
        # 关键词搜索
        if search_params.keyword:
            keyword = f"%{search_params.keyword}%"
            query = query.where(
                or_(
                    BusinessSystem.system_name.ilike(keyword),
                    BusinessSystem.display_name.ilike(keyword),
                    BusinessSystem.description.ilike(keyword),
                    BusinessSystem.business_domain.ilike(keyword)
                )
            )

        # 状态过滤
        if search_params.status:
            query = query.where(BusinessSystem.status == search_params.status.value)

        # 系统类型过滤
        if search_params.system_type:
            query = query.where(BusinessSystem.system_type == search_params.system_type.value)

        # 业务领域过滤
        if search_params.business_domain:
            query = query.where(BusinessSystem.business_domain == search_params.business_domain)

        # 重要程度过滤
        if search_params.criticality_level:
            query = query.where(BusinessSystem.criticality_level == search_params.criticality_level.value)

        return query

    def _apply_sorting_async(self, query, search_params: BusinessSystemSearchParams):
        """应用排序 - 异步版本"""
        order_field = getattr(BusinessSystem, search_params.order_by, BusinessSystem.created_at)

        if search_params.order_desc:
            query = query.order_by(desc(order_field))
        else:
            query = query.order_by(asc(order_field))

        return query


# 全局业务系统服务实例
business_system_service = BusinessSystemService()