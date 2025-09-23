# app/services/user_cluster_service.py
"""
用户计算集群管理服务层
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, and_, or_, func
from sqlalchemy.orm import selectinload
from loguru import logger
from sqlalchemy.testing import db

from app.models.user_cluster import UserCluster, ClusterType, ClusterStatus
from app.utils.response import create_response
from app.services.cluster_node_discovery import WebApiClusterDiscovery


class UserClusterService:
    """用户计算集群管理服务"""

    def __init__(self):
        self.node_discovery = WebApiClusterDiscovery()
        from app.utils.database import async_engine
        self.db_engine = async_engine

    async def get_clusters_list(
            self,
            db: AsyncSession,
            page: int = 1,
            page_size: int = 10,
            search_keyword: str = ""
    ) -> Dict:
        """获取用户集群列表（分页）"""
        try:
            # 构建查询条件
            query = select(UserCluster)

            if search_keyword:
                query = query.where(
                    or_(
                        UserCluster.name.contains(search_keyword),
                        UserCluster.remark.contains(search_keyword)
                    )
                )

            # 计算总数
            count_query = select(func.count(UserCluster.id))
            if search_keyword:
                count_query = count_query.where(
                    or_(
                        UserCluster.name.contains(search_keyword),
                        UserCluster.remark.contains(search_keyword)
                    )
                )

            total_result = await db.execute(count_query)
            total = total_result.scalar()

            # 分页查询
            query = query.order_by(UserCluster.updated_at.desc())
            query = query.offset((page - 1) * page_size).limit(page_size)

            result = await db.execute(query)
            clusters = result.scalars().all()

            return {
                "content": [cluster.to_dict() for cluster in clusters],
                "totalElements": total,
                "currentPage": page,
                "pageSize": page_size,
                "totalPages": (total + page_size - 1) // page_size
            }

        except Exception as e:
            logger.error(f"获取集群列表失败: {e}")
            raise

    async def add_cluster(
            self,
            db: AsyncSession,
            name: str,
            cluster_type: str,
            remark: str = "",
            web_api_config: Dict = None,
            auto_discovery: bool = True
    ) -> Dict:
        """添加新集群（包含自动节点发现）"""
        try:
            # 1. 检查名称是否重复
            existing = await db.execute(
                select(UserCluster).where(UserCluster.name == name)
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"集群名称 '{name}' 已存在")

            # 2. 验证集群类型
            logger.info(f"开始验证集群类型: '{cluster_type}'")
            logger.info(f"ClusterType 枚举定义: {ClusterType}")
            logger.info(f"ClusterType 所有值: {[e.name + '=' + e.value for e in ClusterType]}")

            try:
                cluster_type_enum = ClusterType(cluster_type)
                logger.info(f"成功转换: {cluster_type_enum}")
            except ValueError as ve:
                logger.error(f"转换失败: {ve}")
                # 逐个检查
                for enum_item in ClusterType:
                    logger.info(f"检查 {enum_item.value} == '{cluster_type}': {enum_item.value == cluster_type}")
                raise ValueError(f"不支持的集群类型: {cluster_type}")

            # 3. 如果启用自动发现，先发现节点
            discovery_result = {}
            if auto_discovery and web_api_config:
                logger.info(f"开始自动发现 {cluster_type} 集群节点...")
                discovery_result = await self.node_discovery.discover_cluster_nodes(
                    cluster_type, web_api_config
                )

                if not discovery_result.get('success'):
                    logger.warning(f"节点自动发现失败: {discovery_result.get('error')}")
                else:
                    logger.info(f"成功发现 {discovery_result.get('total_nodes', 0)} 个节点")

            # 4. 创建集群记录
            cluster_config = web_api_config or {}
            if discovery_result.get('success'):
                cluster_config['discovered_nodes'] = discovery_result['nodes']
                cluster_config['cluster_info'] = discovery_result['cluster_info']
                cluster_config['last_discovery'] = discovery_result['discovery_time']

            # 从发现结果中提取详细信息
            nodes = discovery_result.get('nodes', [])
            cluster_info = discovery_result.get('cluster_info', {})
            nodes_by_type = discovery_result.get('nodes_by_type', {})

            # 计算节点统计
            total_nodes = len(nodes)
            active_nodes = len([n for n in nodes if n.get('status') == 'active'])

            # 计算存储信息（从cluster_info中获取）
            total_capacity = cluster_info.get('total_capacity', 0)
            used_capacity = cluster_info.get('used_capacity', 0)

            # 修复存储和内存信息显示逻辑
            if cluster_type_enum == ClusterType.DORIS:
                # Doris: 存储用容量，内存用JVM堆内存
                storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "0GB/0GB"

                # 计算 Frontend JVM 内存汇总
                total_heap_memory = 0
                used_heap_memory = 0
                for node in nodes:
                    if node.get('role') == 'Frontend':
                        total_heap_memory += node.get('jvm_heap_max', 0)
                        used_heap_memory += node.get('jvm_heap_used', 0)

                memory_info = f"{used_heap_memory / (1024 ** 3):.1f}GB/{total_heap_memory / (1024 ** 3):.1f}GB" if total_heap_memory > 0 else "0GB/0GB"

            elif cluster_type_enum == ClusterType.FLINK:
                # Flink: 显示内存而不是存储（因为Flink不是存储系统）
                total_memory = cluster_info.get('total_memory', 0)
                used_memory = cluster_info.get('used_memory', 0)
                storage_info = f"{used_memory / (1024 ** 3):.1f}GB/{total_memory / (1024 ** 3):.1f}GB" if total_memory > 0 else "0GB/0GB"
                memory_info = storage_info  # Flink的存储字段实际显示内存

            elif cluster_type_enum == ClusterType.HADOOP:
                # Hadoop: 存储用HDFS容量，内存用NameNode JVM内存
                storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "未知"

                # 使用NameNode的JVM内存
                namenode_heap_used = cluster_info.get('namenode_jvm_heap_used', 0)
                namenode_heap_max = cluster_info.get('namenode_jvm_heap_max', 0)
                memory_info = f"{namenode_heap_used / (1024 ** 3):.1f}GB/{namenode_heap_max / (1024 ** 3):.1f}GB" if namenode_heap_max > 0 else "0GB/0GB"

            else:
                storage_info = "未知"
                memory_info = "0GB/0GB"
            # 构建URL
            web_ui_url = None
            master_url = None

            if cluster_type == 'hadoop' and web_api_config:
                namenode_host = web_api_config.get('namenode_host')
                namenode_port = web_api_config.get('namenode_web_port', 9870)
                web_ui_url = f"http://{namenode_host}:{namenode_port}"
                master_url = f"hdfs://{namenode_host}:9000"
            elif cluster_type == 'flink' and web_api_config:
                jm_host = web_api_config.get('jobmanager_host')
                jm_port = web_api_config.get('jobmanager_web_port', 8081)
                web_ui_url = f"http://{jm_host}:{jm_port}"
                master_url = f"flink://{jm_host}:6123"
            elif cluster_type == 'doris' and web_api_config:
                fe_host = web_api_config.get('frontend_host')
                fe_web_port = web_api_config.get('frontend_web_port', 8060)
                fe_query_port = 9030  # Doris 查询端口
                web_ui_url = f"http://{fe_host}:{fe_web_port}"
                master_url = f"mysql://{fe_host}:{fe_query_port}"

            new_cluster = UserCluster(
                name=name,
                cluster_type=cluster_type_enum,
                remark=remark,
                config=cluster_config,
                status=ClusterStatus.ACTIVE if discovery_result.get('success') else ClusterStatus.CHECKING,
                node_count=f"{active_nodes}/{total_nodes}",
                storage_info=storage_info,
                master_url=master_url,
                web_ui_url=web_ui_url
            )

            db.add(new_cluster)
            await db.commit()
            await db.refresh(new_cluster)

            # 5. 异步更新集群状态（如果发现成功）
            if discovery_result.get('success'):
                asyncio.create_task(self._update_cluster_nodes_info(new_cluster.id, discovery_result))

            logger.info(f"成功添加集群: {name}")

            # 启动后台任务更新节点信息 - 修改这里
            if auto_discovery and discovery_result.get('success', False):
                # 使用修复后的方法名
                asyncio.create_task(self._update_cluster_nodes_info_background(new_cluster.id))

            return new_cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"添加集群失败: {e}")
            raise

    async def _update_cluster_nodes_info_background(self, cluster_id: int):
        """后台更新集群节点信息的包装方法"""
        try:
            # 延迟几秒再执行，确保事务完成
            await asyncio.sleep(2)

            # 创建新的数据库会话
            from app.utils.database import async_session_maker
            async with async_session_maker() as db:
                await self._update_cluster_nodes_info_fixed(db, cluster_id)

        except Exception as e:
            logger.error(f"后台更新集群 {cluster_id} 节点信息失败: {e}")

    async def _update_cluster_nodes_info_fixed(self, db: AsyncSession, cluster_id: int):
        """修复的集群节点信息更新方法"""
        try:
            # 获取集群信息
            from sqlalchemy import select
            from app.models.cluster import UserCluster, ClusterStatus

            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                logger.error(f"后台更新: 集群 ID {cluster_id} 不存在")
                return

            logger.info(f"开始后台更新集群节点信息: {cluster.name} (ID: {cluster_id})")

            # 获取Web API配置
            web_api_config = cluster.config.get('webApiConfig')
            if not web_api_config:
                web_api_config = cluster.config.get('web_api_config')

            # 兼容现有数据结构
            if not web_api_config:
                if 'namenode_host' in cluster.config:
                    web_api_config = {
                        'namenode_host': cluster.config.get('namenode_host'),
                        'namenode_web_port': cluster.config.get('namenode_web_port'),
                        'namenode_ha_hosts': cluster.config.get('namenode_ha_hosts'),
                        'journalnode_hosts': cluster.config.get('journalnode_hosts')
                    }
                elif 'frontend_host' in cluster.config:
                    web_api_config = {
                        'frontend_host': cluster.config.get('frontend_host'),
                        'frontend_web_port': cluster.config.get('frontend_web_port'),
                        'username': cluster.config.get('username'),
                        'password': cluster.config.get('password')
                    }

            if not web_api_config:
                raise ValueError("集群缺少Web API配置信息")

            logger.info(f"使用配置更新集群 {cluster.name}: {web_api_config}")

            # 执行真实的集群检测
            try:
                discovery_result = await asyncio.wait_for(
                    self.node_discovery.discover_cluster_nodes(
                        cluster.cluster_type.value,
                        web_api_config
                    ),
                    timeout=60  # 60秒超时
                )

                if discovery_result.get('success'):
                    # 更新集群状态
                    nodes = discovery_result.get('nodes', [])
                    cluster_info = discovery_result.get('cluster_info', {})

                    total_nodes = len(nodes)
                    active_nodes = len([n for n in nodes if n.get('status') == 'active'])

                    # 更新集群信息
                    cluster.status = ClusterStatus.ACTIVE
                    cluster.node_count = f"{active_nodes}/{total_nodes}"
                    cluster.check_datetime = datetime.now()

                    # 更新配置信息
                    updated_config = cluster.config.copy()
                    updated_config['discovered_nodes'] = nodes
                    updated_config['cluster_info'] = cluster_info
                    updated_config['last_discovery'] = datetime.now().isoformat()
                    cluster.config = updated_config

                    await db.commit()
                    logger.info(f"成功更新集群 {cluster.name} 的节点信息")

                else:
                    error_msg = discovery_result.get('error', '未知错误')
                    logger.error(f"集群 {cluster.name} 节点发现失败: {error_msg}")

                    cluster.status = ClusterStatus.ERROR
                    await db.commit()

            except asyncio.TimeoutError:
                logger.error(f"集群 {cluster.name} 节点发现超时")
                cluster.status = ClusterStatus.ERROR
                await db.commit()

        except Exception as e:
            logger.error(f"后台更新集群 {cluster_id} 节点信息失败: {e}")
            await db.rollback()
            # 发送告警
            await self._send_cluster_alert(cluster_id, "节点信息更新失败", str(e))

    async def delete_cluster(self, db: AsyncSession, cluster_id: int) -> bool:
        """删除集群"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            cluster_name = cluster.name
            await db.delete(cluster)
            await db.commit()

            logger.info(f"成功删除集群: {cluster_name}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除集群失败: {e}")
            raise

    async def check_cluster(self, db: AsyncSession, cluster_id: int) -> Dict:
        """检测计算集群状态 - 使用真实检测逻辑"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 使用真实的节点发现逻辑，而不是模拟数据
            web_api_config = cluster.config.get('webApiConfig')
            if not web_api_config:
                web_api_config = cluster.config.get('web_api_config')

            # 如果还是没有，检查是否配置直接存储在根级别
            if not web_api_config:
                # 检查根级别是否包含namenode_host等关键字段
                if 'namenode_host' in cluster.config:
                    web_api_config = {
                        'namenode_host': cluster.config.get('namenode_host'),
                        'namenode_web_port': cluster.config.get('namenode_web_port'),
                        'namenode_ha_hosts': cluster.config.get('namenode_ha_hosts'),
                        'journalnode_hosts': cluster.config.get('journalnode_hosts')
                    }
                elif 'frontend_host' in cluster.config:
                    web_api_config = {
                        'frontend_host': cluster.config.get('frontend_host'),
                        'frontend_web_port': cluster.config.get('frontend_web_port'),
                        'username': cluster.config.get('username'),
                        'password': cluster.config.get('password')
                    }
                elif 'jobmanager_host' in cluster.config:
                    web_api_config = {
                        'jobmanager_host': cluster.config.get('jobmanager_host'),
                        'jobmanager_web_port': cluster.config.get('jobmanager_web_port', 8081)
                    }

            if not web_api_config:
                raise ValueError("集群缺少Web API配置信息")

            # 调用真实的节点发现逻辑
            discovery_result = await self.node_discovery.discover_cluster_nodes(
                cluster.cluster_type.value, web_api_config
            )

            if discovery_result.get('success'):
                # 使用真实发现的数据更新集群信息
                nodes = discovery_result.get('nodes', [])
                cluster_info = discovery_result.get('cluster_info', {})

                total_nodes = len(nodes)
                active_nodes = len([n for n in nodes if n.get('status') == 'active'])

                # 计算真实的存储和内存信息 - 修改这里
                total_capacity = cluster_info.get('total_capacity', 0)
                used_capacity = cluster_info.get('used_capacity', 0)

                # 根据集群类型计算存储和内存信息
                if cluster.cluster_type == ClusterType.DORIS:
                    # Doris: 存储用容量，内存用JVM堆内存
                    storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "0GB/0GB"

                    # 计算 Frontend JVM 内存汇总
                    total_heap_memory = 0
                    used_heap_memory = 0
                    for node in nodes:
                        if node.get('role') == 'Frontend':
                            total_heap_memory += node.get('jvm_heap_max', 0)
                            used_heap_memory += node.get('jvm_heap_used', 0)

                    memory_info = f"{used_heap_memory / (1024 ** 3):.1f}GB/{total_heap_memory / (1024 ** 3):.1f}GB" if total_heap_memory > 0 else "0GB/0GB"

                elif cluster.cluster_type == ClusterType.FLINK:
                    # Flink: 显示内存而不是存储
                    total_memory = cluster_info.get('total_memory', 0)
                    used_memory = cluster_info.get('used_memory', 0)
                    storage_info = f"{used_memory / (1024 ** 3):.1f}GB/{total_memory / (1024 ** 3):.1f}GB" if total_memory > 0 else "0GB/0GB"
                    memory_info = storage_info  # Flink的存储字段实际显示内存

                elif cluster.cluster_type == ClusterType.HADOOP:
                    # Hadoop: 存储用HDFS容量，内存用NameNode JVM内存
                    storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "未知"

                    # 使用NameNode的JVM内存
                    namenode_heap_used = cluster_info.get('namenode_jvm_heap_used', 0)
                    namenode_heap_max = cluster_info.get('namenode_jvm_heap_max', 0)
                    memory_info = f"{namenode_heap_used / (1024 ** 3):.1f}GB/{namenode_heap_max / (1024 ** 3):.1f}GB" if namenode_heap_max > 0 else "0GB/0GB"

                else:
                    storage_info = "0GB/0GB"
                    memory_info = "0GB/0GB"

                # 更新集群状态
                cluster.status = ClusterStatus.ACTIVE
                cluster.node_count = f"{active_nodes}/{total_nodes}"
                cluster.storage_info = storage_info
                # 重要：添加内存信息的更新
                cluster.memory_info = memory_info
                cluster.check_datetime = datetime.now()

                # 更新配置信息
                updated_config = cluster.config.copy()
                updated_config['discovered_nodes'] = nodes
                updated_config['cluster_info'] = cluster_info
                updated_config['last_discovery'] = datetime.now().isoformat()
                cluster.config = updated_config

                await db.commit()
                await db.refresh(cluster)

                return {
                    'success': True,
                    'cluster': cluster.to_dict(),
                    'message': '集群检测成功，已更新最新状态'
                }
            else:
                cluster.status = ClusterStatus.ERROR
                cluster.check_datetime = datetime.now()
                await db.commit()

                return {
                    'success': False,
                    'error': discovery_result.get('error'),
                    'message': '集群检测失败'
                }

        except Exception as e:
            logger.error(f"检测计算集群失败: {e}")
            raise

    async def set_default_cluster(self, db: AsyncSession, cluster_id: int) -> bool:
        """设置默认集群"""
        try:
            # 先取消所有默认集群
            await db.execute(
                update(UserCluster).values(is_default=False)
            )

            # 设置新的默认集群
            result = await db.execute(
                update(UserCluster)
                .where(UserCluster.id == cluster_id)
                .values(is_default=True)
            )

            if result.rowcount == 0:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            await db.commit()
            logger.info(f"成功设置默认集群: {cluster_id}")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"设置默认集群失败: {e}")
            raise

    async def get_cluster_summary(self, db: AsyncSession) -> Dict:
        """获取集群汇总信息（用于首页显示）"""
        try:
            # 统计各状态集群数量
            result = await db.execute(
                select(UserCluster.status, func.count(UserCluster.id))
                .group_by(UserCluster.status)
            )
            status_counts = dict(result.fetchall())

            total_clusters = sum(status_counts.values())
            available_clusters = status_counts.get(ClusterStatus.ACTIVE, 0)

            return {
                "total_clusters": total_clusters,
                "available_clusters": available_clusters,
                "unavailable_clusters": total_clusters - available_clusters,
                "status_breakdown": {
                    status.value: count for status, count in status_counts.items()
                }
            }

        except Exception as e:
            logger.error(f"获取集群汇总信息失败: {e}")
            return {
                "total_clusters": 0,
                "available_clusters": 0,
                "unavailable_clusters": 0,
                "status_breakdown": {}
            }

    async def _perform_cluster_check(self, cluster: UserCluster) -> Tuple[ClusterStatus, str, str, str]:
        """执行实际的集群检测 - 使用真实逻辑"""
        try:
            # 调用真实的检测逻辑
            result = await self.check_cluster(db, cluster.id)
            if result['success']:
                cluster_data = result['cluster']
                return (
                    ClusterStatus.ACTIVE,
                    cluster_data['node'],
                    cluster_data['memory'],
                    cluster_data['storage']
                )
            else:
                return ClusterStatus.ERROR, "0/0", "0GB", "0GB"

        except Exception as e:
            logger.error(f"集群检测失败: {e}")
            return ClusterStatus.ERROR, "0/0", "0GB", "0GB"

    async def _check_cluster_status(self, cluster_id: int):
        """后台异步检测集群状态"""
        try:
            # 创建新的数据库会话用于后台任务
            async with AsyncSession(self.db_engine) as db:
                # 获取集群信息
                result = await db.execute(
                    select(UserCluster).where(UserCluster.id == cluster_id)
                )
                cluster = result.scalar_one_or_none()

                if not cluster:
                    logger.error(f"后台检测: 集群 ID {cluster_id} 不存在")
                    return

                logger.info(f"开始后台检测集群: {cluster.name} (ID: {cluster_id})")

                # 获取Web API配置
                web_api_config = None
                if cluster.config:
                    # 优先使用 webApiConfig
                    web_api_config = cluster.config.get('webApiConfig')
                    # 如果没有，尝试 web_api_config
                    if not web_api_config:
                        web_api_config = cluster.config.get('web_api_config')

                if not web_api_config:
                    logger.error(f"集群 {cluster.name} 的配置: {cluster.config}")
                    raise ValueError("集群缺少Web API配置信息")
                logger.info(f"使用配置检测集群 {cluster.name}: {web_api_config}")
                # 执行真实的集群检测
                try:
                    discovery_result = await asyncio.wait_for(
                        self.node_discovery.discover_cluster_nodes(
                            cluster.cluster_type.value,
                            web_api_config
                        ),
                        timeout=60  # 60秒超时
                    )

                    if discovery_result.get('success'):
                        # 更新集群状态为可用
                        nodes = discovery_result.get('nodes', [])
                        cluster_info = discovery_result.get('cluster_info', {})

                        total_nodes = len(nodes)
                        active_nodes = len([n for n in nodes if n.get('status') == 'active'])

                        # 计算存储信息
                        total_capacity = cluster_info.get('total_capacity', 0)
                        used_capacity = cluster_info.get('used_capacity', 0)

                        storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "未知"

                        # 更新集群状态
                        cluster.status = ClusterStatus.ACTIVE
                        cluster.node_count = f"{active_nodes}/{total_nodes}"
                        cluster.storage_info = storage_info
                        cluster.check_datetime = datetime.now()

                        # 更新配置信息
                        updated_config = cluster.config.copy()
                        updated_config['discovered_nodes'] = nodes
                        updated_config['cluster_info'] = cluster_info
                        updated_config['last_discovery'] = datetime.now().isoformat()
                        cluster.config = updated_config

                        await db.commit()

                        logger.info(f"后台检测成功: 集群 {cluster.name} 状态正常，{active_nodes}/{total_nodes} 节点可用")

                        # 发送成功通知（如果需要）
                        await self._send_cluster_alert(cluster_id, "集群检测成功",
                                                       f"发现 {active_nodes}/{total_nodes} 个节点")

                    else:
                        # 检测失败，更新状态
                        cluster.status = ClusterStatus.ERROR
                        cluster.check_datetime = datetime.now()
                        await db.commit()

                        error_msg = discovery_result.get('error', '未知错误')
                        logger.error(f"后台检测失败: 集群 {cluster.name} - {error_msg}")

                        # 发送失败告警
                        await self._send_cluster_alert(cluster_id, "集群检测失败", error_msg)

                except asyncio.TimeoutError:
                    # 检测超时
                    cluster.status = ClusterStatus.ERROR
                    cluster.check_datetime = datetime.now()
                    await db.commit()

                    error_msg = "集群检测超时"
                    logger.error(f"后台检测超时: 集群 {cluster.name}")
                    await self._send_cluster_alert(cluster_id, "集群检测超时", error_msg)

        except Exception as e:
            logger.error(f"后台检测集群 {cluster_id} 失败: {e}")

            # 尝试更新数据库状态为错误
            try:
                async with AsyncSession(self.db_engine) as db:
                    result = await db.execute(
                        select(UserCluster).where(UserCluster.id == cluster_id)
                    )
                    cluster = result.scalar_one_or_none()
                    if cluster:
                        cluster.status = ClusterStatus.ERROR
                        cluster.check_datetime = datetime.now()
                        await db.commit()
            except Exception as db_error:
                logger.error(f"更新集群状态失败: {db_error}")

            # 发送异常告警
            await self._send_cluster_alert(cluster_id, "集群检测异常", str(e))

    async def rediscover_cluster_nodes(self, db: AsyncSession, cluster_id: int) -> Dict:
        """重新发现集群节点"""
        try:
            # 获取集群信息
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 从配置中获取Web API信息
            web_api_config = cluster.config.get('web_api_config')
            if not web_api_config:
                raise ValueError("集群缺少Web API配置信息")

            # 重新发现节点
            discovery_result = await self.node_discovery.discover_cluster_nodes(
                cluster.cluster_type.value, web_api_config
            )

            if discovery_result.get('success'):
                # 更新集群配置
                updated_config = cluster.config.copy()
                updated_config['discovered_nodes'] = discovery_result['nodes']
                updated_config['cluster_info'] = discovery_result['cluster_info']
                updated_config['last_discovery'] = discovery_result['discovery_time']

                cluster.config = updated_config
                cluster.status = ClusterStatus.ACTIVE

                await db.commit()
                await db.refresh(cluster)

                logger.info(f"成功重新发现集群 {cluster.name} 的 {len(discovery_result['nodes'])} 个节点")

                return {
                    'success': True,
                    'cluster': cluster.to_dict(),
                    'discovered_nodes': discovery_result['nodes'],
                    'cluster_info': discovery_result['cluster_info']
                }
            else:
                raise ValueError(f"节点发现失败: {discovery_result.get('error')}")

        except Exception as e:
            logger.error(f"重新发现集群节点失败: {e}")
            raise

    async def _update_cluster_nodes_info(self, cluster_id: int, discovery_result: Dict):
        """后台更新集群节点信息到数据库"""
        try:
            # 创建新的数据库会话用于后台任务
            async with AsyncSession(self.db_engine) as db:
                # 获取集群信息
                result = await db.execute(
                    select(UserCluster).where(UserCluster.id == cluster_id)
                )
                cluster = result.scalar_one_or_none()

                if not cluster:
                    logger.error(f"集群 ID {cluster_id} 不存在")
                    return

                # 更新集群配置信息
                updated_config = cluster.config.copy() if cluster.config else {}
                updated_config['discovered_nodes'] = discovery_result.get('nodes', [])
                updated_config['cluster_info'] = discovery_result.get('cluster_info', {})
                updated_config['last_discovery'] = datetime.now().isoformat()
                updated_config['discovery_method'] = 'background_update'

                # 计算并更新集群统计信息
                nodes = discovery_result.get('nodes', [])
                cluster_info = discovery_result.get('cluster_info', {})

                total_nodes = len(nodes)
                active_nodes = len([n for n in nodes if n.get('status') == 'active'])

                # 计算存储信息
                total_capacity = cluster_info.get('total_capacity', 0)
                used_capacity = cluster_info.get('used_capacity', 0)
                free_capacity = cluster_info.get('free_capacity', 0)

                if total_capacity > 0:
                    storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB"
                else:
                    storage_info = "未知"

                # 计算内存信息（如果有的话）
                memory_info = "0GB"  # 默认值，可以根据实际节点信息计算
                if nodes:
                    total_memory = 0
                    used_memory = 0
                    memory_count = 0

                    for node in nodes:
                        if node.get('memory_total'):
                            total_memory += node.get('memory_total', 0)
                            used_memory += node.get('memory_used', 0)
                            memory_count += 1

                    if memory_count > 0:
                        memory_info = f"{used_memory / (1024 ** 3):.1f}GB/{total_memory / (1024 ** 3):.1f}GB"

                # 更新集群信息
                cluster.config = updated_config
                cluster.node_count = f"{active_nodes}/{total_nodes}"
                cluster.storage_info = storage_info
                cluster.memory_info = memory_info
                cluster.status = ClusterStatus.ACTIVE if discovery_result.get('success') else ClusterStatus.ERROR
                cluster.updated_at = datetime.now()

                await db.commit()

                logger.info(
                    f"成功更新集群 {cluster.name} (ID: {cluster_id}) 的节点信息: {active_nodes}/{total_nodes} 节点")

                # 可选: 清理旧的缓存
                cache_key = f"cluster_detail_{cluster_id}"
                if hasattr(self, 'cache_service'):
                    await self.cache_service.delete(cache_key)

        except Exception as e:
            logger.error(f"后台更新集群 {cluster_id} 节点信息失败: {e}")
            # 可以在这里添加告警通知
            await self._send_cluster_alert(cluster_id, "节点信息更新失败", str(e))

    async def get_cluster_nodes_detail(self, db: AsyncSession, cluster_id: int) -> Dict:
        """获取集群节点详细信息"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            nodes = cluster.config.get('discovered_nodes', [])
            cluster_info = cluster.config.get('cluster_info', {})
            last_discovery = cluster.config.get('last_discovery')

            # 统计节点信息
            node_stats = {
                'total_nodes': len(nodes),
                'master_nodes': len([n for n in nodes if n.get('node_type') == 'master']),
                'worker_nodes': len([n for n in nodes if n.get('node_type') == 'worker']),
                'active_nodes': len([n for n in nodes if n.get('status') == 'active']),
                'inactive_nodes': len([n for n in nodes if n.get('status') != 'active'])
            }

            # 按角色分组节点
            nodes_by_role = {}
            for node in nodes:
                role = node.get('role', 'unknown')
                if role not in nodes_by_role:
                    nodes_by_role[role] = []
                nodes_by_role[role].append(node)

            return {
                'cluster_id': cluster.id,
                'cluster_name': cluster.name,
                'cluster_type': cluster.cluster_type.value,
                'cluster_status': cluster.status.value,
                'last_discovery': last_discovery,
                'cluster_info': cluster_info,
                'node_statistics': node_stats,
                'nodes_by_role': nodes_by_role,
                'all_nodes': nodes
            }

        except Exception as e:
            logger.error(f"获取集群节点详情失败: {e}")
            raise

    async def get_supported_cluster_types(self) -> Dict:
        """获取支持的集群类型和配置模板"""
        return {
            'hadoop': {
                'name': 'Hadoop HDFS',
                'description': 'Hadoop分布式文件系统',
                'required_config': {
                    'namenode_host': '字符串，NameNode主机地址',
                    'namenode_web_port': '数字，NameNode Web端口，默认9870'
                },
                'optional_config': {
                    'namenode_rpc_port': '数字，NameNode RPC端口，默认9000'
                },
                'discovery_method': 'NameNode Web UI API'
            },
            'yarn': {
                'name': 'YARN资源管理',
                'description': 'Hadoop YARN资源管理器',
                'required_config': {
                    'resourcemanager_host': '字符串，ResourceManager主机地址',
                    'resourcemanager_web_port': '数字，ResourceManager Web端口，默认8088'
                },
                'optional_config': {
                    'resourcemanager_rpc_port': '数字，ResourceManager RPC端口，默认8032'
                },
                'discovery_method': 'ResourceManager REST API'
            },
            'flink': {
                'name': 'Apache Flink',
                'description': 'Flink流处理引擎',
                'required_config': {
                    'jobmanager_host': '字符串，JobManager主机地址',
                    'jobmanager_web_port': '数字，JobManager Web端口，默认8081'
                },
                'optional_config': {
                    'jobmanager_rpc_port': '数字，JobManager RPC端口，默认6123'
                },
                'discovery_method': 'JobManager REST API'
            },
            'doris': {
                'name': 'Apache Doris',
                'description': 'Doris实时数据仓库',
                'required_config': {
                    'frontend_host': '字符串，Frontend主机地址',
                    'frontend_web_port': '数字，Frontend Web端口，默认8060'
                },
                'optional_config': {
                    'username': '字符串，认证用户名，默认root',
                    'password': '字符串，认证密码'
                },
                'discovery_method': 'Frontend HTTP API'
            }
        }

    async def _send_cluster_alert(self, cluster_id: int, alert_type: str, message: str):
        """发送集群告警通知"""
        try:
            # 这里可以集成你的告警系统
            logger.warning(f"集群告警 [ID: {cluster_id}] {alert_type}: {message}")

            # 示例：发送到告警系统
            # await self.alert_service.send_alert({
            #     'cluster_id': cluster_id,
            #     'alert_type': alert_type,
            #     'message': message,
            #     'timestamp': datetime.now().isoformat(),
            #     'severity': 'warning' if '成功' in alert_type else 'error'
            # })

        except Exception as e:
            logger.error(f"发送集群告警失败: {e}")


# 创建全局服务实例
user_cluster_service = UserClusterService()