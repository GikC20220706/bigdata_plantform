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

from app.models.user_cluster import UserCluster, ClusterType, ClusterStatus
from app.utils.response import create_response
from app.services.cluster_node_discovery import WebApiClusterDiscovery


class UserClusterService:
    """用户计算集群管理服务"""

    def __init__(self):
        self.node_discovery = WebApiClusterDiscovery()

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

            storage_info = f"{used_capacity / (1024 ** 3):.1f}GB/{total_capacity / (1024 ** 3):.1f}GB" if total_capacity > 0 else "未知"

            # 构建URL
            web_ui_url = None
            master_url = None
            if cluster_type == 'hadoop' and web_api_config:
                namenode_host = web_api_config.get('namenode_host')
                namenode_port = web_api_config.get('namenode_web_port', 9870)
                web_ui_url = f"http://{namenode_host}:{namenode_port}"
                master_url = f"hdfs://{namenode_host}:9000"

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

            # 6. 返回结果包含发现的节点信息
            result = new_cluster.to_dict()
            if discovery_result.get('success'):
                result['discovered_nodes'] = discovery_result['nodes']
                result['cluster_info'] = discovery_result['cluster_info']

            return result

        except Exception as e:
            await db.rollback()
            logger.error(f"添加集群失败: {e}")
            raise

    async def update_cluster(
            self,
            db: AsyncSession,
            cluster_id: int,
            name: str = None,
            cluster_type: str = None,
            remark: str = None,
            config: Dict = None
    ) -> Dict:
        """更新集群信息"""
        try:
            # 获取集群
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 检查名称重复（如果更改了名称）
            if name and name != cluster.name:
                existing = await db.execute(
                    select(UserCluster).where(
                        and_(UserCluster.name == name, UserCluster.id != cluster_id)
                    )
                )
                if existing.scalar_one_or_none():
                    raise ValueError(f"集群名称 '{name}' 已存在")
                cluster.name = name

            # 更新其他字段
            if cluster_type:
                try:
                    cluster.cluster_type = ClusterType(cluster_type)
                except ValueError:
                    raise ValueError(f"不支持的集群类型: {cluster_type}")

            if remark is not None:
                cluster.remark = remark

            if config is not None:
                cluster.config = config

            cluster.updated_at = datetime.now()

            await db.commit()
            await db.refresh(cluster)

            logger.info(f"成功更新集群: {cluster.name}")
            return cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"更新集群失败: {e}")
            raise

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
        """检测集群状态"""
        try:
            result = await db.execute(
                select(UserCluster).where(UserCluster.id == cluster_id)
            )
            cluster = result.scalar_one_or_none()

            if not cluster:
                raise ValueError(f"集群 ID {cluster_id} 不存在")

            # 模拟集群检测逻辑
            status, node_info, memory_info, storage_info = await self._perform_cluster_check(cluster)

            # 更新状态
            cluster.update_status_info(status, node_info, memory_info, storage_info)
            await db.commit()
            await db.refresh(cluster)

            return cluster.to_dict()

        except Exception as e:
            await db.rollback()
            logger.error(f"检测集群失败: {e}")
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
        """执行实际的集群检测"""
        try:
            # 这里应该根据集群类型实现真实的检测逻辑
            # 目前返回模拟数据
            await asyncio.sleep(1)  # 模拟检测时间

            if cluster.cluster_type == ClusterType.YARN:
                return ClusterStatus.ACTIVE, "2/3", "32GB/64GB", "1.2TB/2TB"
            elif cluster.cluster_type == ClusterType.KUBERNETES:
                return ClusterStatus.ACTIVE, "5/5", "64GB/128GB", "2.5TB/5TB"
            else:
                return ClusterStatus.ACTIVE, "1/1", "16GB/32GB", "500GB/1TB"

        except Exception as e:
            logger.error(f"集群检测失败: {e}")
            return ClusterStatus.ERROR, "0/0", "0GB", "0GB"

    async def _check_cluster_status(self, cluster_id: int):
        """后台异步检测集群状态"""
        try:
            # 这里应该实现真实的异步检测逻辑
            await asyncio.sleep(3)  # 模拟检测延迟
            logger.info(f"后台检测集群 {cluster_id} 完成")
        except Exception as e:
            logger.error(f"后台检测集群 {cluster_id} 失败: {e}")

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
        """后台更新集群节点信息"""
        try:
            # 这里可以实现将发现的节点信息存储到数据库
            # 或执行其他后台任务
            logger.info(f"后台更新集群 {cluster_id} 节点信息完成")
        except Exception as e:
            logger.error(f"后台更新集群节点信息失败: {e}")

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


# 创建全局服务实例
user_cluster_service = UserClusterService()