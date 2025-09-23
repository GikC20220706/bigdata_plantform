# app/services/cluster_node_discovery.py
import asyncio
import aiohttp
from typing import Dict, List, Optional, Tuple
from loguru import logger
from datetime import datetime
import json

import config


class WebApiClusterDiscovery:
    """基于Web API的集群节点发现服务"""

    def __init__(self):
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)

    async def discover_cluster_nodes(
            self,
            cluster_type: str,
            web_api_config: Dict
    ) -> Dict:
        """
        通过Web API自动发现集群节点

        Args:
            cluster_type: 集群类型 (hadoop/yarn/flink/doris)
            web_api_config: Web API连接配置

        Returns:
            Dict: 发现的节点信息
        """
        try:
            logger.info(f"开始发现 {cluster_type} 集群节点...")

            # 根据集群类型选择发现策略
            if cluster_type == 'hadoop':
                result = await self._discover_hadoop_cluster(web_api_config)
            elif cluster_type == 'yarn':
                result = await self._discover_yarn_cluster(web_api_config)
            elif cluster_type == 'flink':
                result = await self._discover_flink_cluster(web_api_config)
            elif cluster_type == 'doris':
                result = await self._discover_doris_cluster(web_api_config)
            else:
                raise ValueError(f"不支持的集群类型: {cluster_type}")

            logger.info(f"成功发现 {len(result.get('nodes', []))} 个节点")
            return result

        except Exception as e:
            logger.error(f"集群节点发现失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'nodes': [],
                'cluster_info': {},
                'discovery_time': datetime.now().isoformat()
            }

    # ====================================================================
    # Hadoop集群节点发现 - 通过NameNode Web UI
    # ====================================================================

    async def _discover_hadoop_cluster(self, config: Dict) -> Dict:
        """发现Hadoop集群节点（支持HA完整发现）"""
        namenode_host = config['namenode_host']
        namenode_port = config.get('namenode_web_port', 9870)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                # 1. 发现所有NameNode（包括HA）
                all_namenodes = await self._discover_all_namenodes(session, config)

                # 2. 从Active NameNode获取集群信息和DataNode列表
                active_nn = next((nn for nn in all_namenodes if nn.get('ha_state') == 'ACTIVE'),
                                 all_namenodes[0] if all_namenodes else None)
                if active_nn:
                    cluster_info = await self._get_hadoop_cluster_info(session, active_nn['hostname'], namenode_port,
                                                                       config)
                    datanodes = await self._get_hadoop_datanodes_enhanced(session, active_nn['hostname'], namenode_port,
                                                                          config)
                else:
                    # 如果没有找到Active，使用配置的主NameNode
                    cluster_info = await self._get_hadoop_cluster_info(session, namenode_host, namenode_port, config)
                    datanodes = await self._get_hadoop_datanodes_enhanced(session, namenode_host, namenode_port, config)

                namenode_metrics = await self._get_hadoop_jvm_metrics(session, namenode_host, namenode_port, config)

                # 合并集群信息，添加内存数据
                if namenode_metrics:
                    cluster_info.update({
                        'namenode_jvm_heap_used': namenode_metrics.get('jvm_heap_used', 0),
                        'namenode_jvm_heap_max': namenode_metrics.get('jvm_heap_max', 0),
                        'namenode_system_total_memory': namenode_metrics.get('system_total_memory', 0),
                        'namenode_system_free_memory': namenode_metrics.get('system_free_memory', 0)
                    })

                # 3. 发现JournalNode
                journalnodes = await self._discover_journalnodes(session, config)

                # 4. 合并所有节点
                all_nodes = all_namenodes + datanodes + journalnodes

                return {
                    'success': True,
                    'cluster_info': cluster_info,
                    'nodes': all_nodes,
                    'discovery_time': datetime.now().isoformat(),
                    'total_nodes': len(all_nodes),
                    'nodes_by_type': {
                        'namenodes': len(all_namenodes),
                        'datanodes': len(datanodes),
                        'journalnodes': len(journalnodes)
                    }
                }

            except Exception as e:
                logger.error(f"Hadoop集群发现失败: {e}")
                raise

    async def _get_hadoop_cluster_info(self, session: aiohttp.ClientSession, host: str, port: int,
                                       config: Dict = None) -> Dict:
        """获取Hadoop集群基本信息"""
        try:
            # 从config或环境变量中获取用户名
            username = config.get('username') if config else None
            if not username:
                from config.settings import settings
                username = settings.HDFS_USER  # 从.env中的HDFS_USER获取

            url = f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
            if username:
                url += f"&user.name={username}"

            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                beans = data.get('beans', [])
                if beans:
                    info = beans[0]
                    live_nodes = json.loads(info.get('LiveNodes', '{}'))
                    dead_nodes = json.loads(info.get('DeadNodes', '{}'))

                    return {
                        'cluster_id': info.get('ClusterId'),
                        'version': info.get('Version'),
                        'started': info.get('NameNodeStarted'),
                        'total_capacity': info.get('Total', 0),
                        'used_capacity': info.get('Used', 0),
                        'free_capacity': info.get('Free', 0),
                        'live_nodes_count': len(live_nodes),
                        'dead_nodes_count': len(dead_nodes),
                        'total_files': info.get('TotalFiles', 0),
                        'total_blocks': info.get('TotalBlocks', 0)
                    }
        except Exception as e:
            logger.warning(f"获取Hadoop集群信息失败: {e}")
            return {}

    async def _get_hadoop_datanodes_enhanced(self, session: aiohttp.ClientSession, host: str, port: int,
                                             config: Dict = None) -> List[Dict]:
        """增强的DataNode获取方法，正确解析hostname和IP"""
        nodes = []
        try:
            username = config.get('username') if config else None
            if not username:
                from config.settings import settings
                username = settings.HDFS_USER

            url = f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
            if username:
                url += f"&user.name={username}"

            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                beans = data.get('beans', [])
                if beans:
                    info = beans[0]
                    live_nodes = json.loads(info.get('LiveNodes', '{}'))

                    for node_key, node_info in live_nodes.items():
                        # 从key中提取hostname（如 "hadoop103:9866" -> "hadoop103"）
                        hostname = node_key.split(':')[0] if ':' in node_key else node_key

                        # 从infoAddr中提取IP（如 "192.142.76.244:9864" -> "192.142.76.244"）
                        info_addr = node_info.get('infoAddr', '')
                        ip_address = info_addr.split(':')[0] if ':' in info_addr else ''

                        capacity = node_info.get('capacity', 0)
                        used = node_info.get('used', 0)

                        nodes.append({
                            'hostname': hostname,
                            'ip_address': ip_address,
                            'role': 'DataNode',
                            'node_type': 'worker',
                            'status': 'active',
                            'services': ['DataNode'],
                            'capacity_bytes': capacity,
                            'used_bytes': used,
                            'remaining_bytes': node_info.get('remaining', 0),
                            'usage_percent': (used / capacity * 100) if capacity > 0 else 0,
                            'last_contact': node_info.get('lastContact', 0),
                            'admin_state': node_info.get('adminState', ''),
                            'version': node_info.get('version', ''),
                            'num_blocks': node_info.get('numBlocks', 0),
                            'discovery_method': 'hadoop_web_api',
                            'datanode_ports': {
                                'data_transfer': node_key.split(':')[1] if ':' in node_key else '9866',
                                'info_port': info_addr.split(':')[1] if ':' in info_addr else '9864'
                            }
                        })

        except Exception as e:
            logger.warning(f"获取DataNode列表失败: {e}")

        return nodes

    # ====================================================================
    # YARN集群节点发现 - 通过ResourceManager Web UI
    # ====================================================================

    async def _discover_yarn_cluster(self, config: Dict) -> Dict:
        """发现YARN集群节点"""
        rm_host = config['resourcemanager_host']
        rm_port = config.get('resourcemanager_web_port', 8088)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                # 1. 获取集群信息
                cluster_info = await self._get_yarn_cluster_info(session, rm_host, rm_port)

                # 2. 获取NodeManager节点列表
                node_managers = await self._get_yarn_nodemanagers(session, rm_host, rm_port)

                # 3. 添加ResourceManager信息
                nodes = [{
                    'hostname': rm_host,
                    'ip_address': rm_host,
                    'role': 'ResourceManager',
                    'node_type': 'master',
                    'status': 'active',
                    'services': ['ResourceManager'],
                    'web_ports': {'resourcemanager': rm_port},
                    'discovery_method': 'yarn_web_api'
                }]

                # 4. 添加NodeManager信息
                nodes.extend(node_managers)

                return {
                    'success': True,
                    'cluster_info': cluster_info,
                    'nodes': nodes,
                    'discovery_time': datetime.now().isoformat(),
                    'total_nodes': len(nodes)
                }

            except Exception as e:
                logger.error(f"YARN集群发现失败: {e}")
                raise

    async def _get_yarn_cluster_info(self, session: aiohttp.ClientSession, host: str, port: int) -> Dict:
        """获取YARN集群信息"""
        try:
            # 添加JSON格式参数和用户名
            from config.settings import settings
            username = settings.HDFS_USER  # bigdata

            url = f"http://{host}:{port}/ws/v1/cluster/info"
            params = {
                'user.name': username
            }

            # 设置Accept头部请求JSON格式
            headers = {
                'Accept': 'application/json'
            }

            async with session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()  # 现在应该返回JSON了

                cluster_info = data.get('clusterInfo', {})
                return {
                    'cluster_id': cluster_info.get('id'),
                    'version': cluster_info.get('hadoopVersion'),
                    'started_on': cluster_info.get('startedOn'),
                    'state': cluster_info.get('state'),
                    'ha_state': cluster_info.get('haState'),  # 新增HA状态
                    'resource_manager_version': cluster_info.get('resourceManagerVersion'),
                    'total_memory': cluster_info.get('totalMB', 0),
                    'total_vcores': cluster_info.get('totalVirtualCores', 0),
                    'available_memory': cluster_info.get('availableMB', 0),
                    'available_vcores': cluster_info.get('availableVirtualCores', 0)
                }
        except Exception as e:
            logger.warning(f"获取YARN集群信息失败: {e}")
            return {}

    async def _get_yarn_nodemanagers(self, session: aiohttp.ClientSession, host: str, port: int) -> List[Dict]:
        """获取YARN NodeManager列表"""
        nodes = []
        try:
            from config.settings import settings
            username = settings.HDFS_USER

            url = f"http://{host}:{port}/ws/v1/cluster/nodes"
            params = {
                'user.name': username
            }
            headers = {
                'Accept': 'application/json'
            }

            async with session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                yarn_nodes = data.get('nodes', {}).get('node', [])
                for node in yarn_nodes:
                    nodes.append({
                        'hostname': node.get('nodeHostName', ''),
                        'ip_address': node.get('nodeHTTPAddress', '').split(':')[0],
                        'role': 'NodeManager',
                        'node_type': 'worker',
                        'status': node.get('state', '').lower(),
                        'services': ['NodeManager'],
                        'memory_total': node.get('totalMemoryMB', 0),
                        'memory_used': node.get('usedMemoryMB', 0),
                        'vcores_total': node.get('totalVirtualCores', 0),
                        'vcores_used': node.get('usedVirtualCores', 0),
                        'last_health_update': node.get('lastHealthUpdate', 0),
                        'health_report': node.get('healthReport', ''),
                        'discovery_method': 'yarn_web_api'
                    })
        except Exception as e:
            logger.warning(f"获取NodeManager列表失败: {e}")

        return nodes

    # ====================================================================
    # Flink集群节点发现 - 通过JobManager Web UI
    # ====================================================================

    async def _discover_flink_cluster(self, config: Dict) -> Dict:
        """发现Flink集群节点"""
        jm_host = config['jobmanager_host']
        jm_port = config.get('jobmanager_web_port', 8081)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                # 1. 获取集群概览
                cluster_info = await self._get_flink_cluster_info(session, jm_host, jm_port)

                # 2. 获取JobManager JVM指标
                jobmanager_metrics = await self._get_jobmanager_metrics(session, jm_host, jm_port)

                # 3. 获取TaskManager列表
                task_managers = await self._get_flink_taskmanagers(session, jm_host, jm_port)

                # 4. 计算总体资源信息
                total_memory = 0
                total_slots = 0
                used_memory = 0
                system_memory = 0

                for node in task_managers:
                    total_memory += node.get('physical_memory', 0)
                    total_slots += node.get('slots_total', 0)
                    used_memory += node.get('memory_heap_used', 0)
                    # 从物理内存计算系统总内存
                    system_memory += node.get('physical_memory', 0)

                cluster_info.update({
                    'total_memory': total_memory,
                    'used_memory': used_memory,
                    'total_slots': total_slots,
                    'system_total_memory': system_memory,
                    # 添加JobManager的内存信息
                    'jobmanager_heap_used': jobmanager_metrics.get('heap_used', 0),
                    'jobmanager_heap_max': jobmanager_metrics.get('heap_max', 0)
                })

                # 5. 创建JobManager节点，包含内存信息
                nodes = [{
                    'hostname': jm_host,
                    'ip_address': jm_host,
                    'role': 'JobManager',
                    'node_type': 'master',
                    'status': 'active',
                    'services': ['JobManager'],
                    'web_ports': {'jobmanager': jm_port},
                    'memory_heap_used': jobmanager_metrics.get('heap_used', 0),
                    'memory_heap_max': jobmanager_metrics.get('heap_max', 0),
                    'discovery_method': 'flink_web_api'
                }]

                # 6. 添加TaskManager信息
                nodes.extend(task_managers)

                return {
                    'success': True,
                    'cluster_info': cluster_info,
                    'nodes': nodes,
                    'discovery_time': datetime.now().isoformat(),
                    'total_nodes': len(nodes)
                }

            except Exception as e:
                logger.error(f"Flink集群发现失败: {e}")
                raise

    async def _get_jobmanager_metrics(self, session: aiohttp.ClientSession, host: str, port: int) -> Dict:
        """获取JobManager的JVM指标"""
        try:
            metrics_url = f"http://{host}:{port}/jobmanager/metrics"
            params = {
                'get': 'Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Heap.Max'
            }

            async with session.get(metrics_url, params=params) as response:
                response.raise_for_status()
                metrics_data = await response.json()

                metrics = {}
                for metric in metrics_data:
                    metric_id = metric.get('id', '')
                    value = metric.get('value', 0)

                    if 'Memory.Heap.Used' in metric_id:
                        metrics['heap_used'] = int(value)
                    elif 'Memory.Heap.Max' in metric_id:
                        metrics['heap_max'] = int(value)

                return metrics

        except Exception as e:
            logger.debug(f"获取 JobManager 指标失败: {e}")
            return {}
    async def _get_flink_cluster_info(self, session: aiohttp.ClientSession, host: str, port: int) -> Dict:
        """获取Flink集群信息"""
        try:
            url = f"http://{host}:{port}/overview"
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                return {
                    'version': data.get('flink-version'),
                    'flink_commit': data.get('flink-commit'),
                    'task_managers': data.get('taskmanagers'),
                    'slots_total': data.get('slots-total'),
                    'slots_available': data.get('slots-available'),
                    'jobs_running': data.get('jobs-running'),
                    'jobs_finished': data.get('jobs-finished'),
                    'jobs_cancelled': data.get('jobs-cancelled'),
                    'jobs_failed': data.get('jobs-failed')
                }
        except Exception as e:
            logger.warning(f"获取Flink集群信息失败: {e}")
            return {}

    async def _get_flink_taskmanagers(self, session: aiohttp.ClientSession, host: str, port: int) -> List[Dict]:
        """获取Flink TaskManager列表 - 增强版本包含详细资源信息"""
        nodes = []
        try:
            # 1. 获取 TaskManager 基础信息
            url = f"http://{host}:{port}/taskmanagers"
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                taskmanagers = data.get('taskmanagers', [])

                # 2. 为每个 TaskManager 获取详细指标
                for tm in taskmanagers:
                    tm_id = tm.get('id')
                    tm_host = tm.get('path', '').split('@')[-1].split(':')[0] if '@' in tm.get('path', '') else ''

                    # 获取该 TaskManager 的详细指标
                    tm_metrics = await self._get_taskmanager_metrics(session, host, port, tm_id)

                    node_info = {
                        'hostname': tm.get('path', '').split('@')[-1] if '@' in tm.get('path', '') else '',
                        'ip_address': tm_host,
                        'role': 'TaskManager',
                        'node_type': 'worker',
                        'status': 'active',
                        'services': ['TaskManager'],
                        'task_manager_id': tm_id,
                        'slots_total': tm.get('slotsNumber', 0),
                        'slots_free': tm.get('freeSlots', 0),
                        'discovery_method': 'flink_web_api'
                    }

                    # 添加硬件信息
                    hardware = tm.get('hardware', {})
                    if hardware:
                        node_info.update({
                            'cpu_cores': hardware.get('cpuCores', 0),
                            'physical_memory': hardware.get('physicalMemory', 0),
                            'free_memory': hardware.get('freeMemory', 0),
                            'managed_memory': hardware.get('managedMemory', 0),
                            'network_memory': hardware.get('networkMemory', 0)
                        })

                    # 添加实时指标
                    if tm_metrics:
                        node_info.update({
                            'memory_heap_used': tm_metrics.get('memory_heap_used', 0),
                            'memory_heap_max': tm_metrics.get('memory_heap_max', 0),
                            'memory_non_heap_used': tm_metrics.get('memory_non_heap_used', 0),
                            'cpu_load': tm_metrics.get('cpu_load', 0),
                            'gc_count': tm_metrics.get('gc_count', 0),
                            'gc_time': tm_metrics.get('gc_time', 0)
                        })

                    nodes.append(node_info)

        except Exception as e:
            logger.warning(f"获取TaskManager列表失败: {e}")

        return nodes

    async def _get_taskmanager_metrics(self, session: aiohttp.ClientSession, host: str, port: int, tm_id: str) -> Dict:
        """获取特定 TaskManager 的详细指标"""
        try:
            # Flink 提供的指标端点
            metrics_url = f"http://{host}:{port}/taskmanagers/{tm_id}/metrics"

            # 请求特定的指标
            params = {
                'get': ','.join([
                    'Status.JVM.Memory.Heap.Used',
                    'Status.JVM.Memory.Heap.Max',
                    'Status.JVM.Memory.NonHeap.Used',
                    'Status.JVM.Memory.NonHeap.Max',
                    'Status.JVM.CPU.Load',
                    'Status.JVM.GarbageCollector.G1-Young-Generation.Count',
                    'Status.JVM.GarbageCollector.G1-Young-Generation.Time',
                    'Status.Network.TotalMemorySegments',
                    'Status.Network.AvailableMemorySegments'
                ])
            }

            async with session.get(metrics_url, params=params) as response:
                response.raise_for_status()
                metrics_data = await response.json()

                # 解析指标数据
                metrics = {}
                for metric in metrics_data:
                    metric_id = metric.get('id', '')
                    value = metric.get('value', 0)

                    if 'Memory.Heap.Used' in metric_id:
                        metrics['memory_heap_used'] = int(value)
                    elif 'Memory.Heap.Max' in metric_id:
                        metrics['memory_heap_max'] = int(value)
                    elif 'Memory.NonHeap.Used' in metric_id:
                        metrics['memory_non_heap_used'] = int(value)
                    elif 'CPU.Load' in metric_id:
                        metrics['cpu_load'] = float(value)
                    elif 'GarbageCollector' in metric_id and 'Count' in metric_id:
                        metrics['gc_count'] = int(value)
                    elif 'GarbageCollector' in metric_id and 'Time' in metric_id:
                        metrics['gc_time'] = int(value)

                return metrics

        except Exception as e:
            logger.debug(f"获取 TaskManager {tm_id} 指标失败: {e}")
            return {}

    # ====================================================================
    # Doris集群节点发现 - 通过Frontend Web UI
    # ====================================================================

    async def _discover_doris_cluster(self, config: Dict) -> Dict:
        """发现Doris集群节点 - 完全使用MySQL协议"""
        fe_host = config['frontend_host']
        fe_port = config.get('frontend_web_port', 8060)
        username = config.get('username', 'root')
        password = config.get('password', '')

        try:
            # 直接使用 MySQL 协议获取所有信息
            frontends = await self._get_doris_frontends_via_mysql(fe_host, username, password)
            backends = await self._get_doris_backends_via_mysql(fe_host, username, password)


            # 合并所有节点
            nodes = frontends + backends

            cluster_info = {
                'cluster_name': 'doris-cluster',
                'master_node': fe_host,
                'frontend_count': len(frontends),
                'backend_count': len(backends),
                'total_capacity': 0,
                'used_capacity': 0
            }

            # 计算存储信息
            total_capacity = 0
            used_capacity = 0
            total_memory = 0
            used_memory = 0

            for be in backends:
                total_capacity += be.get('total_capacity', 0)
                used_capacity += be.get('used_capacity', 0)
                total_memory += be.get('memory_total', 0)
                used_memory += be.get('memory_allocated', 0)

            active_backends = len([be for be in backends if be.get('status') == 'active'])
            cluster_info.update({
                'total_capacity': total_capacity,
                'used_capacity': used_capacity,
                'total_memory': total_memory,
                'used_memory': used_memory,
                'active_backends': active_backends,
                'total_backends': len(backends)
            })

            return {
                'success': True,
                'cluster_info': cluster_info,
                'nodes': nodes,
                'discovery_time': datetime.now().isoformat(),
                'total_nodes': len(nodes)
            }

        except Exception as e:
            logger.error(f"Doris集群发现失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'nodes': [],
                'total_nodes': 0
            }

    async def _get_doris_cluster_info(self, session: aiohttp.ClientSession, host: str, port: int, username: str,
                                      password: str) -> Dict:
        """获取Doris集群信息"""
        try:
            # 使用Basic认证
            auth = aiohttp.BasicAuth(username, password) if username else None

            url = f"http://{host}:{port}/api/show_proc?path=/"
            async with session.get(url, auth=auth) as response:
                response.raise_for_status()
                data = await response.json()

                return {
                    'cluster_name': data.get('cluster_name', 'doris-cluster'),
                    'master_node': f"{host}:{port}",
                    'query_time': datetime.now().isoformat()
                }
        except Exception as e:
            logger.warning(f"获取Doris集群信息失败: {e}")
            return {}

    # async def _get_doris_frontends(self, session: aiohttp.ClientSession, host: str, port: int, username: str,
    #                                password: str) -> List[Dict]:
    #     """获取Doris Frontend列表 - 使用正确的端点"""
    #     nodes = []
    #     try:
    #         auth = aiohttp.BasicAuth(username, password) if username else None
    #
    #         # 尝试多个可能的端点
    #         possible_endpoints = [
    #             f"http://{host}:{port}/rest/v1/system?path=frontends",
    #             f"http://{host}:{port}/api/rest/v1/system?path=frontends",
    #             f"http://{host}:{port}/rest/v2/api/show_frontends",
    #             f"http://{host}:{port}/api/show_frontends"
    #         ]
    #
    #         for url in possible_endpoints:
    #             try:
    #                 async with session.get(url, auth=auth) as response:
    #                     if response.status == 200:
    #                         # 检查响应的 Content-Type
    #                         content_type = response.headers.get('content-type', '')
    #                         if 'application/json' in content_type:
    #                             data = await response.json()
    #                             frontends = data.get('data', [])
    #                             for fe in frontends:
    #                                 nodes.append({
    #                                     'hostname': fe.get('Host', ''),
    #                                     'ip_address': fe.get('Host', ''),
    #                                     'role': 'Frontend',
    #                                     'node_type': 'master' if fe.get('IsMaster') == 'true' else 'follower',
    #                                     'status': 'active' if fe.get('Alive') == 'true' else 'inactive',
    #                                     'services': ['Frontend'],
    #                                     'discovery_method': 'doris_web_api'
    #                                 })
    #                             return nodes
    #                         else:
    #                             logger.warning(f"端点 {url} 返回非JSON响应，Content-Type: {content_type}")
    #             except Exception as e:
    #                 logger.debug(f"尝试端点 {url} 失败: {e}")
    #                 continue
    #
    #         # 如果所有 REST API 都失败，尝试使用 MySQL 协议
    #         logger.info("REST API 失败，尝试使用 MySQL 协议获取 Frontend 信息")
    #         return await self._get_doris_frontends_via_mysql(host, username, password)
    #
    #     except Exception as e:
    #         logger.warning(f"获取Doris Frontend列表失败: {e}")
    #         return nodes
    #
    # async def _get_doris_backends(self, session: aiohttp.ClientSession, host: str, port: int, username: str,
    #                               password: str) -> List[Dict]:
    #     """获取Doris Backend列表"""
    #     nodes = []
    #     try:
    #         auth = aiohttp.BasicAuth(username, password) if username else None
    #
    #         url = f"http://{host}:{port}/api/show_backends"
    #         async with session.get(url, auth=auth) as response:
    #             response.raise_for_status()
    #             data = await response.json()
    #
    #             backends = data.get('data', [])
    #             for be in backends:
    #                 nodes.append({
    #                     'hostname': be.get('Host', ''),
    #                     'ip_address': be.get('Host', ''),
    #                     'role': 'Backend',
    #                     'node_type': 'worker',
    #                     'status': 'active' if be.get('Alive') == 'true' else 'inactive',
    #                     'services': ['Backend'],
    #                     'heartbeat_port': be.get('HeartbeatPort'),
    #                     'be_port': be.get('BePort'),
    #                     'http_port': be.get('HttpPort'),
    #                     'brpc_port': be.get('BrpcPort'),
    #                     'version': be.get('Version'),
    #                     'last_heartbeat': be.get('LastHeartbeat'),
    #                     'total_capacity': be.get('TotalCapacity'),
    #                     'used_capacity': be.get('UsedCapacity'),
    #                     'available_capacity': be.get('AvailCapacity'),
    #                     'discovery_method': 'doris_web_api'
    #                 })
    #     except Exception as e:
    #         logger.warning(f"获取Doris Backend列表失败: {e}")
    #
    #     return nodes

    async def _get_doris_backends_via_mysql(self, host: str, username: str, password: str) -> List[Dict]:
        """通过 MySQL 协议获取 Backend 信息 - 增强版本"""
        try:
            import aiomysql

            connection = await aiomysql.connect(
                host=host,
                port=9030,
                user=username,
                password=password,
                connect_timeout=10,
                autocommit=True
            )

            cursor = await connection.cursor()

            # 1. 获取 Backend 基础信息
            await cursor.execute("SHOW BACKENDS")
            results = await cursor.fetchall()
            desc = cursor.description
            columns = [col[0] for col in desc]

            nodes = []
            for row in results:
                row_dict = dict(zip(columns, row))
                is_alive = str(row_dict.get('Alive', '')).lower() == 'true'

                # 2. 解析容量信息（Doris 返回的是字符串格式如 "100.000 GB"）
                total_capacity_str = row_dict.get('TotalCapacity', '0')
                used_capacity_str = row_dict.get('DataUsedCapacity', '0')
                available_capacity_str = row_dict.get('AvailCapacity', '0')

                # 转换容量为字节
                total_capacity_bytes = self._parse_doris_capacity(total_capacity_str)
                used_capacity_bytes = self._parse_doris_capacity(used_capacity_str)
                available_capacity_bytes = self._parse_doris_capacity(available_capacity_str)

                node_info = {
                    'hostname': row_dict.get('Host', ''),
                    'ip_address': row_dict.get('Host', ''),
                    'role': 'Backend',
                    'node_type': 'worker',
                    'status': 'active' if is_alive else 'inactive',
                    'services': ['Backend'],
                    'backend_id': row_dict.get('BackendId'),
                    'heartbeat_port': row_dict.get('HeartbeatPort'),
                    'be_port': row_dict.get('BePort'),
                    'http_port': row_dict.get('HttpPort'),
                    'brpc_port': row_dict.get('BrpcPort'),
                    'version': row_dict.get('Version'),
                    'last_start_time': row_dict.get('LastStartTime'),
                    'last_heartbeat': row_dict.get('LastHeartbeat'),
                    # 存储信息
                    'total_capacity': total_capacity_bytes,
                    'used_capacity': used_capacity_bytes,
                    'available_capacity': available_capacity_bytes,
                    'used_percent': row_dict.get('UsedPct'),
                    'max_disk_used_pct': row_dict.get('MaxDiskUsedPct'),
                    'tablet_num': row_dict.get('TabletNum'),
                    'discovery_method': 'doris_mysql_protocol'
                }

                # 3. 尝试获取更详细的系统信息（通过 Backend HTTP 接口）
                if is_alive and row_dict.get('HttpPort'):
                    try:
                        backend_metrics = await self._get_doris_backend_metrics(
                            row_dict.get('Host'),
                            row_dict.get('HttpPort')
                        )
                        if backend_metrics:
                            node_info.update(backend_metrics)
                    except Exception as e:
                        logger.debug(f"获取 Backend {row_dict.get('Host')} 指标失败: {e}")

                nodes.append(node_info)

            await cursor.close()
            connection.close()

            logger.info(f"通过 MySQL 协议发现 {len(nodes)} 个 Backend 节点")
            return nodes

        except Exception as e:
            logger.error(f"通过 MySQL 协议获取 Backend 信息失败: {e}")
            return []

    def _parse_doris_capacity(self, capacity_str: str) -> int:
        """解析 Doris 容量字符串为字节数"""
        try:
            if not capacity_str or capacity_str == 'N/A':
                return 0

            # 移除空格并转为大写
            capacity_str = capacity_str.replace(' ', '').upper()

            # 提取数字部分
            import re
            match = re.match(r'([0-9.]+)([A-Z]*)', capacity_str)
            if not match:
                return 0

            value = float(match.group(1))
            unit = match.group(2) if match.group(2) else 'B'

            # 转换单位
            unit_multipliers = {
                'B': 1,
                'KB': 1024,
                'MB': 1024 ** 2,
                'GB': 1024 ** 3,
                'TB': 1024 ** 4,
                'PB': 1024 ** 5
            }

            return int(value * unit_multipliers.get(unit, 1))

        except Exception as e:
            logger.debug(f"解析容量字符串失败 '{capacity_str}': {e}")
            return 0

    async def _get_doris_backend_metrics(self, host: str, http_port: str) -> Dict:
        """通过 Backend HTTP 接口获取系统指标"""
        try:
            # 使用更短的超时时间，避免阻塞主流程
            timeout = aiohttp.ClientTimeout(total=3, connect=1)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                metrics_url = f"http://{host}:{http_port}/metrics"

                async with session.get(metrics_url) as response:
                    if response.status == 200:
                        text = await response.text()
                        return self._parse_doris_metrics(text)

        except Exception as e:
            logger.debug(f"获取 Doris Backend 指标失败: {e}")

        return {}

    def _parse_doris_metrics(self, metrics_text: str) -> Dict:
        """解析 Doris 指标文本（Prometheus 格式）"""
        metrics = {}
        try:
            for line in metrics_text.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    # 解析 Prometheus 格式: metric_name{labels} value
                    if '{' in line:
                        # 有标签的情况
                        parts = line.split(' ')
                        if len(parts) >= 2:
                            metric_full = parts[0]
                            value = parts[1]

                            # 提取指标名称
                            if '{' in metric_full:
                                metric_name = metric_full.split('{')[0]
                                labels = metric_full.split('{')[1].split('}')[0]
                            else:
                                metric_name = metric_full
                                labels = ''

                            # 根据你提供的实际指标提取关键信息
                            if metric_name == 'jvm_heap_size_bytes':
                                if 'type="used"' in labels:
                                    metrics['jvm_heap_used'] = int(float(value))
                                elif 'type="max"' in labels:
                                    metrics['jvm_heap_max'] = int(float(value))
                            elif metric_name == 'system_meminfo':
                                if 'name="memory_total"' in labels:
                                    metrics['system_total_memory'] = int(float(value))
                                elif 'name="memory_free"' in labels:
                                    metrics['system_free_memory'] = int(float(value))
                                elif 'name="memory_available"' in labels:
                                    metrics['system_available_memory'] = int(float(value))
                    else:
                        # 简单格式
                        parts = line.split(' ')
                        if len(parts) >= 2:
                            metric_name = parts[0]
                            value = parts[1]
                            metrics[metric_name] = float(value)

        except Exception as e:
            logger.debug(f"解析指标失败: {e}")

        return metrics

    async def _get_doris_frontends_via_mysql(self, host: str, username: str, password: str) -> List[Dict]:
        """通过 MySQL 协议获取 Frontend 信息"""
        try:
            import aiomysql

            connection = await aiomysql.connect(
                host=host,
                port=9030,
                user=username,
                password=password,
                connect_timeout=10,
                autocommit=True
            )

            cursor = await connection.cursor()
            await cursor.execute("SHOW FRONTENDS")
            results = await cursor.fetchall()

            # 获取列名
            desc = cursor.description
            columns = [col[0] for col in desc]

            nodes = []
            for row in results:
                row_dict = dict(zip(columns, row))
                is_master = str(row_dict.get('IsMaster', '')).lower() == 'true'
                is_alive = str(row_dict.get('Alive', '')).lower() == 'true'

                node_info = {
                    'hostname': row_dict.get('Host', ''),
                    'ip_address': row_dict.get('Host', ''),
                    'role': 'Frontend',
                    'node_type': 'master' if is_master else 'follower',
                    'status': 'active' if is_alive else 'inactive',
                    'services': ['Frontend'],
                    'query_port': row_dict.get('QueryPort'),
                    'edit_log_port': row_dict.get('EditLogPort'),
                    'http_port': row_dict.get('HttpPort'),
                    'rpc_port': row_dict.get('RpcPort'),
                    'version': row_dict.get('Version'),
                    'discovery_method': 'doris_mysql_protocol'
                }

                # 获取 Frontend 的 JVM 指标
                if is_alive and row_dict.get('HttpPort'):
                    try:
                        fe_metrics = await self._get_doris_frontend_metrics(
                            row_dict.get('Host'),
                            row_dict.get('HttpPort')
                        )
                        if fe_metrics:
                            node_info.update(fe_metrics)
                    except Exception as e:
                        logger.debug(f"获取 Frontend {row_dict.get('Host')} 指标失败: {e}")

                nodes.append(node_info)

                # 记录主节点
                if is_master:
                    logger.info(f"发现 Doris Master Frontend: {row_dict.get('Host')}")

            await cursor.close()
            connection.close()

            logger.info(f"通过 MySQL 协议发现 {len(nodes)} 个 Frontend 节点")
            return nodes

        except ImportError:
            logger.error("aiomysql 模块未安装，无法使用 MySQL 协议")
            return []
        except Exception as e:
            logger.error(f"通过 MySQL 协议获取 Frontend 信息失败: {e}")
            return []

    async def _get_doris_frontend_metrics(self, host: str, http_port: str) -> Dict:
        """获取 Doris Frontend 指标"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                metrics_url = f"http://{host}:{http_port}/metrics"

                async with session.get(metrics_url) as response:
                    if response.status == 200:
                        text = await response.text()
                        return self._parse_doris_metrics(text)

        except Exception as e:
            logger.debug(f"获取 Doris Frontend 指标失败: {e}")

        return {}

    async def _discover_all_namenodes(self, session: aiohttp.ClientSession, config: Dict) -> List[Dict]:
        """发现所有NameNode（包括HA）"""
        namenodes = []

        # 主NameNode
        primary_host = config.get('namenode_host')
        primary_port = config.get('namenode_web_port', 9870)

        # HA NameNode列表
        ha_hosts = config.get('namenode_ha_hosts', [])

        # 所有可能的NameNode主机
        all_nn_hosts = [primary_host] + [host for host in ha_hosts if host != primary_host]

        for nn_host in all_nn_hosts:
            try:
                # 检查NameNode状态
                ha_info = await self._get_namenode_ha_status(session, nn_host, primary_port, config)

                namenodes.append({
                    'hostname': nn_host,
                    'ip_address': await self._resolve_hostname_to_ip(nn_host),
                    'role': 'NameNode',
                    'node_type': 'master',
                    'status': 'active' if ha_info.get('reachable', False) else 'offline',
                    'services': ['NameNode'],
                    'ha_state': ha_info.get('state', 'UNKNOWN'),
                    'web_ports': {'namenode': primary_port},
                    'discovery_method': 'hadoop_web_api',
                    'additional_info': {
                        'started_time': ha_info.get('started_time'),
                        'version': ha_info.get('version')
                    }
                })

            except Exception as e:
                logger.warning(f"无法连接到NameNode {nn_host}: {e}")
                # 即使无法连接也记录节点（可能是Standby或离线）
                namenodes.append({
                    'hostname': nn_host,
                    'ip_address': await self._resolve_hostname_to_ip(nn_host),
                    'role': 'NameNode',
                    'node_type': 'master',
                    'status': 'unknown',
                    'services': ['NameNode'],
                    'ha_state': 'UNREACHABLE',
                    'web_ports': {'namenode': primary_port},
                    'discovery_method': 'hadoop_web_api'
                })

        return namenodes

    async def _get_namenode_ha_status(self, session: aiohttp.ClientSession, host: str, port: int, config: Dict) -> Dict:
        """获取NameNode HA状态"""
        try:
            username = config.get('username') if config else None
            if not username:
                from config.settings import settings
                username = settings.HDFS_USER

            # 先尝试获取基本的NameNode信息
            url = f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
            if username:
                url += f"&user.name={username}"

            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                beans = data.get('beans', [])
                if beans:
                    info = beans[0]

                    # 尝试获取HA状态
                    ha_state = 'ACTIVE'  # 如果能访问到NameNodeInfo，通常是Active
                    try:
                        ha_url = f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
                        if username:
                            ha_url += f"&user.name={username}"

                        async with session.get(ha_url) as ha_response:
                            if ha_response.status == 200:
                                ha_data = await ha_response.json()
                                ha_beans = ha_data.get('beans', [])
                                if ha_beans:
                                    ha_state = ha_beans[0].get('State', 'UNKNOWN')
                    except:
                        pass  # 如果获取HA状态失败，使用默认值

                    return {
                        'reachable': True,
                        'state': ha_state,
                        'started_time': info.get('NNStartedTimeInMillis'),
                        'version': info.get('Version')
                    }

        except Exception as e:
            logger.debug(f"获取{host}的HA状态失败: {e}")

        return {
            'reachable': False,
            'state': 'UNKNOWN'
        }

    async def _discover_journalnodes(self, session: aiohttp.ClientSession, config: Dict) -> List[Dict]:
        """发现JournalNode"""
        journalnodes = []

        # 方法1：从配置中获取JournalNode列表
        jn_hosts = config.get('journalnode_hosts', [])
        jn_port = config.get('journalnode_port', 8485)

        if jn_hosts:
            for jn_host in jn_hosts:
                journalnodes.append({
                    'hostname': jn_host,
                    'ip_address': await self._resolve_hostname_to_ip(jn_host),
                    'role': 'JournalNode',
                    'node_type': 'support',
                    'status': 'active',  # 假设配置的都是活跃的
                    'services': ['JournalNode'],
                    'journal_port': jn_port,
                    'discovery_method': 'config_based'
                })
        else:
            # 方法2：尝试从Active NameNode的JMX中解析
            try:
                active_nn_host = config.get('namenode_host')
                active_nn_port = config.get('namenode_web_port', 9870)

                username = config.get('username') if config else None
                if not username:
                    from config.settings import settings
                    username = settings.HDFS_USER

                url = f"http://{active_nn_host}:{active_nn_port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
                if username:
                    url += f"&user.name={username}"

                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()

                    beans = data.get('beans', [])
                    if beans:
                        info = beans[0]
                        name_journal_status = info.get('NameJournalStatus', '[]')

                        try:
                            journal_status_list = json.loads(name_journal_status)
                            for journal_info in journal_status_list:
                                manager = journal_info.get('manager', '')
                                # 解析类似 "QJM to [192.142.76.249:8485, 192.142.76.250:8485, 192.142.76.251:8485]"
                                if 'QJM to' in manager and '[' in manager:
                                    jn_list_str = manager.split('[')[1].split(']')[0]
                                    jn_addresses = [addr.strip() for addr in jn_list_str.split(',')]

                                    for jn_addr in jn_addresses:
                                        if ':' in jn_addr:
                                            jn_ip, jn_port = jn_addr.split(':')
                                            journalnodes.append({
                                                'hostname': await self._resolve_ip_to_hostname(jn_ip),
                                                'ip_address': jn_ip,
                                                'role': 'JournalNode',
                                                'node_type': 'support',
                                                'status': 'active',
                                                'services': ['JournalNode'],
                                                'journal_port': int(jn_port),
                                                'discovery_method': 'jmx_parsing'
                                            })
                        except json.JSONDecodeError:
                            logger.warning("无法解析NameJournalStatus")

            except Exception as e:
                logger.warning(f"从JMX解析JournalNode失败: {e}")

        return journalnodes

    async def _resolve_hostname_to_ip(self, hostname: str) -> str:
        """解析hostname到IP地址"""
        try:
            import socket
            ip = socket.gethostbyname(hostname)
            return ip
        except:
            return hostname  # 如果解析失败，返回原hostname

    async def _resolve_ip_to_hostname(self, ip: str) -> str:
        """解析IP地址到hostname"""
        try:
            import socket
            hostname = socket.gethostbyaddr(ip)[0]
            return hostname
        except:
            return ip

    async def _get_hadoop_jvm_metrics(self, session: aiohttp.ClientSession, host: str, port: int,
                                      config: Dict = None) -> Dict:
        """获取 Hadoop 节点的 JVM 内存信息"""
        try:
            username = config.get('username') if config else None
            if not username:
                from config.settings import settings
                username = settings.HDFS_USER

            # 获取 JVM 内存信息
            memory_url = f"http://{host}:{port}/jmx?qry=java.lang:type=Memory"
            if username:
                memory_url += f"&user.name={username}"

            # 获取操作系统信息
            os_url = f"http://{host}:{port}/jmx?qry=java.lang:type=OperatingSystem"
            if username:
                os_url += f"&user.name={username}"

            memory_info = {}

            # 获取内存信息
            async with session.get(memory_url) as response:
                if response.status == 200:
                    data = await response.json()
                    beans = data.get('beans', [])
                    if beans:
                        bean = beans[0]
                        heap_memory = bean.get('HeapMemoryUsage', {})
                        non_heap_memory = bean.get('NonHeapMemoryUsage', {})

                        memory_info.update({
                            'jvm_heap_used': heap_memory.get('used', 0),
                            'jvm_heap_max': heap_memory.get('max', 0),
                            'jvm_heap_committed': heap_memory.get('committed', 0),
                            'jvm_non_heap_used': non_heap_memory.get('used', 0),
                            'jvm_non_heap_committed': non_heap_memory.get('committed', 0)
                        })

            # 获取系统信息
            async with session.get(os_url) as response:
                if response.status == 200:
                    data = await response.json()
                    beans = data.get('beans', [])
                    if beans:
                        bean = beans[0]
                        memory_info.update({
                            'system_total_memory': bean.get('TotalPhysicalMemorySize', 0),
                            'system_free_memory': bean.get('FreePhysicalMemorySize', 0),
                            'system_cpu_load': bean.get('SystemCpuLoad', 0),
                            'available_processors': bean.get('AvailableProcessors', 0)
                        })

            return memory_info

        except Exception as e:
            logger.debug(f"获取 Hadoop JVM 指标失败: {e}")
            return {}