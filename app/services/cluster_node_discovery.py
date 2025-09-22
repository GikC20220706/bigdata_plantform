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

                # 2. 获取TaskManager列表
                task_managers = await self._get_flink_taskmanagers(session, jm_host, jm_port)

                # 3. 添加JobManager信息
                nodes = [{
                    'hostname': jm_host,
                    'ip_address': jm_host,
                    'role': 'JobManager',
                    'node_type': 'master',
                    'status': 'active',
                    'services': ['JobManager'],
                    'web_ports': {'jobmanager': jm_port},
                    'discovery_method': 'flink_web_api'
                }]

                # 4. 添加TaskManager信息
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
        """获取Flink TaskManager列表"""
        nodes = []
        try:
            url = f"http://{host}:{port}/taskmanagers"
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                taskmanagers = data.get('taskmanagers', [])
                for tm in taskmanagers:
                    nodes.append({
                        'hostname': tm.get('path', '').split('@')[-1] if '@' in tm.get('path', '') else '',
                        'ip_address': tm.get('path', '').split('@')[-1].split(':')[0] if '@' in tm.get('path',
                                                                                                       '') else '',
                        'role': 'TaskManager',
                        'node_type': 'worker',
                        'status': 'active',
                        'services': ['TaskManager'],
                        'task_manager_id': tm.get('id'),
                        'slots_total': tm.get('slotsNumber', 0),
                        'slots_free': tm.get('freeSlots', 0),
                        'cpu_cores': tm.get('hardware', {}).get('cpuCores', 0),
                        'physical_memory': tm.get('hardware', {}).get('physicalMemory', 0),
                        'free_memory': tm.get('hardware', {}).get('freeMemory', 0),
                        'managed_memory': tm.get('hardware', {}).get('managedMemory', 0),
                        'discovery_method': 'flink_web_api'
                    })
        except Exception as e:
            logger.warning(f"获取TaskManager列表失败: {e}")

        return nodes

    # ====================================================================
    # Doris集群节点发现 - 通过Frontend Web UI
    # ====================================================================

    async def _discover_doris_cluster(self, config: Dict) -> Dict:
        """发现Doris集群节点"""
        fe_host = config['frontend_host']
        fe_port = config.get('frontend_web_port', 8060)
        username = config.get('username', 'root')
        password = config.get('password', '')

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                # 1. 获取集群基本信息
                cluster_info = await self._get_doris_cluster_info(session, fe_host, fe_port, username, password)

                # 2. 获取Frontend节点列表
                frontends = await self._get_doris_frontends(session, fe_host, fe_port, username, password)

                # 3. 获取Backend节点列表
                backends = await self._get_doris_backends(session, fe_host, fe_port, username, password)

                # 4. 合并所有节点
                nodes = frontends + backends

                return {
                    'success': True,
                    'cluster_info': cluster_info,
                    'nodes': nodes,
                    'discovery_time': datetime.now().isoformat(),
                    'total_nodes': len(nodes)
                }

            except Exception as e:
                logger.error(f"Doris集群发现失败: {e}")
                raise

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

    async def _get_doris_frontends(self, session: aiohttp.ClientSession, host: str, port: int, username: str,
                                   password: str) -> List[Dict]:
        """获取Doris Frontend列表"""
        nodes = []
        try:
            auth = aiohttp.BasicAuth(username, password) if username else None

            url = f"http://{host}:{port}/api/show_frontends"
            async with session.get(url, auth=auth) as response:
                response.raise_for_status()
                data = await response.json()

                frontends = data.get('data', [])
                for fe in frontends:
                    nodes.append({
                        'hostname': fe.get('Host', ''),
                        'ip_address': fe.get('Host', ''),
                        'role': 'Frontend',
                        'node_type': 'master' if fe.get('IsMaster') == 'true' else 'follower',
                        'status': 'active' if fe.get('Alive') == 'true' else 'inactive',
                        'services': ['Frontend'],
                        'edit_log_port': fe.get('EditLogPort'),
                        'http_port': fe.get('HttpPort'),
                        'query_port': fe.get('QueryPort'),
                        'rpc_port': fe.get('RpcPort'),
                        'version': fe.get('Version'),
                        'last_heartbeat': fe.get('LastHeartbeat'),
                        'discovery_method': 'doris_web_api'
                    })
        except Exception as e:
            logger.warning(f"获取Doris Frontend列表失败: {e}")

        return nodes

    async def _get_doris_backends(self, session: aiohttp.ClientSession, host: str, port: int, username: str,
                                  password: str) -> List[Dict]:
        """获取Doris Backend列表"""
        nodes = []
        try:
            auth = aiohttp.BasicAuth(username, password) if username else None

            url = f"http://{host}:{port}/api/show_backends"
            async with session.get(url, auth=auth) as response:
                response.raise_for_status()
                data = await response.json()

                backends = data.get('data', [])
                for be in backends:
                    nodes.append({
                        'hostname': be.get('Host', ''),
                        'ip_address': be.get('Host', ''),
                        'role': 'Backend',
                        'node_type': 'worker',
                        'status': 'active' if be.get('Alive') == 'true' else 'inactive',
                        'services': ['Backend'],
                        'heartbeat_port': be.get('HeartbeatPort'),
                        'be_port': be.get('BePort'),
                        'http_port': be.get('HttpPort'),
                        'brpc_port': be.get('BrpcPort'),
                        'version': be.get('Version'),
                        'last_heartbeat': be.get('LastHeartbeat'),
                        'total_capacity': be.get('TotalCapacity'),
                        'used_capacity': be.get('UsedCapacity'),
                        'available_capacity': be.get('AvailCapacity'),
                        'discovery_method': 'doris_web_api'
                    })
        except Exception as e:
            logger.warning(f"获取Doris Backend列表失败: {e}")

        return nodes

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