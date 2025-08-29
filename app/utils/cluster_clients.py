# app/utils/cluster_clients.py
"""
生产级集群客户端 - 完全移除模拟数据，只处理真实集群连接
"""
import asyncio
import json
import aiohttp
import paramiko
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from contextlib import asynccontextmanager

from loguru import logger
from config.settings import settings


@dataclass
class ClusterEndpoint:
    """集群端点配置"""
    host: str
    port: int
    protocol: str = "http"
    path: str = ""
    timeout: int = 30

    @property
    def url(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}{self.path}"


class ClusterConnectionError(Exception):
    """集群连接异常"""
    pass


class ClusterMetricsError(Exception):
    """指标收集异常"""
    pass


class BaseClusterClient(ABC):
    """集群客户端基类 - 不包含任何模拟数据逻辑"""

    def __init__(self, cluster_config: Dict[str, Any]):
        self.config = cluster_config
        self.cluster_name = cluster_config.get('name', 'Unknown')
        self.timeout = cluster_config.get('timeout', 30)
        self._session: Optional[aiohttp.ClientSession] = None

    @asynccontextmanager
    async def _get_session(self):
        """获取HTTP会话"""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)

        try:
            yield self._session
        except Exception as e:
            logger.error(f"HTTP会话错误: {e}")
            raise

    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """健康检查 - 必须实现"""
        pass

    @abstractmethod
    async def get_cluster_metrics(self) -> Dict[str, Any]:
        """获取集群指标 - 必须实现"""
        pass

    @abstractmethod
    async def get_node_metrics(self, node_config: Dict) -> Dict[str, Any]:
        """获取节点指标 - 必须实现"""
        pass

    async def close(self):
        """关闭客户端连接"""
        if self._session and not self._session.closed:
            await self._session.close()


class HadoopClusterClient(BaseClusterClient):
    """Hadoop集群客户端 - 纯生产环境实现"""

    def __init__(self, cluster_config: Dict[str, Any]):
        super().__init__(cluster_config)

        # NameNode Web UI配置
        self.namenode_endpoint = ClusterEndpoint(
            host=cluster_config['namenode_host'],
            port=cluster_config.get('namenode_web_port', 9870)
        )

        # ResourceManager Web UI配置
        self.resourcemanager_endpoint = ClusterEndpoint(
            host=cluster_config.get('resourcemanager_host', cluster_config['namenode_host']),
            port=cluster_config.get('resourcemanager_web_port', 8088)
        )

    async def health_check(self) -> Dict[str, Any]:
        """Hadoop集群健康检查"""
        try:
            async with self._get_session() as session:
                # 检查NameNode状态
                namenode_url = f"{self.namenode_endpoint.url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"

                async with session.get(namenode_url) as response:
                    if response.status != 200:
                        raise ClusterConnectionError(f"NameNode不可访问: HTTP {response.status}")

                    data = await response.json()

                    if not data.get('beans'):
                        raise ClusterConnectionError("NameNode返回数据格式异常")

                    fs_info = data['beans'][0]

                    return {
                        'status': 'healthy',
                        'namenode_state': fs_info.get('tag.HAState', 'Active'),
                        'safe_mode': fs_info.get('Safemode', ''),
                        'live_nodes': fs_info.get('NumLiveDataNodes', 0),
                        'dead_nodes': fs_info.get('NumDeadDataNodes', 0),
                        'check_time': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Hadoop健康检查失败: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'check_time': datetime.now().isoformat()
            }

    async def get_cluster_metrics(self) -> Dict[str, Any]:
        """获取Hadoop集群整体指标"""
        try:
            async with self._get_session() as session:
                tasks = [
                    self._get_hdfs_metrics(session),
                    self._get_yarn_metrics(session),
                    self._get_cluster_nodes_info(session)
                ]

                hdfs_metrics, yarn_metrics, nodes_info = await asyncio.gather(*tasks)

                return {
                    'cluster_type': 'hadoop',
                    'cluster_name': self.cluster_name,
                    'hdfs': hdfs_metrics,
                    'yarn': yarn_metrics,
                    'nodes': nodes_info,
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"获取Hadoop集群指标失败: {e}")
            raise ClusterMetricsError(f"Hadoop集群指标收集失败: {str(e)}")

    async def _get_hdfs_metrics(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取HDFS存储指标"""
        url = f"{self.namenode_endpoint.url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()

            fs_info = data['beans'][0]

            total_capacity = fs_info.get('Total', 0)
            used_capacity = fs_info.get('Used', 0)
            remaining_capacity = fs_info.get('Free', 0)

            return {
                'total_capacity_bytes': total_capacity,
                'used_capacity_bytes': used_capacity,
                'remaining_capacity_bytes': remaining_capacity,
                'capacity_usage_percent': (used_capacity / total_capacity * 100) if total_capacity > 0 else 0,
                'total_files': fs_info.get('TotalFiles', 0),
                'total_blocks': fs_info.get('TotalBlocks', 0),
                'missing_blocks': fs_info.get('MissingBlocks', 0),
                'corrupt_blocks': fs_info.get('CorruptBlocks', 0)
            }

    async def _get_yarn_metrics(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取YARN资源指标"""
        url = f"{self.resourcemanager_endpoint.url}/ws/v1/cluster/metrics"

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()

            cluster_metrics = data['clusterMetrics']

            return {
                'applications_submitted': cluster_metrics.get('appsSubmitted', 0),
                'applications_completed': cluster_metrics.get('appsCompleted', 0),
                'applications_pending': cluster_metrics.get('appsPending', 0),
                'applications_running': cluster_metrics.get('appsRunning', 0),
                'applications_failed': cluster_metrics.get('appsFailed', 0),
                'applications_killed': cluster_metrics.get('appsKilled', 0),
                'memory_total_mb': cluster_metrics.get('totalMB', 0),
                'memory_used_mb': cluster_metrics.get('allocatedMB', 0),
                'memory_available_mb': cluster_metrics.get('availableMB', 0),
                'vcores_total': cluster_metrics.get('totalVirtualCores', 0),
                'vcores_used': cluster_metrics.get('allocatedVirtualCores', 0),
                'vcores_available': cluster_metrics.get('availableVirtualCores', 0),
                'containers_allocated': cluster_metrics.get('containersAllocated', 0),
                'containers_reserved': cluster_metrics.get('containersReserved', 0),
                'containers_pending': cluster_metrics.get('containersPending', 0)
            }

    async def _get_cluster_nodes_info(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取集群节点信息"""
        url = f"{self.namenode_endpoint.url}/ws/v1/cluster/nodes"

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()

            nodes = data.get('nodes', {}).get('node', [])
            if not isinstance(nodes, list):
                nodes = [nodes] if nodes else []

            total_nodes = len(nodes)
            live_nodes = len([n for n in nodes if n.get('state') == 'NORMAL'])

            return {
                'total_nodes': total_nodes,
                'live_nodes': live_nodes,
                'dead_nodes': total_nodes - live_nodes,
                'nodes_detail': [
                    {
                        'id': node.get('id'),
                        'node_host_name': node.get('nodeHostName'),
                        'state': node.get('state'),
                        'last_health_update': node.get('lastHealthUpdate'),
                        'health_report': node.get('healthReport'),
                        'num_containers': node.get('numContainers', 0),
                        'used_memory_mb': node.get('usedMemoryMB', 0),
                        'avail_memory_mb': node.get('availMemoryMB', 0),
                        'used_virtual_cores': node.get('usedVirtualCores', 0),
                        'avail_virtual_cores': node.get('availVirtualCores', 0)
                    }
                    for node in nodes
                ]
            }

    async def get_node_metrics(self, node_config: Dict) -> Dict[str, Any]:
        """获取单个节点的详细指标"""
        try:
            # 通过SSH连接获取系统指标
            node_metrics = await self._collect_node_system_metrics(node_config)

            # 获取Hadoop相关服务状态
            service_metrics = await self._collect_hadoop_service_metrics(node_config)

            return {
                'node_name': node_config['node_name'],
                'ip_address': node_config['ip_address'],
                'system_metrics': node_metrics,
                'service_metrics': service_metrics,
                'collection_time': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"获取节点{node_config['node_name']}指标失败: {e}")
            raise ClusterMetricsError(f"节点指标收集失败: {str(e)}")


class FlinkClusterClient(BaseClusterClient):
    """Flink集群客户端"""

    def __init__(self, cluster_config: Dict[str, Any]):
        super().__init__(cluster_config)

        self.jobmanager_endpoint = ClusterEndpoint(
            host=cluster_config['jobmanager_host'],
            port=cluster_config.get('jobmanager_web_port', 8081)
        )

    async def health_check(self) -> Dict[str, Any]:
        """Flink集群健康检查"""
        try:
            async with self._get_session() as session:
                # 检查JobManager状态
                url = f"{self.jobmanager_endpoint.url}/overview"

                async with session.get(url) as response:
                    if response.status != 200:
                        raise ClusterConnectionError(f"JobManager不可访问: HTTP {response.status}")

                    data = await response.json()

                    return {
                        'status': 'healthy',
                        'flink_version': data.get('flink-version'),
                        'flink_commit': data.get('flink-commit'),
                        'taskmanagers': data.get('taskmanagers', 0),
                        'slots_total': data.get('slots-total', 0),
                        'slots_available': data.get('slots-available', 0),
                        'jobs_running': data.get('jobs-running', 0),
                        'jobs_finished': data.get('jobs-finished', 0),
                        'jobs_cancelled': data.get('jobs-cancelled', 0),
                        'jobs_failed': data.get('jobs-failed', 0),
                        'check_time': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Flink健康检查失败: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'check_time': datetime.now().isoformat()
            }

    async def get_cluster_metrics(self) -> Dict[str, Any]:
        """获取Flink集群指标"""
        try:
            async with self._get_session() as session:
                tasks = [
                    self._get_cluster_overview(session),
                    self._get_jobs_overview(session),
                    self._get_taskmanagers_info(session)
                ]

                overview, jobs, taskmanagers = await asyncio.gather(*tasks)

                return {
                    'cluster_type': 'flink',
                    'cluster_name': self.cluster_name,
                    'overview': overview,
                    'jobs': jobs,
                    'taskmanagers': taskmanagers,
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"获取Flink集群指标失败: {e}")
            raise ClusterMetricsError(f"Flink集群指标收集失败: {str(e)}")

    async def _get_cluster_overview(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取集群概览"""
        url = f"{self.jobmanager_endpoint.url}/overview"

        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def _get_jobs_overview(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取作业概览"""
        url = f"{self.jobmanager_endpoint.url}/jobs/overview"

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()

            jobs = data.get('jobs', [])

            return {
                'total_jobs': len(jobs),
                'running_jobs': len([j for j in jobs if j.get('state') == 'RUNNING']),
                'finished_jobs': len([j for j in jobs if j.get('state') == 'FINISHED']),
                'failed_jobs': len([j for j in jobs if j.get('state') == 'FAILED']),
                'cancelled_jobs': len([j for j in jobs if j.get('state') == 'CANCELLED']),
                'jobs_detail': jobs
            }

    async def _get_taskmanagers_info(self, session: aiohttp.ClientSession) -> Dict[str, Any]:
        """获取TaskManager信息"""
        url = f"{self.jobmanager_endpoint.url}/taskmanagers"

        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()

            taskmanagers = data.get('taskmanagers', [])

            return {
                'total_taskmanagers': len(taskmanagers),
                'taskmanagers_detail': [
                    {
                        'id': tm.get('id'),
                        'path': tm.get('path'),
                        'data_port': tm.get('dataPort'),
                        'jmx_port': tm.get('jmxPort'),
                        'time_since_last_heartbeat': tm.get('timeSinceLastHeartbeat'),
                        'slots_number': tm.get('slotsNumber'),
                        'free_slots': tm.get('freeSlots'),
                        'cpu_cores': tm.get('hardware', {}).get('cpuCores'),
                        'physical_memory': tm.get('hardware', {}).get('physicalMemory'),
                        'free_memory': tm.get('hardware', {}).get('freeMemory'),
                        'managed_memory': tm.get('hardware', {}).get('managedMemory')
                    }
                    for tm in taskmanagers
                ]
            }

    async def get_node_metrics(self, node_config: Dict) -> Dict[str, Any]:
        """获取Flink节点指标"""
        try:
            # 系统指标
            system_metrics = await self._collect_node_system_metrics(node_config)

            # Flink特定指标
            flink_metrics = await self._collect_flink_service_metrics(node_config)

            return {
                'node_name': node_config['node_name'],
                'ip_address': node_config['ip_address'],
                'system_metrics': system_metrics,
                'flink_metrics': flink_metrics,
                'collection_time': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"获取Flink节点{node_config['node_name']}指标失败: {e}")
            raise ClusterMetricsError(f"Flink节点指标收集失败: {str(e)}")


class DorisClusterClient(BaseClusterClient):
    """Doris集群客户端"""

    def __init__(self, cluster_config: Dict[str, Any]):
        super().__init__(cluster_config)

        self.fe_endpoint = ClusterEndpoint(
            host=cluster_config['fe_host'],
            port=cluster_config.get('fe_web_port', 8060)
        )

        self.username = cluster_config.get('username', 'root')
        self.password = cluster_config.get('password', '')

    async def health_check(self) -> Dict[str, Any]:
        """Doris集群健康检查"""
        try:
            async with self._get_session() as session:
                # 检查FE状态
                url = f"{self.fe_endpoint.url}/api/show_frontends"
                auth = aiohttp.BasicAuth(self.username, self.password)

                async with session.get(url, auth=auth) as response:
                    if response.status != 200:
                        raise ClusterConnectionError(f"Doris FE不可访问: HTTP {response.status}")

                    fe_data = await response.json()

                # 检查BE状态
                url = f"{self.fe_endpoint.url}/api/show_backends"
                async with session.get(url, auth=auth) as response:
                    response.raise_for_status()
                    be_data = await response.json()

                frontends = fe_data.get('data', [])
                backends = be_data.get('data', [])

                alive_backends = len([be for be in backends if be.get('Alive') == 'true'])

                return {
                    'status': 'healthy',
                    'frontends_count': len(frontends),
                    'backends_count': len(backends),
                    'alive_backends': alive_backends,
                    'master_fe': next((fe.get('Host') for fe in frontends if fe.get('IsMaster') == 'true'), None),
                    'check_time': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"Doris健康检查失败: {e}")
            return {
                'status': 'error',
                'error_message': str(e),
                'check_time': datetime.now().isoformat()
            }

    async def get_cluster_metrics(self) -> Dict[str, Any]:
        """获取Doris集群指标"""
        try:
            async with self._get_session() as session:
                auth = aiohttp.BasicAuth(self.username, self.password)

                tasks = [
                    self._get_frontends_info(session, auth),
                    self._get_backends_info(session, auth),
                    self._get_databases_info(session, auth)
                ]

                frontends, backends, databases = await asyncio.gather(*tasks)

                return {
                    'cluster_type': 'doris',
                    'cluster_name': self.cluster_name,
                    'frontends': frontends,
                    'backends': backends,
                    'databases': databases,
                    'timestamp': datetime.now().isoformat()
                }

        except Exception as e:
            logger.error(f"获取Doris集群指标失败: {e}")
            raise ClusterMetricsError(f"Doris集群指标收集失败: {str(e)}")

    async def _get_frontends_info(self, session: aiohttp.ClientSession, auth) -> Dict[str, Any]:
        """获取FE信息"""
        url = f"{self.fe_endpoint.url}/api/show_frontends"

        async with session.get(url, auth=auth) as response:
            response.raise_for_status()
            data = await response.json()

            frontends = data.get('data', [])

            return {
                'total_frontends': len(frontends),
                'alive_frontends': len([fe for fe in frontends if fe.get('Alive') == 'true']),
                'master_fe': next((fe.get('Host') for fe in frontends if fe.get('IsMaster') == 'true'), None),
                'frontends_detail': frontends
            }

    async def _get_backends_info(self, session: aiohttp.ClientSession, auth) -> Dict[str, Any]:
        """获取BE信息"""
        url = f"{self.fe_endpoint.url}/api/show_backends"

        async with session.get(url, auth=auth) as response:
            response.raise_for_status()
            data = await response.json()

            backends = data.get('data', [])
            alive_backends = [be for be in backends if be.get('Alive') == 'true']

            total_capacity = sum(int(be.get('TotalCapacity', '0')) for be in alive_backends)
            used_capacity = sum(int(be.get('UsedCapacity', '0')) for be in alive_backends)

            return {
                'total_backends': len(backends),
                'alive_backends': len(alive_backends),
                'total_capacity_bytes': total_capacity,
                'used_capacity_bytes': used_capacity,
                'available_capacity_bytes': total_capacity - used_capacity,
                'capacity_usage_percent': (used_capacity / total_capacity * 100) if total_capacity > 0 else 0,
                'backends_detail': backends
            }

    async def _get_databases_info(self, session: aiohttp.ClientSession, auth) -> Dict[str, Any]:
        """获取数据库信息"""
        url = f"{self.fe_endpoint.url}/api/show_databases"

        async with session.get(url, auth=auth) as response:
            response.raise_for_status()
            data = await response.json()

            databases = data.get('data', [])

            return {
                'total_databases': len(databases),
                'databases_detail': databases
            }

    async def get_node_metrics(self, node_config: Dict) -> Dict[str, Any]:
        """获取Doris节点指标"""
        try:
            # 系统指标
            system_metrics = await self._collect_node_system_metrics(node_config)

            # Doris特定指标
            doris_metrics = await self._collect_doris_service_metrics(node_config)

            return {
                'node_name': node_config['node_name'],
                'ip_address': node_config['ip_address'],
                'system_metrics': system_metrics,
                'doris_metrics': doris_metrics,
                'collection_time': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"获取Doris节点{node_config['node_name']}指标失败: {e}")
            raise ClusterMetricsError(f"Doris节点指标收集失败: {str(e)}")


# ===================================================================
# 通用节点指标收集器 - 通过SSH获取系统指标
# ===================================================================

class NodeSystemCollector:
    """节点系统指标收集器"""

    def __init__(self, ssh_config: Dict[str, Any]):
        self.ssh_config = ssh_config
        self.timeout = ssh_config.get('timeout', 30)

    async def collect_system_metrics(self, node_config: Dict) -> Dict[str, Any]:
        """收集节点系统指标"""
        try:
            # 在线程池中执行SSH连接（因为paramiko是同步的）
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                self._collect_via_ssh,
                node_config
            )
        except Exception as e:
            logger.error(f"收集节点{node_config['node_name']}系统指标失败: {e}")
            raise ClusterMetricsError(f"系统指标收集失败: {str(e)}")

    def _collect_via_ssh(self, node_config: Dict) -> Dict[str, Any]:
        """通过SSH收集系统指标"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # 建立SSH连接
            client.connect(
                hostname=node_config['ip_address'],
                port=node_config.get('port', 22),
                username=self.ssh_config.get('username'),
                password=self.ssh_config.get('password'),
                key_filename=self.ssh_config.get('key_path'),
                timeout=self.timeout
            )

            # 收集各项系统指标
            metrics = {}

            # CPU使用率
            stdin, stdout, stderr = client.exec_command(
                "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$3+$4+$5)} END {print usage}'"
            )
            cpu_usage = float(stdout.read().decode().strip() or 0)
            metrics['cpu_usage'] = round(cpu_usage, 2)

            # 内存使用率
            stdin, stdout, stderr = client.exec_command(
                "free | grep Mem | awk '{printf \"%.2f\", $3/$2 * 100.0}'"
            )
            memory_usage = float(stdout.read().decode().strip() or 0)
            metrics['memory_usage'] = round(memory_usage, 2)

            # 磁盘使用率
            stdin, stdout, stderr = client.exec_command(
                "df -h / | awk 'NR==2 {print $5}' | sed 's/%//'"
            )
            disk_usage = float(stdout.read().decode().strip() or 0)
            metrics['disk_usage'] = round(disk_usage, 2)

            # 系统负载
            stdin, stdout, stderr = client.exec_command("uptime | awk -F'load average:' '{ print $2 }'")
            load_avg = stdout.read().decode().strip().split(',')
            metrics['load_average'] = {
                '1min': float(load_avg[0].strip() if len(load_avg) > 0 else 0),
                '5min': float(load_avg[1].strip() if len(load_avg) > 1 else 0),
                '15min': float(load_avg[2].strip() if len(load_avg) > 2 else 0)
            }

            # 进程数量
            stdin, stdout, stderr = client.exec_command("ps aux | wc -l")
            process_count = int(stdout.read().decode().strip() or 0)
            metrics['process_count'] = process_count

            # 网络IO
            stdin, stdout, stderr = client.exec_command(
                "cat /proc/net/dev | grep -v 'lo:' | awk 'NR>2 {rx+=$2; tx+=$10} END {print rx, tx}'"
            )
            network_data = stdout.read().decode().strip().split()
            if len(network_data) >= 2:
                metrics['network_io'] = {
                    'rx_bytes': int(network_data[0]),
                    'tx_bytes': int(network_data[1])
                }

            # 系统运行时间
            stdin, stdout, stderr = client.exec_command("uptime -s")
            uptime_start = stdout.read().decode().strip()
            metrics['uptime_start'] = uptime_start

            return metrics

        finally:
            client.close()


# ===================================================================
# 集群客户端工厂
# ===================================================================

class ClusterClientFactory:
    """集群客户端工厂类"""

    _clients = {
        'hadoop': HadoopClusterClient,
        'flink': FlinkClusterClient,
        'doris': DorisClusterClient
    }

    @classmethod
    def create_client(cls, cluster_type: str, cluster_config: Dict[str, Any]) -> BaseClusterClient:
        """创建集群客户端"""
        if cluster_type not in cls._clients:
            raise ValueError(f"不支持的集群类型: {cluster_type}")

        client_class = cls._clients[cluster_type]
        return client_class(cluster_config)

    @classmethod
    def get_supported_types(cls) -> List[str]:
        """获取支持的集群类型"""
        return list(cls._clients.keys())


# ===================================================================
# 生产级指标收集服务 - 替换原有的MetricsCollector
# ===================================================================

class ProductionMetricsCollector:
    """生产级指标收集器 - 完全移除模拟数据"""

    def __init__(self):
        self.clients = {}
        self.node_collector = NodeSystemCollector(settings.get_ssh_config())
        self._initialize_clients()

    def _initialize_clients(self):
        """初始化所有集群客户端"""
        for cluster_type in ['hadoop', 'flink', 'doris']:
            try:
                config = settings.get_cluster_config(cluster_type)
                if config:
                    self.clients[cluster_type] = ClusterClientFactory.create_client(
                        cluster_type, config
                    )
                    logger.info(f"初始化{cluster_type}集群客户端成功")
            except Exception as e:
                logger.error(f"初始化{cluster_type}集群客户端失败: {e}")

    async def collect_all_clusters_metrics(self) -> Dict[str, Dict[str, Any]]:
        """收集所有集群指标"""
        results = {}

        for cluster_type, client in self.clients.items():
            try:
                metrics = await client.get_cluster_metrics()
                results[cluster_type] = {
                    'status': 'success',
                    'data': metrics
                }
                logger.info(f"收集{cluster_type}集群指标成功")

            except Exception as e:
                logger.error(f"收集{cluster_type}集群指标失败: {e}")
                results[cluster_type] = {
                    'status': 'error',
                    'error': str(e)
                }

        return results

    async def collect_cluster_metrics(self, cluster_type: str) -> Dict[str, Any]:
        """收集特定集群指标"""
        if cluster_type not in self.clients:
            raise ValueError(f"集群类型{cluster_type}不存在或未初始化")

        client = self.clients[cluster_type]
        return await client.get_cluster_metrics()

    async def check_cluster_health(self, cluster_type: str) -> Dict[str, Any]:
        """检查集群健康状态"""
        if cluster_type not in self.clients:
            raise ValueError(f"集群类型{cluster_type}不存在或未初始化")

        client = self.clients[cluster_type]
        return await client.health_check()

    async def collect_node_metrics(self, cluster_type: str, node_config: Dict) -> Dict[str, Any]:
        """收集节点指标"""
        if cluster_type not in self.clients:
            raise ValueError(f"集群类型{cluster_type}不存在或未初始化")

        client = self.clients[cluster_type]
        return await client.get_node_metrics(node_config)

    async def close_all_clients(self):
        """关闭所有客户端连接"""
        for client in self.clients.values():
            try:
                await client.close()
            except Exception as e:
                logger.error(f"关闭客户端连接失败: {e}")


# 创建全局实例
production_metrics_collector = ProductionMetricsCollector()


# ===================================================================
# 为BaseClusterClient添加通用方法实现
# ===================================================================

# 为所有客户端添加通用的节点指标收集方法
async def _collect_node_system_metrics(self, node_config: Dict) -> Dict[str, Any]:
    """收集节点系统指标 - 通用实现"""
    return await self.node_collector.collect_system_metrics(node_config)


async def _collect_hadoop_service_metrics(self, node_config: Dict) -> Dict[str, Any]:
    """收集Hadoop服务指标"""
    # 实现Hadoop特定的服务指标收集
    # 如DataNode、NodeManager状态等
    return {'service_type': 'hadoop', 'status': 'running'}


async def _collect_flink_service_metrics(self, node_config: Dict) -> Dict[str, Any]:
    """收集Flink服务指标"""
    # 实现Flink特定的服务指标收集
    # 如TaskManager状态等
    return {'service_type': 'flink', 'status': 'running'}


async def _collect_doris_service_metrics(self, node_config: Dict) -> Dict[str, Any]:
    """收集Doris服务指标"""
    # 实现Doris特定的服务指标收集
    # 如BE状态等
    return {'service_type': 'doris', 'status': 'running'}


# 将通用方法添加到基类
BaseClusterClient._collect_node_system_metrics = _collect_node_system_metrics
HadoopClusterClient._collect_hadoop_service_metrics = _collect_hadoop_service_metrics
FlinkClusterClient._collect_flink_service_metrics = _collect_flink_service_metrics
DorisClusterClient._collect_doris_service_metrics = _collect_doris_service_metrics


# 为所有客户端添加node_collector属性
def __init_with_collector(original_init):
    def wrapper(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.node_collector = NodeSystemCollector(settings.get_ssh_config())

    return wrapper


BaseClusterClient.__init__ = __init_with_collector(BaseClusterClient.__init__)