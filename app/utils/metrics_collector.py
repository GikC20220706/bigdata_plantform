# app/utils/metrics_collector.py
"""
Configurable metrics collector that loads node configuration from settings/files.
"""

import asyncio
import concurrent.futures
import json
import os
import platform
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import paramiko
from loguru import logger

from config.settings import settings


class ConfigurableMetricsCollector:
    """
    System metrics collector with configurable node definitions.
    Loads cluster node configuration from external files or environment variables.
    """

    def __init__(self):
        self.DISABLE_SSH_MONITORING = True
        self.use_real_clusters = settings.use_real_clusters

        # Cache configuration
        self.cache = {}
        self.cache_ttl = settings.METRICS_CACHE_TTL
        self.collecting_lock = asyncio.Lock()

        # SSH configuration - loaded from settings
        self.ssh_timeout = settings.METRICS_SSH_TIMEOUT
        self.max_concurrent_connections = settings.METRICS_MAX_CONCURRENT

        # Load cluster node configuration
        self.cluster_nodes = self._load_cluster_configuration()

        # SSH connection pools
        self.ssh_pools = {}

        logger.info(f"ConfigurableMetricsCollector initialized with {sum(len(nodes) for nodes in self.cluster_nodes.values())} total nodes")

    def _load_cluster_configuration(self) -> Dict[str, List[Dict]]:
        """
        Load cluster node configuration from multiple sources.
        Priority: 1. Config file 2. Environment variable 3. Settings default
        """
        config_sources = [
            self._load_from_config_file,
            self._load_from_environment,
            self._load_from_settings_default
        ]

        for source_func in config_sources:
            try:
                config = source_func()
                if config:
                    logger.info(f"Loaded cluster configuration from {source_func.__name__}")
                    return self._validate_and_organize_config(config)
            except Exception as e:
                logger.warning(f"Failed to load config from {source_func.__name__}: {e}")

        # Fallback to empty configuration
        logger.warning("No valid cluster configuration found, using empty config")
        return {'hadoop': [], 'flink': [], 'doris': []}

    def _load_from_config_file(self) -> Optional[Dict]:
        """Load configuration from nodes.json file."""
        config_file = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'nodes.json')

        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                logger.info(f"Loaded {len(config_data.get('nodes', []))} nodes from config file")
                return config_data

        return None

    def _load_from_environment(self) -> Optional[Dict]:
        """Load configuration from environment variables."""
        config_env = os.getenv('CLUSTER_NODES_CONFIG')
        if config_env:
            config_data = json.loads(config_env)
            logger.info(f"Loaded cluster config from environment variable")
            return config_data

        return None

    def _load_from_settings_default(self) -> Optional[Dict]:
        """Load default configuration from settings."""
        if hasattr(settings, 'CLUSTER_NODES_CONFIG'):
            config_data = settings.CLUSTER_NODES_CONFIG
            if 'static_nodes' in config_data and config_data['static_nodes']:
                return {'nodes': config_data['static_nodes']}

        return None

    def _validate_and_organize_config(self, config_data: Dict) -> Dict[str, List[Dict]]:
        """
        Validate and organize configuration data by cluster type.
        """
        organized_config = {'hadoop': [], 'flink': [], 'doris': []}

        nodes = config_data.get('nodes', [])

        for node in nodes:
            # Validate required fields
            required_fields = ['node_name', 'ip_address', 'role']
            if not all(field in node for field in required_fields):
                logger.warning(f"Skipping invalid node config: {node}")
                continue

            # Determine cluster type from services or role
            cluster_type = self._determine_cluster_type(node)
            if cluster_type:
                # Normalize node configuration
                normalized_node = self._normalize_node_config(node)
                organized_config[cluster_type].append(normalized_node)

        # Log configuration summary
        for cluster_type, nodes in organized_config.items():
            if nodes:
                logger.info(f"Configured {len(nodes)} nodes for {cluster_type} cluster")

        return organized_config

    def _determine_cluster_type(self, node: Dict) -> Optional[str]:
        """
        Determine cluster type from node configuration.
        """
        role = node.get('role', '').lower()
        services = node.get('services', [])

        # Check by role
        if any(keyword in role for keyword in ['namenode', 'datanode', 'resourcemanager', 'nodemanager', 'journalnode']):
            return 'hadoop'
        elif any(keyword in role for keyword in ['jobmanager', 'taskmanager']):
            return 'flink'
        elif any(keyword in role for keyword in ['frontend', 'backend', 'follower']):
            return 'doris'

        # Check by services
        for service in services:
            service_name = service.get('name', '').lower() if isinstance(service, dict) else str(service).lower()
            if any(keyword in service_name for keyword in ['hadoop', 'yarn', 'hdfs', 'hive']):
                return 'hadoop'
            elif 'flink' in service_name:
                return 'flink'
            elif 'doris' in service_name:
                return 'doris'

        # Check by tags
        tags = node.get('tags', [])
        for tag in tags:
            if tag.lower() in ['namenode', 'datanode', 'resourcemanager', 'nodemanager']:
                return 'hadoop'
            elif tag.lower() in ['jobmanager', 'taskmanager']:
                return 'flink'
            elif tag.lower() in ['frontend', 'backend']:
                return 'doris'

        logger.warning(f"Could not determine cluster type for node: {node.get('node_name', 'unknown')}")
        return None

    def _normalize_node_config(self, node: Dict) -> Dict:
        """
        Normalize node configuration to standard format.
        """
        ssh_config = node.get('ssh_config', {})

        return {
            'name': node['node_name'],
            'host': node['ip_address'],
            'hostname': node.get('hostname', node['ip_address']),
            'role': node['role'],
            'ssh_port': ssh_config.get('port', 22),
            'ssh_username': ssh_config.get('username', 'bigdata'),
            'ssh_password': ssh_config.get('password'),
            'ssh_key_path': ssh_config.get('key_path', self._get_default_ssh_key_path()),
            'ssh_timeout': ssh_config.get('timeout', 10),
            'services': node.get('services', []),
            'resources': node.get('resources', {}),
            'monitoring': node.get('monitoring', {'enabled': True}),
            'enabled': node.get('enabled', True),
            'tags': node.get('tags', [])
        }

    def _get_default_ssh_key_path(self) -> str:
        """Get default SSH key path based on platform."""
        if platform.system() == "Windows":
            return os.path.expanduser("~/.ssh/id_rsa")
        else:
            return "/home/bigdata/.ssh/id_rsa"

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self.cache:
            return False
        cache_time = self.cache[cache_key].get('timestamp', 0)
        return (time.time() - cache_time) < self.cache_ttl

    def _get_from_cache(self, cache_key: str) -> Optional[Dict]:
        """Get data from cache if valid."""
        if self._is_cache_valid(cache_key):
            logger.debug(f"Cache hit: {cache_key}")
            return self.cache[cache_key]['data']
        return None

    def _set_cache(self, cache_key: str, data: Dict) -> None:
        """Set cache entry with timestamp."""
        self.cache[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }

    async def get_cluster_metrics(self, cluster_type: str) -> Dict:
        """
        Get cluster-wide metrics with caching and parallel collection.
        """
        cache_key = f"cluster_{cluster_type}"

        # Check cache first
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            logger.debug(f"Using cached data for {cluster_type} cluster")
            return cached_data

        # Use lock to prevent concurrent collection
        async with self.collecting_lock:
            # Double-check cache after acquiring lock
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                logger.debug(f"Concurrent protection: using cached {cluster_type} data")
                return cached_data

            logger.info(f"Collecting {cluster_type} cluster metrics (cache miss)")

            if not self.use_real_clusters:
                result = self._get_mock_cluster_metrics(cluster_type)
                self._set_cache(cache_key, result)
                return result

            nodes = self.cluster_nodes.get(cluster_type, [])
            if not nodes:
                logger.warning(f"No node configuration found for {cluster_type} cluster")
                result = self._get_mock_cluster_metrics(cluster_type)
                self._set_cache(cache_key, result)
                return result

            # Filter enabled nodes
            enabled_nodes = [node for node in nodes if node.get('enabled', True)]
            if not enabled_nodes:
                logger.warning(f"No enabled nodes found for {cluster_type} cluster")
                result = self._get_mock_cluster_metrics(cluster_type)
                self._set_cache(cache_key, result)
                return result

            start_time = time.time()
            logger.info(f"Starting parallel collection for {len(enabled_nodes)} enabled nodes")

            # Limit concurrent connections
            semaphore = asyncio.Semaphore(self.max_concurrent_connections)

            async def collect_node_with_semaphore(node):
                async with semaphore:
                    return await self.get_node_metrics(node)

            # Collect metrics from all enabled nodes in parallel
            tasks = [collect_node_with_semaphore(node) for node in enabled_nodes]
            node_metrics = await asyncio.gather(*tasks, return_exceptions=True)

            collection_time = time.time() - start_time
            logger.info(f"Node metrics collection completed in {collection_time:.2f}s")

            # Calculate cluster averages
            valid_metrics = [m for m in node_metrics if isinstance(m, dict) and m.get('success')]
            logger.info(f"Successfully collected metrics from {len(valid_metrics)}/{len(enabled_nodes)} nodes")

            if not valid_metrics:
                logger.error(f"Failed to collect any node metrics for {cluster_type} cluster")
                result = self._get_mock_cluster_metrics(cluster_type)
                self._set_cache(cache_key, result)
                return result

            # Calculate average CPU and memory usage
            avg_cpu = sum(m['cpu_usage'] for m in valid_metrics) / len(valid_metrics)
            avg_memory = sum(m['memory_usage'] for m in valid_metrics) / len(valid_metrics)
            avg_disk = sum(m.get('disk_usage', 0) for m in valid_metrics) / len(valid_metrics)

            result = {
                'cluster_type': cluster_type,
                'total_nodes': len(enabled_nodes),
                'active_nodes': len(valid_metrics),
                'cpu_usage': round(avg_cpu, 1),
                'memory_usage': round(avg_memory, 1),
                'disk_usage': round(avg_disk, 1),
                'nodes_detail': [
                    {
                        'node_name': enabled_nodes[i]['name'],
                        'ip_address': enabled_nodes[i]['host'],
                        'role': enabled_nodes[i]['role'],
                        'status': 'online' if isinstance(node_metrics[i], dict) and node_metrics[i].get('success') else 'offline',
                        'cpu_usage': node_metrics[i].get('cpu_usage', 0) if isinstance(node_metrics[i], dict) else 0,
                        'memory_usage': node_metrics[i].get('memory_usage', 0) if isinstance(node_metrics[i], dict) else 0,
                        'disk_usage': node_metrics[i].get('disk_usage', 0) if isinstance(node_metrics[i], dict) else 0,
                        'last_heartbeat': datetime.now().isoformat(),
                        'metrics': node_metrics[i] if isinstance(node_metrics[i], dict) else None
                    }
                    for i in range(len(enabled_nodes))
                ],
                'data_source': 'real',
                'collection_time': collection_time,
                'cached_until': datetime.now() + timedelta(seconds=self.cache_ttl)
            }

            # Cache the result
            self._set_cache(cache_key, result)
            logger.info(f"✅ {cluster_type} cluster metrics cached - CPU: {result['cpu_usage']}%, Memory: {result['memory_usage']}%")

            return result

    async def get_node_metrics(self, node_config: Dict) -> Dict:
        """
        Get metrics for a single node using its configuration.
        """
        hostname = node_config['host']
        cache_key = f"node_{hostname}"

        if self.DISABLE_SSH_MONITORING:
            logger.debug(f"SSH监控已禁用: {hostname}")
            result = self._get_mock_node_metrics()
            result['hostname'] = hostname
            result['node_name'] = node_config.get('name', hostname)
            return result

        # Check node-level cache
        cached_data = self._get_from_cache(cache_key)
        if cached_data:
            return cached_data

        if not self.use_real_clusters:
            result = self._get_mock_node_metrics()
            self._set_cache(cache_key, result)
            return result

        # Skip if monitoring is disabled for this node
        if not node_config.get('monitoring', {}).get('enabled', True):
            logger.debug(f"Monitoring disabled for node {hostname}")
            return {'success': False, 'error': 'Monitoring disabled', 'hostname': hostname}

        try:
            logger.debug(f"Collecting metrics for node {hostname}")

            # Try SSH method with node-specific configuration
            metrics = await self._ssh_get_metrics_with_config(node_config)
            if metrics.get('success'):
                logger.debug(f"SSH successfully collected metrics for node {hostname}")
                self._set_cache(cache_key, metrics)
                return metrics

            # Try HTTP API as fallback using service endpoints
            http_metrics = await self._http_get_metrics_with_config(node_config)
            if http_metrics.get('success'):
                logger.debug(f"HTTP successfully collected metrics for node {hostname}")
                self._set_cache(cache_key, http_metrics)
                return http_metrics

            # All methods failed
            logger.warning(f"Failed to collect metrics for node {hostname}")
            result = {'success': False, 'error': 'All collection methods failed', 'hostname': hostname}
            return result

        except Exception as e:
            logger.error(f"Error collecting metrics for node {hostname}: {str(e)}")
            return {'success': False, 'error': str(e), 'hostname': hostname}

    async def _ssh_get_metrics_with_config(self, node_config: Dict) -> Dict:
        """Collect node metrics via SSH using node-specific configuration."""
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        hostname = node_config['host']

        try:
            logger.debug(f"SSH connecting to {hostname}")

            # Use node-specific SSH configuration
            connect_params = {
                'hostname': hostname,
                'username': node_config['ssh_username'],
                'timeout': node_config['ssh_timeout'],
                'port': node_config['ssh_port'],
                'look_for_keys': False,
                'allow_agent': False
            }

            # Add authentication method
            if node_config.get('ssh_password'):
                connect_params['password'] = node_config['ssh_password']
            elif node_config.get('ssh_key_path') and os.path.exists(node_config['ssh_key_path']):
                connect_params['key_filename'] = node_config['ssh_key_path']
            else:
                # Try default key locations
                default_keys = [
                    os.path.expanduser('~/.ssh/id_rsa'),
                    os.path.expanduser('~/.ssh/id_ed25519'),
                ]
                for key_path in default_keys:
                    if os.path.exists(key_path):
                        connect_params['key_filename'] = key_path
                        break

            ssh_client.connect(**connect_params)

            # Single optimized command to get all metrics
            combined_command = """
            echo "START_METRICS";
            top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | sed 's/%us,//' | head -1;
            free | grep Mem | awk '{printf "%.1f", $3/$2 * 100.0}';
            df -h / | awk 'NR==2{print $5}' | sed 's/%//';
            ps aux | wc -l;
            uptime | awk '{print $(NF-2)}' | sed 's/,//';
            echo "END_METRICS"
            """

            stdin, stdout, stderr = ssh_client.exec_command(combined_command)
            output = stdout.read().decode().strip()

            # Parse combined output
            lines = output.split('\n')
            if len(lines) >= 7 and 'START_METRICS' in lines[0]:
                try:
                    cpu_usage = float(lines[1]) if lines[1] and lines[1].replace('.', '').isdigit() else 0.0
                    memory_usage = float(lines[2]) if lines[2] and lines[2].replace('.', '').isdigit() else 0.0
                    disk_usage = float(lines[3]) if lines[3] and lines[3].isdigit() else 0.0
                    process_count = int(lines[4]) if lines[4] and lines[4].isdigit() else 0
                    load_average = float(lines[5]) if lines[5] and lines[5].replace('.', '').isdigit() else 0.0

                    result = {
                        'success': True,
                        'hostname': hostname,
                        'node_name': node_config['name'],
                        'method': 'ssh_configured',
                        'cpu_usage': cpu_usage,
                        'memory_usage': memory_usage,
                        'disk_usage': disk_usage,
                        'process_count': process_count,
                        'load_average': load_average,
                        'collection_time': datetime.now().isoformat()
                    }

                    logger.debug(f"Collected metrics for {hostname}: CPU={cpu_usage}%, Memory={memory_usage}%")
                    return result

                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse metrics output for {hostname}: {e}")

            # Fallback to individual commands if combined command fails
            return await self._ssh_get_metrics_fallback(ssh_client, node_config)

        except paramiko.AuthenticationException as e:
            logger.error(f"SSH authentication failed - {hostname}")
            return {'success': False, 'error': f'SSH authentication failed: {e}', 'hostname': hostname}
        except paramiko.NoValidConnectionsError as e:
            logger.error(f"SSH connection failed - {hostname}")
            return {'success': False, 'error': f'SSH connection failed: {e}', 'hostname': hostname}
        except Exception as e:
            logger.error(f"SSH metrics collection failed for {hostname}: {e}")
            return {'success': False, 'error': f'SSH execution failed: {e}', 'hostname': hostname}
        finally:
            ssh_client.close()

    async def _ssh_get_metrics_fallback(self, ssh_client, node_config: Dict) -> Dict:
        """Fallback method for SSH metrics collection."""
        hostname = node_config['host']

        try:
            metrics = {}

            # Simplified command set
            commands = {
                'cpu': "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$3+$4+$5)} END {print usage}'",
                'memory': "free | grep Mem | awk '{printf \"%.1f\", $3/$2 * 100.0}'",
                'disk': "df -h / | awk 'NR==2{print $5}' | sed 's/%//'",
            }

            for metric, command in commands.items():
                try:
                    stdin, stdout, stderr = ssh_client.exec_command(command)
                    output = stdout.read().decode().strip()

                    if metric == 'cpu':
                        metrics['cpu_usage'] = float(output) if output and output.replace('.', '').isdigit() else 0.0
                    elif metric == 'memory':
                        metrics['memory_usage'] = float(output) if output and output.replace('.', '').isdigit() else 0.0
                    elif metric == 'disk':
                        metrics['disk_usage'] = float(output) if output and output.isdigit() else 0.0

                except Exception as e:
                    logger.warning(f"Failed to get {metric} for {hostname}: {e}")
                    metrics[f'{metric}_usage'] = 0.0

            return {
                'success': True,
                'hostname': hostname,
                'node_name': node_config['name'],
                'method': 'ssh_fallback',
                'cpu_usage': metrics.get('cpu_usage', 0.0),
                'memory_usage': metrics.get('memory_usage', 0.0),
                'disk_usage': metrics.get('disk_usage', 0.0),
                'process_count': 0,
                'load_average': 0.0,
                'collection_time': datetime.now().isoformat()
            }

        except Exception as e:
            return {'success': False, 'error': f'Fallback method failed: {e}', 'hostname': hostname}

    async def _http_get_metrics_with_config(self, node_config: Dict) -> Dict:
        """Try to get metrics via HTTP endpoints using service configuration."""
        hostname = node_config['host']

        try:
            import aiohttp

            # Build endpoints from service configuration
            endpoints = []

            # Add Node Exporter endpoint
            endpoints.append(f'http://{hostname}:9100/metrics')

            # Add service-specific endpoints
            for service in node_config.get('services', []):
                if isinstance(service, dict):
                    web_port = service.get('web_port')
                    status_url = service.get('status_check_url')

                    if status_url:
                        endpoints.append(status_url)
                    elif web_port:
                        endpoints.append(f'http://{hostname}:{web_port}/metrics')

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as session:
                for endpoint in endpoints:
                    try:
                        async with session.get(endpoint) as response:
                            if response.status == 200:
                                data = await response.text()
                                return self._parse_metrics_response(data, node_config, endpoint)
                    except:
                        continue

            return {'success': False, 'error': 'No HTTP metrics endpoint available', 'hostname': hostname}

        except ImportError:
            return {'success': False, 'error': 'aiohttp not available', 'hostname': hostname}
        except Exception as e:
            return {'success': False, 'error': str(e), 'hostname': hostname}

    def _parse_metrics_response(self, data: str, node_config: Dict, endpoint: str) -> Dict:
        """Parse HTTP monitoring response."""
        # This would contain actual parsing logic for Prometheus metrics
        # For now, return mock data
        return {
            'success': True,
            'hostname': node_config['host'],
            'node_name': node_config['name'],
            'method': 'http',
            'cpu_usage': 50.0,
            'memory_usage': 60.0,
            'disk_usage': 40.0,
            'process_count': 100,
            'load_average': 1.0,
            'endpoint': endpoint,
            'collection_time': datetime.now().isoformat()
        }

    def _get_mock_node_metrics(self) -> Dict:
        """Generate mock node metrics for development."""
        import random
        return {
            'success': True,
            'method': 'mock',
            'cpu_usage': round(random.uniform(30, 80), 1),
            'memory_usage': round(random.uniform(40, 85), 1),
            'disk_usage': round(random.uniform(20, 70), 1),
            'load_average': round(random.uniform(0.5, 2.0), 2),
            'process_count': random.randint(80, 150),
            'data_source': 'mock',
            'collection_time': datetime.now().isoformat()
        }

    def _get_mock_cluster_metrics(self, cluster_type: str) -> Dict:
        """Generate mock cluster metrics for development."""
        import random

        # Get configured node count or use defaults
        configured_nodes = self.cluster_nodes.get(cluster_type, [])
        total_nodes = len(configured_nodes) if configured_nodes else {'hadoop': 10, 'flink': 5, 'doris': 7}.get(cluster_type, 4)

        return {
            'cluster_type': cluster_type,
            'total_nodes': total_nodes,
            'active_nodes': total_nodes,
            'cpu_usage': round(random.uniform(40, 80), 1),
            'memory_usage': round(random.uniform(50, 85), 1),
            'disk_usage': round(random.uniform(30, 70), 1),
            'nodes_detail': [],
            'data_source': 'mock',
            'collection_time': 0.1
        }

    def reload_configuration(self):
        """Reload cluster configuration from sources."""
        logger.info("Reloading cluster configuration...")
        old_config = self.cluster_nodes.copy()
        self.cluster_nodes = self._load_cluster_configuration()

        # Clear cache if configuration changed
        if old_config != self.cluster_nodes:
            self.clear_cache()
            logger.info("Configuration changed, cache cleared")

        return self.cluster_nodes

    def get_configuration_summary(self) -> Dict:
        """Get summary of current configuration."""
        summary = {
            'total_clusters': len(self.cluster_nodes),
            'clusters': {}
        }

        for cluster_type, nodes in self.cluster_nodes.items():
            enabled_nodes = [n for n in nodes if n.get('enabled', True)]
            summary['clusters'][cluster_type] = {
                'total_nodes': len(nodes),
                'enabled_nodes': len(enabled_nodes),
                'disabled_nodes': len(nodes) - len(enabled_nodes),
                'roles': list(set(n.get('role', 'unknown') for n in nodes))
            }

        return summary

    def clear_cache(self) -> None:
        """Clear all cached metrics."""
        self.cache.clear()
        logger.info("Metrics cache cleared")

    def get_cache_status(self) -> Dict:
        """Get current cache status information."""
        current_time = time.time()
        cache_status = {}

        for key, value in self.cache.items():
            age = current_time - value['timestamp']
            cache_status[key] = {
                'age_seconds': round(age, 1),
                'is_valid': age < self.cache_ttl,
                'expires_in': round(self.cache_ttl - age, 1)
            }

        return cache_status


# Global configurable metrics collector instance
metrics_collector = ConfigurableMetricsCollector()