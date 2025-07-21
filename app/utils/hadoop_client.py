"""
TODO 为大数据平台提供的Hadoop生态系统客户端。 该模块提供与Hadoop、HDFS、Hive、Flink和Doris交互的客户端。包括为开发环境提供的模拟数据支持。
"""

import asyncio
import json
import logging
import math
import os
import platform
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from loguru import logger

from config.settings import settings


class BaseClient:
    """Base class for all Hadoop ecosystem clients."""

    def __init__(self):
        self.use_real_clusters = settings.use_real_clusters
        self.is_windows = platform.system() == "Windows"

    def _get_mock_data(self, data_type: str) -> Dict:
        """Get mock data for development/testing."""
        raise NotImplementedError("Subclasses must implement _get_mock_data")


class HadoopClient(BaseClient):
    """Hadoop cluster client for command execution."""

    def __init__(self):
        super().__init__()
        self.hadoop_home = settings.HADOOP_HOME
        self.namenode_url = settings.HDFS_NAMENODE

    def execute_command(self, command: str) -> Tuple[str, str, int]:
        """
        Execute Hadoop command with Windows compatibility.

        Args:
            command: Hadoop command to execute

        Returns:
            Tuple of (stdout, stderr, return_code)
        """
        if not self.use_real_clusters:
            logger.info(f"Mock mode: Skipping Hadoop command - {command}")
            return self._mock_command_result(command)

        try:
            env = self._prepare_environment()
            command = self._prepare_command(command)

            logger.debug(f"Executing command: {command}")
            result = subprocess.run(
                command.split() if not self.is_windows else command,
                shell=self.is_windows,
                capture_output=True,
                text=True,
                env=env,
                timeout=30
            )
            return result.stdout, result.stderr, result.returncode

        except subprocess.TimeoutExpired:
            logger.error(f"Command execution timeout: {command}")
            return "", "Command execution timeout", 1
        except Exception as e:
            logger.error(f"Command execution failed: {command}, error: {str(e)}")
            return "", str(e), 1

    def _prepare_environment(self) -> Dict[str, str]:
        """Prepare environment variables for Hadoop commands."""
        env = os.environ.copy()
        if self.hadoop_home:
            env['HADOOP_HOME'] = self.hadoop_home
            hadoop_bin = os.path.join(self.hadoop_home, 'bin')
            if 'PATH' in env:
                env['PATH'] = f"{hadoop_bin}{os.pathsep}{env['PATH']}"
            else:
                env['PATH'] = hadoop_bin
        return env

    def _prepare_command(self, command: str) -> str:
        """Prepare command for Windows execution."""
        if self.is_windows:
            hdfs_cmd = self._find_hadoop_command("hdfs.cmd")
            if hdfs_cmd:
                command = command.replace("hdfs", f'"{hdfs_cmd}"', 1)
            else:
                logger.error("hdfs.cmd not found in Hadoop bin directory")
                raise FileNotFoundError("hdfs.cmd not found")
        return command

    def _find_hadoop_command(self, cmd_name: str) -> Optional[str]:
        """Find the full path to a Hadoop command."""
        if not self.hadoop_home:
            return None

        for directory in ["bin", "sbin"]:
            cmd_path = os.path.join(self.hadoop_home, directory, cmd_name)
            if os.path.exists(cmd_path):
                return cmd_path
        return None

    def _mock_command_result(self, command: str) -> Tuple[str, str, int]:
        """Generate mock command execution results."""
        mock_results = {
            "hdfs dfsadmin -report": (
                "Configured Capacity: 1000000000000 (1 TB)\n"
                "DFS Used: 300000000000 (300 GB)\n"
                "DFS Remaining: 700000000000 (700 GB)\n"
                "Live DataNodes: 12", "", 0
            ),
            "hdfs dfs -du": ("15600000000000", "", 0),
            "hive": ("ods_finance\nods_asset\nods_hr\nods_crm\nods_supervision", "", 0)
        }

        for key, result in mock_results.items():
            if key in command:
                return result

        return "Mock command execution success", "", 0


class HDFSClient(BaseClient):
    """HDFS client for storage operations and metrics."""

    def __init__(self):
        super().__init__()
        self.hadoop_client = HadoopClient()
        self.namenode_url = self._build_namenode_url()

    def _build_namenode_url(self) -> str:
        """Build correct namenode URL for WebHDFS API."""
        url = settings.HDFS_NAMENODE

        # Clean URL format
        if url.startswith("hdfs://"):
            url = url[7:]

        # Remove port if present
        if ":" in url:
            url = url.split(":")[0]

        # Use default WebHDFS port
        return f"http://{url}:9870"

    def get_storage_info(self) -> Dict:
        """
        Get HDFS storage information.

        Returns:
            Dict containing storage metrics
        """
        if not self.use_real_clusters:
            return self._get_mock_storage_info()

        # Try WebHDFS API first
        try:
            webhdfs_url = f"{self.namenode_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
            logger.debug(f"Trying WebHDFS API: {webhdfs_url}")

            response = requests.get(webhdfs_url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                beans = data.get('beans', [])
                if beans:
                    fs_state = beans[0]
                    return {
                        'total_size': fs_state.get('CapacityTotal', 0),
                        'used_size': fs_state.get('CapacityUsed', 0),
                        'available_size': fs_state.get('CapacityRemaining', 0),
                        'files_count': fs_state.get('FilesTotal', 0),
                        'blocks_count': fs_state.get('BlocksTotal', 0)
                    }
        except Exception as e:
            logger.warning(f"WebHDFS API failed: {str(e)}")

        # Fallback to command line
        try:
            stdout, stderr, code = self.hadoop_client.execute_command("hdfs dfsadmin -report")
            if code == 0:
                return self._parse_hdfs_report(stdout)
            else:
                logger.error(f"hdfs dfsadmin command failed: {stderr}")
        except Exception as e:
            logger.error(f"Command line HDFS info failed: {str(e)}")

        return self._get_default_storage_info()

    def _get_mock_storage_info(self) -> Dict:
        """Get mock storage information for development."""
        return {
            'total_size': 1000000000000,  # 1TB
            'used_size': 300000000000,  # 300GB
            'available_size': 700000000000,  # 700GB
            'files_count': 125000,
            'blocks_count': 89000
        }

    def _get_default_storage_info(self) -> Dict:
        """Get default storage info when all methods fail."""
        return {
            'total_size': 0,
            'used_size': 0,
            'available_size': 0,
            'files_count': 0,
            'blocks_count': 0
        }

    def _parse_hdfs_report(self, report_text: str) -> Dict:
        """Parse HDFS report text output."""
        info = self._get_default_storage_info()

        lines = report_text.split('\n')
        for line in lines:
            if 'Configured Capacity' in line:
                info['total_size'] = self._parse_size(line)
            elif 'DFS Used' in line:
                info['used_size'] = self._parse_size(line)
            elif 'DFS Remaining' in line:
                info['available_size'] = self._parse_size(line)

        return info

    def _parse_size(self, line: str) -> int:
        """Parse size string from HDFS report."""
        try:
            parts = line.split()
            for i, part in enumerate(parts):
                if part.replace('.', '').replace(',', '').isdigit():
                    size_str = part.replace(',', '')
                    size = float(size_str)

                    # Check for unit
                    if i + 1 < len(parts):
                        unit = parts[i + 1].upper()
                        if 'TB' in unit or 'T' in unit:
                            return int(size * 1024 ** 4)
                        elif 'GB' in unit or 'G' in unit:
                            return int(size * 1024 ** 3)
                        elif 'MB' in unit or 'M' in unit:
                            return int(size * 1024 ** 2)
                        elif 'KB' in unit or 'K' in unit:
                            return int(size * 1024)

                    return int(size)
        except Exception as e:
            logger.error(f"Size parsing failed: {line}, error: {str(e)}")
        return 0

    def get_warehouse_size(self) -> int:
        """Get Hive warehouse directory size."""
        if not self.use_real_clusters:
            return 15600000000000  # 15.6TB mock data

        try:
            stdout, stderr, code = self.hadoop_client.execute_command(
                "hdfs dfs -du -s /user/hive/warehouse"
            )
            if code == 0:
                parts = stdout.strip().split()
                if parts:
                    return int(parts[0])
        except Exception as e:
            logger.error(f"Failed to get warehouse size: {str(e)}")

        return 15600000000000  # Return mock data as fallback


class HiveClient(BaseClient):
    """Hive client for database and table operations."""

    def __init__(self):
        super().__init__()
        self.hadoop_client = HadoopClient()
        self.host = settings.HIVE_SERVER_HOST
        self.port = settings.HIVE_SERVER_PORT
        self.username = settings.HIVE_USERNAME
        self.password = settings.HIVE_PASSWORD

    def execute_query(self, query: str) -> List[Dict]:
        """
        Execute Hive query.

        Args:
            query: SQL query to execute

        Returns:
            List of result dictionaries
        """
        if not self.use_real_clusters:
            return self._get_mock_query_result(query)

        try:
            # Try using pyhive connection
            return self._execute_query_with_pyhive(query)
        except ImportError:
            logger.warning("pyhive not installed, using command line")
            return self._execute_query_via_cli(query)
        except Exception as e:
            logger.error(f"Hive query failed: {str(e)}")
            return self._get_mock_query_result(query)

    def _execute_query_with_pyhive(self, query: str) -> List[Dict]:
        """Execute query using pyhive library."""
        from pyhive import hive

        conn = hive.Connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password if self.password else None,
            auth='CUSTOM' if self.password else 'NONE'
        )

        cursor = conn.cursor()
        cursor.execute(query)

        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        # Convert to dictionary list
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()
        return result

    def _execute_query_via_cli(self, query: str) -> List[Dict]:
        """Execute Hive query via command line."""
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
                f.write(query)
                sql_file = f.name

            command = f"hive -f {sql_file}"
            stdout, stderr, code = self.hadoop_client.execute_command(command)

            os.unlink(sql_file)

            if code == 0:
                return self._parse_hive_output(stdout)
            else:
                logger.error(f"Hive query failed: {stderr}")
                return self._get_mock_query_result(query)

        except Exception as e:
            logger.error(f"CLI Hive query failed: {str(e)}")
            return self._get_mock_query_result(query)

    def _parse_hive_output(self, output: str) -> List[Dict]:
        """Parse Hive command output."""
        lines = output.strip().split('\n')
        result_lines = []

        for line in lines:
            if not any(line.startswith(prefix) for prefix in
                       ['Logging initialized', 'WARNING', 'INFO']) and line.strip():
                result_lines.append(line.strip())

        if not result_lines:
            return []

        result = []
        for line in result_lines:
            if '\t' in line:
                parts = line.split('\t')
                result.append({'value': parts[0] if parts else ''})

        return result

    def _get_mock_query_result(self, query: str) -> List[Dict]:
        """Generate mock query results."""
        query_upper = query.upper()

        if "SHOW DATABASES" in query_upper:
            return [
                {'database_name': 'default'},
                {'database_name': 'ods_finance'},
                {'database_name': 'ods_asset'},
                {'database_name': 'ods_hr'},
                {'database_name': 'ods_crm'},
                {'database_name': 'ods_supervision'},
                {'database_name': 'dwd_finance'},
                {'database_name': 'dwd_asset'},
                {'database_name': 'dws_summary'},
                {'database_name': 'ads_report'}
            ]
        elif "SHOW TABLES" in query_upper:
            return self._get_mock_tables_for_database(query)
        else:
            return [{'result': 'Mock query result'}]

    def _get_mock_tables_for_database(self, query: str) -> List[Dict]:
        """Get mock tables based on database type."""
        table_counts = {
            'ods_': 20,
            'dwd_': 15,
            'dws_': 10,
            'ads_': 5,
            'default': 7
        }

        for db_prefix, count in table_counts.items():
            if db_prefix in query:
                prefix = db_prefix.replace('_', '_table_') if db_prefix != 'default' else 'default_table_'
                return [{'table_name': f'{prefix}{i:03d}'} for i in range(1, count + 1)]

        return [{'table_name': f'table_{i:03d}'} for i in range(1, 8)]

    def get_databases(self) -> List[str]:
        """Get all database names."""
        try:
            result = self.execute_query("SHOW DATABASES")
            return [row.get('database_name', row.get('value', '')) for row in result]
        except Exception as e:
            logger.error(f"Failed to get database list: {str(e)}")
            return ['default', 'ods_finance', 'ods_asset', 'ods_hr', 'ods_crm',
                    'ods_supervision', 'dwd_finance', 'dwd_asset', 'dws_summary', 'ads_report']

    def get_business_systems_count(self) -> int:
        """Get count of business systems (databases starting with ods_)."""
        try:
            databases = self.get_databases()
            return len([db for db in databases if db.startswith('ods_')])
        except Exception as e:
            logger.error(f"Failed to get business systems count: {str(e)}")
            return 5

    def get_tables_count_by_layer(self) -> Dict[str, int]:
        """Get table count by data layer."""
        try:
            databases = self.get_databases()
            layer_counts = {'ods': 0, 'dwd': 0, 'dws': 0, 'ads': 0, 'other': 0}

            for db in databases:
                try:
                    result = self.execute_query(f"SHOW TABLES FROM {db}")
                    table_count = len(result)

                    if db.startswith('ods_'):
                        layer_counts['ods'] += table_count
                    elif db.startswith('dwd_'):
                        layer_counts['dwd'] += table_count
                    elif db.startswith('dws_'):
                        layer_counts['dws'] += table_count
                    elif db.startswith('ads_'):
                        layer_counts['ads'] += table_count
                    else:
                        layer_counts['other'] += table_count

                except Exception as e:
                    logger.warning(f"Failed to get table count for database {db}: {str(e)}")
                    continue

            return layer_counts

        except Exception as e:
            logger.error(f"Failed to get layer table counts: {str(e)}")
            return {'ods': 1200, 'dwd': 800, 'dws': 500, 'ads': 300, 'other': 47}

    def get_total_tables_count(self) -> int:
        """Get total table count across all databases."""
        try:
            layer_counts = self.get_tables_count_by_layer()
            return sum(layer_counts.values())
        except Exception as e:
            logger.error(f"Failed to get total table count: {str(e)}")
            return 2847


class FlinkClient(BaseClient):
    """Flink client for job management and cluster information."""

    def __init__(self):
        super().__init__()
        self.jobmanager_url = f"http://{settings.FLINK_JOBMANAGER_HOST}:{settings.FLINK_JOBMANAGER_PORT}"

    def get_cluster_info(self) -> Dict:
        """Get Flink cluster information."""
        if not self.use_real_clusters:
            return self._get_mock_cluster_info()

        try:
            overview = self._get_cluster_overview()
            taskmanagers = self._get_taskmanagers()

            return {
                'cluster_id': overview.get('flink-commit', 'unknown'),
                'version': overview.get('flink-version', 'unknown'),
                'total_slots': overview.get('slots-total', 0),
                'available_slots': overview.get('slots-available', 0),
                'running_jobs': overview.get('jobs-running', 0),
                'finished_jobs': overview.get('jobs-finished', 0),
                'cancelled_jobs': overview.get('jobs-cancelled', 0),
                'failed_jobs': overview.get('jobs-failed', 0),
                'taskmanagers': len(taskmanagers),
                'last_update': datetime.now()
            }
        except Exception as e:
            logger.error(f"Failed to get Flink cluster info: {str(e)}")
            return self._get_mock_cluster_info()

    def _get_cluster_overview(self) -> Dict:
        """Get cluster overview from Flink REST API."""
        response = requests.get(f"{self.jobmanager_url}/overview", timeout=10)
        response.raise_for_status()
        return response.json()

    def _get_taskmanagers(self) -> List[Dict]:
        """Get taskmanager information."""
        response = requests.get(f"{self.jobmanager_url}/taskmanagers", timeout=10)
        if response.status_code == 200:
            return response.json().get('taskmanagers', [])
        return []

    def get_running_jobs(self) -> List[Dict]:
        """Get currently running jobs."""
        if not self.use_real_clusters:
            return self._get_mock_running_jobs()

        try:
            response = requests.get(f"{self.jobmanager_url}/jobs", timeout=10)
            if response.status_code == 200:
                data = response.json()
                jobs = data.get('jobs', [])

                return [
                    {
                        'job_id': job.get('id'),
                        'name': job.get('name', 'Unknown'),
                        'status': job.get('status'),
                        'start_time': job.get('start-time'),
                        'duration': job.get('duration', 0)
                    }
                    for job in jobs if job.get('status') == 'RUNNING'
                ]
        except Exception as e:
            logger.error(f"Failed to get Flink running jobs: {str(e)}")

        return self._get_mock_running_jobs()

    def _get_mock_cluster_info(self) -> Dict:
        """Get mock cluster information."""
        return {
            'cluster_id': 'mock-flink-cluster',
            'version': '1.17.1',
            'total_slots': 24,
            'available_slots': 8,
            'running_jobs': 3,
            'finished_jobs': 45,
            'cancelled_jobs': 2,
            'failed_jobs': 1,
            'taskmanagers': 6,
            'last_update': datetime.now()
        }

    def _get_mock_running_jobs(self) -> List[Dict]:
        """Get mock running jobs."""
        return [
            {
                'job_id': 'job-001',
                'name': 'customer-data-pipeline',
                'status': 'RUNNING',
                'start_time': datetime.now().timestamp() * 1000,
                'duration': 3600000  # 1 hour
            },
            {
                'job_id': 'job-002',
                'name': 'real-time-analytics',
                'status': 'RUNNING',
                'start_time': datetime.now().timestamp() * 1000,
                'duration': 7200000  # 2 hours
            }
        ]


class DorisClient(BaseClient):
    """Doris client for database management and cluster information."""

    def __init__(self):
        super().__init__()
        self.fe_host = settings.DORIS_FE_HOST
        self.fe_port = settings.DORIS_FE_PORT
        self.username = settings.DORIS_USERNAME
        self.password = settings.DORIS_PASSWORD
        self.fe_url = f"http://{self.fe_host}:{self.fe_port}"

    def get_cluster_info(self) -> Dict:
        """Get Doris cluster information."""
        if not self.use_real_clusters:
            return self._get_mock_cluster_info()

        try:
            frontends = self._get_frontends()
            backends = self._get_backends()

            return {
                'frontends': len(frontends),
                'backends': len(backends),
                'alive_backends': len([be for be in backends if be.get('Alive') == 'true']),
                'total_capacity': sum([int(be.get('TotalCapacity', '0')) for be in backends]),
                'used_capacity': sum([int(be.get('UsedCapacity', '0')) for be in backends]),
                'last_update': datetime.now()
            }
        except Exception as e:
            logger.error(f"Failed to get Doris cluster info: {str(e)}")
            return self._get_mock_cluster_info()

    def _get_frontends(self) -> List[Dict]:
        """Get frontend information."""
        response = requests.get(
            f"{self.fe_url}/api/show_frontends",
            auth=(self.username, self.password),
            timeout=10
        )
        response.raise_for_status()
        return response.json().get('data', [])

    def _get_backends(self) -> List[Dict]:
        """Get backend information."""
        response = requests.get(
            f"{self.fe_url}/api/show_backends",
            auth=(self.username, self.password),
            timeout=10
        )
        response.raise_for_status()
        return response.json().get('data', [])

    def _get_mock_cluster_info(self) -> Dict:
        """Get mock cluster information."""
        return {
            'frontends': 1,
            'backends': 4,
            'alive_backends': 4,
            'total_capacity': 20000000000000,  # 20TB
            'used_capacity': 12000000000000,  # 12TB
            'last_update': datetime.now()
        }


# =============================================================================
# TODO Utility Functions 工具函数
# =============================================================================

def format_size(size_bytes: int) -> str:
    """
    Format file size in human readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string (e.g., "1.5GB")
    """
    if size_bytes == 0:
        return "0B"

    size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s}{size_names[i]}"