"""
TODO 大数据平台的集群管理服务。 该服务处理与集群相关的操作，包括集群健康监控、节点管理和集群配置。
"""

from typing import Dict, List, Optional
from datetime import datetime

from app.schemas.cluster import (
    ClusterInfo, ClusterCreateRequest, ClusterUpdateRequest,
    ClusterListResponse
)
from app.utils.metrics_collector import metrics_collector
from config.settings import settings


class ClusterService:
    """Service for cluster management operations."""

    def __init__(self):
        """Initialize cluster service."""
        self.use_real_clusters = settings.use_real_clusters

    async def get_all_clusters(self) -> ClusterListResponse:
        """
        Get information for all clusters.

        Returns:
            ClusterListResponse: List of all clusters with their status
        """
        clusters = []

        for cluster_type in ['hadoop', 'flink', 'doris']:
            cluster_metrics = await metrics_collector.get_cluster_metrics(cluster_type)

            cluster_info = ClusterInfo(
                name=f"{cluster_type.capitalize()} Cluster",
                status=self._determine_status(cluster_metrics),
                total_nodes=cluster_metrics.get('total_nodes', 0),
                active_nodes=cluster_metrics.get('active_nodes', 0),
                cpu_usage=cluster_metrics.get('cpu_usage', 0.0),
                memory_usage=cluster_metrics.get('memory_usage', 0.0),
                disk_usage=cluster_metrics.get('disk_usage', 0.0),
                last_update=datetime.now()
            )
            clusters.append(cluster_info)

        return ClusterListResponse(
            clusters=clusters,
            total=len(clusters)
        )

    async def get_cluster_by_type(self, cluster_type: str) -> Optional[ClusterInfo]:
        """
        Get specific cluster information.

        Args:
            cluster_type: Type of cluster (hadoop, flink, doris)

        Returns:
            ClusterInfo: Cluster information or None if not found
        """
        if cluster_type not in ['hadoop', 'flink', 'doris']:
            return None

        cluster_metrics = await metrics_collector.get_cluster_metrics(cluster_type)

        return ClusterInfo(
            name=f"{cluster_type.capitalize()} Cluster",
            status=self._determine_status(cluster_metrics),
            total_nodes=cluster_metrics.get('total_nodes', 0),
            active_nodes=cluster_metrics.get('active_nodes', 0),
            cpu_usage=cluster_metrics.get('cpu_usage', 0.0),
            memory_usage=cluster_metrics.get('memory_usage', 0.0),
            disk_usage=cluster_metrics.get('disk_usage', 0.0),
            last_update=datetime.now()
        )

    def _determine_status(self, cluster_metrics: Dict) -> str:
        """Determine cluster status based on metrics."""
        active_nodes = cluster_metrics.get('active_nodes', 0)
        total_nodes = cluster_metrics.get('total_nodes', 0)

        if active_nodes == 0:
            return "error"
        elif active_nodes < total_nodes:
            return "warning"
        else:
            return "normal"