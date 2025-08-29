# app/services/cluster_service.py
"""
集群管理业务服务层 - 重构后的生产级版本
负责集群相关的业务逻辑处理，协调数据访问层和外部服务
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from app.schemas.cluster import ClusterInfo, ClusterListResponse
from app.services.cluster_database_service import cluster_db_service
from app.utils.metrics_collector import metrics_collector
from config.settings import settings


class ClusterService:
    """集群管理业务服务层"""

    def __init__(self):
        """初始化集群服务"""
        self.db_service = cluster_db_service
        self.metrics_collector = metrics_collector
        self.supported_clusters = self._get_supported_clusters()
        self.data_freshness_threshold = 300  # 5分钟数据有效期

    def _get_supported_clusters(self) -> List[str]:
        """获取支持的集群类型列表"""
        # 可以从配置文件读取，便于扩展
        return getattr(settings, 'SUPPORTED_CLUSTER_TYPES', ['hadoop', 'flink', 'doris'])

    async def get_all_clusters_overview(self, db: AsyncSession, force_refresh: bool = False) -> Dict:
        """
        获取所有集群概览信息
        
        Args:
            db: 数据库会话
            force_refresh: 是否强制刷新数据
        
        Returns:
            Dict: 包含所有集群信息的字典
        """
        try:
            logger.info("获取集群概览信息")
            
            # 从数据库获取集群数据
            clusters = await self.db_service.get_all_clusters(db)
            
            # 检查数据新鲜度
            if not clusters or force_refresh or self._is_data_stale(clusters):
                logger.info("集群数据需要刷新")
                await self._refresh_all_clusters_data(db)
                # 重新获取更新后的数据
                clusters = await self.db_service.get_all_clusters(db)
            
            # 业务逻辑处理
            cluster_list = []
            for cluster in clusters:
                cluster_info = self._build_cluster_info(cluster)
                cluster_list.append(cluster_info)
            
            # 生成汇总统计
            summary = self._generate_cluster_summary(cluster_list)
            
            return {
                "clusters": cluster_list,
                "summary": summary,
                "last_refresh": datetime.now(),
                "data_source": "database",
                "is_fresh": not self._is_data_stale(clusters)
            }

        except Exception as e:
            logger.error(f"获取集群概览失败: {e}")
            raise

    async def get_cluster_detail(
        self, 
        db: AsyncSession, 
        cluster_type: str,
        include_nodes: bool = True,
        include_history: bool = False,
        history_hours: int = 24
    ) -> Optional[Dict]:
        """
        获取指定集群的详细信息
        
        Args:
            db: 数据库会话
            cluster_type: 集群类型
            include_nodes: 是否包含节点信息
            include_history: 是否包含历史数据
            history_hours: 历史数据时长
            
        Returns:
            Optional[Dict]: 集群详细信息
        """
        try:
            # 验证集群类型
            if not self.is_supported_cluster(cluster_type):
                raise ValueError(f"不支持的集群类型: {cluster_type}")
            
            logger.info(f"获取 {cluster_type} 集群详细信息")
            
            # 从数据库获取集群信息
            cluster = await self.db_service.get_cluster_with_nodes(db, cluster_type.lower())
            
            if not cluster:
                # 尝试初始化集群数据
                await self._initialize_single_cluster(db, cluster_type)
                cluster = await self.db_service.get_cluster_with_nodes(db, cluster_type.lower())
                
                if not cluster:
                    return None
            
            # 构建详细信息
            cluster_detail = self._build_detailed_cluster_info(cluster, cluster_type)
            
            # 添加节点信息
            if include_nodes and cluster.nodes:
                cluster_detail["nodes"] = self._build_nodes_info(cluster.nodes, cluster_type)
            
            # 添加历史数据
            if include_history:
                historical_metrics = await self.db_service.get_cluster_historical_metrics(
                    db, cluster.id, history_hours
                )
                cluster_detail["historical_data"] = self._process_historical_data(historical_metrics)
            
            return cluster_detail

        except Exception as e:
            logger.error(f"获取集群详细信息失败: {e}")
            raise

    async def refresh_cluster_data(
        self, 
        db: AsyncSession, 
        cluster_types: Optional[List[str]] = None
    ) -> Dict:
        """
        刷新集群数据
        
        Args:
            db: 数据库会话
            cluster_types: 要刷新的集群类型列表
            
        Returns:
            Dict: 刷新结果
        """
        try:
            types_to_refresh = cluster_types or self.supported_clusters
            
            # 验证集群类型
            invalid_types = [t for t in types_to_refresh if not self.is_supported_cluster(t)]
            if invalid_types:
                raise ValueError(f"不支持的集群类型: {invalid_types}")
            
            logger.info(f"开始刷新集群数据: {types_to_refresh}")
            
            refresh_results = {}
            
            for cluster_type in types_to_refresh:
                try:
                    result = await self._sync_single_cluster_data(db, cluster_type)
                    refresh_results[cluster_type] = {
                        "success": True,
                        "updated_at": datetime.now(),
                        "details": result
                    }
                except Exception as e:
                    logger.error(f"刷新 {cluster_type} 集群数据失败: {e}")
                    refresh_results[cluster_type] = {
                        "success": False,
                        "error": str(e),
                        "updated_at": datetime.now()
                    }
            
            return {
                "refresh_initiated": True,
                "results": refresh_results,
                "total_clusters": len(types_to_refresh),
                "successful_clusters": len([r for r in refresh_results.values() if r["success"]]),
                "completion_time": datetime.now()
            }

        except Exception as e:
            logger.error(f"刷新集群数据失败: {e}")
            raise

    async def get_cluster_alerts(
        self, 
        db: AsyncSession, 
        cluster_type: Optional[str] = None,
        level: Optional[str] = None
    ) -> List[Dict]:
        """
        获取集群告警信息
        
        Args:
            db: 数据库会话
            cluster_type: 集群类型过滤
            level: 告警级别过滤
            
        Returns:
            List[Dict]: 告警列表
        """
        try:
            logger.info("获取集群告警信息")
            
            # 从数据库获取基础告警
            alerts = await self.db_service.get_cluster_alerts(db, cluster_type)
            
            # 业务逻辑处理：添加更多告警判断
            enhanced_alerts = self._enhance_alerts(alerts)
            
            # 应用级别过滤
            if level:
                enhanced_alerts = [alert for alert in enhanced_alerts if alert.get('level') == level]
            
            # 按优先级和时间排序
            enhanced_alerts.sort(
                key=lambda x: (
                    self._get_alert_priority(x.get('level', 'info')),
                    x.get('timestamp', datetime.now())
                ),
                reverse=True
            )
            
            return enhanced_alerts

        except Exception as e:
            logger.error(f"获取集群告警失败: {e}")
            return []

    async def validate_cluster_health(self, db: AsyncSession) -> Dict:
        """
        验证集群健康状态
        
        Args:
            db: 数据库会话
            
        Returns:
            Dict: 健康状态报告
        """
        try:
            logger.info("开始集群健康检查")
            
            clusters = await self.db_service.get_all_clusters(db)
            health_report = {
                "overall_status": "healthy",
                "total_clusters": len(clusters),
                "healthy_clusters": 0,
                "warning_clusters": 0,
                "error_clusters": 0,
                "cluster_details": [],
                "recommendations": [],
                "check_time": datetime.now()
            }
            
            for cluster in clusters:
                cluster_health = self._assess_cluster_health(cluster)
                health_report["cluster_details"].append(cluster_health)
                
                # 统计计数
                status = cluster_health["status"]
                if status == "healthy":
                    health_report["healthy_clusters"] += 1
                elif status == "warning":
                    health_report["warning_clusters"] += 1
                else:
                    health_report["error_clusters"] += 1
            
            # 确定整体状态
            if health_report["error_clusters"] > 0:
                health_report["overall_status"] = "critical"
            elif health_report["warning_clusters"] > 0:
                health_report["overall_status"] = "warning"
            
            # 生成建议
            health_report["recommendations"] = self._generate_health_recommendations(clusters)
            
            return health_report

        except Exception as e:
            logger.error(f"集群健康检查失败: {e}")
            raise

    # ==================== 私有方法 ====================

    def is_supported_cluster(self, cluster_type: str) -> bool:
        """检查是否支持指定的集群类型"""
        return cluster_type.lower() in [t.lower() for t in self.supported_clusters]

    def _is_data_stale(self, clusters: List) -> bool:
        """检查数据是否过期"""
        if not clusters:
            return True
        
        threshold = datetime.now() - timedelta(seconds=self.data_freshness_threshold)
        
        for cluster in clusters:
            if not cluster.last_health_check or cluster.last_health_check < threshold:
                return True
        
        return False

    async def _refresh_all_clusters_data(self, db: AsyncSession):
        """刷新所有集群数据"""
        for cluster_type in self.supported_clusters:
            try:
                await self._sync_single_cluster_data(db, cluster_type)
            except Exception as e:
                logger.error(f"刷新 {cluster_type} 失败: {e}")

    async def _initialize_single_cluster(self, db: AsyncSession, cluster_type: str):
        """初始化单个集群"""
        try:
            logger.info(f"初始化 {cluster_type} 集群")
            
            # 创建集群记录
            cluster = await self.db_service.get_or_create_cluster(
                db, 
                f"{cluster_type.capitalize()} Cluster",
                cluster_type.lower()
            )
            
            # 同步数据
            await self._sync_single_cluster_data(db, cluster_type)
            
        except Exception as e:
            logger.error(f"初始化 {cluster_type} 集群失败: {e}")
            raise

    async def _sync_single_cluster_data(self, db: AsyncSession, cluster_type: str) -> Dict:
        """同步单个集群数据"""
        try:
            logger.info(f"同步 {cluster_type} 集群数据")
            
            # 从实际集群收集数据
            cluster_metrics = await self.metrics_collector.get_cluster_metrics(cluster_type.lower())
            
            if not cluster_metrics.get('success', False):
                raise Exception(f"获取 {cluster_type} 集群指标失败: {cluster_metrics.get('error', 'Unknown')}")
            
            # 获取或创建集群记录
            cluster = await self.db_service.get_or_create_cluster(
                db,
                f"{cluster_type.capitalize()} Cluster",
                cluster_type.lower()
            )
            
            # 更新集群指标
            await self.db_service.update_cluster_metrics(db, cluster, cluster_metrics)
            
            # 更新节点信息
            if cluster_metrics.get('nodes'):
                await self.db_service.update_cluster_nodes(db, cluster, cluster_metrics['nodes'])
            
            return {
                "cluster_id": cluster.id,
                "total_nodes": cluster_metrics.get('total_nodes', 0),
                "active_nodes": cluster_metrics.get('active_nodes', 0),
                "sync_time": datetime.now()
            }

        except Exception as e:
            logger.error(f"同步 {cluster_type} 集群数据失败: {e}")
            raise

    def _build_cluster_info(self, cluster) -> Dict:
        """构建集群信息字典"""
        return {
            "id": cluster.id,
            "name": cluster.name,
            "type": cluster.cluster_type,
            "status": cluster.status,
            "total_nodes": cluster.total_nodes,
            "active_nodes": cluster.active_nodes,
            "cpu_usage": cluster.cpu_usage or 0.0,
            "memory_usage": cluster.memory_usage or 0.0,
            "disk_usage": cluster.disk_usage or 0.0,
            "last_update": cluster.last_health_check or cluster.updated_at,
            "uptime_percentage": self._calculate_uptime_percentage(cluster),
            "health_score": self._calculate_health_score(cluster),
            "data_source": "database"
        }

    def _build_detailed_cluster_info(self, cluster, cluster_type: str) -> Dict:
        """构建集群详细信息"""
        detail = {
            "id": cluster.id,
            "name": cluster.name,
            "cluster_type": cluster.cluster_type,
            "status": cluster.status,
            "total_nodes": cluster.total_nodes,
            "active_nodes": cluster.active_nodes,
            "cpu_usage": cluster.cpu_usage or 0.0,
            "memory_usage": cluster.memory_usage or 0.0,
            "disk_usage": cluster.disk_usage or 0.0,
            "last_health_check": cluster.last_health_check,
            "config": cluster.config or {},
            "endpoints": cluster.endpoints or {},
            "health_score": self._calculate_health_score(cluster),
            "data_source": "database"
        }
        
        # 添加集群类型特定的资源信息
        detail.update(self._get_cluster_specific_info(cluster, cluster_type))
        
        return detail

    def _build_nodes_info(self, nodes, cluster_type: str) -> List[Dict]:
        """构建节点信息列表"""
        nodes_list = []
        
        for node in nodes:
            node_info = {
                "id": node.id,
                "hostname": node.hostname,
                "ip_address": node.ip_address,
                "role": node.role,
                "status": node.status,
                "cpu_usage": node.cpu_usage or 0.0,
                "memory_usage": node.memory_usage or 0.0,
                "disk_usage": node.disk_usage or 0.0,
                "cpu_cores": node.cpu_cores,
                "memory_total": node.memory_total,
                "disk_total": node.disk_total,
                "last_seen": node.last_seen,
                "uptime": self._calculate_node_uptime(node)
            }
            
            # 添加集群类型特定的节点信息
            node_info.update(self._get_node_specific_info(node, cluster_type))
            
            nodes_list.append(node_info)
        
        return nodes_list

    def _get_cluster_specific_info(self, cluster, cluster_type: str) -> Dict:
        """获取集群类型特定信息"""
        specific_info = {}
        
        if cluster_type.lower() == "hadoop":
            total_storage = sum((node.disk_total or 0) for node in cluster.nodes) / (1024**3)  # 转TB
            specific_info.update({
                "hdfs_info": {
                    "total_capacity_tb": round(total_storage, 2),
                    "used_capacity_tb": round(total_storage * (cluster.disk_usage or 0) / 100, 2),
                    "available_capacity_tb": round(total_storage * (100 - (cluster.disk_usage or 0)) / 100, 2)
                }
            })
        elif cluster_type.lower() == "flink":
            taskmanager_count = len([n for n in cluster.nodes if 'TaskManager' in n.role])
            specific_info.update({
                "flink_info": {
                    "job_managers": len([n for n in cluster.nodes if 'JobManager' in n.role]),
                    "task_managers": taskmanager_count,
                    "total_slots": taskmanager_count * 4,
                    "available_slots": int(taskmanager_count * 4 * (100 - (cluster.cpu_usage or 0)) / 100)
                }
            })
        elif cluster_type.lower() == "doris":
            specific_info.update({
                "doris_info": {
                    "frontends": len([n for n in cluster.nodes if 'FRONTEND' in n.role]),
                    "backends": len([n for n in cluster.nodes if 'BACKEND' in n.role]),
                    "alive_backends": len([n for n in cluster.nodes if 'BACKEND' in n.role and n.status == 'online'])
                }
            })
        
        return specific_info

    def _get_node_specific_info(self, node, cluster_type: str) -> Dict:
        """获取节点类型特定信息"""
        specific_info = {}
        
        if cluster_type.lower() == "hadoop":
            specific_info.update({
                "hdfs_used_gb": int((node.disk_usage or 0) * (node.disk_total or 0) / 100 / 1024) if node.disk_total else 0,
                "hdfs_capacity_gb": int((node.disk_total or 0) / 1024) if node.disk_total else 0,
                "block_pool_used_percent": node.disk_usage or 0
            })
        elif cluster_type.lower() == "flink" and 'TaskManager' in node.role:
            specific_info.update({
                "slots_total": 4,
                "slots_available": 4 - int(4 * (node.cpu_usage or 0) / 100),
                "slots_used": int(4 * (node.cpu_usage or 0) / 100)
            })
        elif cluster_type.lower() == "doris" and 'BACKEND' in node.role:
            specific_info.update({
                "tablets_num": 1000 if node.status == 'online' else 0,
                "total_capacity_gb": int((node.disk_total or 0) / 1024) if node.disk_total else 0,
                "queries_per_second": 50 if node.status == 'online' else 0
            })
        
        return specific_info

    def _generate_cluster_summary(self, cluster_list: List[Dict]) -> Dict:
        """生成集群汇总统计"""
        total_clusters = len(cluster_list)
        normal_clusters = len([c for c in cluster_list if c.get('status') == 'normal'])
        warning_clusters = len([c for c in cluster_list if c.get('status') == 'warning'])
        error_clusters = len([c for c in cluster_list if c.get('status') == 'error'])
        
        return {
            "total_clusters": total_clusters,
            "normal_clusters": normal_clusters,
            "warning_clusters": warning_clusters,
            "error_clusters": error_clusters,
            "overall_health": self._determine_overall_health(normal_clusters, warning_clusters, error_clusters, total_clusters),
            "average_health_score": sum(c.get('health_score', 0) for c in cluster_list) / total_clusters if total_clusters > 0 else 0,
            "total_nodes": sum(c.get('total_nodes', 0) for c in cluster_list),
            "total_active_nodes": sum(c.get('active_nodes', 0) for c in cluster_list)
        }

    def _determine_overall_health(self, normal: int, warning: int, error: int, total: int) -> str:
        """确定整体健康状态"""
        if total == 0:
            return "unknown"
        elif error > 0:
            return "critical"
        elif warning >= total / 2:
            return "warning"
        else:
            return "healthy"

    def _calculate_uptime_percentage(self, cluster) -> float:
        """计算集群正常运行时间百分比"""
        if not cluster.total_nodes or cluster.total_nodes == 0:
            return 0.0
        return round((cluster.active_nodes / cluster.total_nodes) * 100, 1)

    def _calculate_health_score(self, cluster) -> float:
        """计算集群健康评分 (0-100)"""
        try:
            # 基础分数
            base_score = 100
            
            # 节点健康度评分 (40%)
            if cluster.total_nodes > 0:
                node_health = (cluster.active_nodes / cluster.total_nodes) * 40
            else:
                node_health = 0
            
            # 资源使用评分 (60%)
            cpu_score = max(0, 20 - (cluster.cpu_usage or 0) * 0.2)  # CPU使用率越高扣分越多
            memory_score = max(0, 20 - (cluster.memory_usage or 0) * 0.2)
            disk_score = max(0, 20 - (cluster.disk_usage or 0) * 0.2)
            
            total_score = node_health + cpu_score + memory_score + disk_score
            return round(max(0, min(100, total_score)), 1)
            
        except:
            return 50.0  # 默认评分

    def _calculate_node_uptime(self, node) -> str:
        """计算节点运行时间"""
        if not node.last_seen or node.status != 'online':
            return "离线"
        
        # 简化计算，实际应基于历史数据
        time_diff = datetime.now() - node.last_seen
        hours = int(time_diff.total_seconds() / 3600)
        
        if hours < 1:
            return "刚上线"
        elif hours < 24:
            return f"{hours}小时"
        else:
            days = hours // 24
            return f"{days}天"

    def _process_historical_data(self, historical_metrics) -> List[Dict]:
        """处理历史指标数据"""
        return [
            {
                "timestamp": metric.metric_timestamp,
                "cpu_usage": metric.cpu_usage or 0.0,
                "memory_usage": metric.memory_usage or 0.0,
                "disk_usage": metric.disk_usage or 0.0,
                "active_nodes": metric.active_nodes or 0
            }
            for metric in historical_metrics[-100:]  # 最近100条记录
        ]

    def _enhance_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """增强告警信息"""
        # 在基础告警上添加业务逻辑判断
        enhanced = alerts.copy()
        
        # 可以添加更多业务规则产生的告警
        # 例如：趋势分析、预测性告警等
        
        return enhanced

    def _get_alert_priority(self, level: str) -> int:
        """获取告警优先级"""
        priority_map = {"error": 3, "warning": 2, "info": 1}
        return priority_map.get(level, 0)

    def _assess_cluster_health(self, cluster) -> Dict:
        """评估单个集群健康状态"""
        health_score = self._calculate_health_score(cluster)
        
        if health_score >= 80:
            status = "healthy"
        elif health_score >= 60:
            status = "warning"
        else:
            status = "critical"
        
        return {
            "cluster_name": cluster.name,
            "cluster_type": cluster.cluster_type,
            "status": status,
            "health_score": health_score,
            "total_nodes": cluster.total_nodes,
            "active_nodes": cluster.active_nodes,
            "last_check": cluster.last_health_check
        }

    def _generate_health_recommendations(self, clusters) -> List[str]:
        """生成健康建议"""
        recommendations = []
        
        for cluster in clusters:
            if cluster.status == "error":
                recommendations.append(f"紧急：{cluster.name} 集群状态异常，请立即检查")
            elif (cluster.cpu_usage or 0) > 90:
                recommendations.append(f"建议：{cluster.name} CPU使用率过高，考虑扩容或负载均衡")
            elif (cluster.memory_usage or 0) > 85:
                recommendations.append(f"建议：{cluster.name} 内存使用率过高，建议优化或扩容")
            elif (cluster.disk_usage or 0) > 90:
                recommendations.append(f"警告：{cluster.name} 磁盘空间不足，请及时清理或扩容")
        
        return recommendations[:10]  # 限制建议数量


# 创建全局实例
cluster_service = ClusterService()