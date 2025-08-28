# app/services/workflow_validation_service.py
"""
工作流验证服务
提供工作流定义验证、依赖分析、健康检查等功能
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from loguru import logger

from app.schemas.workflow import (
    CreateWorkflowRequest, WorkflowValidationResult,
    WorkflowDependencyGraph, WorkflowHealthCheck
)
from app.models.workflow import WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition


class WorkflowValidationService:
    """工作流验证服务"""

    async def validate_workflow_definition(
            self,
            workflow_request: CreateWorkflowRequest,
            db: AsyncSession
    ) -> WorkflowValidationResult:
        """验证工作流定义"""
        errors = []
        warnings = []

        try:
            # 基础信息验证
            if not workflow_request.workflow_name.strip():
                errors.append("工作流名称不能为空")

            # 节点验证
            if not workflow_request.nodes:
                errors.append("工作流至少需要包含一个节点")
            else:
                start_nodes = [n for n in workflow_request.nodes if n.is_start_node]
                if len(start_nodes) != 1:
                    errors.append("工作流必须有且仅有一个开始节点")

                end_nodes = [n for n in workflow_request.nodes if n.is_end_node]
                if not end_nodes:
                    warnings.append("建议至少设置一个结束节点")

            # 边验证
            if workflow_request.edges:
                node_ids = {n.node_id for n in workflow_request.nodes}
                for edge in workflow_request.edges:
                    if edge.source_node_id not in node_ids:
                        errors.append(f"边的源节点 {edge.source_node_id} 不存在")
                    if edge.target_node_id not in node_ids:
                        errors.append(f"边的目标节点 {edge.target_node_id} 不存在")

            # 循环依赖检查
            if workflow_request.nodes and workflow_request.edges:
                has_cycle = await self._check_cycles(workflow_request.nodes, workflow_request.edges)
                if has_cycle:
                    errors.append("工作流存在循环依赖")

            return WorkflowValidationResult(
                is_valid=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                validation_time=datetime.now()
            )

        except Exception as e:
            logger.error(f"工作流验证异常: {e}")
            return WorkflowValidationResult(
                is_valid=False,
                errors=[f"验证过程异常: {str(e)}"],
                warnings=warnings,
                validation_time=datetime.now()
            )

    async def build_dependency_graph(
            self,
            nodes: List[Any],
            edges: List[Any]
    ) -> WorkflowDependencyGraph:
        """构建依赖关系图"""
        try:
            # 构建节点信息
            graph_nodes = []
            for node in nodes:
                graph_nodes.append({
                    "node_id": node.node_id,
                    "node_name": node.node_name,
                    "node_type": node.node_type.value if hasattr(node.node_type, 'value') else str(node.node_type),
                    "is_start_node": node.is_start_node,
                    "is_end_node": node.is_end_node
                })

            # 构建边信息
            graph_edges = []
            for edge in edges:
                graph_edges.append({
                    "edge_id": getattr(edge, 'edge_id', f"{edge.source_node_id}->{edge.target_node_id}"),
                    "source_node_id": edge.source_node_id,
                    "target_node_id": edge.target_node_id,
                    "condition_type": edge.condition_type
                })

            # 拓扑排序
            layers = await self._topological_sort(nodes, edges)
            max_depth = len(layers)

            # 检查循环
            has_cycles = await self._check_cycles(nodes, edges)

            return WorkflowDependencyGraph(
                workflow_id=0,  # 在调用时设置
                nodes=graph_nodes,
                edges=graph_edges,
                layers=layers,
                max_depth=max_depth,
                has_cycles=has_cycles
            )

        except Exception as e:
            logger.error(f"构建依赖图失败: {e}")
            raise

    async def perform_health_check(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> WorkflowHealthCheck:
        """执行工作流健康检查"""
        try:
            issues = []
            recommendations = []
            health_score = 100.0

            # 获取工作流信息
            result = await db.execute(
                select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                return WorkflowHealthCheck(
                    workflow_id=workflow_id,
                    is_healthy=False,
                    health_score=0,
                    issues=["工作流不存在"],
                    recommendations=[],
                    check_time=datetime.now()
                )

            # 检查工作流状态
            if not workflow.is_active:
                issues.append("工作流未激活")
                health_score -= 20

            # 检查最近执行情况
            if workflow.failed_executions > workflow.successful_executions:
                issues.append("失败执行次数超过成功次数")
                recommendations.append("检查工作流配置和依赖服务")
                health_score -= 30

            # 检查平均执行时间
            if workflow.avg_execution_duration and workflow.avg_execution_duration > 3600:
                issues.append("平均执行时间过长")
                recommendations.append("优化工作流性能")
                health_score -= 10

            return WorkflowHealthCheck(
                workflow_id=workflow_id,
                is_healthy=len(issues) == 0,
                health_score=max(0, health_score),
                issues=issues,
                recommendations=recommendations,
                check_time=datetime.now()
            )

        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            raise

    async def _check_cycles(self, nodes: List[Any], edges: List[Any]) -> bool:
        """检查是否存在循环依赖"""
        try:
            # 构建邻接表
            adj_list = {}
            node_ids = {n.node_id for n in nodes}

            for node_id in node_ids:
                adj_list[node_id] = []

            for edge in edges:
                if edge.source_node_id in adj_list and edge.target_node_id in adj_list:
                    adj_list[edge.source_node_id].append(edge.target_node_id)

            # DFS检查循环
            visited = set()
            rec_stack = set()

            def dfs(node):
                if node in rec_stack:
                    return True
                if node in visited:
                    return False

                visited.add(node)
                rec_stack.add(node)

                for neighbor in adj_list.get(node, []):
                    if dfs(neighbor):
                        return True

                rec_stack.remove(node)
                return False

            for node_id in node_ids:
                if node_id not in visited:
                    if dfs(node_id):
                        return True

            return False

        except Exception as e:
            logger.error(f"循环检查失败: {e}")
            return False

    async def _topological_sort(self, nodes: List[Any], edges: List[Any]) -> List[List[str]]:
        """拓扑排序分层"""
        try:
            # 构建邻接表和入度统计
            adj_list = {}
            in_degree = {}
            node_ids = {n.node_id for n in nodes}

            for node_id in node_ids:
                adj_list[node_id] = []
                in_degree[node_id] = 0

            for edge in edges:
                if edge.source_node_id in adj_list and edge.target_node_id in adj_list:
                    adj_list[edge.source_node_id].append(edge.target_node_id)
                    in_degree[edge.target_node_id] += 1

            # Kahn算法分层
            layers = []
            queue = [node_id for node_id in node_ids if in_degree[node_id] == 0]

            while queue:
                current_layer = queue[:]
                layers.append(current_layer)
                queue = []

                for node_id in current_layer:
                    for neighbor in adj_list[node_id]:
                        in_degree[neighbor] -= 1
                        if in_degree[neighbor] == 0:
                            queue.append(neighbor)

            return layers

        except Exception as e:
            logger.error(f"拓扑排序失败: {e}")
            return []


# 创建服务实例
workflow_validation_service = WorkflowValidationService()