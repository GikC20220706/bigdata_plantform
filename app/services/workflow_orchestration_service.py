# app/services/workflow_orchestration_service.py
"""
工作流编排核心服务
提供工作流定义、执行、监控和管理功能
"""

import os
import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, asc, update, delete
from sqlalchemy.orm import selectinload
from loguru import logger

from app.models.workflow import (
    WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition,
    WorkflowExecution, WorkflowNodeExecution, WorkflowTemplate,
    WorkflowVariable, WorkflowAlert,
    WorkflowStatus, NodeType, NodeStatus, TriggerType
)
from app.schemas.workflow import (
    CreateWorkflowRequest, UpdateWorkflowRequest, TriggerWorkflowRequest,
    WorkflowResponse, WorkflowListResponse, WorkflowSearchParams,
    WorkflowExecutionResponse, WorkflowStatistics
)
from app.services.dag_generator_service import dag_generator_service
from app.services.executor_service import executor_service
from app.services.monitoring_service import monitoring_service
from app.utils.cache_service import cache_service
from config.settings import settings


class WorkflowOrchestrationService:
    """工作流编排服务"""

    def __init__(self):
        self.cache_prefix = "workflow"
        self.cache_ttl = 300  # 5分钟缓存
        self.execution_cache_ttl = 60  # 执行状态缓存1分钟

    # ==================== 工作流定义管理 ====================

    async def create_workflow(
            self,
            db: AsyncSession,
            workflow_request: CreateWorkflowRequest,
            owner_id: Optional[str] = None,
            owner_name: Optional[str] = None
    ) -> WorkflowResponse:
        """创建工作流"""
        try:
            # 检查工作流ID是否已存在
            result = await db.execute(
                select(WorkflowDefinition).where(
                    WorkflowDefinition.workflow_id == workflow_request.workflow_id
                )
            )
            existing = result.scalar_one_or_none()

            if existing:
                raise ValueError(f"工作流ID '{workflow_request.workflow_id}' 已存在")

            # 验证工作流配置
            validation_result = await self._validate_workflow_config(workflow_request)
            if not validation_result["is_valid"]:
                raise ValueError(f"工作流配置验证失败: {validation_result['errors']}")

            # 创建工作流定义
            workflow = WorkflowDefinition(
                workflow_id=workflow_request.workflow_id,
                workflow_name=workflow_request.workflow_name,
                display_name=workflow_request.display_name,
                description=workflow_request.description,
                category=workflow_request.category,
                business_domain=workflow_request.business_domain,
                tags=workflow_request.tags,
                trigger_type=workflow_request.trigger_type,
                schedule_config=workflow_request.schedule_config.dict() if workflow_request.schedule_config else None,
                workflow_config=workflow_request.workflow_config,
                default_timeout=workflow_request.default_timeout,
                max_parallel_tasks=workflow_request.max_parallel_tasks,
                retry_policy=workflow_request.retry_policy.dict() if workflow_request.retry_policy else None,
                canvas_config=workflow_request.canvas_config,
                layout_config=workflow_request.layout_config,
                owner_id=owner_id,
                owner_name=owner_name,
                visibility=workflow_request.visibility or "private"
            )

            db.add(workflow)
            await db.flush()

            # 创建节点定义
            node_id_mapping = {}  # 用于映射请求中的节点ID到数据库ID
            for node_req in workflow_request.nodes:
                node = WorkflowNodeDefinition(
                    node_id=node_req.node_id,
                    node_name=node_req.node_name,
                    display_name=node_req.display_name,
                    description=node_req.description,
                    workflow_id=workflow.id,
                    node_type=node_req.node_type,
                    task_config=node_req.task_config.dict() if node_req.task_config else None,
                    timeout=node_req.timeout,
                    retry_times=node_req.retry_policy.retry_times if node_req.retry_policy else 0,
                    retry_interval=node_req.retry_policy.retry_interval if node_req.retry_policy else 60,
                    condition_expression=node_req.condition_expression,
                    required_cpu=node_req.required_cpu,
                    required_memory_mb=node_req.required_memory_mb,
                    position_x=node_req.position.x if node_req.position else None,
                    position_y=node_req.position.y if node_req.position else None,
                    width=node_req.position.width if node_req.position else 120,
                    height=node_req.position.height if node_req.position else 60,
                    style_config=node_req.style.dict() if node_req.style else None,
                    is_start_node=node_req.is_start_node,
                    is_end_node=node_req.is_end_node,
                    is_critical=node_req.is_critical
                )
                db.add(node)
                await db.flush()
                node_id_mapping[node_req.node_id] = node.id

            # 创建边定义
            if workflow_request.edges:
                for edge_req in workflow_request.edges:
                    edge = WorkflowEdgeDefinition(
                        edge_id=edge_req.edge_id,
                        edge_name=edge_req.edge_name,
                        workflow_id=workflow.id,
                        source_node_id=node_id_mapping[edge_req.source_node_id],
                        target_node_id=node_id_mapping[edge_req.target_node_id],
                        condition_type=edge_req.condition_type.value,
                        condition_expression=edge_req.condition_expression,
                        path_config=edge_req.path_config.dict() if edge_req.path_config else None,
                        style_config=edge_req.style_config.dict() if edge_req.style_config else None
                    )
                    db.add(edge)

            # 创建变量定义
            if workflow_request.variables:
                for var_req in workflow_request.variables:
                    variable = WorkflowVariable(
                        variable_key=var_req.key,
                        variable_name=var_req.name,
                        description=var_req.description,
                        workflow_id=workflow.id,
                        variable_type=var_req.type,
                        default_value=var_req.default_value,
                        is_required=var_req.is_required,
                        is_sensitive=var_req.is_sensitive,
                        validation_rules=[rule.dict() for rule in
                                          var_req.validation_rules] if var_req.validation_rules else None
                    )
                    db.add(variable)

            await db.commit()
            await db.refresh(workflow)

            # 清除相关缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics"
            ])

            logger.info(f"工作流创建成功: {workflow_request.workflow_id}")
            return await self._build_workflow_response(db, workflow)

        except Exception as e:
            await db.rollback()
            logger.error(f"创建工作流失败: {e}")
            raise

    async def get_workflow_by_id(
            self,
            db: AsyncSession,
            workflow_id: int,
            include_nodes: bool = True,
            include_edges: bool = True
    ) -> Optional[WorkflowResponse]:
        """根据ID获取工作流详情"""
        cache_key = f"{self.cache_prefix}:detail:{workflow_id}:{include_nodes}:{include_edges}"

        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return WorkflowResponse(**cached_data)

        # 构建查询
        query = select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)

        if include_nodes:
            query = query.options(selectinload(WorkflowDefinition.nodes))
        if include_edges:
            query = query.options(selectinload(WorkflowDefinition.edges))

        result = await db.execute(query)
        workflow = result.scalar_one_or_none()

        if not workflow:
            return None

        workflow_response = await self._build_workflow_response(db, workflow)

        # 缓存结果
        await cache_service.set(cache_key, workflow_response.dict(), self.cache_ttl)

        return workflow_response

    async def get_workflow_by_workflow_id(
            self,
            db: AsyncSession,
            workflow_id: str,
            include_nodes: bool = True,
            include_edges: bool = True
    ) -> Optional[WorkflowResponse]:
        """根据工作流ID获取工作流详情"""
        query = select(WorkflowDefinition).where(WorkflowDefinition.workflow_id == workflow_id)

        if include_nodes:
            query = query.options(selectinload(WorkflowDefinition.nodes))
        if include_edges:
            query = query.options(selectinload(WorkflowDefinition.edges))

        result = await db.execute(query)
        workflow = result.scalar_one_or_none()

        if not workflow:
            return None

        return await self._build_workflow_response(db, workflow)

    async def update_workflow(
            self,
            db: AsyncSession,
            workflow_id: int,
            update_request: UpdateWorkflowRequest
    ) -> WorkflowResponse:
        """更新工作流"""
        try:
            # 获取现有工作流
            result = await db.execute(
                select(WorkflowDefinition)
                .options(
                    selectinload(WorkflowDefinition.nodes),
                    selectinload(WorkflowDefinition.edges)
                )
                .where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                raise ValueError(f"工作流不存在: {workflow_id}")

            # 检查是否可以更新
            if workflow.status in [WorkflowStatus.RUNNING]:
                raise ValueError("运行中的工作流不能更新")

            # 更新基本信息
            update_data = {}
            for field, value in update_request.dict(exclude_unset=True).items():
                if field in ['nodes', 'edges', 'variables']:
                    continue  # 这些字段需要特殊处理
                if hasattr(workflow, field):
                    if field in ['schedule_config', 'retry_policy'] and value:
                        update_data[field] = value.dict() if hasattr(value, 'dict') else value
                    else:
                        update_data[field] = value

            # 执行更新
            if update_data:
                await db.execute(
                    update(WorkflowDefinition)
                    .where(WorkflowDefinition.id == workflow_id)
                    .values(**update_data)
                )

            # 处理节点更新
            if update_request.nodes is not None:
                await self._update_workflow_nodes(db, workflow.id, update_request.nodes)

            # 处理边更新
            if update_request.edges is not None:
                await self._update_workflow_edges(db, workflow.id, update_request.edges)

            # 处理变量更新
            if update_request.variables is not None:
                await self._update_workflow_variables(db, workflow.id, update_request.variables)

            await db.commit()

            # 清除缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:detail:{workflow_id}:*",
                f"{self.cache_prefix}:list:*"
            ])

            # 返回更新后的工作流
            return await self.get_workflow_by_id(db, workflow_id)

        except Exception as e:
            await db.rollback()
            logger.error(f"更新工作流失败: {e}")
            raise

    async def delete_workflow(
            self,
            db: AsyncSession,
            workflow_id: int,
            force: bool = False
    ) -> Dict[str, Any]:
        """删除工作流"""
        try:
            # 获取工作流
            result = await db.execute(
                select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                raise ValueError(f"工作流不存在: {workflow_id}")

            # 检查是否可以删除
            if not force and workflow.status == WorkflowStatus.RUNNING:
                raise ValueError("运行中的工作流不能删除，请使用force参数强制删除")

            # 检查是否有执行记录
            execution_result = await db.execute(
                select(func.count(WorkflowExecution.id))
                .where(WorkflowExecution.workflow_id == workflow_id)
            )
            execution_count = execution_result.scalar()

            # 删除工作流（级联删除相关数据）
            await db.execute(
                delete(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )

            await db.commit()

            # 清除缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:detail:{workflow_id}:*",
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics"
            ])

            logger.info(f"工作流删除成功: {workflow.workflow_id}")
            return {
                "success": True,
                "message": f"工作流 '{workflow.workflow_name}' 删除成功",
                "workflow_id": workflow.workflow_id,
                "execution_count_deleted": execution_count
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"删除工作流失败: {e}")
            raise

    async def search_workflows(
            self,
            db: AsyncSession,
            search_params: WorkflowSearchParams,
            user_id: Optional[str] = None
    ) -> WorkflowListResponse:
        """搜索工作流"""
        cache_key = f"{self.cache_prefix}:search:{hash(str(search_params.dict()))}"

        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return WorkflowListResponse(**cached_data)

        # 构建基础查询
        query = select(WorkflowDefinition)

        # 应用过滤条件
        query = self._apply_search_filters(query, search_params, user_id)

        # 计算总数
        count_query = select(func.count(WorkflowDefinition.id))
        count_query = self._apply_search_filters(count_query, search_params, user_id)
        total_result = await db.execute(count_query)
        total = total_result.scalar()

        # 应用排序和分页
        query = self._apply_search_sorting(query, search_params)
        offset = (search_params.page - 1) * search_params.page_size
        query = query.offset(offset).limit(search_params.page_size)

        # 执行查询
        result = await db.execute(query)
        workflows = result.scalars().all()

        # 构建响应数据
        workflow_responses = []
        for workflow in workflows:
            workflow_response = await self._build_workflow_response(db, workflow, include_details=False)
            workflow_responses.append(workflow_response)

        total_pages = (total + search_params.page_size - 1) // search_params.page_size

        response = WorkflowListResponse(
            workflows=workflow_responses,
            total=total,
            page=search_params.page,
            page_size=search_params.page_size,
            total_pages=total_pages,
            has_next=search_params.page < total_pages,
            has_prev=search_params.page > 1
        )

        # 缓存结果
        await cache_service.set(cache_key, response.dict(), self.cache_ttl)

        return response

    # ==================== 工作流执行管理 ====================

    async def trigger_workflow(
            self,
            db: AsyncSession,
            workflow_id: int,
            trigger_request: TriggerWorkflowRequest,
            trigger_user: Optional[str] = None
    ) -> Dict[str, Any]:
        """触发工作流执行"""
        try:
            # 获取工作流定义
            workflow = await self.get_workflow_by_id(db, workflow_id, include_nodes=True, include_edges=True)
            if not workflow:
                raise ValueError(f"工作流不存在: {workflow_id}")

            if workflow.status != WorkflowStatus.PUBLISHED:
                raise ValueError("只能执行已发布的工作流")

            # 生成执行ID
            execution_id = f"{workflow.workflow_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

            # 创建执行记录
            execution = WorkflowExecution(
                execution_id=execution_id,
                execution_name=trigger_request.execution_name,
                workflow_id=workflow_id,
                workflow_version=workflow.version,
                status=WorkflowStatus.RUNNING,
                trigger_type=TriggerType.MANUAL,
                trigger_user=trigger_user,
                start_time=datetime.now(),
                execution_date=trigger_request.execution_date or datetime.now(),
                total_nodes=len(workflow.nodes),
                execution_config=trigger_request.execution_config,
                runtime_variables=trigger_request.runtime_variables
            )

            db.add(execution)
            await db.flush()

            # 创建节点执行记录
            node_executions = []
            for node in workflow.nodes:
                node_execution = WorkflowNodeExecution(
                    node_execution_id=f"{execution_id}_{node.node_id}",
                    workflow_execution_id=execution.id,
                    node_definition_id=node.id,
                    status=NodeStatus.PENDING,
                    max_retry_count=node.retry_times
                )
                db.add(node_execution)
                node_executions.append(node_execution)

            await db.commit()

            # 异步执行工作流
            asyncio.create_task(self._execute_workflow(execution.id))

            logger.info(f"工作流执行已启动: {execution_id}")
            return {
                "success": True,
                "execution_id": execution_id,
                "message": f"工作流 '{workflow.workflow_name}' 执行已启动",
                "workflow_execution_id": execution.id
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"触发工作流执行失败: {e}")
            raise

    async def get_workflow_execution(
            self,
            db: AsyncSession,
            execution_id: str,
            include_node_executions: bool = True
    ) -> Optional[WorkflowExecutionResponse]:
        """获取工作流执行详情"""
        cache_key = f"{self.cache_prefix}:execution:{execution_id}:{include_node_executions}"

        # 尝试从缓存获取（执行状态缓存时间较短）
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return WorkflowExecutionResponse(**cached_data)

        # 构建查询
        query = select(WorkflowExecution).where(WorkflowExecution.execution_id == execution_id)

        if include_node_executions:
            query = query.options(selectinload(WorkflowExecution.node_executions))

        result = await db.execute(query)
        execution = result.scalar_one_or_none()

        if not execution:
            return None

        execution_response = await self._build_execution_response(db, execution)

        # 缓存结果（执行中的任务缓存时间更短）
        cache_ttl = self.execution_cache_ttl if execution.status == WorkflowStatus.RUNNING else self.cache_ttl
        await cache_service.set(cache_key, execution_response.dict(), cache_ttl)

        return execution_response

    async def cancel_workflow_execution(
            self,
            db: AsyncSession,
            execution_id: str,
            reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """取消工作流执行"""
        try:
            # 获取执行记录
            result = await db.execute(
                select(WorkflowExecution).where(WorkflowExecution.execution_id == execution_id)
            )
            execution = result.scalar_one_or_none()

            if not execution:
                raise ValueError(f"执行记录不存在: {execution_id}")

            if execution.status not in [WorkflowStatus.RUNNING]:
                raise ValueError(f"只能取消运行中的执行，当前状态: {execution.status}")

            # 更新执行状态
            await db.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.id == execution.id)
                .values(
                    status=WorkflowStatus.CANCELLED,
                    end_time=datetime.now(),
                    error_message=reason or "用户取消执行"
                )
            )

            # 更新未完成的节点执行状态
            await db.execute(
                update(WorkflowNodeExecution)
                .where(
                    and_(
                        WorkflowNodeExecution.workflow_execution_id == execution.id,
                        WorkflowNodeExecution.status.in_([NodeStatus.PENDING, NodeStatus.RUNNING])
                    )
                )
                .values(
                    status=NodeStatus.CANCELLED,
                    end_time=datetime.now()
                )
            )

            await db.commit()

            # 清除缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:execution:{execution_id}:*"
            ])

            logger.info(f"工作流执行已取消: {execution_id}")
            return {
                "success": True,
                "message": f"工作流执行 '{execution_id}' 已取消",
                "cancelled_at": datetime.now().isoformat()
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"取消工作流执行失败: {e}")
            raise

    # ==================== 内部辅助方法 ====================

    async def _validate_workflow_config(self, workflow_request: CreateWorkflowRequest) -> Dict[str, Any]:
        """验证工作流配置"""
        errors = []
        warnings = []

        try:
            # 检查节点配置
            node_ids = set()
            start_nodes = []
            end_nodes = []

            for node in workflow_request.nodes:
                # 检查节点ID唯一性
                if node.node_id in node_ids:
                    errors.append(f"重复的节点ID: {node.node_id}")
                node_ids.add(node.node_id)

                # 收集开始和结束节点
                if node.is_start_node:
                    start_nodes.append(node.node_id)
                if node.is_end_node:
                    end_nodes.append(node.node_id)

                # 验证节点类型特定的配置
                if node.node_type == NodeType.SQL and node.task_config:
                    if not (node.task_config.sql or node.task_config.sql_file):
                        errors.append(f"SQL节点 {node.node_id} 缺少SQL语句或文件配置")

                elif node.node_type == NodeType.SHELL and node.task_config:
                    if not (node.task_config.command or node.task_config.script_file):
                        errors.append(f"Shell节点 {node.node_id} 缺少命令或脚本文件配置")

                elif node.node_type == NodeType.CONDITION:
                    if not node.condition_expression:
                        errors.append(f"条件节点 {node.node_id} 缺少条件表达式")

            # 检查是否有开始和结束节点
            if not start_nodes:
                errors.append("工作流必须至少有一个开始节点")
            if not end_nodes:
                errors.append("工作流必须至少有一个结束节点")

            # 检查边配置
            if workflow_request.edges:
                edge_ids = set()
                for edge in workflow_request.edges:
                    # 检查边ID唯一性
                    if edge.edge_id in edge_ids:
                        errors.append(f"重复的边ID: {edge.edge_id}")
                    edge_ids.add(edge.edge_id)

                    # 检查源节点和目标节点是否存在
                    if edge.source_node_id not in node_ids:
                        errors.append(f"边 {edge.edge_id} 的源节点 {edge.source_node_id} 不存在")
                    if edge.target_node_id not in node_ids:
                        errors.append(f"边 {edge.edge_id} 的目标节点 {edge.target_node_id} 不存在")

                    # 检查是否有自循环
                    if edge.source_node_id == edge.target_node_id:
                        errors.append(f"边 {edge.edge_id} 不能是自循环")

                # 检查是否有循环依赖
                cycle_check_result = await self._check_workflow_cycles(workflow_request.nodes, workflow_request.edges)
                if cycle_check_result["has_cycles"]:
                    errors.extend([f"检测到循环依赖: {cycle}" for cycle in cycle_check_result["cycles"]])

            return {
                "is_valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings
            }

        except Exception as e:
            logger.error(f"验证工作流配置时出错: {e}")
            return {
                "is_valid": False,
                "errors": [f"配置验证失败: {str(e)}"],
                "warnings": warnings
            }

    async def _check_workflow_cycles(self, nodes, edges) -> Dict[str, Any]:
        """检查工作流是否存在循环依赖"""
        if not edges:
            return {"has_cycles": False, "cycles": []}

        # 构建邻接表
        graph = {}
        for node in nodes:
            graph[node.node_id] = []

        for edge in edges:
            if edge.source_node_id in graph:
                graph[edge.source_node_id].append(edge.target_node_id)

        # 使用DFS检测循环
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node, path):
            if node in rec_stack:
                # 找到循环
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(" -> ".join(cycle))
                return True

            if node in visited:
                return False

            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if dfs(neighbor, path):
                    return True

            path.pop()
            rec_stack.remove(node)
            return False

        has_cycles = False
        for node_id in graph:
            if node_id not in visited:
                if dfs(node_id, []):
                    has_cycles = True

        return {
            "has_cycles": has_cycles,
            "cycles": cycles
        }

    def _apply_search_filters(self, query, search_params: WorkflowSearchParams, user_id: Optional[str]):
        """应用搜索过滤条件"""
        # 关键词搜索
        if search_params.keyword:
            keyword = f"%{search_params.keyword}%"
            query = query.filter(
                or_(
                    WorkflowDefinition.workflow_name.like(keyword),
                    WorkflowDefinition.display_name.like(keyword),
                    WorkflowDefinition.description.like(keyword)
                )
            )

        # 分类过滤
        if search_params.category:
            query = query.filter(WorkflowDefinition.category == search_params.category)

        # 业务领域过滤
        if search_params.business_domain:
            query = query.filter(WorkflowDefinition.business_domain == search_params.business_domain)

        # 状态过滤
        if search_params.status:
            query = query.filter(WorkflowDefinition.status == search_params.status)

            # 触发方式过滤
            if search_params.trigger_type:
                query = query.filter(WorkflowDefinition.trigger_type == search_params.trigger_type)

            # 拥有者过滤
            if search_params.owner_id:
                query = query.filter(WorkflowDefinition.owner_id == search_params.owner_id)

            # 可见性过滤
            if search_params.visibility:
                if user_id:
                    # 用户可以看到自己的私有工作流和公开的工作流
                    query = query.filter(
                        or_(
                            WorkflowDefinition.visibility == "public",
                            and_(
                                WorkflowDefinition.visibility == search_params.visibility,
                                WorkflowDefinition.owner_id == user_id
                            )
                        )
                    )
                else:
                    # 未登录用户只能看到公开的工作流
                    query = query.filter(WorkflowDefinition.visibility == "public")
            else:
                # 默认权限控制
                if user_id:
                    query = query.filter(
                        or_(
                            WorkflowDefinition.visibility == "public",
                            WorkflowDefinition.owner_id == user_id
                        )
                    )
                else:
                    query = query.filter(WorkflowDefinition.visibility == "public")

            # 模板过滤
            if search_params.is_template is not None:
                query = query.filter(WorkflowDefinition.is_template == search_params.is_template)

            # 标签过滤
            if search_params.tags:
                for tag in search_params.tags:
                    query = query.filter(WorkflowDefinition.tags.contains([tag]))

            # 创建时间过滤
            if search_params.created_start:
                query = query.filter(WorkflowDefinition.created_at >= search_params.created_start)
            if search_params.created_end:
                query = query.filter(WorkflowDefinition.created_at <= search_params.created_end)

            # 只查询激活的工作流
            query = query.filter(WorkflowDefinition.is_active == True)

            return query

    def _apply_search_sorting(self, query, search_params: WorkflowSearchParams):
        """应用搜索排序"""
        sort_field = getattr(WorkflowDefinition, search_params.sort_by, WorkflowDefinition.created_at)
        if search_params.sort_order == "desc":
            query = query.order_by(desc(sort_field))
        else:
            query = query.order_by(asc(sort_field))
        return query

    async def _build_workflow_response(
            self,
            db: AsyncSession,
            workflow: WorkflowDefinition,
            include_details: bool = True
    ) -> WorkflowResponse:
        """构建工作流响应对象"""
        workflow_dict = {
            "id": workflow.id,
            "workflow_id": workflow.workflow_id,
            "workflow_name": workflow.workflow_name,
            "display_name": workflow.display_name,
            "description": workflow.description,
            "category": workflow.category,
            "business_domain": workflow.business_domain,
            "tags": workflow.tags,
            "status": workflow.status,
            "version": workflow.version,
            "trigger_type": workflow.trigger_type,
            "schedule_config": workflow.schedule_config,
            "workflow_config": workflow.workflow_config,
            "default_timeout": workflow.default_timeout,
            "max_parallel_tasks": workflow.max_parallel_tasks,
            "retry_policy": workflow.retry_policy,
            "canvas_config": workflow.canvas_config,
            "layout_config": workflow.layout_config,
            "owner_id": workflow.owner_id,
            "owner_name": workflow.owner_name,
            "visibility": workflow.visibility,
            "total_executions": workflow.total_executions,
            "successful_executions": workflow.successful_executions,
            "failed_executions": workflow.failed_executions,
            "last_execution_time": workflow.last_execution_time,
            "avg_execution_duration": workflow.avg_execution_duration,
            "is_active": workflow.is_active,
            "is_template": workflow.is_template,
            "created_at": workflow.created_at,
            "updated_at": workflow.updated_at
        }
        if include_details:
            # 添加节点信息
            if hasattr(workflow, 'nodes') and workflow.nodes:
                workflow_dict["nodes"] = [
                    self._build_node_response(node) for node in workflow.nodes
                ]
            # 添加边信息
            if hasattr(workflow, 'edges') and workflow.edges:
                workflow_dict["edges"] = [
                    self._build_edge_response(edge) for edge in workflow.edges
                ]
        return WorkflowResponse(**workflow_dict)

    def _build_node_response(self, node: WorkflowNodeDefinition) -> Dict[str, Any]:
        """构建节点响应对象"""
        return {
            "id": node.id,
            "node_id": node.node_id,
            "node_name": node.node_name,
            "display_name": node.display_name,
            "description": node.description,
            "node_type": node.node_type,
            "task_config": node.task_config,
            "timeout": node.timeout,
            "retry_times": node.retry_times,
            "retry_interval": node.retry_interval,
            "condition_expression": node.condition_expression,
            "required_cpu": node.required_cpu,
            "required_memory_mb": node.required_memory_mb,
            "position_x": node.position_x,
            "position_y": node.position_y,
            "width": node.width,
            "height": node.height,
            "style_config": node.style_config,
            "is_start_node": node.is_start_node,
            "is_end_node": node.is_end_node,
            "is_critical": node.is_critical,
            "created_at": node.created_at,
            "updated_at": node.updated_at
        }

    def _build_edge_response(self, edge: WorkflowEdgeDefinition) -> Dict[str, Any]:
        """构建边响应对象"""
        return {
            "id": edge.id,
            "edge_id": edge.edge_id,
            "edge_name": edge.edge_name,
            "source_node_id": edge.source_node_id,
            "target_node_id": edge.target_node_id,
            "condition_type": edge.condition_type,
            "condition_expression": edge.condition_expression,
            "path_config": edge.path_config,
            "style_config": edge.style_config,
            "created_at": edge.created_at,
            "updated_at": edge.updated_at
        }

    async def _build_execution_response(
            self,
            db: AsyncSession,
            execution: WorkflowExecution
    ) -> WorkflowExecutionResponse:
        """构建执行响应对象"""
        # 计算执行时长
        duration_seconds = None
        if execution.end_time and execution.start_time:
            duration_seconds = int((execution.end_time - execution.start_time).total_seconds())
        # 计算执行进度
        progress_percent = None
        if execution.total_nodes > 0:
            progress_percent = (execution.completed_nodes / execution.total_nodes) * 100
        execution_dict = {
            "id": execution.id,
            "execution_id": execution.execution_id,
            "execution_name": execution.execution_name,
            "workflow_id": execution.workflow_id,
            "workflow_version": execution.workflow_version,
            "status": execution.status,
            "trigger_type": execution.trigger_type,
            "trigger_user": execution.trigger_user,
            "start_time": execution.start_time,
            "end_time": execution.end_time,
            "execution_date": execution.execution_date,
            "total_nodes": execution.total_nodes,
            "completed_nodes": execution.completed_nodes,
            "failed_nodes": execution.failed_nodes,
            "skipped_nodes": execution.skipped_nodes,
            "result_summary": execution.result_summary,
            "error_message": execution.error_message,
            "execution_config": execution.execution_config,
            "runtime_variables": execution.runtime_variables,
            "log_file_path": execution.log_file_path,
            "metrics_data": execution.metrics_data,
            "duration_seconds": duration_seconds,
            "progress_percent": progress_percent,
            "estimated_remaining_time": None,
            "created_at": execution.created_at,
            "updated_at": execution.updated_at
        }
        return WorkflowExecutionResponse(**execution_dict)

    async def _update_workflow_nodes(
            self,
            db: AsyncSession,
            workflow_id: int,
            nodes: List
    ):
        """更新工作流节点"""
        # 删除现有节点
        await db.execute(
            delete(WorkflowNodeDefinition).where(WorkflowNodeDefinition.workflow_id == workflow_id)
        )
        # 添加新节点
        for node_req in nodes:
            node = WorkflowNodeDefinition(
                node_id=node_req.node_id,
                node_name=node_req.node_name,
                display_name=node_req.display_name,
                description=node_req.description,
                workflow_id=workflow_id,
                node_type=node_req.node_type,
                task_config=node_req.task_config.dict() if node_req.task_config else None,
                timeout=node_req.timeout,
                retry_times=node_req.retry_policy.retry_times if node_req.retry_policy else 0,
                retry_interval=node_req.retry_policy.retry_interval if node_req.retry_policy else 60,
                condition_expression=node_req.condition_expression,
                required_cpu=node_req.required_cpu,
                required_memory_mb=node_req.required_memory_mb,
                position_x=node_req.position.x if node_req.position else None,
                position_y=node_req.position.y if node_req.position else None,
                width=node_req.position.width if node_req.position else 120,
                height=node_req.position.height if node_req.position else 60,
                style_config=node_req.style.dict() if node_req.style else None,
                is_start_node=node_req.is_start_node,
                is_end_node=node_req.is_end_node,
                is_critical=node_req.is_critical
            )
            db.add(node)

    async def _update_workflow_edges(
            self,
            db: AsyncSession,
            workflow_id: int,
            edges: List
    ):
        """更新工作流边"""
        # 删除现有边
        await db.execute(
            delete(WorkflowEdgeDefinition).where(WorkflowEdgeDefinition.workflow_id == workflow_id)
        )
        # 获取节点映射
        node_result = await db.execute(
            select(WorkflowNodeDefinition.node_id, WorkflowNodeDefinition.id)
            .where(WorkflowNodeDefinition.workflow_id == workflow_id)
        )
        node_mapping = dict(node_result.all())
        # 添加新边
        for edge_req in edges:
            edge = WorkflowEdgeDefinition(
                edge_id=edge_req.edge_id,
                edge_name=edge_req.edge_name,
                workflow_id=workflow_id,
                source_node_id=node_mapping[edge_req.source_node_id],
                target_node_id=node_mapping[edge_req.target_node_id],
                condition_type=edge_req.condition_type.value,
                condition_expression=edge_req.condition_expression,
                path_config=edge_req.path_config.dict() if edge_req.path_config else None,
                style_config=edge_req.style_config.dict() if edge_req.style_config else None
            )
            db.add(edge)

    async def _update_workflow_variables(
            self,
            db: AsyncSession,
            workflow_id: int,
            variables: List
    ):
        """更新工作流变量"""
        # 删除现有变量
        await db.execute(
            delete(WorkflowVariable).where(WorkflowVariable.workflow_id == workflow_id)
        )
        # 添加新变量
        for var_req in variables:
            variable = WorkflowVariable(
                variable_key=var_req.key,
                variable_name=var_req.name,
                description=var_req.description,
                workflow_id=workflow_id,
                variable_type=var_req.type,
                default_value=var_req.default_value,
                is_required=var_req.is_required,
                is_sensitive=var_req.is_sensitive,
                validation_rules=[rule.dict() for rule in
                                  var_req.validation_rules] if var_req.validation_rules else None
            )
            db.add(variable)

    async def _execute_workflow(self, execution_id: int):
        """异步执行工作流"""
        from app.utils.database import async_session_maker
        async with async_session_maker() as db:
            try:
                # 获取执行信息
                result = await db.execute(
                    select(WorkflowExecution)
                    .options(
                        selectinload(WorkflowExecution.workflow),
                        selectinload(WorkflowExecution.node_executions)
                    )
                    .where(WorkflowExecution.id == execution_id)
                )
                execution = result.scalar_one_or_none()
                if not execution:
                    logger.error(f"执行记录不存在: {execution_id}")
                    return
                logger.info(f"开始执行工作流: {execution.execution_id}")
                # 获取工作流定义
                workflow = await self.get_workflow_by_id(
                    db, execution.workflow_id, include_nodes=True, include_edges=True
                )
                # 构建执行图
                execution_graph = await self._build_execution_graph(workflow)
                # 执行工作流
                await self._run_workflow_execution(db, execution, execution_graph)
            except Exception as e:
                logger.error(f"工作流执行失败 {execution_id}: {e}")
                # 更新执行状态为失败
                await db.execute(
                    update(WorkflowExecution)
                    .where(WorkflowExecution.id == execution_id)
                    .values(
                        status=WorkflowStatus.FAILED,
                        end_time=datetime.now(),
                        error_message=str(e)
                    )
                )
                await db.commit()

    async def _build_execution_graph(self, workflow: WorkflowResponse) -> Dict[str, Any]:
        """构建执行图"""
        graph = {
            "nodes": {},
            "edges": [],
            "start_nodes": [],
            "end_nodes": []
        }
        # 添加节点
        for node in workflow.nodes:
            graph["nodes"][node.node_id] = {
                "node": node,
                "dependencies": [],
                "dependents": []
            }
            if node.is_start_node:
                graph["start_nodes"].append(node.node_id)
            if node.is_end_node:
                graph["end_nodes"].append(node.node_id)
        # 添加边和依赖关系
        for edge in workflow.edges:
            graph["edges"].append(edge)
            # 构建依赖关系
            source_node = next(n for n in workflow.nodes if n.id == edge.source_node_id)
            target_node = next(n for n in workflow.nodes if n.id == edge.target_node_id)
            graph["nodes"][target_node.node_id]["dependencies"].append({
                "node_id": source_node.node_id,
                "condition": edge.condition_type,
                "expression": edge.condition_expression
            })
            graph["nodes"][source_node.node_id]["dependents"].append({
                "node_id": target_node.node_id,
                "condition": edge.condition_type,
                "expression": edge.condition_expression
            })
        return graph

    async def _run_workflow_execution(
            self,
            db: AsyncSession,
            execution: WorkflowExecution,
            execution_graph: Dict[str, Any]
    ):
        """运行工作流执行"""
        try:
            # 开始执行开始节点
            ready_nodes = execution_graph["start_nodes"].copy()
            completed_nodes = set()
            failed_nodes = set()
            while ready_nodes:
                # 并行执行准备好的节点
                current_batch = ready_nodes.copy()
                ready_nodes.clear()
                tasks = []
                for node_id in current_batch:
                    task = asyncio.create_task(
                        self._execute_workflow_node(db, execution.id, node_id, execution_graph)
                    )
                    tasks.append((node_id, task))
                # 等待当前批次完成
                for node_id, task in tasks:
                    try:
                        result = await task
                        if result["status"] == "success":
                            completed_nodes.add(node_id)
                        else:
                            failed_nodes.add(node_id)
                            logger.error(f"节点执行失败 {node_id}: {result.get('error')}")
                    except Exception as e:
                        failed_nodes.add(node_id)
                        logger.error(f"节点执行异常 {node_id}: {e}")
                # 检查是否有节点准备好执行
                for node_id, node_info in execution_graph["nodes"].items():
                    if node_id in completed_nodes or node_id in failed_nodes:
                        continue
                    # 检查所有依赖是否满足
                    dependencies_met = True
                    for dep in node_info["dependencies"]:
                        dep_node_id = dep["node_id"]
                        condition = dep["condition"]
                        if condition == "success" and dep_node_id not in completed_nodes:
                            dependencies_met = False
                            break
                        elif condition == "failed" and dep_node_id not in failed_nodes:
                            dependencies_met = False
                            break
                        elif condition == "always" and dep_node_id not in (completed_nodes | failed_nodes):
                            dependencies_met = False
                            break
                    if dependencies_met and node_id not in ready_nodes:
                        ready_nodes.append(node_id)
                # 如果没有更多节点可执行且工作流未完成，检查是否因为失败而停止
                if not ready_nodes:
                    remaining_nodes = set(execution_graph["nodes"].keys()) - completed_nodes - failed_nodes
                    if remaining_nodes and failed_nodes:
                        # 有失败节点导致无法继续执行
                        logger.warning(f"工作流因节点失败而终止，剩余节点: {remaining_nodes}")
                        break
            # 更新执行状态
            total_completed = len(completed_nodes)
            total_failed = len(failed_nodes)
            final_status = WorkflowStatus.SUCCESS
            if failed_nodes:
                # 检查失败的节点是否包含关键节点
                critical_failed = any(
                    execution_graph["nodes"][node_id]["node"].is_critical
                    for node_id in failed_nodes
                )
                final_status = WorkflowStatus.FAILED if critical_failed else WorkflowStatus.SUCCESS
            await db.execute(
                update(WorkflowExecution)
                .where(WorkflowExecution.id == execution.id)
                .values(
                    status=final_status,
                    end_time=datetime.now(),
                    completed_nodes=total_completed,
                    failed_nodes=total_failed
                )
            )
            await db.commit()
            logger.info(f"工作流执行完成: {execution.execution_id}, 状态: {final_status}")
        except Exception as e:
            logger.error(f"运行工作流执行失败: {e}")
            raise

    async def _execute_workflow_node(
            self,
            db: AsyncSession,
            execution_id: int,
            node_id: str,
            execution_graph: Dict[str, Any]
    ) -> Dict[str, Any]:
        """执行单个工作流节点"""
        try:
            node_info = execution_graph["nodes"][node_id]
            node = node_info["node"]
            logger.info(f"开始执行节点: {node_id} ({node.node_type})")
            # 获取节点执行记录
            result = await db.execute(
                select(WorkflowNodeExecution)
                .where(
                    and_(
                        WorkflowNodeExecution.workflow_execution_id == execution_id,
                        WorkflowNodeExecution.node_definition_id == node.id
                    )
                )
            )
            node_execution = result.scalar_one_or_none()
            if not node_execution:
                logger.error(f"节点执行记录不存在: {node_id}")
                return {"status": "error", "error": "节点执行记录不存在"}
            # 更新节点状态为运行中
            await db.execute(
                update(WorkflowNodeExecution)
                .where(WorkflowNodeExecution.id == node_execution.id)
                .values(
                    status=NodeStatus.RUNNING,
                    start_time=datetime.now()
                )
            )
            await db.commit()
            # 根据节点类型执行任务
            execution_result = await self._execute_node_task(node, node_execution)
            # 更新节点执行结果
            end_time = datetime.now()
            duration = int(
                (end_time - node_execution.start_time).total_seconds()) if node_execution.start_time else 0
            await db.execute(
                update(WorkflowNodeExecution)
                .where(WorkflowNodeExecution.id == node_execution.id)
                .values(
                    status=NodeStatus.SUCCESS if execution_result["success"] else NodeStatus.FAILED,
                    end_time=end_time,
                    duration_seconds=duration,
                    result_data=execution_result.get("result_data"),
                    output_variables=execution_result.get("output_variables"),
                    error_message=execution_result.get("error_message"),
                    error_stack_trace=execution_result.get("error_stack_trace")
                )
            )
            await db.commit()
            logger.info(f"节点执行完成: {node_id}, 状态: {'成功' if execution_result['success'] else '失败'}")
            return {
                "status": "success" if execution_result["success"] else "failed",
                "result": execution_result,
                "duration": duration
            }
        except Exception as e:
            logger.error(f"执行节点失败 {node_id}: {e}")
            # 更新节点状态为失败
            if 'node_execution' in locals():
                await db.execute(
                    update(WorkflowNodeExecution)
                    .where(WorkflowNodeExecution.id == node_execution.id)
                    .values(
                        status=NodeStatus.FAILED,
                        end_time=datetime.now(),
                        error_message=str(e)
                    )
                )
                await db.commit()
            return {"status": "error", "error": str(e)}

    async def _execute_node_task(
            self,
            node,
            node_execution: WorkflowNodeExecution
    ) -> Dict[str, Any]:
        """执行节点任务"""
        try:
            if node.node_type in [NodeType.START, NodeType.END]:
                # 开始和结束节点直接返回成功
                return {
                    "success": True,
                    "result_data": {"message": f"{node.node_type.value} node completed"}
                }
            elif node.node_type == NodeType.SQL:
                # 执行SQL任务
                return await self._execute_sql_node(node, node_execution)
            elif node.node_type == NodeType.SHELL:
                # 执行Shell任务
                return await self._execute_shell_node(node, node_execution)
            elif node.node_type == NodeType.DATAX:
                # 执行DataX任务
                return await self._execute_datax_node(node, node_execution)
            elif node.node_type == NodeType.PYTHON:
                # 执行Python任务
                return await self._execute_python_node(node, node_execution)
            elif node.node_type == NodeType.CONDITION:
                # 执行条件判断
                return await self._execute_condition_node(node, node_execution)
            elif node.node_type == NodeType.NOTIFICATION:
                # 发送通知
                return await self._execute_notification_node(node, node_execution)
            else:
                logger.warning(f"不支持的节点类型: {node.node_type}")
                return {
                    "success": False,
                    "error_message": f"不支持的节点类型: {node.node_type}"
                }
        except Exception as e:
            logger.error(f"执行节点任务失败: {e}")
            return {
                "success": False,
                "error_message": str(e),
                "error_stack_trace": str(e.__traceback__)
            }

    async def _execute_sql_node(self, node, node_execution) -> Dict[str, Any]:
        """执行SQL节点"""
        try:
            task_config = node.task_config
            if not task_config:
                return {"success": False, "error_message": "SQL任务配置为空"}
            # 构建执行器任务配置
            executor_config = {
                "task_id": f"{node_execution.node_execution_id}",
                "sql": task_config.get("sql"),
                "sql_file": task_config.get("sql_file"),
                "connection_id": task_config.get("connection_id")
            }
            # 调用执行器服务执行SQL任务
            result = await executor_service.execute_task(
                task_type="sql",
                task_config=executor_config,
                timeout=node.timeout
            )
            return {
                "success": result.status.value == "success",
                "result_data": result.result_data,
                "error_message": result.error_message,
                "output_variables": result.output_data
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e)
            }

    async def _execute_shell_node(self, node, node_execution) -> Dict[str, Any]:
        """执行Shell节点"""
        try:
            task_config = node.task_config
            if not task_config:
                return {"success": False, "error_message": "Shell任务配置为空"}
            # 构建执行器任务配置
            executor_config = {
                "task_id": f"{node_execution.node_execution_id}",
                "command": task_config.get("command"),
                "script_file": task_config.get("script_file"),
                "working_directory": task_config.get("working_directory"),
                "environment": task_config.get("environment")
            }
            # 调用执行器服务执行Shell任务
            result = await executor_service.execute_task(
                task_type="shell",
                task_config=executor_config,
                timeout=node.timeout
            )
            return {
                "success": result.status.value == "success",
                "result_data": result.result_data,
                "error_message": result.error_message
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e)
            }

    async def _execute_datax_node(self, node, node_execution) -> Dict[str, Any]:
        """执行DataX节点"""
        try:
            task_config = node.task_config
            if not task_config:
                return {"success": False, "error_message": "DataX任务配置为空"}
            # 构建执行器任务配置
            executor_config = {
                "task_id": f"{node_execution.node_execution_id}",
                "datax_config": task_config.get("datax_config"),
                "datax_config_file": task_config.get("datax_config_file")
            }
            # 调用执行器服务执行DataX任务
            result = await executor_service.execute_task(
                task_type="datax",
                task_config=executor_config,
                timeout=node.timeout
            )
            return {
                "success": result.status.value == "success",
                "result_data": result.result_data,
                "error_message": result.error_message
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e)
            }

    async def _execute_python_node(self, node, node_execution) -> Dict[str, Any]:
        """执行Python节点"""
        try:
            task_config = node.task_config
            if not task_config:
                return {"success": False, "error_message": "Python任务配置为空"}
            # 构建执行器任务配置
            executor_config = {
                "task_id": f"{node_execution.node_execution_id}",
                "python_code": task_config.get("python_code"),
                "python_file": task_config.get("python_file"),
                "dependencies": task_config.get("python_dependencies")
            }
            # 调用执行器服务执行Python任务
            result = await executor_service.execute_task(
                task_type="python",
                task_config=executor_config,
                timeout=node.timeout
            )
            return {
                "success": result.status.value == "success",
                "result_data": result.result_data,
                "error_message": result.error_message,
                "output_variables": result.output_data
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e)
            }

    async def _execute_condition_node(self, node, node_execution) -> Dict[str, Any]:
        """执行条件节点"""
        try:
            if not node.condition_expression:
                return {"success": False, "error_message": "条件表达式为空"}
            # 这里可以实现条件表达式的评估逻辑
            # 简单示例：支持基本的比较操作
            condition_result = await self._evaluate_condition_expression(
                node.condition_expression,
                node_execution
            )
            return {
                "success": True,
                "result_data": {
                    "condition_result": condition_result,
                    "expression": node.condition_expression
                },
                "output_variables": {
                    "condition_result": condition_result
                }
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": f"条件评估失败: {str(e)}"
            }
    async def _execute_notification_node(self, node, node_execution) -> Dict[str, Any]:
        """执行通知节点"""
        try:
            task_config = node.task_config
            if not task_config:
                return {"success": False, "error_message": "通知任务配置为空"}
            notification_config = task_config.get("notification")
            if not notification_config:
                return {"success": False, "error_message": "通知配置为空"}
            # 发送通知
            await self._send_workflow_notification(
                notification_config,
                node_execution,
                f"工作流节点 {node.node_name} 执行完成"
            )
            return {
                "success": True,
                "result_data": {"message": "通知发送成功"}
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": f"发送通知失败: {str(e)}"
            }
    async def _evaluate_condition_expression(
            self,
            expression: str,
            node_execution: WorkflowNodeExecution
    ) -> bool:
        """评估条件表达式"""
        try:
            # 这里实现条件表达式的评估逻辑
            # 可以支持复杂的条件表达式，包括变量替换等
            # 简单实现：支持基本的布尔表达式
            # 在实际应用中，可以使用更复杂的表达式解析器
            # 示例条件表达式：
            # "task_status == 'success'"
            # "data_count > 1000"
            # "error_rate < 0.01"
            # 获取上下文变量（从之前的节点执行结果中获取）
            context_vars = await self._get_execution_context_variables(node_execution)
            # 简单的表达式评估
            # 注意：在生产环境中需要使用安全的表达式评估器
            safe_vars = {k: v for k, v in context_vars.items() if isinstance(k, str) and k.isidentifier()}
            # 使用eval进行简单评估（生产环境中应该使用更安全的方法）
            result = eval(expression, {"__builtins__": {}}, safe_vars)
            return bool(result)
        except Exception as e:
            logger.error(f"条件表达式评估失败: {e}")
            return False
    async def _get_execution_context_variables(
            self,
            node_execution: WorkflowNodeExecution
    ) -> Dict[str, Any]:
        """获取执行上下文变量"""
        # 从工作流执行中获取运行时变量
        # 从之前完成的节点执行中获取输出变量
        # 返回合并后的上下文变量字典
        context_vars = {
            "current_time": datetime.now(),
            "execution_date": node_execution.created_at,
            "node_id": node_execution.node_execution_id
        }
        return context_vars
    async def _send_workflow_notification(
            self,
            notification_config: Dict[str, Any],
            node_execution: WorkflowNodeExecution,
            message: str
    ):
        """发送工作流通知"""
        try:
            # 集成现有的监控服务发送通知
            if hasattr(monitoring_service, 'send_notification'):
                await monitoring_service.send_notification(
                    channels=notification_config.get("channels", []),
                    recipients=notification_config.get("recipients", []),
                    message=message,
                    title=f"工作流通知 - {node_execution.node_execution_id}"
                )
            else:
                logger.warning("监控服务不支持通知功能")
        except Exception as e:
            logger.error(f"发送工作流通知失败: {e}")
            raise
    async def _invalidate_workflow_cache_patterns(self, patterns: List[str]):
        """清除工作流相关缓存"""
        try:
            for pattern in patterns:
                await cache_service.delete_pattern(pattern)
        except Exception as e:
            logger.error(f"清除缓存失败: {e}")
    # ==================== 统计和监控方法 ====================
    async def get_workflow_statistics(self, db: AsyncSession) -> WorkflowStatistics:
        """获取工作流统计信息"""
        cache_key = f"{self.cache_prefix}:statistics"
        # 尝试从缓存获取
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return WorkflowStatistics(**cached_data)
        try:
            # 总工作流数
            total_result = await db.execute(select(func.count(WorkflowDefinition.id)))
            total_workflows = total_result.scalar()
            # 活跃工作流数
            active_result = await db.execute(
                select(func.count(WorkflowDefinition.id))
                .where(WorkflowDefinition.status == WorkflowStatus.PUBLISHED)
            )
            active_workflows = active_result.scalar()
            # 草稿工作流数
            draft_result = await db.execute(
                select(func.count(WorkflowDefinition.id))
                .where(WorkflowDefinition.status == WorkflowStatus.DRAFT)
            )
            draft_workflows = draft_result.scalar()
            # 模板工作流数
            template_result = await db.execute(
                select(func.count(WorkflowDefinition.id))
                .where(WorkflowDefinition.is_template == True)
            )
            template_workflows = template_result.scalar()
            # 今日执行统计
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_total_result = await db.execute(
                select(func.count(WorkflowExecution.id))
                .where(WorkflowExecution.start_time >= today_start)
            )
            total_executions_today = today_total_result.scalar()
            today_success_result = await db.execute(
                select(func.count(WorkflowExecution.id))
                .where(
                    and_(
                        WorkflowExecution.start_time >= today_start,
                        WorkflowExecution.status == WorkflowStatus.SUCCESS
                    )
                )
            )
            successful_executions_today = today_success_result.scalar()
            today_failed_result = await db.execute(
                select(func.count(WorkflowExecution.id))
                .where(
                    and_(
                        WorkflowExecution.start_time >= today_start,
                        WorkflowExecution.status == WorkflowStatus.FAILED
                    )
                )
            )
            failed_executions_today = today_failed_result.scalar()
            # 当前运行中的执行数
            running_result = await db.execute(
                select(func.count(WorkflowExecution.id))
                .where(WorkflowExecution.status == WorkflowStatus.RUNNING)
            )
            running_executions = running_result.scalar()
            # 计算成功率
            success_rate = 0.0
            if total_executions_today > 0:
                success_rate = (successful_executions_today / total_executions_today) * 100
            # 最常用节点类型统计
            node_type_result = await db.execute(
                select(
                    WorkflowNodeDefinition.node_type,
                    func.count(WorkflowNodeDefinition.id).label('count')
                )
                .join(WorkflowDefinition)
                .where(WorkflowDefinition.is_active == True)
                .group_by(WorkflowNodeDefinition.node_type)
                .order_by(desc('count'))
                .limit(10)
            )
            most_used_node_types = [
                {"node_type": row[0].value, "count": row[1]}
                for row in node_type_result.all()
            ]
            # 工作流分类统计
            category_result = await db.execute(
                select(
                    WorkflowDefinition.category,
                    func.count(WorkflowDefinition.id).label('count')
                )
                .where(
                    and_(
                        WorkflowDefinition.is_active == True,
                        WorkflowDefinition.category.isnot(None)
                    )
                )
                .group_by(WorkflowDefinition.category)
                .order_by(desc('count'))
                .limit(10)
            )
            workflow_categories = [
                {"category": row[0], "count": row[1]}
                for row in category_result.all()
            ]
            statistics = WorkflowStatistics(
                total_workflows=total_workflows,
                active_workflows=active_workflows,
                draft_workflows=draft_workflows,
                template_workflows=template_workflows,
                total_executions_today=total_executions_today,
                successful_executions_today=successful_executions_today,
                failed_executions_today=failed_executions_today,
                running_executions=running_executions,
                avg_execution_duration=None,
                success_rate=success_rate,
                most_used_node_types=most_used_node_types,
                workflow_categories=workflow_categories
            )
            # 缓存统计结果
            await cache_service.set(cache_key, statistics.dict(), self.cache_ttl)
            return statistics
        except Exception as e:
            logger.error(f"获取工作流统计信息失败: {e}")
            raise
    async def publish_workflow(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Dict[str, Any]:
        """发布工作流"""
        try:
            # 获取工作流
            workflow = await self.get_workflow_by_id(db, workflow_id, include_nodes=True, include_edges=True)
            if not workflow:
                raise ValueError(f"工作流不存在: {workflow_id}")
            if workflow.status == WorkflowStatus.PUBLISHED:
                return {"success": True, "message": "工作流已经是发布状态"}
            # 验证工作流配置
            validation_result = await self._validate_workflow_config_for_publish(workflow)
            if not validation_result["is_valid"]:
                raise ValueError(f"工作流发布前验证失败: {validation_result['errors']}")
            # 更新工作流状态
            await db.execute(
                update(WorkflowDefinition)
                .where(WorkflowDefinition.id == workflow_id)
                .values(status=WorkflowStatus.PUBLISHED)
            )
            # 如果配置了调度，生成对应的DAG
            if workflow.trigger_type == TriggerType.SCHEDULE and workflow.schedule_config:
                await self._generate_workflow_dag(workflow)
            await db.commit()
            # 清除缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:detail:{workflow_id}:*",
                f"{self.cache_prefix}:list:*",
                f"{self.cache_prefix}:statistics"
            ])
            logger.info(f"工作流发布成功: {workflow.workflow_id}")
            return {
                "success": True,
                "message": f"工作流 '{workflow.workflow_name}' 发布成功",
                "workflow_id": workflow.workflow_id,
                "published_at": datetime.now().isoformat()
            }
        except Exception as e:
            await db.rollback()
            logger.error(f"发布工作流失败: {e}")
            raise
    async def _validate_workflow_config_for_publish(self, workflow: WorkflowResponse) -> Dict[str, Any]:
        """验证工作流发布前的配置"""
        errors = []
        warnings = []
        try:
            # 检查是否有开始节点和结束节点
            start_nodes = [node for node in workflow.nodes if node.is_start_node]
            end_nodes = [node for node in workflow.nodes if node.is_end_node]
            if not start_nodes:
                errors.append("工作流必须有至少一个开始节点")
            if not end_nodes:
                errors.append("工作流必须有至少一个结束节点")
            # 检查节点连通性
            if len(workflow.nodes) > 1 and not workflow.edges:
                errors.append("多节点工作流必须定义节点间的依赖关系")
            # 检查调度配置
            if workflow.trigger_type == TriggerType.SCHEDULE:
                if not workflow.schedule_config:
                    errors.append("调度触发的工作流必须配置调度参数")
                elif not workflow.schedule_config.get("schedule_interval"):
                    errors.append("调度配置缺少调度表达式")
            # 检查节点配置完整性
            for node in workflow.nodes:
                if node.node_type == NodeType.SQL and node.task_config:
                    if not (node.task_config.get("sql") or node.task_config.get("sql_file")):
                        errors.append(f"SQL节点 {node.node_id} 缺少SQL语句或文件配置")
                elif node.node_type == NodeType.SHELL and node.task_config:
                    if not (node.task_config.get("command") or node.task_config.get("script_file")):
                        errors.append(f"Shell节点 {node.node_id} 缺少命令或脚本文件配置")
            return {
                "is_valid": len(errors) == 0,
                "errors": errors,
                "warnings": warnings
            }
        except Exception as e:
            return {
                "is_valid": False,
                "errors": [f"验证配置时出错: {str(e)}"],
                "warnings": warnings
            }
    async def _generate_workflow_dag(self, workflow: WorkflowResponse):
        """为工作流生成DAG"""
        try:
            # 构建DAG配置
            dag_config = {
                "dag_id": f"workflow_{workflow.workflow_id}",
                "display_name": workflow.display_name,
                "description": workflow.description,
                "task_type": "multi",
                "tasks": self._convert_nodes_to_dag_tasks(workflow.nodes),
                "dependencies": self._convert_edges_to_dag_dependencies(workflow.nodes, workflow.edges),
                "owner": workflow.owner_name or "workflow-system"
            }
            # 调度配置
            schedule_config = None
            if workflow.schedule_config:
                schedule_config = {
                    "schedule_interval": workflow.schedule_config.get("schedule_interval"),
                    "start_date": workflow.schedule_config.get("start_date",
                                                               datetime.now().strftime("%Y-%m-%d")),
                    "catchup": workflow.schedule_config.get("catchup", False),
                    "max_active_runs": workflow.schedule_config.get("max_active_runs", 1)
                }
            # 生成DAG
            result = await dag_generator_service.generate_dag(dag_config, schedule_config)
            if result["success"]:
                logger.info(f"工作流DAG生成成功: {dag_config['dag_id']}")
            else:
                logger.error(f"工作流DAG生成失败: {result.get('error')}")
                raise ValueError(f"DAG生成失败: {result.get('error')}")
        except Exception as e:
            logger.error(f"生成工作流DAG失败: {e}")
            raise
    def _convert_nodes_to_dag_tasks(self, nodes) -> List[Dict[str, Any]]:
        """将工作流节点转换为DAG任务"""
        dag_tasks = []
        for node in nodes:
            if node.node_type in [NodeType.START, NodeType.END]:
                # 开始和结束节点转换为DummyOperator
                dag_tasks.append({
                    "task_id": node.node_id,
                    "type": "dummy",
                    "description": node.description or f"{node.node_type.value} node"
                })
            elif node.node_type == NodeType.SQL:
                # SQL节点
                task_config = node.task_config or {}
                dag_tasks.append({
                    "task_id": node.node_id,
                    "type": "sql",
                    "description": node.description,
                    "sql": task_config.get("sql"),
                    "sql_file": task_config.get("sql_file"),
                    "connection": {
                        "connection_id": task_config.get("connection_id")
                    },
                    "timeout": node.timeout
                })
            elif node.node_type == NodeType.SHELL:
                # Shell节点
                task_config = node.task_config or {}
                dag_tasks.append({
                    "task_id": node.node_id,
                    "type": "shell",
                    "description": node.description,
                    "command": task_config.get("command"),
                    "script_file": task_config.get("script_file"),
                    "timeout": node.timeout
                })
            elif node.node_type == NodeType.DATAX:
                # DataX节点
                task_config = node.task_config or {}
                dag_tasks.append({
                    "task_id": node.node_id,
                    "type": "datax",
                    "description": node.description,
                    "config": task_config.get("datax_config"),
                    "config_file": task_config.get("datax_config_file"),
                    "timeout": node.timeout
                })
            else:
                # 其他类型节点暂时转换为Python任务
                dag_tasks.append({
                    "task_id": node.node_id,
                    "type": "python",
                    "description": node.description,
                    "python_code": f"print('Executing {node.node_type.value} node: {node.node_id}')",
                    "timeout": node.timeout
                })
        return dag_tasks
    def _convert_edges_to_dag_dependencies(self, nodes, edges) -> List[Dict[str, str]]:
        """将工作流边转换为DAG依赖关系"""
        dependencies = []
        # 创建节点ID映射
        node_id_map = {node.id: node.node_id for node in nodes}
        for edge in edges:
            source_node_id = node_id_map.get(edge.source_node_id)
            target_node_id = node_id_map.get(edge.target_node_id)
            if source_node_id and target_node_id:
                dependencies.append({
                    "upstream": source_node_id,
                    "downstream": target_node_id
                })
        return dependencies

    async def update_workflow_status(
            self,
            db: AsyncSession,
            workflow_id: int,
            status: str,
            reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """更新工作流状态"""
        try:
            result = await db.execute(
                select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                raise ValueError("工作流不存在")

            old_status = workflow.status
            workflow.status = WorkflowStatus(status)
            workflow.updated_at = datetime.now()

            await db.commit()

            # 清除缓存
            await self._invalidate_workflow_cache_patterns([
                f"{self.cache_prefix}:detail:{workflow_id}:*",
                f"{self.cache_prefix}:list:*"
            ])

            logger.info(f"工作流状态更新: {workflow_id} {old_status} -> {status}")

            return {
                "workflow_id": workflow_id,
                "old_status": old_status.value if old_status else None,
                "new_status": status,
                "reason": reason,
                "message": f"工作流状态已更新为: {status}"
            }
        except Exception as e:
            await db.rollback()
            raise

    async def get_workflow_executions(
            self,
            db: AsyncSession,
            workflow_id: int,
            status: Optional[str] = None,
            start_time_begin: Optional[datetime] = None,
            start_time_end: Optional[datetime] = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """获取工作流执行历史"""
        try:
            # 构建查询条件
            conditions = [WorkflowExecution.workflow_id == workflow_id]

            if status:
                conditions.append(WorkflowExecution.status == status)
            if start_time_begin:
                conditions.append(WorkflowExecution.start_time >= start_time_begin)
            if start_time_end:
                conditions.append(WorkflowExecution.start_time <= start_time_end)

            # 查询总数
            count_result = await db.execute(
                select(func.count()).select_from(WorkflowExecution).where(and_(*conditions))
            )
            total = count_result.scalar()

            # 分页查询
            offset = (page - 1) * page_size
            result = await db.execute(
                select(WorkflowExecution)
                .where(and_(*conditions))
                .order_by(desc(WorkflowExecution.start_time))
                .offset(offset)
                .limit(page_size)
            )
            executions = result.scalars().all()

            # 构建响应
            execution_list = []
            for exec in executions:
                execution_list.append({
                    "execution_id": exec.execution_id,
                    "workflow_id": exec.workflow_id,
                    "status": exec.status.value,
                    "start_time": exec.start_time,
                    "end_time": exec.end_time,
                    "duration_seconds": exec.duration_seconds,
                    "trigger_type": exec.trigger_type,
                    "trigger_user": exec.trigger_user,
                    "input_variables": exec.input_variables,
                    "output_variables": exec.output_variables,
                    "error_message": exec.error_message
                })

            total_pages = (total + page_size - 1) // page_size

            return {
                "executions": execution_list,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        except Exception as e:
            logger.error(f"获取工作流执行历史失败: {e}")
            raise

    def _build_node_execution_response(self, node_execution: WorkflowNodeExecution) -> Dict[str, Any]:
        """构建节点执行响应对象"""
        return {
            "execution_id": node_execution.execution_id,
            "node_id": node_execution.node_id,
            "node_name": node_execution.node_name,
            "status": node_execution.status.value,
            "start_time": node_execution.start_time,
            "end_time": node_execution.end_time,
            "duration_seconds": node_execution.duration_seconds,
            "retry_count": node_execution.retry_count,
            "result_data": node_execution.result_data,
            "output_variables": node_execution.output_variables,
            "error_message": node_execution.error_message,
            "created_at": node_execution.created_at,
            "updated_at": node_execution.updated_at
        }

    # 在 workflow_orchestration_service.py 文件顶部添加这些导入

    import os
    import json
    import uuid
    from datetime import datetime, timedelta
    from typing import Dict, List, Optional, Any, Tuple

    # 添加新的模型导入
    from app.models.workflow import (
        WorkflowDefinition, WorkflowNodeDefinition, WorkflowEdgeDefinition,
        WorkflowExecution, WorkflowNodeExecution, WorkflowTemplate,
        WorkflowVariable, WorkflowAlert,  # 新增 WorkflowAlert
        WorkflowStatus, NodeType, NodeStatus, TriggerType
    )

    # 添加新的schema导入
    from app.schemas.workflow import (
        CreateWorkflowRequest, UpdateWorkflowRequest, TriggerWorkflowRequest,
        WorkflowResponse, WorkflowListResponse, WorkflowSearchParams,
        WorkflowExecutionResponse, WorkflowStatistics,
        CreateWorkflowTemplateRequest, CreateWorkflowFromTemplateRequest,
        WorkflowExportRequest, WorkflowImportRequest, WorkflowImportResult,
        CreateWorkflowAlertRequest
    )

    # ==================== 剩余的服务方法 ====================

    # 在 WorkflowOrchestrationService 类中继续添加：

    async def get_workflow_alerts(
            self,
            db: AsyncSession,
            workflow_id: int,
            alert_type: Optional[str] = None,
            severity: Optional[str] = None,
            is_enabled: Optional[bool] = None,
            page: int = 1,
            page_size: int = 20
    ) -> Dict[str, Any]:
        """获取工作流告警列表"""
        try:
            conditions = [WorkflowAlert.workflow_id == workflow_id]

            if alert_type:
                conditions.append(WorkflowAlert.alert_type == alert_type)
            if severity:
                conditions.append(WorkflowAlert.severity == severity)
            if is_enabled is not None:
                conditions.append(WorkflowAlert.is_enabled == is_enabled)

            # 查询总数
            count_result = await db.execute(
                select(func.count()).select_from(WorkflowAlert).where(and_(*conditions))
            )
            total = count_result.scalar()

            # 分页查询
            offset = (page - 1) * page_size
            result = await db.execute(
                select(WorkflowAlert)
                .where(and_(*conditions))
                .order_by(desc(WorkflowAlert.created_at))
                .offset(offset)
                .limit(page_size)
            )
            alerts = result.scalars().all()

            alert_list = []
            for alert in alerts:
                alert_list.append({
                    "id": alert.id,
                    "alert_name": alert.alert_name,
                    "description": alert.description,
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "is_enabled": alert.is_enabled,
                    "notification_channels": alert.notification_channels,
                    "recipients": alert.recipients,
                    "created_at": alert.created_at,
                    "updated_at": alert.updated_at
                })

            total_pages = (total + page_size - 1) // page_size

            return {
                "alerts": alert_list,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }

        except Exception as e:
            logger.error(f"获取工作流告警列表失败: {e}")
            raise

    async def get_workflow_alert_by_id(
            self,
            db: AsyncSession,
            alert_id: int
    ) -> Optional[Dict[str, Any]]:
        """根据ID获取告警详情"""
        try:
            result = await db.execute(
                select(WorkflowAlert).where(WorkflowAlert.id == alert_id)
            )
            alert = result.scalar_one_or_none()

            if not alert:
                return None

            return {
                "id": alert.id,
                "alert_name": alert.alert_name,
                "description": alert.description,
                "workflow_id": alert.workflow_id,
                "alert_type": alert.alert_type,
                "severity": alert.severity,
                "conditions": alert.conditions,
                "condition_logic": alert.condition_logic,
                "notification_channels": alert.notification_channels,
                "recipients": alert.recipients,
                "is_enabled": alert.is_enabled,
                "suppress_duration_minutes": alert.suppress_duration_minutes,
                "max_alerts_per_hour": alert.max_alerts_per_hour,
                "custom_message_template": alert.custom_message_template,
                "include_execution_context": alert.include_execution_context,
                "auto_resolve": alert.auto_resolve,
                "resolve_conditions": alert.resolve_conditions,
                "created_by": alert.created_by,
                "created_at": alert.created_at,
                "updated_at": alert.updated_at
            }

        except Exception as e:
            logger.error(f"获取告警详情失败: {e}")
            raise

    async def update_workflow_alert(
            self,
            db: AsyncSession,
            alert_id: int,
            alert_request: CreateWorkflowAlertRequest
    ) -> Dict[str, Any]:
        """更新告警规则"""
        try:
            result = await db.execute(
                select(WorkflowAlert).where(WorkflowAlert.id == alert_id)
            )
            alert = result.scalar_one_or_none()

            if not alert:
                raise ValueError("告警规则不存在")

            # 更新告警信息
            alert.alert_name = alert_request.alert_name
            alert.description = alert_request.description
            alert.alert_type = alert_request.alert_type.value
            alert.severity = alert_request.severity
            alert.conditions = [condition.dict() for condition in alert_request.conditions]
            alert.condition_logic = alert_request.condition_logic
            alert.notification_channels = [channel.value for channel in alert_request.notification_channels]
            alert.recipients = alert_request.recipients
            alert.is_enabled = alert_request.is_enabled
            alert.suppress_duration_minutes = alert_request.suppress_duration_minutes
            alert.max_alerts_per_hour = alert_request.max_alerts_per_hour
            alert.custom_message_template = alert_request.custom_message_template
            alert.include_execution_context = alert_request.include_execution_context
            alert.auto_resolve = alert_request.auto_resolve
            alert.resolve_conditions = [condition.dict() for condition in
                                        alert_request.resolve_conditions] if alert_request.resolve_conditions else None
            alert.updated_at = datetime.now()

            await db.commit()

            logger.info(f"告警规则更新成功: {alert_id}")

            return {
                "alert_id": alert_id,
                "alert_name": alert.alert_name,
                "updated_at": alert.updated_at
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"更新告警规则失败: {e}")
            raise

    async def delete_workflow_alert(
            self,
            db: AsyncSession,
            alert_id: int
    ) -> Dict[str, Any]:
        """删除告警规则"""
        try:
            result = await db.execute(
                select(WorkflowAlert).where(WorkflowAlert.id == alert_id)
            )
            alert = result.scalar_one_or_none()

            if not alert:
                raise ValueError("告警规则不存在")

            alert_name = alert.alert_name

            # 删除告警规则
            await db.delete(alert)
            await db.commit()

            logger.info(f"告警规则删除成功: {alert_id}")

            return {
                "alert_id": alert_id,
                "alert_name": alert_name,
                "message": "告警规则删除成功"
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"删除告警规则失败: {e}")
            raise

    async def test_workflow_alert(
            self,
            db: AsyncSession,
            alert_id: int,
            test_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """测试告警规则"""
        try:
            alert = await self.get_workflow_alert_by_id(db, alert_id)
            if not alert:
                raise ValueError("告警规则不存在")

            # 模拟测试数据
            if not test_data:
                test_data = {
                    "execution_duration": 3600,  # 1小时
                    "error_count": 1,
                    "success_rate": 0.8,
                    "memory_usage": 1024  # MB
                }

            # 评估告警条件
            triggered_conditions = []
            for condition in alert["conditions"]:
                metric = condition["metric"]
                operator = condition["operator"]
                threshold = condition["threshold"]

                if metric in test_data:
                    value = test_data[metric]
                    triggered = self._evaluate_condition(value, operator, threshold)

                    if triggered:
                        triggered_conditions.append({
                            "metric": metric,
                            "value": value,
                            "operator": operator,
                            "threshold": threshold,
                            "triggered": True
                        })

            # 根据条件逻辑判断是否触发告警
            should_trigger = False
            if alert["condition_logic"] == "AND":
                should_trigger = len(triggered_conditions) == len(alert["conditions"])
            elif alert["condition_logic"] == "OR":
                should_trigger = len(triggered_conditions) > 0

            return {
                "alert_id": alert_id,
                "alert_name": alert["alert_name"],
                "test_data": test_data,
                "triggered_conditions": triggered_conditions,
                "should_trigger": should_trigger,
                "notification_channels": alert["notification_channels"] if should_trigger else [],
                "recipients": alert["recipients"] if should_trigger else [],
                "test_time": datetime.now()
            }

        except Exception as e:
            logger.error(f"测试告警规则失败: {e}")
            raise

    async def get_workflow_variables(
            self,
            db: AsyncSession,
            workflow_id: int,
            variable_type: Optional[str] = None,
            is_required: Optional[bool] = None,
            is_sensitive: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """获取工作流变量"""
        try:
            conditions = [WorkflowVariable.workflow_id == workflow_id]

            if variable_type:
                conditions.append(WorkflowVariable.variable_type == variable_type)
            if is_required is not None:
                conditions.append(WorkflowVariable.is_required == is_required)
            if is_sensitive is not None:
                conditions.append(WorkflowVariable.is_sensitive == is_sensitive)

            result = await db.execute(
                select(WorkflowVariable)
                .where(and_(*conditions))
                .order_by(WorkflowVariable.variable_key)
            )
            variables = result.scalars().all()

            variable_list = []
            for variable in variables:
                variable_dict = {
                    "id": variable.id,
                    "variable_key": variable.variable_key,
                    "variable_name": variable.variable_name,
                    "description": variable.description,
                    "variable_type": variable.variable_type,
                    "is_required": variable.is_required,
                    "is_sensitive": variable.is_sensitive,
                    "validation_rules": variable.validation_rules,
                    "created_at": variable.created_at,
                    "updated_at": variable.updated_at
                }

                # 敏感变量不返回默认值
                if not variable.is_sensitive:
                    variable_dict["default_value"] = variable.default_value

                variable_list.append(variable_dict)

            return variable_list

        except Exception as e:
            logger.error(f"获取工作流变量失败: {e}")
            raise

    async def update_workflow_variables(
            self,
            db: AsyncSession,
            workflow_id: int,
            variables: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """更新工作流变量"""
        try:
            # 验证工作流是否存在
            workflow = await self.get_workflow_by_id(db, workflow_id, False, False)
            if not workflow:
                raise ValueError("工作流不存在")

            # 删除现有变量
            await db.execute(
                delete(WorkflowVariable).where(WorkflowVariable.workflow_id == workflow_id)
            )

            # 添加新变量
            created_variables = []
            for var_data in variables:
                variable = WorkflowVariable(
                    variable_key=var_data["key"],
                    variable_name=var_data["name"],
                    description=var_data.get("description"),
                    workflow_id=workflow_id,
                    variable_type=var_data.get("type", "string"),
                    default_value=var_data.get("default_value"),
                    is_required=var_data.get("is_required", False),
                    is_sensitive=var_data.get("is_sensitive", False),
                    validation_rules=var_data.get("validation_rules")
                )
                db.add(variable)
                created_variables.append({
                    "key": variable.variable_key,
                    "name": variable.variable_name,
                    "type": variable.variable_type
                })

            await db.commit()

            logger.info(f"工作流变量更新成功: {workflow_id}, 变量数量: {len(variables)}")

            return {
                "workflow_id": workflow_id,
                "variable_count": len(variables),
                "variables": created_variables,
                "updated_at": datetime.now()
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"更新工作流变量失败: {e}")
            raise

    async def set_workflow_schedule(
            self,
            db: AsyncSession,
            workflow_id: int,
            schedule_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """设置工作流调度"""
        try:
            result = await db.execute(
                select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                raise ValueError("工作流不存在")

            # 验证调度配置
            if "cron_expression" not in schedule_config and "interval_seconds" not in schedule_config:
                raise ValueError("必须提供cron表达式或间隔秒数")

            # 更新调度配置
            workflow.schedule_config = schedule_config
            workflow.trigger_type = TriggerType.SCHEDULED
            workflow.updated_at = datetime.now()

            await db.commit()

            logger.info(f"工作流调度设置成功: {workflow_id}")

            return {
                "workflow_id": workflow_id,
                "schedule_config": schedule_config,
                "trigger_type": "scheduled",
                "updated_at": workflow.updated_at
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"设置工作流调度失败: {e}")
            raise

    async def remove_workflow_schedule(
            self,
            db: AsyncSession,
            workflow_id: int
    ) -> Dict[str, Any]:
        """删除工作流调度"""
        try:
            result = await db.execute(
                select(WorkflowDefinition).where(WorkflowDefinition.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()

            if not workflow:
                raise ValueError("工作流不存在")

            # 清除调度配置
            workflow.schedule_config = None
            workflow.trigger_type = TriggerType.MANUAL
            workflow.updated_at = datetime.now()

            await db.commit()

            logger.info(f"工作流调度删除成功: {workflow_id}")

            return {
                "workflow_id": workflow_id,
                "message": "工作流调度已删除",
                "trigger_type": "manual",
                "updated_at": workflow.updated_at
            }

        except Exception as e:
            await db.rollback()
            logger.error(f"删除工作流调度失败: {e}")
            raise

    async def get_export_file(
            self,
            db: AsyncSession,
            export_task_id: str
    ) -> Optional[Dict[str, Any]]:
        """获取导出文件信息"""
        try:
            task_status = await self.get_export_task_status(db, export_task_id)

            if task_status.get("status") != "completed":
                return None

            file_path = task_status.get("file_path")
            if not file_path or not os.path.exists(file_path):
                return None

            return {
                "file_path": file_path,
                "filename": os.path.basename(file_path),
                "file_size": os.path.getsize(file_path),
                "media_type": "application/octet-stream"
            }

        except Exception as e:
            logger.error(f"获取导出文件失败: {e}")
            return None

    # ==================== 私有辅助方法 ====================

    def _evaluate_condition(self, value: float, operator: str, threshold: float) -> bool:
        """评估告警条件"""
        if operator == "gt":
            return value > threshold
        elif operator == "lt":
            return value < threshold
        elif operator == "gte":
            return value >= threshold
        elif operator == "lte":
            return value <= threshold
        elif operator == "eq":
            return value == threshold
        elif operator == "ne":
            return value != threshold
        else:
            return False

    async def _parse_import_data(self, data: str, format_type: str) -> Dict[str, Any]:
        """解析导入数据"""
        try:
            if format_type == "json":
                return json.loads(data)
            elif format_type == "yaml":
                import yaml
                return yaml.safe_load(data)
            else:
                raise ValueError(f"不支持的导入格式: {format_type}")
        except Exception as e:
            raise ValueError(f"解析导入数据失败: {e}")

    async def _import_workflows(
            self,
            db: AsyncSession,
            workflows: List[Dict[str, Any]],
            import_request: WorkflowImportRequest,
            importer_id: Optional[str]
    ) -> Dict[str, Any]:
        """导入工作流数据"""
        try:
            success_results = []
            failed_results = []
            skipped_results = []

            for workflow_data in workflows:
                try:
                    workflow_name = workflow_data.get("workflow_name")

                    # 检查名称冲突
                    existing = await db.execute(
                        select(WorkflowDefinition).where(
                            WorkflowDefinition.workflow_name == workflow_name
                        )
                    )
                    existing_workflow = existing.scalar_one_or_none()

                    if existing_workflow:
                        if import_request.conflict_resolution == "skip":
                            skipped_results.append({
                                "original_name": workflow_name,
                                "imported_name": workflow_name,
                                "status": "skipped",
                                "message": "工作流名称已存在，跳过导入"
                            })
                            continue
                        elif import_request.conflict_resolution == "rename":
                            workflow_name = f"{workflow_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            workflow_data["workflow_name"] = workflow_name

                    # 创建工作流（这里需要根据实际数据结构调整）
                    # workflow = await self.create_workflow(db, workflow_data, importer_id)

                    success_results.append({
                        "original_name": workflow_data.get("workflow_name"),
                        "imported_name": workflow_name,
                        "workflow_id": None,  # workflow.id,
                        "status": "success",
                        "message": "导入成功"
                    })

                except Exception as e:
                    failed_results.append({
                        "original_name": workflow_data.get("workflow_name", "Unknown"),
                        "imported_name": workflow_data.get("workflow_name", "Unknown"),
                        "status": "failed",
                        "message": str(e)
                    })

            return {
                "total_count": len(workflows),
                "success_count": len(success_results),
                "failed_count": len(failed_results),
                "skipped_count": len(skipped_results),
                "success_results": success_results,
                "failed_results": failed_results,
                "skipped_results": skipped_results
            }

        except Exception as e:
            logger.error(f"导入工作流数据失败: {e}")
            raise

    async def _generate_export_file(
            self,
            export_data: Dict[str, Any],
            export_request: WorkflowExportRequest,
            export_task_id: str
    ) -> str:
        """生成导出文件"""
        try:
            # 创建导出目录
            export_dir = "/tmp/workflow_exports"
            os.makedirs(export_dir, exist_ok=True)

            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"workflow_export_{export_task_id}_{timestamp}"

            if export_request.export_format == "json":
                filename += ".json"
                file_path = os.path.join(export_dir, filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
            elif export_request.export_format == "yaml":
                import yaml
                filename += ".yaml"
                file_path = os.path.join(export_dir, filename)
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml.dump(export_data, f, allow_unicode=True, default_flow_style=False)
            else:
                raise ValueError(f"不支持的导出格式: {export_request.export_format}")

            # 如果需要压缩
            if export_request.compress:
                import gzip
                compressed_path = file_path + ".gz"
                with open(file_path, 'rb') as f_in:
                    with gzip.open(compressed_path, 'wb') as f_out:
                        f_out.writelines(f_in)
                os.remove(file_path)  # 删除原文件
                file_path = compressed_path

            return file_path

        except Exception as e:
            logger.error(f"生成导出文件失败: {e}")
            raise

# 创建全局工作流编排服务实例
workflow_orchestration_service = WorkflowOrchestrationService()
