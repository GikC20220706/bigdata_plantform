"""
Airflow API客户端
提供与Airflow REST API交互的封装
"""

import aiohttp
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from loguru import logger

from config.airflow_config import airflow_config


class AirflowClient:
    """Airflow API客户端"""

    def __init__(self):
        self.base_url = airflow_config.airflow_api_url
        self.headers = airflow_config.auth_headers
        self.timeout = aiohttp.ClientTimeout(total=30)

    async def _request(
            self,
            method: str,
            endpoint: str,
            data: Optional[Dict] = None,
            params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """发起HTTP请求"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.request(
                        method=method,
                        url=url,
                        headers=self.headers,
                        json=data if data else None,
                        params=params if params else None
                ) as response:
                    # 记录请求信息
                    logger.debug(f"Airflow API: {method} {url} -> {response.status}")

                    if response.status >= 400:
                        error_text = await response.text()
                        logger.error(f"Airflow API错误: {response.status} - {error_text}")
                        raise AirflowAPIError(
                            f"API请求失败: {response.status}",
                            status_code=response.status,
                            response_text=error_text
                        )

                    return await response.json()

        except aiohttp.ClientError as e:
            logger.error(f"Airflow连接错误: {e}")
            raise AirflowConnectionError(f"无法连接到Airflow: {e}")
        except asyncio.TimeoutError:
            logger.error("Airflow API请求超时")
            raise AirflowConnectionError("Airflow API请求超时")

    # ==================== DAG管理 ====================

    async def get_dags(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """获取DAG列表"""
        params = {"limit": limit, "offset": offset}
        return await self._request("GET", "dags", params=params)

    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """获取指定DAG详情"""
        return await self._request("GET", f"dags/{dag_id}")

    async def trigger_dag(
            self,
            dag_id: str,
            conf: Optional[Dict] = None,
            execution_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """触发DAG运行"""
        data = {
            "conf": conf or {},
        }
        if execution_date:
            data["execution_date"] = execution_date

        return await self._request("POST", f"dags/{dag_id}/dagRuns", data=data)

    async def pause_dag(self, dag_id: str, is_paused: bool = True) -> Dict[str, Any]:
        """暂停或启用DAG"""
        data = {"is_paused": is_paused}
        return await self._request("PATCH", f"dags/{dag_id}", data=data)

    async def delete_dag(self, dag_id: str) -> bool:
        """删除DAG"""
        try:
            await self._request("DELETE", f"dags/{dag_id}")
            return True
        except AirflowAPIError as e:
            if e.status_code == 404:
                logger.warning(f"DAG {dag_id} 不存在")
                return True
            raise

    # ==================== DAG运行管理 ====================

    async def get_dag_runs(
            self,
            dag_id: str,
            limit: int = 25,
            offset: int = 0,
            execution_date_gte: Optional[str] = None,
            execution_date_lte: Optional[str] = None,
            state: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """获取DAG运行历史"""
        params = {"limit": limit, "offset": offset}

        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte
        if state:
            params["state"] = state

        return await self._request("GET", f"dags/{dag_id}/dagRuns", params=params)

    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """获取指定DAG运行详情"""
        return await self._request("GET", f"dags/{dag_id}/dagRuns/{dag_run_id}")

    async def delete_dag_run(self, dag_id: str, dag_run_id: str) -> bool:
        """删除DAG运行记录"""
        try:
            await self._request("DELETE", f"dags/{dag_id}/dagRuns/{dag_run_id}")
            return True
        except AirflowAPIError as e:
            if e.status_code == 404:
                return True
            raise

    # ==================== 任务管理 ====================

    async def get_task_instances(
            self,
            dag_id: str,
            dag_run_id: str,
            limit: int = 100
    ) -> Dict[str, Any]:
        """获取任务实例列表"""
        params = {"limit": limit}
        return await self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=params
        )

    async def get_task_instance(
            self,
            dag_id: str,
            dag_run_id: str,
            task_id: str
    ) -> Dict[str, Any]:
        """获取任务实例详情"""
        return await self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )

    async def get_task_logs(
            self,
            dag_id: str,
            dag_run_id: str,
            task_id: str,
            task_try_number: int = 1
    ) -> Dict[str, Any]:
        """获取任务日志"""
        return await self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
        )

    # ==================== 连接和变量管理 ====================

    async def get_connections(self, limit: int = 100) -> Dict[str, Any]:
        """获取连接列表"""
        params = {"limit": limit}
        return await self._request("GET", "connections", params=params)

    async def create_connection(self, connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建连接"""
        return await self._request("POST", "connections", data=connection_data)

    async def update_connection(self, conn_id: str, connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新连接"""
        return await self._request("PATCH", f"connections/{conn_id}", data=connection_data)

    async def delete_connection(self, conn_id: str) -> bool:
        """删除连接"""
        try:
            await self._request("DELETE", f"connections/{conn_id}")
            return True
        except AirflowAPIError as e:
            if e.status_code == 404:
                return True
            raise

    async def get_variables(self, limit: int = 100) -> Dict[str, Any]:
        """获取变量列表"""
        params = {"limit": limit}
        return await self._request("GET", "variables", params=params)

    async def create_variable(self, key: str, value: str, description: Optional[str] = None) -> Dict[str, Any]:
        """创建变量"""
        data = {"key": key, "value": value}
        if description:
            data["description"] = description
        return await self._request("POST", "variables", data=data)

    # ==================== 健康检查和状态 ====================

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return await self._request("GET", "health")

    async def get_version(self) -> Dict[str, Any]:
        """获取Airflow版本信息"""
        return await self._request("GET", "version")

    async def get_config(self) -> Dict[str, Any]:
        """获取配置信息"""
        return await self._request("GET", "config")

    # ==================== 批量操作 ====================

    async def batch_trigger_dags(self, dag_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量触发DAG"""
        results = []
        for config in dag_configs:
            try:
                result = await self.trigger_dag(
                    dag_id=config["dag_id"],
                    conf=config.get("conf"),
                    execution_date=config.get("execution_date")
                )
                results.append({
                    "dag_id": config["dag_id"],
                    "success": True,
                    "result": result
                })
            except Exception as e:
                results.append({
                    "dag_id": config["dag_id"],
                    "success": False,
                    "error": str(e)
                })
        return results


class AirflowAPIError(Exception):
    """Airflow API错误"""

    def __init__(self, message: str, status_code: int = None, response_text: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class AirflowConnectionError(Exception):
    """Airflow连接错误"""
    pass


# 创建全局客户端实例
airflow_client = AirflowClient()


# ==================== 便捷函数 ====================

async def ensure_airflow_connection() -> bool:
    """确保Airflow连接正常"""
    try:
        health = await airflow_client.health_check()
        logger.info(f"Airflow健康状态: {health.get('metadatabase', {}).get('status', 'unknown')}")
        return True
    except Exception as e:
        logger.error(f"Airflow连接失败: {e}")
        return False


async def get_dag_status_summary(dag_id: str, days: int = 7) -> Dict[str, Any]:
    """获取DAG状态摘要"""
    try:
        # 获取最近几天的运行记录
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        runs = await airflow_client.get_dag_runs(
            dag_id=dag_id,
            execution_date_gte=start_date.isoformat(),
            execution_date_lte=end_date.isoformat(),
            limit=100
        )

        # 统计状态
        status_count = {}
        total_runs = runs.get("total_entries", 0)

        for run in runs.get("dag_runs", []):
            state = run.get("state", "unknown")
            status_count[state] = status_count.get(state, 0) + 1

        return {
            "dag_id": dag_id,
            "total_runs": total_runs,
            "period_days": days,
            "status_summary": status_count,
            "success_rate": round(
                (status_count.get("success", 0) / total_runs * 100) if total_runs > 0 else 0,
                2
            )
        }
    except Exception as e:
        logger.error(f"获取DAG {dag_id} 状态摘要失败: {e}")
        return {
            "dag_id": dag_id,
            "error": str(e)
        }