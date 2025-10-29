"""
HTTP执行器 - 执行HTTP请求
支持各种HTTP方法、认证方式、重试机制
"""
import asyncio
import json
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
import aiohttp
from aiohttp import ClientTimeout, ClientSession
import time

from .base_executor import JobExecutor


class HttpExecutor(JobExecutor):
    """HTTP请求执行器"""

    def __init__(self):
        super().__init__("http_executor")

    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行HTTP请求"""
        try:
            url = work_config.get('url')
            method = work_config.get('method', 'GET').upper()
            headers = work_config.get('headers', {})
            params = work_config.get('params', {})
            body = work_config.get('body')
            body_type = work_config.get('bodyType', 'json')  # json, form, text
            timeout = work_config.get('timeout', 30)
            auth_type = work_config.get('authType')  # basic, bearer, apikey
            auth_config = work_config.get('authConfig', {})
            retry_times = work_config.get('retryTimes', 0)
            retry_interval = work_config.get('retryInterval', 1)
            expected_status = work_config.get('expectedStatus', [200])  # 期望的状态码列表

            if not url:
                return {
                    "success": False,
                    "message": "URL不能为空",
                    "data": None,
                    "error": "URL is required"
                }

            # 上下文变量替换（简单实现）
            if context:
                url = self._replace_variables(url, context)
                headers = self._replace_dict_variables(headers, context)
                params = self._replace_dict_variables(params, context)

            # 设置认证
            headers = self._setup_auth(headers, auth_type, auth_config)

            # 执行请求（支持重试）
            result = await self._execute_with_retry(
                method, url, headers, params, body, body_type,
                timeout, retry_times, retry_interval
            )

            # 检查状态码
            if result['success']:
                status_code = result['data']['statusCode']
                if status_code not in expected_status:
                    return {
                        "success": False,
                        "message": f"HTTP状态码不符合预期: {status_code}",
                        "data": result['data'],
                        "error": f"Expected status codes: {expected_status}, got: {status_code}"
                    }

            return result

        except Exception as e:
            logger.error(f"HTTP执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def _execute_with_retry(
            self,
            method: str,
            url: str,
            headers: Dict,
            params: Dict,
            body: Any,
            body_type: str,
            timeout: int,
            retry_times: int,
            retry_interval: int
    ) -> Dict[str, Any]:
        """执行HTTP请求（带重试）"""
        last_error = None

        for attempt in range(retry_times + 1):
            try:
                if attempt > 0:
                    logger.info(f"HTTP请求重试 {attempt}/{retry_times}")
                    await asyncio.sleep(retry_interval)

                result = await self._execute_request(
                    method, url, headers, params, body, body_type, timeout
                )

                return result

            except Exception as e:
                last_error = e
                logger.warning(f"HTTP请求失败 (尝试 {attempt + 1}/{retry_times + 1}): {e}")

        return {
            "success": False,
            "message": f"HTTP请求失败（重试{retry_times}次后）",
            "data": None,
            "error": str(last_error)
        }

    async def _execute_request(
            self,
            method: str,
            url: str,
            headers: Dict,
            params: Dict,
            body: Any,
            body_type: str,
            timeout: int
    ) -> Dict[str, Any]:
        """执行单次HTTP请求"""
        start_time = time.time()

        timeout_config = ClientTimeout(total=timeout)

        async with ClientSession(timeout=timeout_config) as session:
            # 准备请求体
            request_kwargs = {
                'headers': headers,
                'params': params
            }

            if method in ['POST', 'PUT', 'PATCH'] and body:
                if body_type == 'json':
                    request_kwargs['json'] = body
                elif body_type == 'form':
                    request_kwargs['data'] = body
                elif body_type == 'text':
                    request_kwargs['data'] = str(body)

            # 发送请求
            async with session.request(method, url, **request_kwargs) as response:
                # 读取响应
                response_text = await response.text()

                # 尝试解析JSON
                response_data = None
                try:
                    response_data = json.loads(response_text)
                except:
                    response_data = response_text

                elapsed_time = time.time() - start_time

                return {
                    "success": True,
                    "message": f"HTTP请求成功 ({response.status})",
                    "data": {
                        "statusCode": response.status,
                        "headers": dict(response.headers),
                        "body": response_data,
                        "elapsed": round(elapsed_time, 3)
                    },
                    "error": None
                }

    def _setup_auth(
            self,
            headers: Dict,
            auth_type: Optional[str],
            auth_config: Dict
    ) -> Dict:
        """设置认证信息"""
        if not auth_type:
            return headers

        headers = headers.copy()

        if auth_type == 'bearer':
            token = auth_config.get('token')
            if token:
                headers['Authorization'] = f'Bearer {token}'

        elif auth_type == 'basic':
            import base64
            username = auth_config.get('username')
            password = auth_config.get('password')
            if username and password:
                credentials = f"{username}:{password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                headers['Authorization'] = f'Basic {encoded}'

        elif auth_type == 'apikey':
            key = auth_config.get('key')
            value = auth_config.get('value')
            location = auth_config.get('location', 'header')  # header, query

            if location == 'header' and key and value:
                headers[key] = value
            # query类型的在params中处理，这里只处理header

        return headers

    def _replace_variables(self, text: str, context: Dict) -> str:
        """替换文本中的变量 ${varName}"""
        if not text or not context:
            return text

        import re
        pattern = r'\$\{(\w+)\}'

        def replacer(match):
            var_name = match.group(1)
            return str(context.get(var_name, match.group(0)))

        return re.sub(pattern, replacer, text)

    def _replace_dict_variables(self, data: Dict, context: Dict) -> Dict:
        """替换字典中的变量"""
        if not data or not context:
            return data

        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = self._replace_variables(value, context)
            else:
                result[key] = value

        return result

    async def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证配置"""
        errors = []

        if not config.get('url'):
            errors.append("缺少url")

        method = config.get('method', 'GET').upper()
        if method not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']:
            errors.append(f"不支持的HTTP方法: {method}")

        auth_type = config.get('authType')
        if auth_type and auth_type not in ['basic', 'bearer', 'apikey']:
            errors.append(f"不支持的认证类型: {auth_type}")

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }