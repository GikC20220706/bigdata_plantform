"""
API执行器 - RESTful API调用封装
提供更高级的API调用功能，包括响应解析、数据提取、条件判断等
"""
import json
from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger
import aiohttp
from aiohttp import ClientTimeout

from .base_executor import JobExecutor


class ApiExecutor(JobExecutor):
    """RESTful API执行器"""

    def __init__(self):
        super().__init__("api_executor")

    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行API调用"""
        try:
            url = work_config.get('url')
            method = work_config.get('method', 'GET').upper()
            headers = work_config.get('headers', {})
            params = work_config.get('params', {})
            body = work_config.get('body')
            timeout = work_config.get('timeout', 30)

            # API特定配置
            auth = work_config.get('auth', {})  # 认证配置
            response_handler = work_config.get('responseHandler', {})  # 响应处理
            data_extract = work_config.get('dataExtract', {})  # 数据提取
            success_condition = work_config.get('successCondition', {})  # 成功条件

            if not url:
                return {
                    "success": False,
                    "message": "URL不能为空",
                    "data": None,
                    "error": "URL is required"
                }

            # 上下文变量替换
            if context:
                url = self._replace_variables(url, context)
                headers = self._replace_dict_variables(headers, context)
                params = self._replace_dict_variables(params, context)
                body = self._replace_body_variables(body, context)

            # 设置认证
            headers = self._setup_auth(headers, auth)

            # 设置默认Content-Type
            if 'Content-Type' not in headers and method in ['POST', 'PUT', 'PATCH']:
                headers['Content-Type'] = 'application/json'

            # 执行API请求
            response_data = await self._call_api(
                method, url, headers, params, body, timeout
            )

            if not response_data['success']:
                return response_data

            # 处理响应
            processed_response = self._process_response(
                response_data['data'],
                response_handler
            )

            # 提取数据
            extracted_data = self._extract_data(
                processed_response,
                data_extract
            )

            # 判断成功条件
            is_success = self._check_success_condition(
                processed_response,
                success_condition
            )

            if not is_success:
                return {
                    "success": False,
                    "message": "API调用不满足成功条件",
                    "data": {
                        "response": processed_response,
                        "extracted": extracted_data
                    },
                    "error": "Success condition not met"
                }

            return {
                "success": True,
                "message": "API调用成功",
                "data": {
                    "response": processed_response,
                    "extracted": extracted_data
                },
                "error": None
            }

        except Exception as e:
            logger.error(f"API执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def _call_api(
            self,
            method: str,
            url: str,
            headers: Dict,
            params: Dict,
            body: Any,
            timeout: int
    ) -> Dict[str, Any]:
        """调用API"""
        try:
            timeout_config = ClientTimeout(total=timeout)

            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                request_kwargs = {
                    'headers': headers,
                    'params': params
                }

                if method in ['POST', 'PUT', 'PATCH'] and body:
                    if isinstance(body, (dict, list)):
                        request_kwargs['json'] = body
                    else:
                        request_kwargs['data'] = str(body)

                async with session.request(method, url, **request_kwargs) as response:
                    response_text = await response.text()

                    # 尝试解析JSON
                    response_data = None
                    try:
                        response_data = json.loads(response_text)
                    except:
                        response_data = response_text

                    return {
                        "success": True,
                        "data": {
                            "statusCode": response.status,
                            "headers": dict(response.headers),
                            "body": response_data
                        }
                    }

        except Exception as e:
            logger.error(f"API调用失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _process_response(
            self,
            response: Dict,
            handler_config: Dict
    ) -> Dict:
        """处理响应"""
        if not handler_config:
            return response

        body = response.get('body', {})

        # 如果配置了响应路径，提取指定路径的数据
        response_path = handler_config.get('responsePath')
        if response_path and isinstance(body, dict):
            body = self._get_nested_value(body, response_path)

        return {
            "statusCode": response.get('statusCode'),
            "body": body
        }

    def _extract_data(
            self,
            response: Dict,
            extract_config: Dict
    ) -> Dict:
        """从响应中提取数据"""
        if not extract_config:
            return {}

        body = response.get('body', {})
        extracted = {}

        # 支持多个字段提取
        fields = extract_config.get('fields', [])
        for field_config in fields:
            field_name = field_config.get('name')
            field_path = field_config.get('path')

            if field_name and field_path:
                value = self._get_nested_value(body, field_path)
                extracted[field_name] = value

        return extracted

    def _check_success_condition(
            self,
            response: Dict,
            condition_config: Dict
    ) -> bool:
        """检查成功条件"""
        if not condition_config:
            # 默认：状态码为2xx即为成功
            status_code = response.get('statusCode', 0)
            return 200 <= status_code < 300

        condition_type = condition_config.get('type', 'status')

        if condition_type == 'status':
            # 检查状态码
            expected_status = condition_config.get('expectedStatus', [200])
            status_code = response.get('statusCode')
            return status_code in expected_status

        elif condition_type == 'field':
            # 检查字段值
            field_path = condition_config.get('fieldPath')
            expected_value = condition_config.get('expectedValue')
            operator = condition_config.get('operator', 'equals')  # equals, contains, exists

            body = response.get('body', {})
            actual_value = self._get_nested_value(body, field_path)

            if operator == 'equals':
                return actual_value == expected_value
            elif operator == 'contains':
                return expected_value in str(actual_value)
            elif operator == 'exists':
                return actual_value is not None

        return True

    def _get_nested_value(self, data: Any, path: str) -> Any:
        """获取嵌套字段的值

        支持的路径格式：
        - "field" - 获取根级别字段
        - "field.subfield" - 获取嵌套字段
        - "field[0]" - 获取数组元素
        - "field.array[0].subfield" - 组合使用
        """
        if not path:
            return data

        import re

        # 分割路径
        parts = re.split(r'\.|\[|\]', path)
        parts = [p for p in parts if p]  # 移除空字符串

        current = data
        for part in parts:
            if current is None:
                return None

            # 检查是否是数组索引
            if part.isdigit():
                index = int(part)
                if isinstance(current, list) and 0 <= index < len(current):
                    current = current[index]
                else:
                    return None
            else:
                # 字典键
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    return None

        return current

    def _setup_auth(self, headers: Dict, auth_config: Dict) -> Dict:
        """设置认证"""
        if not auth_config:
            return headers

        headers = headers.copy()
        auth_type = auth_config.get('type')

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
            if key and value:
                headers[key] = value

        return headers

    def _replace_variables(self, text: str, context: Dict) -> str:
        """替换变量 ${varName}"""
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

    def _replace_body_variables(self, body: Any, context: Dict) -> Any:
        """替换请求体中的变量"""
        if not body or not context:
            return body

        if isinstance(body, str):
            return self._replace_variables(body, context)
        elif isinstance(body, dict):
            return self._replace_dict_variables(body, context)
        elif isinstance(body, list):
            return [self._replace_body_variables(item, context) for item in body]

        return body

    async def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证配置"""
        errors = []

        if not config.get('url'):
            errors.append("缺少url")

        method = config.get('method', 'GET').upper()
        if method not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
            errors.append(f"不支持的HTTP方法: {method}")

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }