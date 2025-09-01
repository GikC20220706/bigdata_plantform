# =====================================================================================
# Phase 3: API文档自动生成功能
# =====================================================================================

# ===== 文件1: app/utils/api_docs_generator.py =====
"""
API文档自动生成器 - 为自定义API生成完整的OpenAPI文档
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.custom_api import CustomAPI, APIParameter, ParameterType, HTTPMethod, ResponseFormat
from app.services.custom_api_service import custom_api_service


class APIDocumentationGenerator:
    """API文档自动生成器"""

    def __init__(self):
        self.type_mapping = {
            ParameterType.STRING: {"type": "string"},
            ParameterType.INTEGER: {"type": "integer", "format": "int64"},
            ParameterType.FLOAT: {"type": "number", "format": "float"},
            ParameterType.BOOLEAN: {"type": "boolean"},
            ParameterType.DATE: {"type": "string", "format": "date"},
            ParameterType.DATETIME: {"type": "string", "format": "date-time"},
        }

    async def generate_single_api_docs(
            self,
            db: AsyncSession,
            api_id: int
    ) -> Dict[str, Any]:
        """为单个API生成完整的文档"""
        try:
            api = await custom_api_service.get_api(db, api_id)
            if not api:
                raise ValueError(f"API ID {api_id} 不存在")

            # 生成OpenAPI规范
            openapi_spec = {
                "openapi": "3.0.3",
                "info": {
                    "title": f"自定义API: {api.api_name}",
                    "description": api.description or f"基于SQL查询自动生成的API接口",
                    "version": "1.0.0",
                    "contact": {
                        "name": api.created_by or "API生成器",
                        "email": "api-generator@bigdata-platform.com"
                    },
                    "license": {
                        "name": "MIT",
                        "url": "https://opensource.org/licenses/MIT"
                    }
                },
                "servers": [
                    {
                        "url": "/api/custom",
                        "description": "自定义API服务器"
                    }
                ],
                "paths": {
                    api.api_path.replace("/api/custom", ""): {
                        api.http_method.value.lower(): self._generate_path_spec(api)
                    }
                },
                "components": {
                    "schemas": self._generate_schemas(api),
                    "parameters": self._generate_parameter_components(api),
                    "responses": self._generate_response_components(api)
                }
            }

            # 生成额外的文档信息
            additional_docs = {
                "api_info": {
                    "api_id": api.id,
                    "api_name": api.api_name,
                    "api_path": api.api_path,
                    "data_source": api.data_source.name if api.data_source else None,
                    "created_at": api.created_at,
                    "last_updated": api.updated_at,
                    "total_calls": api.total_calls,
                    "success_rate": (api.success_calls / api.total_calls * 100) if api.total_calls > 0 else 0
                },
                "sql_info": {
                    "sql_template": api.sql_template,
                    "data_source_type": api.data_source.source_type if api.data_source else None,
                    "extracted_parameters": self._extract_sql_parameters(api.sql_template)
                },
                "usage_examples": self._generate_usage_examples(api),
                "performance_info": {
                    "cache_ttl": api.cache_ttl,
                    "rate_limit": api.rate_limit,
                    "expected_response_time": "< 100ms (缓存命中时 < 10ms)"
                }
            }

            return {
                "openapi_spec": openapi_spec,
                "additional_docs": additional_docs,
                "generated_at": datetime.now()
            }

        except Exception as e:
            logger.error(f"生成API文档失败: {e}")
            raise

    async def generate_all_apis_docs(
            self,
            db: AsyncSession,
            include_inactive: bool = False
    ) -> Dict[str, Any]:
        """生成所有API的集合文档"""
        try:
            apis, _ = await custom_api_service.get_apis_list(
                db=db,
                is_active=None if include_inactive else True,
                limit=1000
            )

            if not apis:
                return {
                    "message": "没有找到任何API",
                    "total_apis": 0,
                    "generated_at": datetime.now()
                }

            # 生成集合OpenAPI规范
            paths = {}
            all_schemas = {}
            all_parameters = {}
            all_responses = {}

            for api in apis:
                if not include_inactive and not api.is_active:
                    continue

                api_path = api.api_path.replace("/api/custom", "")
                paths[api_path] = {
                    api.http_method.value.lower(): self._generate_path_spec(api)
                }

                # 合并组件
                schemas = self._generate_schemas(api)
                parameters = self._generate_parameter_components(api)
                responses = self._generate_response_components(api)

                all_schemas.update(schemas)
                all_parameters.update(parameters)
                all_responses.update(responses)

            collection_spec = {
                "openapi": "3.0.3",
                "info": {
                    "title": "大数据平台 - 自定义API集合",
                    "description": f"包含 {len(apis)} 个基于SQL查询自动生成的API接口",
                    "version": "1.0.0",
                    "contact": {
                        "name": "大数据平台API团队",
                        "email": "api-team@bigdata-platform.com"
                    }
                },
                "servers": [
                    {
                        "url": "/api/custom",
                        "description": "自定义API服务器"
                    }
                ],
                "paths": paths,
                "components": {
                    "schemas": all_schemas,
                    "parameters": all_parameters,
                    "responses": all_responses
                },
                "tags": self._generate_tags(apis)
            }

            # 生成API索引
            api_index = []
            for api in apis:
                api_index.append({
                    "api_id": api.id,
                    "api_name": api.api_name,
                    "api_path": api.api_path,
                    "description": api.description,
                    "http_method": api.http_method.value,
                    "response_format": api.response_format.value,
                    "data_source": api.data_source.name if api.data_source else None,
                    "is_active": api.is_active,
                    "parameter_count": len(api.parameters),
                    "total_calls": api.total_calls,
                    "success_rate": (api.success_calls / api.total_calls * 100) if api.total_calls > 0 else 0
                })

            return {
                "openapi_spec": collection_spec,
                "api_index": api_index,
                "statistics": {
                    "total_apis": len(apis),
                    "active_apis": len([api for api in apis if api.is_active]),
                    "inactive_apis": len([api for api in apis if not api.is_active]),
                    "total_calls": sum(api.total_calls for api in apis),
                    "avg_success_rate": sum(
                        (api.success_calls / api.total_calls * 100) if api.total_calls > 0 else 0 for api in
                        apis) / len(apis) if apis else 0
                },
                "generated_at": datetime.now()
            }

        except Exception as e:
            logger.error(f"生成API集合文档失败: {e}")
            raise

    def _generate_path_spec(self, api: CustomAPI) -> Dict[str, Any]:
        """生成路径规范"""
        parameters = []
        request_body = None

        # 处理参数
        if api.parameters:
            for param in api.parameters:
                if api.http_method == HTTPMethod.GET:
                    # GET请求参数作为query参数
                    parameters.append({
                        "name": param.param_name,
                        "in": "query",
                        "description": param.description or f"{param.param_name} 参数",
                        "required": param.is_required,
                        "schema": self._get_parameter_schema(param),
                        "example": self._get_parameter_example(param)
                    })
                else:
                    # POST请求参数作为请求体
                    if request_body is None:
                        request_body = {
                            "description": "请求参数",
                            "required": True,
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {},
                                        "required": []
                                    },
                                    "example": {}
                                }
                            }
                        }

                    schema = self._get_parameter_schema(param)
                    request_body["content"]["application/json"]["schema"]["properties"][param.param_name] = {
                        **schema,
                        "description": param.description or f"{param.param_name} 参数"
                    }

                    if param.is_required:
                        request_body["content"]["application/json"]["schema"]["required"].append(param.param_name)

                    request_body["content"]["application/json"]["example"][
                        param.param_name] = self._get_parameter_example(param)

        # 构建路径规范
        path_spec = {
            "summary": api.api_name,
            "description": self._generate_detailed_description(api),
            "tags": [api.data_source.name if api.data_source else "未知数据源"],
            "parameters": parameters if parameters else [],
            "responses": self._generate_responses_spec(api),
            "x-custom-info": {
                "api_id": api.id,
                "data_source_type": api.data_source.source_type if api.data_source else None,
                "cache_ttl": api.cache_ttl,
                "rate_limit": api.rate_limit,
                "sql_template": api.sql_template
            }
        }

        if request_body:
            path_spec["requestBody"] = request_body

        return path_spec

    def _generate_detailed_description(self, api: CustomAPI) -> str:
        """生成详细的API描述"""
        description_parts = []

        # 基本描述
        if api.description:
            description_parts.append(api.description)
        else:
            description_parts.append(f"基于SQL查询自动生成的API接口")

        # 数据源信息
        if api.data_source:
            description_parts.append(f"**数据源**: {api.data_source.name} ({api.data_source.source_type})")

        # SQL模板
        description_parts.append(f"**SQL查询模板**:")
        description_parts.append(f"```sql\n{api.sql_template}\n```")

        # 性能信息
        description_parts.append(f"**性能配置**:")
        description_parts.append(f"- 缓存时间: {api.cache_ttl}秒")
        description_parts.append(f"- 频率限制: {api.rate_limit}次/分钟")

        # 统计信息
        if api.total_calls > 0:
            success_rate = (api.success_calls / api.total_calls * 100)
            description_parts.append(f"**使用统计**:")
            description_parts.append(f"- 总调用次数: {api.total_calls}")
            description_parts.append(f"- 成功率: {success_rate:.1f}%")

        return "\n\n".join(description_parts)

    def _generate_responses_spec(self, api: CustomAPI) -> Dict[str, Any]:
        """生成响应规范"""
        responses = {
            "200": {
                "description": "请求成功",
                "content": {}
            },
            "400": {
                "description": "请求参数错误",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "success": {"type": "boolean", "example": False},
                                "message": {"type": "string", "example": "参数验证失败"},
                                "error_code": {"type": "string", "example": "VALIDATION_ERROR"}
                            }
                        }
                    }
                }
            },
            "429": {
                "description": "请求频率超限",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "success": {"type": "boolean", "example": False},
                                "message": {"type": "string", "example": "请求频率超限，最大允许100次/分钟"}
                            }
                        }
                    }
                }
            },
            "500": {
                "description": "服务器内部错误",
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "success": {"type": "boolean", "example": False},
                                "message": {"type": "string", "example": "查询执行失败"}
                            }
                        }
                    }
                }
            }
        }

        # 根据响应格式设置成功响应
        if api.response_format == ResponseFormat.JSON:
            responses["200"]["content"]["application/json"] = {
                "schema": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean", "example": True},
                        "data": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "查询结果数组"
                        },
                        "total_count": {"type": "integer", "example": 10},
                        "response_time_ms": {"type": "integer", "example": 45},
                        "cached": {"type": "boolean", "example": False},
                        "api_info": {
                            "type": "object",
                            "properties": {
                                "api_name": {"type": "string", "example": api.api_name},
                                "api_version": {"type": "string", "example": "1.0"},
                                "data_source": {"type": "string",
                                                "example": api.data_source.name if api.data_source else None}
                            }
                        }
                    }
                },
                "example": {
                    "success": True,
                    "data": [
                        {"id": 1, "name": "示例数据1", "value": 100},
                        {"id": 2, "name": "示例数据2", "value": 200}
                    ],
                    "total_count": 2,
                    "response_time_ms": 45,
                    "cached": False,
                    "api_info": {
                        "api_name": api.api_name,
                        "api_version": "1.0",
                        "data_source": api.data_source.name if api.data_source else None
                    }
                }
            }
        elif api.response_format == ResponseFormat.CSV:
            responses["200"]["content"]["text/csv"] = {
                "schema": {"type": "string"},
                "example": "id,name,value\n1,示例数据1,100\n2,示例数据2,200"
            }
        elif api.response_format == ResponseFormat.EXCEL:
            responses["200"]["content"]["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"] = {
                "schema": {"type": "string", "format": "binary"}
            }

        return responses

    def _get_parameter_schema(self, param: APIParameter) -> Dict[str, Any]:
        """获取参数的JSON Schema"""
        schema = self.type_mapping.get(param.param_type, {"type": "string"}).copy()

        # 添加验证规则
        if param.validation_rule:
            rules = param.validation_rule

            # 字符串验证规则
            if param.param_type == ParameterType.STRING:
                if "min_length" in rules:
                    schema["minLength"] = rules["min_length"]
                if "max_length" in rules:
                    schema["maxLength"] = rules["max_length"]
                if "pattern" in rules:
                    schema["pattern"] = rules["pattern"]
                if "enum" in rules:
                    schema["enum"] = rules["enum"]

            # 数值验证规则
            elif param.param_type in [ParameterType.INTEGER, ParameterType.FLOAT]:
                if "min_value" in rules:
                    schema["minimum"] = rules["min_value"]
                if "max_value" in rules:
                    schema["maximum"] = rules["max_value"]
                if "enum" in rules:
                    schema["enum"] = rules["enum"]

            # 日期验证规则
            elif param.param_type in [ParameterType.DATE, ParameterType.DATETIME]:
                if "min_date" in rules:
                    schema["minDate"] = rules["min_date"]
                if "max_date" in rules:
                    schema["maxDate"] = rules["max_date"]

        # 添加默认值
        if param.default_value is not None:
            schema["default"] = param.default_value

        return schema

    def _get_parameter_example(self, param: APIParameter) -> Any:
        """获取参数示例值"""
        if param.default_value is not None:
            return param.default_value

        # 根据类型和验证规则生成示例
        if param.param_type == ParameterType.STRING:
            if param.validation_rule and "enum" in param.validation_rule:
                return param.validation_rule["enum"][0]
            return "示例字符串"
        elif param.param_type == ParameterType.INTEGER:
            if param.validation_rule:
                if "enum" in param.validation_rule:
                    return param.validation_rule["enum"][0]
                elif "min_value" in param.validation_rule:
                    return param.validation_rule["min_value"]
            return 123
        elif param.param_type == ParameterType.FLOAT:
            if param.validation_rule:
                if "enum" in param.validation_rule:
                    return param.validation_rule["enum"][0]
                elif "min_value" in param.validation_rule:
                    return param.validation_rule["min_value"]
            return 123.45
        elif param.param_type == ParameterType.BOOLEAN:
            return True
        elif param.param_type == ParameterType.DATE:
            return "2024-08-31"
        elif param.param_type == ParameterType.DATETIME:
            return "2024-08-31T14:30:00Z"

        return "示例值"

    def _generate_schemas(self, api: CustomAPI) -> Dict[str, Any]:
        """生成组件schemas"""
        schemas = {}

        # 生成请求schema（如果是POST）
        if api.http_method == HTTPMethod.POST and api.parameters:
            request_properties = {}
            required_fields = []

            for param in api.parameters:
                request_properties[param.param_name] = {
                    **self._get_parameter_schema(param),
                    "description": param.description or f"{param.param_name} 参数"
                }

                if param.is_required:
                    required_fields.append(param.param_name)

            schemas[f"{api.api_name}Request"] = {
                "type": "object",
                "properties": request_properties,
                "required": required_fields
            }

        # 生成响应schema
        schemas[f"{api.api_name}Response"] = {
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "data": {
                    "type": "array",
                    "items": {"type": "object"}
                },
                "total_count": {"type": "integer"},
                "response_time_ms": {"type": "integer"},
                "cached": {"type": "boolean"},
                "api_info": {
                    "type": "object",
                    "properties": {
                        "api_name": {"type": "string"},
                        "api_version": {"type": "string"},
                        "data_source": {"type": "string"}
                    }
                }
            }
        }

        return schemas

    def _generate_parameter_components(self, api: CustomAPI) -> Dict[str, Any]:
        """生成参数组件"""
        parameters = {}

        for param in api.parameters:
            parameters[f"{api.api_name}_{param.param_name}"] = {
                "name": param.param_name,
                "in": "query" if api.http_method == HTTPMethod.GET else "body",
                "description": param.description or f"{param.param_name} 参数",
                "required": param.is_required,
                "schema": self._get_parameter_schema(param)
            }

        return parameters

    def _generate_response_components(self, api: CustomAPI) -> Dict[str, Any]:
        """生成响应组件"""
        responses = {
            f"{api.api_name}Success": {
                "description": "请求成功",
                "content": {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{api.api_name}Response"}
                    }
                }
            }
        }

        return responses

    def _generate_tags(self, apis: List[CustomAPI]) -> List[Dict[str, Any]]:
        """生成API标签"""
        tags = []
        data_sources = set()

        for api in apis:
            if api.data_source and api.data_source.name:
                data_sources.add(api.data_source.name)

        for ds_name in data_sources:
            tags.append({
                "name": ds_name,
                "description": f"来自数据源 {ds_name} 的API接口"
            })

        return tags

    def _extract_sql_parameters(self, sql_template: str) -> List[str]:
        """从SQL模板中提取参数"""
        import re
        pattern = r'\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\|[^}]*)?\s*\}\}'
        return list(set(re.findall(pattern, sql_template)))

    def _generate_usage_examples(self, api: CustomAPI) -> Dict[str, Any]:
        """生成使用示例"""
        examples = {
            "curl_example": self._generate_curl_example(api),
            "javascript_example": self._generate_javascript_example(api),
            "python_example": self._generate_python_example(api)
        }

        return examples

    def _generate_curl_example(self, api: CustomAPI) -> str:
        """生成cURL示例"""
        if api.http_method == HTTPMethod.GET:
            params = []
            for param in api.parameters:
                example_value = self._get_parameter_example(param)
                params.append(f"{param.param_name}={example_value}")

            query_string = "&".join(params) if params else ""
            url = f"{api.api_path}?{query_string}" if query_string else api.api_path

            return f'curl -X GET "{url}"'

        else:  # POST
            request_body = {}
            for param in api.parameters:
                request_body[param.param_name] = self._get_parameter_example(param)

            return f'''curl -X POST "{api.api_path}" \\
  -H "Content-Type: application/json" \\
  -d '{json.dumps(request_body, ensure_ascii=False, indent=2)}' '''

    def _generate_javascript_example(self, api: CustomAPI) -> str:
        """生成JavaScript示例"""
        if api.http_method == HTTPMethod.GET:
            params = []
            for param in api.parameters:
                example_value = self._get_parameter_example(param)
                params.append(f"{param.param_name}={example_value}")

            query_string = "&".join(params) if params else ""
            url = f"{api.api_path}?{query_string}" if query_string else api.api_path

            return f'''// JavaScript示例
const response = await fetch('{url}');
const data = await response.json();
console.log(data);'''

        else:  # POST
            request_body = {}
            for param in api.parameters:
                request_body[param.param_name] = self._get_parameter_example(param)

            return f'''// JavaScript示例
const response = await fetch('{api.api_path}', {{
  method: 'POST',
  headers: {{
    'Content-Type': 'application/json',
  }},
  body: JSON.stringify({json.dumps(request_body, ensure_ascii=False)})
}});

const data = await response.json();
console.log(data);'''

    def _generate_python_example(self, api: CustomAPI) -> str:
        """生成Python示例"""
        if api.http_method == HTTPMethod.GET:
            params = {}
            for param in api.parameters:
                params[param.param_name] = self._get_parameter_example(param)

            return f'''# Python示例
import requests

params = {json.dumps(params, ensure_ascii=False, indent=4)}
response = requests.get('{api.api_path}', params=params)
data = response.json()
print(data)'''

        else:  # POST
            request_body = {}
            for param in api.parameters:
                request_body[param.param_name] = self._get_parameter_example(param)

            return f'''# Python示例
import requests

data = {json.dumps(request_body, ensure_ascii=False, indent=4)}
response = requests.post('{api.api_path}', json=data)
result = response.json()
print(result)'''


# 创建全局文档生成器实例
api_docs_generator = APIDocumentationGenerator()