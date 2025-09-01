"""
增强的参数验证器 - 支持复杂验证规则和类型转换
"""

import re
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union
from decimal import Decimal, InvalidOperation
from loguru import logger
from app.models.custom_api import APIParameter, ParameterType


class EnhancedParameterValidator:
    """增强的参数验证器"""

    def __init__(self):
        self.type_converters = {
            ParameterType.STRING: self._convert_to_string,
            ParameterType.INTEGER: self._convert_to_integer,
            ParameterType.FLOAT: self._convert_to_float,
            ParameterType.BOOLEAN: self._convert_to_boolean,
            ParameterType.DATE: self._convert_to_date,
            ParameterType.DATETIME: self._convert_to_datetime,
        }

    async def validate_and_convert_parameters(
            self,
            api_parameters: List[APIParameter],
            request_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """验证并转换请求参数"""
        processed_params = {}
        validation_errors = []

        try:
            for param_def in api_parameters:
                param_name = param_def.param_name
                param_value = request_params.get(param_name)

                # 1. 检查必填参数
                if param_def.is_required:
                    if param_value is None or param_value == '':
                        validation_errors.append(f"缺少必填参数: {param_name}")
                        continue

                # 2. 使用默认值
                if param_value is None or param_value == '':
                    if param_def.default_value is not None:
                        param_value = param_def.default_value
                    elif not param_def.is_required:
                        continue  # 跳过可选参数

                # 3. 类型转换
                if param_value is not None:
                    try:
                        converted_value = await self._convert_parameter_type(
                            param_value, param_def.param_type
                        )

                        # 4. 自定义验证规则
                        validation_result = await self._validate_parameter_rules(
                            converted_value, param_def
                        )

                        if not validation_result["valid"]:
                            validation_errors.append(
                                f"参数 {param_name} 验证失败: {validation_result['error']}"
                            )
                            continue

                        processed_params[param_name] = converted_value

                    except ValueError as e:
                        validation_errors.append(
                            f"参数 {param_name} 类型转换失败: {str(e)}"
                        )
                        continue

            if validation_errors:
                raise ValueError("; ".join(validation_errors))

            return processed_params

        except Exception as e:
            logger.error(f"参数验证失败: {e}")
            raise

    async def _convert_parameter_type(self, value: Any, param_type: ParameterType) -> Any:
        """转换参数类型"""
        converter = self.type_converters.get(param_type)
        if not converter:
            raise ValueError(f"不支持的参数类型: {param_type}")

        return await converter(value)

    async def _validate_parameter_rules(
            self,
            value: Any,
            param_def: APIParameter
    ) -> Dict[str, Any]:
        """验证自定义参数规则"""
        if not param_def.validation_rule:
            return {"valid": True}

        try:
            rules = param_def.validation_rule

            # 字符串长度验证
            if param_def.param_type == ParameterType.STRING:
                if "min_length" in rules and len(str(value)) < rules["min_length"]:
                    return {
                        "valid": False,
                        "error": f"字符串长度不能少于{rules['min_length']}个字符"
                    }

                if "max_length" in rules and len(str(value)) > rules["max_length"]:
                    return {
                        "valid": False,
                        "error": f"字符串长度不能超过{rules['max_length']}个字符"
                    }

                if "pattern" in rules:
                    if not re.match(rules["pattern"], str(value)):
                        return {
                            "valid": False,
                            "error": f"字符串格式不匹配要求的模式"
                        }

                if "enum" in rules:
                    if str(value) not in rules["enum"]:
                        return {
                            "valid": False,
                            "error": f"值必须是以下之一: {', '.join(rules['enum'])}"
                        }

            # 数值范围验证
            elif param_def.param_type in [ParameterType.INTEGER, ParameterType.FLOAT]:
                if "min_value" in rules and value < rules["min_value"]:
                    return {
                        "valid": False,
                        "error": f"数值不能小于{rules['min_value']}"
                    }

                if "max_value" in rules and value > rules["max_value"]:
                    return {
                        "valid": False,
                        "error": f"数值不能大于{rules['max_value']}"
                    }

                if "enum" in rules:
                    if value not in rules["enum"]:
                        return {
                            "valid": False,
                            "error": f"值必须是以下之一: {', '.join(map(str, rules['enum']))}"
                        }

            # 日期范围验证
            elif param_def.param_type in [ParameterType.DATE, ParameterType.DATETIME]:
                if "min_date" in rules:
                    min_date = datetime.fromisoformat(rules["min_date"]).date()
                    if isinstance(value, datetime):
                        value_date = value.date()
                    else:
                        value_date = value

                    if value_date < min_date:
                        return {
                            "valid": False,
                            "error": f"日期不能早于{rules['min_date']}"
                        }

                if "max_date" in rules:
                    max_date = datetime.fromisoformat(rules["max_date"]).date()
                    if isinstance(value, datetime):
                        value_date = value.date()
                    else:
                        value_date = value

                    if value_date > max_date:
                        return {
                            "valid": False,
                            "error": f"日期不能晚于{rules['max_date']}"
                        }

            return {"valid": True}

        except Exception as e:
            logger.error(f"参数规则验证异常: {e}")
            return {
                "valid": False,
                "error": f"验证规则执行失败: {str(e)}"
            }

    # 类型转换方法
    async def _convert_to_string(self, value: Any) -> str:
        """转换为字符串"""
        return str(value)

    async def _convert_to_integer(self, value: Any) -> int:
        """转换为整数"""
        if isinstance(value, int):
            return value

        if isinstance(value, str):
            # 去除空白字符
            value = value.strip()
            if not value:
                raise ValueError("空字符串无法转换为整数")

        try:
            return int(value)
        except (ValueError, TypeError):
            raise ValueError(f"'{value}' 无法转换为整数")

    async def _convert_to_float(self, value: Any) -> float:
        """转换为浮点数"""
        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("空字符串无法转换为浮点数")

        try:
            return float(value)
        except (ValueError, TypeError):
            raise ValueError(f"'{value}' 无法转换为浮点数")

    async def _convert_to_boolean(self, value: Any) -> bool:
        """转换为布尔值"""
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ['true', '1', 'yes', 'on', '是']:
                return True
            elif value_lower in ['false', '0', 'no', 'off', '否']:
                return False
            else:
                raise ValueError(f"'{value}' 无法转换为布尔值")

        if isinstance(value, (int, float)):
            return bool(value)

        raise ValueError(f"'{value}' 无法转换为布尔值")

    async def _convert_to_date(self, value: Any) -> date:
        """转换为日期"""
        if isinstance(value, date):
            return value

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("空字符串无法转换为日期")

            # 尝试多种日期格式
            date_formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S"
            ]

            for fmt in date_formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue

            raise ValueError(f"'{value}' 无法转换为日期，支持格式: YYYY-MM-DD")

        raise ValueError(f"'{value}' 无法转换为日期")

    async def _convert_to_datetime(self, value: Any) -> datetime:
        """转换为日期时间"""
        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("空字符串无法转换为日期时间")

            # 尝试多种日期时间格式
            datetime_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%Y/%m/%d",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%m/%d/%Y",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ"
            ]

            for fmt in datetime_formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

            raise ValueError(f"'{value}' 无法转换为日期时间")

        raise ValueError(f"'{value}' 无法转换为日期时间")


# 创建全局参数验证器实例
enhanced_parameter_validator = EnhancedParameterValidator()