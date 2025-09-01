"""
自定义API生成器服务类
"""

import json
import re
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, delete, update
from sqlalchemy.orm import selectinload
from loguru import logger
from jinja2 import Template, TemplateSyntaxError

from app.models.custom_api import CustomAPI, APIParameter, APIAccessLog
from app.models.data_source import DataSource
from app.schemas.custom_api import (
    CreateAPIRequest, UpdateAPIRequest, CustomAPIResponse,
    SQLValidationResult, APIExecutionResult, ParameterType
)
from app.utils.data_integration_clients import DatabaseClientFactory
from app.utils.database import get_async_db
from app.utils.enhanced_parameter_validator import enhanced_parameter_validator
from app.utils.custom_api_cache import custom_api_cache
from app.utils.custom_api_rate_limiter import rate_limiter


class CustomAPIService:
    """自定义API生成器服务"""

    def __init__(self):
        # SQL模板参数提取正则表达式
        self.parameter_pattern = re.compile(r'\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\|[^}]*)?\s*\}\}')

    async def create_api(
            self,
            db: AsyncSession,
            api_request: CreateAPIRequest,
            creator: Optional[str] = None
    ) -> CustomAPI:
        """创建自定义API"""
        try:
            # 1. 验证数据源是否存在且可用
            data_source = await self._get_data_source(db, api_request.data_source_id)
            if not data_source:
                raise ValueError("指定的数据源不存在")

            if not data_source.is_active:
                raise ValueError("指定的数据源已被禁用")

            # 2. 验证API名称和路径是否已存在
            await self._check_api_uniqueness(db, api_request.api_name, api_request.api_path)

            # 3. 验证SQL模板
            validation_result = await self.validate_sql_template(api_request.sql_template, data_source)
            if not validation_result.is_valid:
                raise ValueError(f"SQL模板验证失败: {validation_result.error_message}")

            # 4. 创建API记录
            api = CustomAPI(
                api_name=api_request.api_name,
                api_path=api_request.api_path,
                description=api_request.description,
                data_source_id=api_request.data_source_id,
                sql_template=api_request.sql_template,
                http_method=api_request.http_method,
                response_format=api_request.response_format,
                cache_ttl=api_request.cache_ttl,
                rate_limit=api_request.rate_limit,
                created_by=creator
            )

            db.add(api)
            await db.flush()  # 获取生成的ID

            # 5. 创建参数记录
            for param_data in api_request.parameters:
                parameter = APIParameter(
                    api_id=api.id,
                    param_name=param_data.param_name,
                    param_type=param_data.param_type,
                    is_required=param_data.is_required,
                    default_value=param_data.default_value,
                    description=param_data.description,
                    validation_rule=param_data.validation_rule
                )
                db.add(parameter)

            await db.commit()
            logger.info(f"成功创建自定义API: {api.api_name} (ID: {api.id})")

            return api

        except Exception as e:
            await db.rollback()
            logger.error(f"创建自定义API失败: {e}")
            raise

    async def get_api(self, db: AsyncSession, api_id: int) -> Optional[CustomAPI]:
        """获取API详情"""
        result = await db.execute(
            select(CustomAPI)
            .options(selectinload(CustomAPI.parameters), selectinload(CustomAPI.data_source))
            .where(CustomAPI.id == api_id)
        )
        return result.scalar_one_or_none()

    async def get_api_by_name(self, db: AsyncSession, api_name: str) -> Optional[CustomAPI]:
        """根据API名称获取API"""
        result = await db.execute(
            select(CustomAPI)
            .options(selectinload(CustomAPI.parameters), selectinload(CustomAPI.data_source))
            .where(CustomAPI.api_name == api_name)
        )
        return result.scalar_one_or_none()

    async def get_apis_list(
            self,
            db: AsyncSession,
            skip: int = 0,
            limit: int = 100,
            data_source_id: Optional[int] = None,
            is_active: Optional[bool] = None,
            search_keyword: Optional[str] = None
    ) -> Tuple[List[CustomAPI], int]:
        """获取API列表"""
        query = select(CustomAPI).options(
            selectinload(CustomAPI.parameters),
            selectinload(CustomAPI.data_source)
        )

        # 添加筛选条件
        conditions = []
        if data_source_id:
            conditions.append(CustomAPI.data_source_id == data_source_id)
        if is_active is not None:
            conditions.append(CustomAPI.is_active == is_active)
        if search_keyword:
            search_pattern = f"%{search_keyword}%"
            conditions.append(
                CustomAPI.api_name.like(search_pattern) |
                CustomAPI.description.like(search_pattern)
            )

        if conditions:
            query = query.where(and_(*conditions))

        # 获取总数
        count_query = select(func.count(CustomAPI.id))
        if conditions:
            count_query = count_query.where(and_(*conditions))

        total_result = await db.execute(count_query)
        total_count = total_result.scalar()

        # 分页查询
        query = query.order_by(CustomAPI.created_at.desc()).offset(skip).limit(limit)
        result = await db.execute(query)
        apis = result.scalars().all()

        return apis, total_count

    async def update_api(
            self,
            db: AsyncSession,
            api_id: int,
            update_request: UpdateAPIRequest
    ) -> Optional[CustomAPI]:
        """更新API"""
        try:
            api = await self.get_api(db, api_id)
            if not api:
                return None

            # 更新基本信息
            update_data = update_request.dict(exclude_unset=True, exclude={'parameters'})
            for field, value in update_data.items():
                setattr(api, field, value)

            # 如果更新了SQL模板，需要重新验证
            if update_request.sql_template:
                validation_result = await self.validate_sql_template(
                    update_request.sql_template,
                    api.data_source
                )
                if not validation_result.is_valid:
                    raise ValueError(f"SQL模板验证失败: {validation_result.error_message}")

            # 更新参数
            if update_request.parameters is not None:
                # 删除原有参数
                await db.execute(delete(APIParameter).where(APIParameter.api_id == api_id))

                # 创建新参数
                for param_data in update_request.parameters:
                    parameter = APIParameter(
                        api_id=api_id,
                        param_name=param_data.param_name,
                        param_type=param_data.param_type,
                        is_required=param_data.is_required,
                        default_value=param_data.default_value,
                        description=param_data.description,
                        validation_rule=param_data.validation_rule
                    )
                    db.add(parameter)

            api.updated_at = datetime.now()
            await db.commit()

            logger.info(f"成功更新自定义API: {api.api_name} (ID: {api_id})")
            return api

        except Exception as e:
            await db.rollback()
            logger.error(f"更新自定义API失败: {e}")
            raise

    async def delete_api(self, db: AsyncSession, api_id: int) -> bool:
        """删除API"""
        try:
            api = await self.get_api(db, api_id)
            if not api:
                return False

            await db.delete(api)
            await db.commit()

            logger.info(f"成功删除自定义API: {api.api_name} (ID: {api_id})")
            return True

        except Exception as e:
            await db.rollback()
            logger.error(f"删除自定义API失败: {e}")
            raise

    async def validate_sql_template(
            self,
            sql_template: str,
            data_source: Optional[DataSource] = None
    ) -> SQLValidationResult:
        """验证SQL模板"""
        try:
            # 1. 检查Jinja2模板语法
            try:
                template = Template(sql_template)
            except TemplateSyntaxError as e:
                return SQLValidationResult(
                    is_valid=False,
                    error_message=f"模板语法错误: {str(e)}"
                )

            # 2. 提取模板参数
            parameters = list(set(self.parameter_pattern.findall(sql_template)))

            # 3. 生成示例SQL（使用默认参数值）
            test_params = {}
            for param in parameters:
                test_params[param] = "test_value"

            try:
                rendered_sql = template.render(**test_params)
            except Exception as e:
                return SQLValidationResult(
                    is_valid=False,
                    error_message=f"模板渲染失败: {str(e)}"
                )

            # 4. 基础SQL语法检查
            if not self._is_valid_select_sql(rendered_sql):
                return SQLValidationResult(
                    is_valid=False,
                    error_message="生成的SQL不是有效的SELECT语句"
                )

            return SQLValidationResult(
                is_valid=True,
                extracted_parameters=parameters,
                rendered_sql_example=rendered_sql,
                parameter_count=len(parameters)
            )

        except Exception as e:
            logger.error(f"SQL模板验证异常: {e}")
            return SQLValidationResult(
                is_valid=False,
                error_message=f"验证过程发生错误: {str(e)}"
            )

    async def execute_api_query(
            self,
            db: AsyncSession,
            api_id: int,
            request_params: Dict[str, Any],
            client_ip: Optional[str] = None,
            user_agent: Optional[str] = None
    ) -> APIExecutionResult:
        """执行API查询"""
        start_time = time.time()
        executed_sql = None
        error_message = None
        status_code = 200

        try:
            # 1. 获取API配置
            api = await self.get_api(db, api_id)
            if not api:
                raise ValueError("API不存在")

            if not api.is_active:
                raise ValueError("API已被禁用")

            # 2. 处理和验证参数
            processed_params = await self._process_request_parameters(api.parameters, request_params)

            # 3. 渲染SQL模板
            template = Template(api.sql_template)
            executed_sql = template.render(**processed_params)

            # 4. 执行查询
            data_source_config = json.loads(api.data_source.connection_config)
            client = DatabaseClientFactory.create_client(
                api.data_source.source_type,
                data_source_config
            )

            query_result = await client.execute_query(executed_sql)

            if not query_result.get('success', False):
                raise Exception(query_result.get('error', '查询执行失败'))

            result_data = query_result.get('data', [])
            response_time_ms = int((time.time() - start_time) * 1000)

            # 5. 更新API统计
            await self._update_api_statistics(db, api_id, success=True)

            # 6. 记录访问日志
            await self._log_api_access(
                db=db,
                api_id=api_id,
                client_ip=client_ip,
                user_agent=user_agent,
                request_params=request_params,
                status_code=status_code,
                response_time_ms=response_time_ms,
                executed_sql=executed_sql,
                result_count=len(result_data),
                error_message=None
            )

            return APIExecutionResult(
                success=True,
                data=result_data,
                total_count=len(result_data),
                response_time_ms=response_time_ms,
                executed_sql=executed_sql
            )

        except Exception as e:
            error_message = str(e)
            status_code = 500
            response_time_ms = int((time.time() - start_time) * 1000)

            # 更新失败统计
            await self._update_api_statistics(db, api_id, success=False)

            # 记录错误日志
            await self._log_api_access(
                db=db,
                api_id=api_id,
                client_ip=client_ip,
                user_agent=user_agent,
                request_params=request_params,
                status_code=status_code,
                response_time_ms=response_time_ms,
                executed_sql=executed_sql,
                result_count=0,
                error_message=error_message
            )

            logger.error(f"API执行失败 (ID: {api_id}): {error_message}")

            return APIExecutionResult(
                success=False,
                response_time_ms=response_time_ms,
                executed_sql=executed_sql,
                error_message=error_message
            )

    # ===== 私有方法 =====

    async def _get_data_source(self, db: AsyncSession, data_source_id: int) -> Optional[DataSource]:
        """获取数据源"""
        result = await db.execute(
            select(DataSource).where(DataSource.id == data_source_id)
        )
        return result.scalar_one_or_none()

    async def _check_api_uniqueness(self, db: AsyncSession, api_name: str, api_path: str):
        """检查API名称和路径的唯一性"""
        # 检查API名称
        name_result = await db.execute(
            select(CustomAPI).where(CustomAPI.api_name == api_name)
        )
        if name_result.scalar_one_or_none():
            raise ValueError(f"API名称 '{api_name}' 已存在")

        # 检查API路径
        path_result = await db.execute(
            select(CustomAPI).where(CustomAPI.api_path == api_path)
        )
        if path_result.scalar_one_or_none():
            raise ValueError(f"API路径 '{api_path}' 已存在")

    def _is_valid_select_sql(self, sql: str) -> bool:
        """检查是否是有效的SELECT语句"""
        sql_clean = sql.strip().lower()
        return sql_clean.startswith('select') and 'from' in sql_clean

    async def _process_request_parameters(
            self,
            api_parameters: List[APIParameter],
            request_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """处理和验证请求参数 - 使用增强验证器（Phase 2升级版）"""

        # 如果没有定义参数，直接返回请求参数
        if not api_parameters:
            return request_params

        # 使用增强验证器处理参数
        try:
            processed_params = await enhanced_parameter_validator.validate_and_convert_parameters(
                api_parameters, request_params
            )
            return processed_params

        except Exception as e:
            # 如果增强验证器出错，回退到原始逻辑
            logger.warning(f"增强验证器失败，使用基础验证: {e}")
            return await self._basic_process_parameters(api_parameters, request_params)

    # 保留原始的参数处理逻辑作为备用
    async def _basic_process_parameters(
            self,
            api_parameters: List[APIParameter],
            request_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """基础参数处理逻辑（原始版本）"""
        processed = {}

        for param_def in api_parameters:
            param_name = param_def.param_name
            param_value = request_params.get(param_name)

            # 检查必填参数
            if param_def.is_required and (param_value is None or param_value == ''):
                raise ValueError(f"缺少必填参数: {param_name}")

            # 使用默认值
            if param_value is None or param_value == '':
                if param_def.default_value is not None:
                    param_value = param_def.default_value
                elif not param_def.is_required:
                    continue  # 可选参数且无默认值，跳过

            # 基础类型转换
            if param_value is not None:
                try:
                    if param_def.param_type == ParameterType.INTEGER:
                        param_value = int(param_value)
                    elif param_def.param_type == ParameterType.FLOAT:
                        param_value = float(param_value)
                    elif param_def.param_type == ParameterType.BOOLEAN:
                        if isinstance(param_value, str):
                            param_value = param_value.lower() in ['true', '1', 'yes']
                        else:
                            param_value = bool(param_value)
                    elif param_def.param_type in [ParameterType.STRING, ParameterType.DATE, ParameterType.DATETIME]:
                        param_value = str(param_value)

                    processed[param_name] = param_value

                except (ValueError, TypeError) as e:
                    raise ValueError(f"参数 {param_name} 类型转换失败: {str(e)}")

        return processed

    async def _update_api_statistics(self, db: AsyncSession, api_id: int, success: bool):
        """更新API调用统计"""
        try:
            if success:
                await db.execute(
                    update(CustomAPI)
                    .where(CustomAPI.id == api_id)
                    .values(
                        total_calls=CustomAPI.total_calls + 1,
                        success_calls=CustomAPI.success_calls + 1,
                        last_call_time=datetime.now()
                    )
                )
            else:
                await db.execute(
                    update(CustomAPI)
                    .where(CustomAPI.id == api_id)
                    .values(
                        total_calls=CustomAPI.total_calls + 1,
                        last_call_time=datetime.now()
                    )
                )
            await db.commit()
        except Exception as e:
            logger.error(f"更新API统计失败: {e}")

    async def _log_api_access(
            self,
            db: AsyncSession,
            api_id: int,
            client_ip: Optional[str],
            user_agent: Optional[str],
            request_params: Dict[str, Any],
            status_code: int,
            response_time_ms: int,
            executed_sql: Optional[str] = None,
            result_count: int = 0,
            error_message: Optional[str] = None
    ):
        """记录API访问日志"""
        try:
            log_entry = APIAccessLog(
                api_id=api_id,
                client_ip=client_ip,
                user_agent=user_agent,
                request_params=request_params,
                response_time_ms=response_time_ms,
                status_code=status_code,
                executed_sql=executed_sql,
                result_count=result_count,
                error_message=error_message,
                error_type=type(Exception).__name__ if error_message else None
            )
            db.add(log_entry)
            await db.commit()
        except Exception as e:
            logger.error(f"记录API访问日志失败: {e}")


# 创建服务实例
custom_api_service = CustomAPIService()