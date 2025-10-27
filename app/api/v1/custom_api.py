"""
自定义API管理的REST API端点
"""
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.custom_api import CustomAPI
from app.schemas.custom_api import (
    CreateAPIRequest, UpdateAPIRequest, CustomAPIResponse,
    SQLValidationResult, APIExecutionResult
)
from app.services.custom_api_service import custom_api_service
from app.utils.database import get_async_db
from app.utils.response import create_response
from loguru import logger
from app.utils.dynamic_api_router import dynamic_api_router
from app.utils.custom_api_cache import custom_api_cache
from app.utils.custom_api_rate_limiter import rate_limiter
from fastapi import BackgroundTasks
from sqlalchemy import select, and_, desc
from app.models.custom_api import APIAccessLog
from app.utils.api_docs_generator import api_docs_generator

router = APIRouter(prefix="/custom-api", tags=["自定义API管理"])


@router.post("/", summary="创建自定义API")
async def create_custom_api(
        api_request: CreateAPIRequest,
        db: AsyncSession = Depends(get_async_db),
        creator: Optional[str] = Query(None, description="创建者")
):
    """
    创建新的自定义API

    - **api_name**: API名称，必须唯一
    - **api_path**: API路径，必须以 /api/custom/ 开头
    - **data_source_id**: 关联的数据源ID
    - **sql_template**: SQL查询模板，支持Jinja2语法
    - **parameters**: API参数定义列表
    """
    try:
        api = await custom_api_service.create_api(db, api_request, creator)

        # 构造响应数据
        response_data = {
            "api_id": api.id,
            "api_name": api.api_name,
            "api_path": api.api_path,
            "created_at": datetime.now(),
            "parameter_count": len(api_request.parameters)
        }

        return create_response(
            data=response_data,
            message=f"自定义API '{api.api_name}' 创建成功"
        )

    except ValueError as e:
        logger.warning(f"创建API参数错误: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建API失败: {e}")
        raise HTTPException(status_code=500, detail="创建API失败")


@router.get("/", summary="获取自定义API列表")
async def get_custom_apis(
        db: AsyncSession = Depends(get_async_db),
        skip: int = Query(0, ge=0, description="跳过记录数"),
        limit: int = Query(20, ge=1, le=100, description="每页记录数"),
        data_source_id: Optional[int] = Query(None, description="数据源ID过滤"),
        is_active: Optional[bool] = Query(None, description="状态过滤"),
        search: Optional[str] = Query(None, description="搜索关键词")
):
    """获取自定义API列表，支持分页和筛选"""
    try:
        apis, total_count = await custom_api_service.get_apis_list(
            db=db,
            skip=skip,
            limit=limit,
            data_source_id=data_source_id,
            is_active=is_active,
            search_keyword=search
        )

        # 构造响应数据
        api_list = []
        for api in apis:
            api_data = {
                "id": api.id,
                "api_name": api.api_name,
                "api_path": api.api_path,
                "description": api.description,
                "data_source_name": api.data_source.name if api.data_source else None,
                "data_source_type": api.data_source.source_type if api.data_source else None,
                "http_method": api.http_method,
                "response_format": api.response_format,
                "is_active": api.is_active,
                "total_calls": api.total_calls,
                "success_calls": api.success_calls,
                "success_rate": (api.success_calls / api.total_calls * 100) if api.total_calls > 0 else 0,
                "last_call_time": api.last_call_time,
                "parameter_count": len(api.parameters),
                "created_by": api.created_by,
                "created_at": api.created_at
            }
            api_list.append(api_data)

        return create_response(
            data={
                "apis": api_list,
                "total_count": total_count,
                "page_info": {
                    "skip": skip,
                    "limit": limit,
                    "has_next": skip + limit < total_count
                }
            },
            message="获取API列表成功"
        )

    except Exception as e:
        logger.error(f"获取API列表失败: {e}")
        raise HTTPException(status_code=500, detail="获取API列表失败")


@router.get("/{api_id}", summary="获取API详情")
async def get_custom_api(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API的详细信息"""
    try:
        api = await custom_api_service.get_api(db, api_id)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        # 构造详细响应数据
        response_data = {
            "id": api.id,
            "api_name": api.api_name,
            "api_path": api.api_path,
            "description": api.description,
            "data_source": {
                "id": api.data_source.id,
                "name": api.data_source.name,
                "type": api.data_source.source_type,
                "host": _safe_get_host(api.data_source.connection_config)
            } if api.data_source else None,
            "sql_template": api.sql_template,
            "http_method": api.http_method,
            "response_format": api.response_format,
            "is_active": api.is_active,
            "cache_ttl": api.cache_ttl,
            "rate_limit": api.rate_limit,
            "statistics": {
                "total_calls": api.total_calls,
                "success_calls": api.success_calls,
                "failed_calls": api.total_calls - api.success_calls,
                "success_rate": (api.success_calls / api.total_calls * 100) if api.total_calls > 0 else 0,
                "last_call_time": api.last_call_time
            },
            "parameters": [
                {
                    "id": param.id,
                    "param_name": param.param_name,
                    "param_type": param.param_type,
                    "is_required": param.is_required,
                    "default_value": param.default_value,
                    "description": param.description,
                    "validation_rule": param.validation_rule
                }
                for param in api.parameters
            ],
            "created_by": api.created_by,
            "created_at": api.created_at,
            "updated_at": api.updated_at
        }

        return create_response(data=response_data, message="获取API详情成功")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取API详情失败: {e}")
        raise HTTPException(status_code=500, detail="获取API详情失败")


@router.put("/{api_id}", summary="更新API")
async def update_custom_api(
        api_id: int,
        update_request: UpdateAPIRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """更新指定的自定义API"""
    try:
        api = await custom_api_service.update_api(db, api_id, update_request)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        return create_response(
            data={
                "api_id": api.id,
                "api_name": api.api_name,
                "updated_at": api.updated_at
            },
            message=f"API '{api.api_name}' 更新成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新API失败: {e}")
        raise HTTPException(status_code=500, detail="更新API失败")


@router.delete("/{api_id}", summary="删除API")
async def delete_custom_api(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """删除指定的自定义API"""
    try:
        success = await custom_api_service.delete_api(db, api_id)
        if not success:
            raise HTTPException(status_code=404, detail="API不存在")

        return create_response(message="API删除成功")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除API失败: {e}")
        raise HTTPException(status_code=500, detail="删除API失败")


@router.post("/{api_id}/toggle", summary="切换API状态")
async def toggle_api_status(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """启用/禁用API"""
    try:
        api = await custom_api_service.get_api(db, api_id)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        # 切换状态
        new_status = not api.is_active
        update_request = UpdateAPIRequest(is_active=new_status)

        updated_api = await custom_api_service.update_api(db, api_id, update_request)

        status_text = "启用" if new_status else "禁用"
        return create_response(
            data={
                "api_id": api_id,
                "api_name": updated_api.api_name,
                "is_active": new_status
            },
            message=f"API已{status_text}"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换API状态失败: {e}")
        raise HTTPException(status_code=500, detail="操作失败")


@router.post("/validate-sql", summary="验证SQL模板")
async def validate_sql_template(
        sql_template: str = Body(..., description="SQL模板"),
        data_source_id: int = Body(..., description="数据源ID"),
        db: AsyncSession = Depends(get_async_db)
):
    """验证SQL模板的有效性"""
    try:
        # 获取数据源
        data_source = await custom_api_service._get_data_source(db, data_source_id)
        if not data_source:
            raise HTTPException(status_code=404, detail="数据源不存在")

        # 验证SQL模板
        validation_result = await custom_api_service.validate_sql_template(sql_template, data_source)

        return create_response(
            data={
                "is_valid": validation_result.is_valid,
                "error_message": validation_result.error_message,
                "extracted_parameters": validation_result.extracted_parameters,
                "parameter_count": validation_result.parameter_count,
                "rendered_sql_example": validation_result.rendered_sql_example
            },
            message="SQL模板验证完成"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"SQL验证失败: {e}")
        raise HTTPException(status_code=500, detail="验证失败")


@router.post("/{api_id}/test", summary="测试API执行")
async def test_api_execution(
        api_id: int,
        request: Request,
        test_params: Optional[Dict[str, Any]] = Body(None, description="测试参数"),
        api_key: Optional[str] = Header(None, alias="X-API-Key"),  # API密钥（可选）
        db: AsyncSession = Depends(get_async_db)
):
    """
    测试API执行，用于开发调试

    支持两种测试模式：
    1. 管理员测试：不提供API Key，直接测试（用于开发调试）
    2. 用户测试：提供API Key，按真实权限验证（用于验证权限配置）
    """
    try:
        # 获取API详情
        api = await custom_api_service.get_api_by_id(db, api_id)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        # 获取客户端信息
        client_ip = request.client.host
        user_agent = request.headers.get("user-agent")

        # 🔧 认证信息（用于记录日志）
        auth_info = None

        # 🔧 如果提供了API Key，进行权限验证
        if api_key:
            from app.services.api_user_service import api_user_service

            logger.info(f"用户测试模式：使用API Key验证权限")

            # 验证API Key
            valid, error_msg, key_record = await api_user_service.validate_api_key(
                db=db,
                api_key=api_key,
                api_id=api_id,
                client_ip=client_ip
            )

            if not valid:
                logger.warning(f"API Key验证失败: {error_msg}")
                raise HTTPException(
                    status_code=401,
                    detail=error_msg or "API Key无效或已过期"
                )

            # 🔧 如果API是限定用户模式，检查用户权限
            if api.access_level == "restricted":
                from sqlalchemy import select, and_
                from app.models.api_user import APIUserPermission

                # 查询用户是否有权限
                query = select(APIUserPermission).where(
                    and_(
                        APIUserPermission.api_id == api_id,
                        APIUserPermission.user_id == key_record.user_id
                    )
                )
                result = await db.execute(query)
                permission = result.scalar_one_or_none()

                if not permission:
                    logger.warning(
                        f"用户无权限访问API: user_id={key_record.user_id}, api_id={api_id}"
                    )
                    raise HTTPException(
                        status_code=403,
                        detail="您没有权限访问此API"
                    )

                logger.info(f"权限验证通过: user_id={key_record.user_id}")

            # 设置认证信息
            auth_info = {
                "auth_type": "api_key",
                "api_key_id": key_record.id,
                "user_id": key_record.user_id
            }
        else:
            logger.info(f"管理员测试模式：跳过权限验证")

        # 使用提供的测试参数，如果没有则使用空字典
        params = test_params or {}

        # 执行API查询
        result = await custom_api_service.execute_api_query(
            db=db,
            api_id=api_id,
            request_params=params,
            client_ip=client_ip,
            user_agent=user_agent,
            auth_info=auth_info  # 🔧 传递认证信息用于日志记录
        )

        # 🔧 返回结果，包含测试模式提示
        test_mode = "用户测试模式（已验证权限）" if api_key else "管理员测试模式（未验证权限）"

        return create_response(
            data={
                "success": result.success,
                "data": result.data[:10] if result.data else [],  # 只返回前10条记录用于测试
                "total_count": result.total_count,
                "response_time_ms": result.response_time_ms,
                "executed_sql": result.executed_sql,
                "error_message": result.error_message,
                "test_mode": test_mode,  # 🔧 显示测试模式
                "test_note": "测试模式：只显示前10条记录" if result.success and len(result.data) > 10 else None
            },
            message="API测试完成"
        )

    except HTTPException:
        # 重新抛出HTTP异常（如401、403、404）
        raise
    except Exception as e:
        logger.error(f"API测试失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"测试失败: {str(e)}")


@router.post("/management/refresh-routes", summary="刷新所有动态路由")
async def refresh_dynamic_routes(
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_async_db)
):
    """刷新所有动态API路由"""
    try:
        result = await dynamic_api_router.refresh_all_apis(db)

        return create_response(
            data=result,
            message="动态路由刷新完成"
        )

    except Exception as e:
        logger.error(f"刷新动态路由失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/management/routes-info", summary="获取动态路由信息")
async def get_dynamic_routes_info():
    """获取当前已注册的动态路由信息"""
    try:
        routes_info = dynamic_api_router.get_registered_apis_info()

        return create_response(
            data=routes_info,
            message="获取动态路由信息成功"
        )

    except Exception as e:
        logger.error(f"获取动态路由信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{api_id}/management/register-route", summary="注册单个API路由")
async def register_single_api_route(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """注册指定API的动态路由"""
    try:
        api = await custom_api_service.get_api(db, api_id)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        if not api.is_active:
            raise HTTPException(status_code=400, detail="API未激活，无法注册路由")

        success = await dynamic_api_router.register_api(api)

        if success:
            return create_response(
                data={
                    "api_id": api_id,
                    "api_name": api.api_name,
                    "api_path": api.api_path,
                    "registered": True
                },
                message=f"API路由注册成功: {api.api_name}"
            )
        else:
            raise HTTPException(status_code=500, detail="路由注册失败")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"注册API路由失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/management/{api_name}/unregister-route", summary="注销API路由")
async def unregister_api_route(api_name: str):
    """注销指定API的动态路由"""
    try:
        success = await dynamic_api_router.unregister_api(api_name)

        if success:
            return create_response(
                data={"api_name": api_name, "unregistered": True},
                message=f"API路由注销成功: {api_name}"
            )
        else:
            raise HTTPException(status_code=404, detail="API路由不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"注销API路由失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/management/cache/stats", summary="获取缓存统计信息")
async def get_cache_statistics():
    """获取API缓存统计信息"""
    try:
        cache_stats = custom_api_cache.get_cache_stats()

        return create_response(
            data=cache_stats,
            message="获取缓存统计成功"
        )

    except Exception as e:
        logger.error(f"获取缓存统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/management/cache/clear", summary="清理缓存")
async def clear_api_cache(
        api_name: Optional[str] = Query(None, description="API名称"),
        clear_all: bool = Query(False, description="是否清理所有缓存")
):
    """清理API缓存"""
    try:
        if clear_all:
            result = await custom_api_cache.clear_all_cache()
            message = f"清理所有缓存成功，共清理 {result['total_cleared']} 项"
        elif api_name:
            cleared_count = await custom_api_cache.clear_api_cache(api_name)
            result = {"api_name": api_name, "cleared_count": cleared_count}
            message = f"清理API缓存成功: {api_name}"
        else:
            raise HTTPException(status_code=400, detail="请指定API名称或设置清理全部")

        return create_response(
            data=result,
            message=message
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"清理缓存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/management/rate-limit/stats", summary="获取频率限制统计")
async def get_rate_limit_statistics():
    """获取频率限制统计信息"""
    try:
        stats = rate_limiter.get_rate_limit_stats()

        return create_response(
            data=stats,
            message="获取频率限制统计成功"
        )

    except Exception as e:
        logger.error(f"获取频率限制统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/management/rate-limit/reset", summary="重置客户端频率限制")
async def reset_client_rate_limit(
        api_id: int = Query(..., description="API ID"),
        client_ip: str = Query(..., description="客户端IP")
):
    """重置指定客户端的频率限制"""
    try:
        success = await rate_limiter.reset_client_limit(api_id, client_ip)

        if success:
            return create_response(
                data={
                    "api_id": api_id,
                    "client_ip": client_ip,
                    "reset": True
                },
                message="客户端频率限制重置成功"
            )
        else:
            raise HTTPException(status_code=500, detail="重置失败")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"重置客户端频率限制失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{api_id}/management/access-logs", summary="获取API访问日志")
async def get_api_access_logs(
        api_id: int,
        db: AsyncSession = Depends(get_async_db),
        skip: int = Query(0, ge=0, description="跳过记录数"),
        limit: int = Query(100, ge=1, le=1000, description="每页记录数"),
        status_code: Optional[int] = Query(None, description="HTTP状态码过滤"),
        client_ip: Optional[str] = Query(None, description="客户端IP过滤")
):
    """获取指定API的访问日志"""
    try:
        # 构建查询条件
        conditions = [APIAccessLog.api_id == api_id]

        if status_code:
            conditions.append(APIAccessLog.status_code == status_code)

        if client_ip:
            conditions.append(APIAccessLog.client_ip == client_ip)

        # 查询访问日志
        query = (
            select(APIAccessLog)
            .where(and_(*conditions))
            .order_by(desc(APIAccessLog.access_time))
            .offset(skip)
            .limit(limit)
        )

        result = await db.execute(query)
        logs = result.scalars().all()

        # 转换为字典格式
        log_data = []
        for log in logs:
            log_data.append({
                "id": log.id,
                "client_ip": log.client_ip,
                "user_agent": log.user_agent,
                "request_params": log.request_params,
                "response_time_ms": log.response_time_ms,
                "status_code": log.status_code,
                "result_count": log.result_count,
                "error_message": log.error_message,
                "error_type": log.error_type,
                "access_time": log.access_time
            })

        return create_response(
            data={
                "logs": log_data,
                "total_count": len(log_data),
                "api_id": api_id,
                "filters": {
                    "status_code": status_code,
                    "client_ip": client_ip,
                    "skip": skip,
                    "limit": limit
                }
            },
            message="获取访问日志成功"
        )

    except Exception as e:
        logger.error(f"获取访问日志失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/management/system/status", summary="获取自定义API系统状态")
async def get_custom_api_system_status(
        db: AsyncSession = Depends(get_async_db)
):
    """获取自定义API系统整体状态"""
    try:
        # 获取API统计
        apis, total_apis = await custom_api_service.get_apis_list(db, limit=10000)
        active_apis = len([api for api in apis if api.is_active])

        # 获取缓存统计
        cache_stats = custom_api_cache.get_cache_stats()

        # 获取频率限制统计
        rate_limit_stats = rate_limiter.get_rate_limit_stats()

        # 获取动态路由信息
        routes_info = dynamic_api_router.get_registered_apis_info()

        # 计算总调用次数
        total_calls = sum(api.total_calls for api in apis)
        total_success_calls = sum(api.success_calls for api in apis)
        success_rate = (total_success_calls / total_calls * 100) if total_calls > 0 else 0

        system_status = {
            "system_info": {
                "total_apis": total_apis,
                "active_apis": active_apis,
                "inactive_apis": total_apis - active_apis,
                "registered_routes": routes_info["total_registered"],
                "total_calls": total_calls,
                "success_rate": round(success_rate, 2)
            },
            "cache_info": cache_stats,
            "rate_limit_info": rate_limit_stats,
            "routes_info": {
                "registered_count": routes_info["total_registered"],
                "route_prefix": routes_info["router_info"]["prefix"]
            },
            "last_updated": datetime.now()
        }

        return create_response(
            data=system_status,
            message="获取系统状态成功"
        )

    except Exception as e:
        logger.error(f"获取系统状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{api_id}/docs/openapi", summary="获取API的OpenAPI规范")
async def get_api_openapi_spec(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API的OpenAPI 3.0规范"""
    try:
        docs = await api_docs_generator.generate_single_api_docs(db, api_id)

        return create_response(
            data=docs["openapi_spec"],
            message=f"获取API {api_id} OpenAPI规范成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取OpenAPI规范失败: {e}")
        raise HTTPException(status_code=500, detail="获取规范失败")


@router.get("/{api_id}/docs/full", summary="获取API完整文档")
async def get_api_full_docs(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API的完整文档信息"""
    try:
        docs = await api_docs_generator.generate_single_api_docs(db, api_id)

        return create_response(
            data=docs,
            message=f"获取API {api_id} 完整文档成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取完整文档失败: {e}")
        raise HTTPException(status_code=500, detail="获取文档失败")

def _safe_get_host(connection_config):
    """安全获取主机地址"""
    try:
        if isinstance(connection_config, dict):
            return connection_config.get("host", "")
        elif isinstance(connection_config, str):
            return json.loads(connection_config).get("host", "")
        else:
            return ""
    except Exception:
        return ""
