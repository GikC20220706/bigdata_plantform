# app/api/v1/user_cluster.py
"""
用户计算集群管理API - 对应前端计算集群管理页面
"""
import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Body
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Optional, List
from pydantic import BaseModel, Field, validator, field_validator
from loguru import logger

from app.services.cluster_node_discovery import WebApiClusterDiscovery
from app.utils.response import create_response
from app.utils.database import get_async_db
from app.services.user_cluster_service import user_cluster_service

router = APIRouter()


# 请求模型
class ClusterPageRequest(BaseModel):
    """分页查询请求"""
    page: int = Field(1, ge=1, description="页码")
    pageSize: int = Field(10, ge=1, le=100, description="每页大小")
    searchKeyWord: str = Field("", description="搜索关键词")


class AddClusterRequest(BaseModel):
    """添加集群请求"""
    name: str = Field(..., max_length=200, description="集群名称")
    clusterType: str = Field(..., description="集群类型")
    remark: str = Field("", description="备注")

    # Web API连接配置（扩展支持HA）
    webApiConfig: Dict = Field(default={}, description="Web API连接配置")
    autoDiscovery: bool = Field(default=True, description="是否自动发现节点")

    @field_validator('webApiConfig')
    def validate_web_api_config(cls, v, values):
        """验证Web API配置"""
        cluster_type = values.data.get('clusterType', '').lower() if hasattr(values, 'data') else ''

        if cluster_type == 'hadoop':
            required_fields = ['namenode_host', 'namenode_web_port']
            for field in required_fields:
                if field not in v:
                    raise ValueError(f"Hadoop集群缺少必需配置: {field}")

            # 检查HA配置（可选）
            if 'namenode_ha_hosts' in v and not isinstance(v['namenode_ha_hosts'], list):
                raise ValueError("namenode_ha_hosts必须是数组")

            if 'journalnode_hosts' in v and not isinstance(v['journalnode_hosts'], list):
                raise ValueError("journalnode_hosts必须是数组")

        return v


class UpdateClusterRequest(BaseModel):
    """更新集群请求"""
    id: int = Field(..., description="集群ID")
    name: str = Field(..., max_length=200, description="集群名称")
    clusterType: str = Field(..., description="集群类型")
    remark: str = Field("", description="备注")


class ClusterIdRequest(BaseModel):
    """集群ID请求"""
    clusterId: int = Field(..., description="集群ID")


# API端点
@router.post("/pageCluster", summary="分页查询计算集群")
async def page_clusters(
        request: ClusterPageRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    分页查询计算集群列表
    对应前端 GetComputerGroupList 函数
    """
    try:
        result = await user_cluster_service.get_clusters_list(
            db=db,
            page=request.page,
            page_size=request.pageSize,
            search_keyword=request.searchKeyWord
        )

        return create_response(
            data=result,
            message="获取计算集群列表成功"
        )

    except Exception as e:
        logger.error(f"分页查询计算集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/addCluster", summary="添加计算集群（支持自动节点发现）")
async def add_cluster(
        request: AddClusterRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    添加新的计算集群，支持自动节点发现
    """
    try:
        result = await user_cluster_service.add_cluster(
            db=db,
            name=request.name,
            cluster_type=request.clusterType,
            remark=request.remark,
            web_api_config=request.webApiConfig,
            auto_discovery=request.autoDiscovery
        )

        return create_response(
            data=result,
            message=f"成功添加计算集群: {request.name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"添加计算集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/updateCluster", summary="更新计算集群")
async def update_cluster(
        request: UpdateClusterRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    更新计算集群信息
    对应前端 UpdateComputerGroupData 函数
    """
    try:
        result = await user_cluster_service.update_cluster(
            db=db,
            cluster_id=request.id,
            name=request.name,
            cluster_type=request.clusterType,
            remark=request.remark
        )

        return create_response(
            data=result,
            message=f"成功更新计算集群: {request.name}"
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新计算集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deleteCluster", summary="删除计算集群")
async def delete_cluster(
        request: ClusterIdRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    删除计算集群
    对应前端 DeleteComputerGroupData 函数
    """
    try:
        success = await user_cluster_service.delete_cluster(
            db=db,
            cluster_id=request.clusterId
        )

        return create_response(
            data={"success": success},
            message="成功删除计算集群"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"删除计算集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/checkCluster", summary="检测计算集群")
async def check_cluster(
        request: ClusterIdRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    检测计算集群状态
    对应前端 CheckComputerGroupData 函数
    """
    try:
        result = await user_cluster_service.check_cluster(
            db=db,
            cluster_id=request.clusterId
        )

        return create_response(
            data=result,
            message="集群检测完成"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"检测计算集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/setDefaultCluster", summary="设置默认集群")
async def set_default_cluster(
        request: ClusterIdRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    设置默认计算集群
    对应前端 SetDefaultComputerGroup 函数
    """
    try:
        success = await user_cluster_service.set_default_cluster(
            db=db,
            cluster_id=request.clusterId
        )

        return create_response(
            data={"success": success},
            message="成功设置默认集群"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"设置默认集群失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", summary="获取集群汇总信息")
async def get_cluster_summary(db: AsyncSession = Depends(get_async_db)):
    """
    获取集群汇总信息（用于首页系统监控）
    """
    try:
        result = await user_cluster_service.get_cluster_summary(db)

        return create_response(
            data=result,
            message="获取集群汇总信息成功"
        )

    except Exception as e:
        logger.error(f"获取集群汇总信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/rediscoverNodes", summary="重新发现集群节点")
async def rediscover_cluster_nodes(
        request: ClusterIdRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    重新发现指定集群的节点信息
    """
    try:
        result = await user_cluster_service.rediscover_cluster_nodes(
            db=db,
            cluster_id=request.clusterId
        )

        return create_response(
            data=result,
            message="重新发现集群节点成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"重新发现集群节点失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/getClusterNodes/{cluster_id}", summary="获取集群节点详情")
async def get_cluster_nodes(
        cluster_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """
    获取指定集群的节点详细信息
    """
    try:
        result = await user_cluster_service.get_cluster_nodes_detail(
            db=db,
            cluster_id=cluster_id
        )

        return create_response(
            data=result,
            message="获取集群节点信息成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"获取集群节点信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/testConnection", summary="测试集群连接")
async def test_cluster_connection(
        connection_config: Dict = Body(..., description="连接配置")
):
    """
    测试集群连接配置是否正确
    """
    try:
        cluster_type = connection_config.get('cluster_type')
        web_api_config = connection_config.get('web_api_config')

        if not cluster_type or not web_api_config:
            raise ValueError("缺少必要的连接配置参数")

        # 创建临时发现服务实例测试连接
        discovery_service = WebApiClusterDiscovery()

        # 执行连接测试（限制超时时间）
        test_result = await asyncio.wait_for(
            discovery_service.discover_cluster_nodes(cluster_type, web_api_config),
            timeout=30  # 30秒超时
        )

        if test_result.get('success'):
            return create_response(
                data={
                    'connection_status': 'success',
                    'nodes_discovered': len(test_result.get('nodes', [])),
                    'cluster_info': test_result.get('cluster_info', {}),
                    'test_time': datetime.now().isoformat()
                },
                message="集群连接测试成功"
            )
        else:
            return create_response(
                data={
                    'connection_status': 'failed',
                    'error': test_result.get('error'),
                    'test_time': datetime.now().isoformat()
                },
                message="集群连接测试失败",
                success=False
            )

    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="连接测试超时")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"集群连接测试失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supportedTypes", summary="获取支持的集群类型")
async def get_supported_cluster_types():
    """
    获取支持的集群类型和配置说明
    """
    try:
        supported_types = await user_cluster_service.get_supported_cluster_types()

        return create_response(
            data=supported_types,
            message="获取支持的集群类型成功"
        )

    except Exception as e:
        logger.error(f"获取支持的集群类型失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))