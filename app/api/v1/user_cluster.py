# app/api/v1/user_cluster.py
"""
用户计算集群管理API - 对应前端计算集群管理页面
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, Optional
from pydantic import BaseModel, Field
from loguru import logger

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


@router.post("/addCluster", summary="添加计算集群")
async def add_cluster(
        request: AddClusterRequest,
        db: AsyncSession = Depends(get_async_db)
):
    """
    添加新的计算集群
    对应前端 AddComputerGroupData 函数
    """
    try:
        result = await user_cluster_service.add_cluster(
            db=db,
            name=request.name,
            cluster_type=request.clusterType,
            remark=request.remark
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