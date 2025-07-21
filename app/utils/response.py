"""
TODO 为大数据平台提供的标准化API响应工具。该模块为所有API端点提供一致的响应格式，包括成功、错误和验证响应。
"""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
from fastapi.encoders import jsonable_encoder


class ApiResponse(BaseModel):
    """Standard API response model."""
    code: int = Field(..., description="HTTP status code")
    message: str = Field(..., description="Response message")
    data: Optional[Any] = Field(None, description="Response data")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")

    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


def create_response(
        data: Optional[Any] = None,
        message: str = "操作成功",
        code: int = 200
) -> ApiResponse:
    """
    Create a standard API response.

    Args:
        data: Response data
        message: Response message
        code: HTTP status code

    Returns:
        ApiResponse: Formatted response object
    """

    return jsonable_encoder(ApiResponse(
        code=code,
        message=message,
        data=data,
        timestamp=datetime.now()
    ))


def create_success_response(
        data: Optional[Any] = None,
        message: str = "操作成功"
) -> ApiResponse:
    """
    Create a success response.

    Args:
        data: Response data
        message: Success message

    Returns:
        ApiResponse: Success response object
    """
    return create_response(data=data, message=message, code=200)


def create_error_response(
        message: str = "操作失败",
        code: int = 500,
        data: Optional[Any] = None
) -> ApiResponse:
    """
    Create an error response.

    Args:
        message: Error message
        code: HTTP error code
        data: Optional error data

    Returns:
        ApiResponse: Error response object
    """
    return jsonable_encoder(ApiResponse(
        code=code,
        message=message,
        data=data,
        timestamp=datetime.now()
    ))


def create_validation_error_response(
        message: str = "参数验证失败",
        errors: Optional[Dict] = None
) -> ApiResponse:
    """
    Create a validation error response.

    Args:
        message: Validation error message
        errors: Detailed validation errors

    Returns:
        ApiResponse: Validation error response object
    """
    return ApiResponse(
        code=400,
        message=message,
        data={"errors": errors} if errors else None,
        timestamp=datetime.now()
    )


def create_not_found_response(
        message: str = "资源不存在"
) -> ApiResponse:
    """
    Create a resource not found response.

    Args:
        message: Not found message

    Returns:
        ApiResponse: Not found response object
    """
    return create_error_response(message=message, code=404)


def create_unauthorized_response(
        message: str = "未授权访问"
) -> ApiResponse:
    """
    Create an unauthorized access response.

    Args:
        message: Unauthorized message

    Returns:
        ApiResponse: Unauthorized response object
    """
    return create_error_response(message=message, code=401)


def create_forbidden_response(
        message: str = "禁止访问"
) -> ApiResponse:
    """
    Create a forbidden access response.

    Args:
        message: Forbidden message

    Returns:
        ApiResponse: Forbidden response object
    """
    return create_error_response(message=message, code=403)