"""
API文档生成和查看的REST端点
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Dict, Any
import json

from app.utils.api_docs_generator import api_docs_generator
from app.utils.database import get_async_db
from app.utils.response import create_response
from loguru import logger

router = APIRouter(prefix="/api-docs", tags=["API文档生成"])


@router.get("/{api_id}/openapi.json", summary="获取单个API的OpenAPI规范")
async def get_single_api_openapi(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """获取指定API的完整OpenAPI 3.0规范（JSON格式）"""
    try:
        docs = await api_docs_generator.generate_single_api_docs(db, api_id)

        return JSONResponse(
            content=docs["openapi_spec"],
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=300"  # 缓存5分钟
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"生成API OpenAPI规范失败: {e}")
        raise HTTPException(status_code=500, detail="生成文档失败")


@router.get("/{api_id}/docs", summary="获取单个API的完整文档")
async def get_single_api_docs(
        api_id: int,
        db: AsyncSession = Depends(get_async_db),
        format: str = Query("json", description="文档格式: json, html")
):
    """获取指定API的完整文档，包括OpenAPI规范和扩展信息"""
    try:
        docs = await api_docs_generator.generate_single_api_docs(db, api_id)

        if format.lower() == "html":
            return HTMLResponse(
                content=_generate_html_docs(docs),
                headers={"Cache-Control": "public, max-age=300"}
            )
        else:
            return create_response(
                data=docs,
                message=f"获取API {api_id} 文档成功"
            )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"生成API文档失败: {e}")
        raise HTTPException(status_code=500, detail="生成文档失败")


@router.get("/collection/openapi.json", summary="获取所有API的OpenAPI规范")
async def get_all_apis_openapi(
        db: AsyncSession = Depends(get_async_db),
        include_inactive: bool = Query(False, description="是否包含未激活的API")
):
    """获取所有API的集合OpenAPI 3.0规范（JSON格式）"""
    try:
        docs = await api_docs_generator.generate_all_apis_docs(db, include_inactive)

        return JSONResponse(
            content=docs["openapi_spec"],
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "public, max-age=600"  # 缓存10分钟
            }
        )

    except Exception as e:
        logger.error(f"生成API集合OpenAPI规范失败: {e}")
        raise HTTPException(status_code=500, detail="生成文档失败")


@router.get("/collection/docs", summary="获取所有API的集合文档")
async def get_all_apis_docs(
        db: AsyncSession = Depends(get_async_db),
        include_inactive: bool = Query(False, description="是否包含未激活的API"),
        format: str = Query("json", description="文档格式: json, html")
):
    """获取所有API的集合文档"""
    try:
        docs = await api_docs_generator.generate_all_apis_docs(db, include_inactive)

        if format.lower() == "html":
            return HTMLResponse(
                content=_generate_collection_html_docs(docs),
                headers={"Cache-Control": "public, max-age=600"}
            )
        else:
            return create_response(
                data=docs,
                message="获取API集合文档成功"
            )

    except Exception as e:
        logger.error(f"生成API集合文档失败: {e}")
        raise HTTPException(status_code=500, detail="生成文档失败")


@router.get("/collection/index", summary="获取API索引")
async def get_apis_index(
        db: AsyncSession = Depends(get_async_db),
        include_inactive: bool = Query(False, description="是否包含未激活的API")
):
    """获取所有API的索引信息"""
    try:
        docs = await api_docs_generator.generate_all_apis_docs(db, include_inactive)

        return create_response(
            data={
                "api_index": docs["api_index"],
                "statistics": docs["statistics"],
                "generated_at": docs["generated_at"]
            },
            message="获取API索引成功"
        )

    except Exception as e:
        logger.error(f"获取API索引失败: {e}")
        raise HTTPException(status_code=500, detail="获取索引失败")


@router.get("/{api_id}/swagger", summary="Swagger UI页面")
async def get_swagger_ui(
        api_id: int,
        db: AsyncSession = Depends(get_async_db)
):
    """为单个API生成Swagger UI页面"""
    try:
        # 检查API是否存在
        from app.services.custom_api_service import custom_api_service
        api = await custom_api_service.get_api(db, api_id)
        if not api:
            raise HTTPException(status_code=404, detail="API不存在")

        swagger_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{api.api_name} - API文档</title>
            <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui.css" />
            <style>
                html {{ box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }}
                *, *:before, *:after {{ box-sizing: inherit; }}
                body {{ margin:0; background: #fafafa; }}
            </style>
        </head>
        <body>
            <div id="swagger-ui"></div>
            <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-bundle.js"></script>
            <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-standalone-preset.js"></script>
            <script>
                window.onload = function() {{
                    const ui = SwaggerUIBundle({{
                        url: '/api/v1/api-docs/{api_id}/openapi.json',
                        dom_id: '#swagger-ui',
                        deepLinking: true,
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIStandalonePreset
                        ],
                        plugins: [
                            SwaggerUIBundle.plugins.DownloadUrl
                        ],
                        layout: "StandaloneLayout"
                    }});
                }};
            </script>
        </body>
        </html>
        """

        return HTMLResponse(
            content=swagger_html,
            headers={"Cache-Control": "public, max-age=300"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"生成Swagger UI失败: {e}")
        raise HTTPException(status_code=500, detail="生成Swagger UI失败")


@router.get("/collection/swagger", summary="所有API的Swagger UI页面")
async def get_collection_swagger_ui():
    """为所有API生成集合Swagger UI页面"""
    try:
        swagger_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>大数据平台 - 自定义API集合文档</title>
            <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui.css" />
            <style>
                html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
                *, *:before, *:after { box-sizing: inherit; }
                body { margin:0; background: #fafafa; }
            </style>
        </head>
        <body>
            <div id="swagger-ui"></div>
            <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-bundle.js"></script>
            <script src="https://unpkg.com/swagger-ui-dist@4.15.5/swagger-ui-standalone-preset.js"></script>
            <script>
                window.onload = function() {
                    const ui = SwaggerUIBundle({
                        url: '/api/v1/api-docs/collection/openapi.json',
                        dom_id: '#swagger-ui',
                        deepLinking: true,
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIStandalonePreset
                        ],
                        plugins: [
                            SwaggerUIBundle.plugins.DownloadUrl
                        ],
                        layout: "StandaloneLayout"
                    });
                };
            </script>
        </body>
        </html>
        """

        return HTMLResponse(
            content=swagger_html,
            headers={"Cache-Control": "public, max-age=600"}
        )

    except Exception as e:
        logger.error(f"生成集合Swagger UI失败: {e}")
        raise HTTPException(status_code=500, detail="生成Swagger UI失败")


@router.post("/{api_id}/regenerate", summary="重新生成API文档")
async def regenerate_api_docs(
        api_id: int,
        db: AsyncSession = Depends(get_async_db),
        clear_cache: bool = Query(True, description="是否清除缓存")
):
    """重新生成指定API的文档（强制刷新）"""
    try:
        if clear_cache:
            # 清除相关缓存
            from app.utils.custom_api_cache import custom_api_cache
            await custom_api_cache.clear_api_cache(f"docs_{api_id}")

        docs = await api_docs_generator.generate_single_api_docs(db, api_id)

        return create_response(
            data={
                "api_id": api_id,
                "regenerated_at": docs["generated_at"],
                "openapi_url": f"/api/v1/api-docs/{api_id}/openapi.json",
                "swagger_url": f"/api/v1/api-docs/{api_id}/swagger",
                "docs_url": f"/api/v1/api-docs/{api_id}/docs"
            },
            message=f"API {api_id} 文档重新生成成功"
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"重新生成API文档失败: {e}")
        raise HTTPException(status_code=500, detail="重新生成文档失败")


def _generate_html_docs(docs: Dict[str, Any]) -> str:
    """生成单个API的HTML文档"""
    api_info = docs["additional_docs"]["api_info"]
    sql_info = docs["additional_docs"]["sql_info"]
    usage_examples = docs["additional_docs"]["usage_examples"]
    performance_info = docs["additional_docs"]["performance_info"]

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{api_info['api_name']} - API文档</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }}
            .container {{ max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px 8px 0 0; }}
            .content {{ padding: 30px; }}
            .section {{ margin-bottom: 30px; }}
            .section h3 {{ color: #333; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
            .info-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0; }}
            .info-card {{ background: #f8f9fa; padding: 15px; border-radius: 6px; border-left: 4px solid #667eea; }}
            .code-block {{ background: #2d3748; color: #e2e8f0; padding: 20px; border-radius: 6px; overflow-x: auto; }}
            .example-tabs {{ display: flex; border-bottom: 1px solid #e2e8f0; margin-bottom: 20px; }}
            .tab-btn {{ padding: 10px 20px; border: none; background: none; cursor: pointer; border-bottom: 2px solid transparent; }}
            .tab-btn.active {{ border-bottom-color: #667eea; color: #667eea; }}
            .tab-content {{ display: none; }}
            .tab-content.active {{ display: block; }}
            .badge {{ display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
            .badge.success {{ background: #d4edda; color: #155724; }}
            .badge.warning {{ background: #fff3cd; color: #856404; }}
            .parameters-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            .parameters-table th, .parameters-table td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
            .parameters-table th {{ background: #f8f9fa; font-weight: 600; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>{api_info['api_name']}</h1>
                <p>基于SQL查询自动生成的API接口文档</p>
                <div style="margin-top: 15px;">
                    <span class="badge {'success' if api_info.get('success_rate', 0) > 90 else 'warning'}">
                        成功率: {api_info.get('success_rate', 0):.1f}%
                    </span>
                    <span class="badge success" style="margin-left: 10px;">
                        总调用: {api_info.get('total_calls', 0)} 次
                    </span>
                </div>
            </div>

            <div class="content">
                <!-- 基本信息 -->
                <div class="section">
                    <h3>📋 基本信息</h3>
                    <div class="info-grid">
                        <div class="info-card">
                            <strong>API路径</strong><br>
                            <code>{api_info['api_path']}</code>
                        </div>
                        <div class="info-card">
                            <strong>数据源</strong><br>
                            {api_info.get('data_source', '未知')}
                        </div>
                        <div class="info-card">
                            <strong>创建时间</strong><br>
                            {api_info['created_at']}
                        </div>
                        <div class="info-card">
                            <strong>最后更新</strong><br>
                            {api_info['last_updated']}
                        </div>
                    </div>
                </div>

                <!-- SQL查询 -->
                <div class="section">
                    <h3>🗃️ SQL查询模板</h3>
                    <p><strong>数据源类型:</strong> {sql_info.get('data_source_type', '未知')}</p>
                    <div class="code-block">
                        <pre>{sql_info['sql_template']}</pre>
                    </div>
                    <p><strong>提取的参数:</strong> {', '.join(sql_info['extracted_parameters']) if sql_info['extracted_parameters'] else '无'}</p>
                </div>

                <!-- 性能配置 -->
                <div class="section">
                    <h3>⚡ 性能配置</h3>
                    <div class="info-grid">
                        <div class="info-card">
                            <strong>缓存时间</strong><br>
                            {performance_info['cache_ttl']} 秒
                        </div>
                        <div class="info-card">
                            <strong>频率限制</strong><br>
                            {performance_info['rate_limit']} 次/分钟
                        </div>
                        <div class="info-card">
                            <strong>预期响应时间</strong><br>
                            {performance_info['expected_response_time']}
                        </div>
                    </div>
                </div>

                <!-- 使用示例 -->
                <div class="section">
                    <h3>💡 使用示例</h3>
                    <div class="example-tabs">
                        <button class="tab-btn active" onclick="showTab('curl')">cURL</button>
                        <button class="tab-btn" onclick="showTab('javascript')">JavaScript</button>
                        <button class="tab-btn" onclick="showTab('python')">Python</button>
                    </div>

                    <div id="curl-content" class="tab-content active">
                        <div class="code-block">
                            <pre>{usage_examples['curl_example']}</pre>
                        </div>
                    </div>

                    <div id="javascript-content" class="tab-content">
                        <div class="code-block">
                            <pre>{usage_examples['javascript_example']}</pre>
                        </div>
                    </div>

                    <div id="python-content" class="tab-content">
                        <div class="code-block">
                            <pre>{usage_examples['python_example']}</pre>
                        </div>
                    </div>
                </div>

                <!-- 快速链接 -->
                <div class="section">
                    <h3>🔗 快速链接</h3>
                    <div style="display: flex; gap: 15px; flex-wrap: wrap;">
                        <a href="/api/v1/api-docs/{api_info['api_id']}/swagger" target="_blank" 
                           style="background: #667eea; color: white; padding: 10px 20px; text-decoration: none; border-radius: 6px;">
                            📖 Swagger UI
                        </a>
                        <a href="/api/v1/api-docs/{api_info['api_id']}/openapi.json" target="_blank"
                           style="background: #48bb78; color: white; padding: 10px 20px; text-decoration: none; border-radius: 6px;">
                            📄 OpenAPI JSON
                        </a>
                        <a href="/api/v1/custom-api/{api_info['api_id']}/test" target="_blank"
                           style="background: #ed8936; color: white; padding: 10px 20px; text-decoration: none; border-radius: 6px;">
                            🧪 测试API
                        </a>
                    </div>
                </div>
            </div>
        </div>

        <script>
            function showTab(tabName) {{
                // 隐藏所有标签内容
                document.querySelectorAll('.tab-content').forEach(content => {{
                    content.classList.remove('active');
                }});

                // 移除所有按钮的active类
                document.querySelectorAll('.tab-btn').forEach(btn => {{
                    btn.classList.remove('active');
                }});

                // 显示选中的标签内容
                document.getElementById(tabName + '-content').classList.add('active');

                // 激活选中的按钮
                event.target.classList.add('active');
            }}
        </script>
    </body>
    </html>
    """

    return html


def _generate_collection_html_docs(docs: Dict[str, Any]) -> str:
    """生成API集合的HTML文档"""
    api_index = docs["api_index"]
    statistics = docs["statistics"]

    api_cards = ""
    for api in api_index:
        status_badge = "success" if api["is_active"] else "warning"
        status_text = "活跃" if api["is_active"] else "未激活"

        api_cards += f"""
        <div class="api-card">
            <div class="api-header">
                <h4>{api['api_name']}</h4>
                <span class="badge {status_badge}">{status_text}</span>
            </div>
            <p class="api-description">{api.get('description', '无描述')}</p>
            <div class="api-meta">
                <span><strong>路径:</strong> <code>{api['api_path']}</code></span><br>
                <span><strong>方法:</strong> {api['http_method']}</span> | 
                <span><strong>格式:</strong> {api['response_format']}</span><br>
                <span><strong>数据源:</strong> {api.get('data_source', '未知')}</span><br>
                <span><strong>参数数量:</strong> {api['parameter_count']}</span> | 
                <span><strong>调用次数:</strong> {api['total_calls']}</span> |
                <span><strong>成功率:</strong> {api['success_rate']:.1f}%</span>
            </div>
            <div class="api-actions">
                <a href="/api/v1/api-docs/{api['api_id']}/docs" target="_blank">📖 文档</a>
                <a href="/api/v1/api-docs/{api['api_id']}/swagger" target="_blank">🔧 Swagger</a>
                <a href="/api/v1/custom-api/{api['api_id']}/test" target="_blank">🧪 测试</a>
            </div>
        </div>
        """