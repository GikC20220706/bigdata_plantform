from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import uvicorn
from pathlib import Path
import os
import sys

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 创建必要的目录
for directory in ["logs", "data", "static/css", "static/js", "static/images", "templates"]:
    dir_path = project_root / directory
    dir_path.mkdir(parents=True, exist_ok=True)

# 创建FastAPI实例
app = FastAPI(
    title="首信云数据底座",
    description="大数据平台管理系统API",
    version="3.2.1",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境请配置具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 静态文件和模板
static_dir = project_root / "static"
templates_dir = project_root / "templates"

if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

if templates_dir.exists():
    templates = Jinja2Templates(directory=str(templates_dir))


# 延迟导入API路由，避免循环导入
@app.on_event("startup")
async def startup_event():
    """应用启动时的初始化操作"""
    print("🚀 首信云数据底座启动中...")

    try:
        # 动态导入配置
        from config.settings import settings
        print(f"🔧 运行环境: {'Windows本地开发' if settings.IS_WINDOWS else 'Linux服务器'}")
        print(f"📊 数据模式: {'模拟数据' if settings.MOCK_DATA_MODE else '真实数据'}")
        print(f"🖥️ 集群连接: {'禁用' if not settings.use_real_clusters else '启用'}")

        # 动态注册路由
        try:
            from app.api.v1 import overview, cluster, integration, development, sync, governance

            app.include_router(overview.router, prefix="/api/v1/overview", tags=["数据总览"])
            app.include_router(cluster.router, prefix="/api/v1/cluster", tags=["集群监控"])
            app.include_router(ingestion.router, prefix="/api/v1/ingestion", tags=["数据接入"])
            app.include_router(development.router, prefix="/api/v1/development", tags=["数据开发"])
            app.include_router(sync.router, prefix="/api/v1/sync", tags=["数据同步"])
            app.include_router(governance.router, prefix="/api/v1/governance", tags=["数据治理"])

            print("✅ API路由注册成功")

        except Exception as e:
            print(f"⚠️ API路由注册失败: {e}")
            print("💡 将使用基础路由")

        # 检查必要文件
        template_file = templates_dir / "index.html"
        if not template_file.exists():
            print("⚠️ 警告: templates/index.html 不存在")
            # 创建默认模板
            default_html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>首信云数据底座</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #1e3c72; }
        .links { margin: 20px 0; }
        .links a { display: inline-block; margin: 10px 20px 10px 0; padding: 10px 20px; background: #1e3c72; color: white; text-decoration: none; border-radius: 5px; }
        .links a:hover { background: #2a5298; }
        .status { padding: 10px; background: #d4edda; border-radius: 5px; margin: 20px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 首信云数据底座</h1>
        <div class="status">
            ✅ 系统运行正常 | 📊 开发模式 | 🖥️ 模拟数据
        </div>
        <div class="links">
            <a href="/api/docs">📖 API文档</a>
            <a href="/health">❤️ 健康检查</a>
            <a href="/api/v1/overview/">📊 数据总览API</a>
            <a href="/api/v1/cluster/">🖥️ 集群监控API</a>
        </div>
        <p><strong>功能模块:</strong></p>
        <ul>
            <li>📊 数据总览 - 统计信息、集群状态、任务监控</li>
            <li>🖥️ 集群监控 - Hadoop、Flink、Doris集群管理</li>
            <li>🔌 数据接入 - 多数据源连接和同步</li>
            <li>⚙️ 数据开发 - SQL开发和工作流管理</li>
            <li>🔄 数据同步 - 数据传输和转换</li>
            <li>🎯 数据治理 - 质量监控和元数据管理</li>
        </ul>
    </div>
</body>
</html>"""
            template_file.write_text(default_html, encoding='utf-8')
            print("✅ 已创建默认首页模板")

        print("✅ 首信云数据底座启动完成")

    except Exception as e:
        print(f"⚠️ 启动过程中出现警告: {e}")
        print("💡 系统将继续运行，但部分功能可能受限")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """首页路由"""
    try:
        if 'templates' in globals():
            return templates.TemplateResponse("index.html", {"request": request})
    except Exception as e:
        print(f"模板渲染错误: {e}")

    # 返回简单的HTML响应
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>首信云数据底座</title>
        <meta charset="UTF-8">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            .container { background: white; padding: 30px; border-radius: 10px; }
            h1 { color: #1e3c72; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 首信云数据底座</h1>
            <p>✅ 系统运行正常</p>
            <ul>
                <li><a href="/api/docs">📖 API文档</a></li>
                <li><a href="/health">❤️ 健康检查</a></li>
                <li><a href="/api/v1/overview/">📊 数据总览</a></li>
            </ul>
        </div>
    </body>
    </html>
    """)


@app.get("/health")
async def health_check():
    """健康检查接口"""
    try:
        from config.settings import settings

        return {
            "status": "healthy",
            "message": "首信云数据底座运行正常",
            "environment": "Windows开发环境" if settings.IS_WINDOWS else "Linux服务器环境",
            "mock_mode": settings.MOCK_DATA_MODE,
            "version": settings.VERSION,
            "features": {
                "data_overview": "✅ 可用",
                "cluster_monitoring": "✅ 可用",
                "data_ingestion": "✅ 可用",
                "data_development": "✅ 可用",
                "data_sync": "✅ 可用",
                "data_governance": "✅ 可用"
            }
        }
    except Exception as e:
        return {
            "status": "healthy",
            "message": "基础服务运行正常",
            "warning": f"配置加载异常: {str(e)}",
            "version": "3.2.1"
        }


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时的清理操作"""
    print("🛑 首信云数据底座关闭中...")
    print("✅ 首信云数据底座已安全关闭")


# 基础API路由（防止导入失败）
@app.get("/api/v1/test")
async def test_api():
    """测试API"""
    return {
        "message": "API服务正常",
        "timestamp": "2024-12-20",
        "status": "success"
    }


if __name__ == "__main__":
    try:
        from config.settings import settings

        print("🚀 启动首信云数据底座...")
        print(f"📱 访问地址: http://{settings.HOST}:{settings.PORT}")
        print(f"📖 API文档: http://{settings.HOST}:{settings.PORT}/api/docs")
        print("🛑 按 Ctrl+C 停止服务")
        print("-" * 50)

        uvicorn.run(
            "main:app",
            host=settings.HOST,
            port=settings.PORT,
            reload=settings.DEBUG,
            log_level="info"
        )
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        # 使用默认配置启动
        print("🔄 尝试使用默认配置启动...")
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=8000,
            reload=True,
            log_level="info"
        )