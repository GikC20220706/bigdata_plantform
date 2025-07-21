@echo off
echo 🚀 启动首信云数据底座 (Windows开发环境)...

REM 检查Python版本
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python未安装或未添加到PATH
    pause
    exit /b 1
)

REM 激活虚拟环境（如果存在）
if exist venv\Scripts\activate.bat (
    echo 📦 激活虚拟环境...
    call venv\Scripts\activate.bat
)

REM 安装依赖
echo 📦 检查依赖...
pip install -r requirements-windows.txt

REM 创建必要的目录
if not exist logs mkdir logs
if not exist data mkdir data
if not exist static\css mkdir static\css
if not exist static\js mkdir static\js
if not exist static\images mkdir static\images
if not exist templates mkdir templates

REM 复制前端文件
if exist Untitled-1.html (
    echo 📄 复制前端文件...
    copy Untitled-1.html templates\index.html
)

REM 复制Windows开发环境配置
if not exist .env (
    echo ⚙️ 创建开发环境配置...
    copy .env.windows .env
)

REM 启动应用
echo 🌟 启动应用服务器...
echo 📱 访问地址: http://127.0.0.1:8000
echo 📊 API文档: http://127.0.0.1:8000/api/docs
echo.
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

pause

# install_windows.bat - Windows安装脚本
@echo off
echo 📦 安装首信云数据底座 (Windows开发环境)...

REM 检查Python版本
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python未安装，请先安装Python 3.9+
    echo 📥 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

REM 创建虚拟环境
echo 🔧 创建虚拟环境...
python -m venv venv
call venv\Scripts\activate.bat

REM 升级pip
echo ⬆️ 升级pip...
python -m pip install --upgrade pip

REM 安装依赖
echo 📦 安装依赖包...
pip install -r requirements-windows.txt

REM 创建目录结构
echo 📁 创建目录结构...
if not exist logs mkdir logs
if not exist data\sample_data mkdir data\sample_data
if not exist static\css mkdir static\css
if not exist static\js mkdir static\js
if not exist static\images mkdir static\images
if not exist templates mkdir templates

REM 复制配置文件
if not exist .env (
    echo ⚙️ 创建环境配置文件...
    copy .env.windows .env
)

echo.
echo ✅ 安装完成！
echo 📖 使用说明：
echo    1. 双击 start_windows.bat 启动服务