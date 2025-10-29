"""
Python执行器 - 执行Python脚本
支持多版本Python、虚拟环境、依赖管理
"""
import asyncio
import os
import tempfile
import json
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from .base_executor import JobExecutor


class PythonExecutor(JobExecutor):
    """Python脚本执行器"""

    def __init__(self):
        super().__init__("python_executor")

    async def execute(
        self,
        db: AsyncSession,
        work_config: Dict[str, Any],
        instance_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行Python脚本"""
        try:
            script = work_config.get('script')
            python_version = work_config.get('pythonVersion', 'python3')
            work_dir = work_config.get('workDir', '/tmp')
            env_vars = work_config.get('envVars', {})
            timeout = work_config.get('timeout', 300)
            requirements = work_config.get('requirements', [])  # 依赖包列表
            use_venv = work_config.get('useVenv', False)  # 是否使用虚拟环境

            if not script:
                return {
                    "success": False,
                    "message": "脚本内容为空",
                    "data": None,
                    "error": "Script is required"
                }

            # 将脚本写入临时文件
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.py',
                delete=False,
                dir=work_dir,
                encoding='utf-8'
            ) as f:
                # 如果有上下文，注入到脚本环境中
                if context:
                    context_code = f"# 注入的上下文变量\n"
                    context_code += f"__context__ = {json.dumps(context, ensure_ascii=False)}\n\n"
                    f.write(context_code)

                f.write(script)
                script_path = f.name

            try:
                # 准备环境变量
                env = os.environ.copy()
                env.update(env_vars)
                env['PYTHONIOENCODING'] = 'utf-8'

                # 如果使用虚拟环境
                if use_venv and requirements:
                    venv_result = await self._setup_virtual_env(
                        work_dir, requirements, timeout
                    )
                    if not venv_result['success']:
                        return venv_result

                    python_version = venv_result['python_path']

                # 执行Python脚本
                process = await asyncio.create_subprocess_exec(
                    python_version, script_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=work_dir,
                    env=env
                )

                # 等待执行完成（带超时）
                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    process.kill()
                    return {
                        "success": False,
                        "message": "脚本执行超时",
                        "data": None,
                        "error": f"Timeout after {timeout} seconds"
                    }

                # 解码输出
                stdout_text = stdout.decode('utf-8', errors='replace')
                stderr_text = stderr.decode('utf-8', errors='replace')

                # 检查返回码
                if process.returncode == 0:
                    return {
                        "success": True,
                        "message": "Python脚本执行成功",
                        "data": {
                            "stdout": stdout_text,
                            "stderr": stderr_text,
                            "returnCode": process.returncode
                        },
                        "error": None
                    }
                else:
                    return {
                        "success": False,
                        "message": "Python脚本执行失败",
                        "data": {
                            "stdout": stdout_text,
                            "stderr": stderr_text,
                            "returnCode": process.returncode
                        },
                        "error": f"Script failed with return code {process.returncode}\n{stderr_text}"
                    }

            finally:
                # 清理临时文件
                try:
                    os.unlink(script_path)
                except:
                    pass

        except Exception as e:
            logger.error(f"Python执行失败: {e}")
            return {
                "success": False,
                "message": "执行失败",
                "data": None,
                "error": str(e)
            }

    async def validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证配置"""
        errors = []

        if not config.get('script'):
            errors.append("缺少script脚本内容")

        work_dir = config.get('workDir')
        if work_dir and not os.path.exists(work_dir):
            errors.append(f"工作目录不存在: {work_dir}")

        python_version = config.get('pythonVersion', 'python3')
        # 检查Python版本是否可用
        try:
            process = await asyncio.create_subprocess_exec(
                python_version, '--version',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await process.communicate()
            if process.returncode != 0:
                errors.append(f"Python版本不可用: {python_version}")
        except:
            errors.append(f"找不到Python: {python_version}")

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }

    async def _setup_virtual_env(
        self,
        work_dir: str,
        requirements: list,
        timeout: int
    ) -> Dict[str, Any]:
        """
        设置虚拟环境并安装依赖
        注意：这是简化版本，生产环境建议预先创建虚拟环境
        """
        try:
            venv_path = os.path.join(work_dir, '.venv')

            # 检查虚拟环境是否已存在
            if not os.path.exists(venv_path):
                logger.info(f"创建虚拟环境: {venv_path}")

                # 创建虚拟环境
                process = await asyncio.create_subprocess_exec(
                    'python3', '-m', 'venv', venv_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                await asyncio.wait_for(
                    process.communicate(),
                    timeout=60
                )

                if process.returncode != 0:
                    return {
                        "success": False,
                        "error": "创建虚拟环境失败"
                    }

            # 虚拟环境中的Python路径
            python_path = os.path.join(venv_path, 'bin', 'python')
            pip_path = os.path.join(venv_path, 'bin', 'pip')

            # 安装依赖包
            if requirements:
                logger.info(f"安装依赖包: {requirements}")

                for package in requirements:
                    process = await asyncio.create_subprocess_exec(
                        pip_path, 'install', package,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    await asyncio.wait_for(
                        process.communicate(),
                        timeout=timeout
                    )

            return {
                "success": True,
                "python_path": python_path
            }

        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": "虚拟环境设置超时"
            }
        except Exception as e:
            logger.error(f"设置虚拟环境失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }