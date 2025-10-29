"""
Bash执行器 - 执行Bash脚本
"""
import asyncio
import os
import tempfile
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from .base_executor import JobExecutor


class BashExecutor(JobExecutor):
    """Bash脚本执行器"""

    def __init__(self):
        super().__init__("bash_executor")

    async def execute(
            self,
            db: AsyncSession,
            work_config: Dict[str, Any],
            instance_id: str,
            context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行Bash脚本"""
        try:
            script = work_config.get('script')
            work_dir = work_config.get('workDir', '/tmp')
            env_vars = work_config.get('envVars', {})
            timeout = work_config.get('timeout', 300)

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
                    suffix='.sh',
                    delete=False,
                    dir=work_dir
            ) as f:
                f.write(script)
                script_path = f.name

            try:
                # 给脚本添加执行权限
                os.chmod(script_path, 0o755)

                # 准备环境变量
                env = os.environ.copy()
                env.update(env_vars)

                # 执行脚本
                process = await asyncio.create_subprocess_exec(
                    'bash', script_path,
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

                # 检查返回码
                if process.returncode == 0:
                    return {
                        "success": True,
                        "message": "脚本执行成功",
                        "data": {
                            "stdout": stdout.decode('utf-8'),
                            "stderr": stderr.decode('utf-8'),
                            "returnCode": process.returncode
                        },
                        "error": None
                    }
                else:
                    return {
                        "success": False,
                        "message": "脚本执行失败",
                        "data": {
                            "stdout": stdout.decode('utf-8'),
                            "stderr": stderr.decode('utf-8'),
                            "returnCode": process.returncode
                        },
                        "error": f"Script failed with return code {process.returncode}"
                    }

            finally:
                # 清理临时文件
                try:
                    os.unlink(script_path)
                except:
                    pass

        except Exception as e:
            logger.error(f"Bash执行失败: {e}")
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

        return {
            "valid": len(errors) == 0,
            "errors": errors
        }