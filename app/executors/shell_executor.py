"""
Shell任务执行器
支持Shell脚本和命令的执行
"""

import os
import asyncio
import subprocess
import tempfile
import signal
import psutil
from datetime import datetime
from typing import Dict, List, Optional, Any
from loguru import logger
from pathlib import Path

from .base_executor import BaseExecutor, ExecutionContext, ExecutionResult, ExecutionStatus


class ShellExecutor(BaseExecutor):
    """Shell任务执行器"""

    def __init__(self):
        super().__init__("ShellExecutor")

        # Shell执行配置
        self.default_shell = "/bin/bash"
        self.allow_dangerous_commands = False
        self.max_output_size = 10 * 1024 * 1024  # 10MB输出限制
        self.working_dir = "/tmp"

        # 危险命令列表
        self.dangerous_commands = [
            "rm -rf /", "format", "fdisk", "mkfs",
            "dd if=/dev/zero", ":(){ :|:& };:", "chmod -R 777 /",
            "chown -R", "passwd", "userdel", "groupdel"
        ]

        logger.info("Shell执行器初始化完成")

    def validate_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证Shell任务配置

        Args:
            task_config: 任务配置

        Returns:
            bool: 配置是否有效
        """
        required_fields = ["task_id"]

        # 检查必要字段
        for field in required_fields:
            if field not in task_config:
                logger.error(f"Shell任务配置缺少必要字段: {field}")
                return False

        # 检查命令或脚本文件
        if not task_config.get("command") and not task_config.get("script_file"):
            logger.error("Shell任务必须指定command命令或script_file脚本文件")
            return False

        # 检查危险命令
        if not self.allow_dangerous_commands:
            command = task_config.get("command", "")
            if self._contains_dangerous_command(command):
                logger.error(f"检测到危险命令，执行被阻止: {command}")
                return False

        return True

    async def execute_task(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        执行Shell任务

        Args:
            task_config: Shell任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        start_time = datetime.now()
        task_id = task_config["task_id"]

        logger.info(f"开始执行Shell任务: {task_id}")

        try:
            # 1. 准备执行脚本
            script_path = await self._prepare_script(task_config, context)

            # 2. 设置执行环境
            exec_env = self._prepare_environment(task_config, context)

            # 3. 执行脚本
            process_result = await self._execute_script(
                script_path, exec_env, task_config, context
            )

            # 4. 清理临时文件
            if script_path and task_config.get("cleanup_temp_files", True):
                self.cleanup_temp_file(script_path)

            # 5. 构建执行结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            status = ExecutionStatus.SUCCESS if process_result["returncode"] == 0 else ExecutionStatus.FAILED

            result = ExecutionResult(
                status=status,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                exit_code=process_result["returncode"],
                stdout=process_result.get("stdout", ""),
                stderr=process_result.get("stderr", ""),
                cpu_usage_percent=process_result.get("cpu_usage", 0),
                memory_usage_mb=process_result.get("memory_usage", 0),
                task_result={
                    "script_path": script_path,
                    "command_executed": process_result.get("command", ""),
                    "working_directory": process_result.get("cwd", ""),
                    "environment_vars": len(exec_env),
                    "files_created": process_result.get("files_created", [])
                }
            )

            if status == ExecutionStatus.SUCCESS:
                logger.info(f"Shell任务 {task_id} 执行成功，耗时 {duration:.2f}秒")
            else:
                logger.error(f"Shell任务 {task_id} 执行失败，退出码: {process_result['returncode']}")

            return result

        except Exception as e:
            # 构建失败结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_msg = str(e)
            logger.error(f"Shell任务 {task_id} 执行异常: {error_msg}")

            result = ExecutionResult(
                status=ExecutionStatus.FAILED,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                exit_code=-1,
                error_message=error_msg,
                exception_details={
                    "type": type(e).__name__,
                    "message": error_msg
                }
            )

            return result

    async def _prepare_script(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> str:
        """
        准备要执行的脚本

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            str: 脚本文件路径
        """
        if task_config.get("script_file"):
            # 使用现有脚本文件
            script_file = task_config["script_file"]

            # 支持相对路径
            if not os.path.isabs(script_file):
                script_file = os.path.join(context.working_directory or ".", script_file)

            if not os.path.exists(script_file):
                raise FileNotFoundError(f"脚本文件不存在: {script_file}")

            # 确保脚本可执行
            os.chmod(script_file, 0o755)
            return script_file

        elif task_config.get("command"):
            # 创建临时脚本文件
            command = task_config["command"]

            # 处理命令模板变量
            command = self._process_command_template(command, context)

            # 创建脚本内容
            script_content = self._generate_script_content(command, task_config)

            # 写入临时文件
            script_path = self.create_temp_script(script_content, executable=True)

            logger.debug(f"创建临时脚本: {script_path}")
            return script_path

        else:
            raise ValueError("未指定命令或脚本文件")

    def _process_command_template(self, command: str, context: ExecutionContext) -> str:
        """
        处理命令模板变量

        Args:
            command: 原始命令
            context: 执行上下文

        Returns:
            str: 处理后的命令
        """
        # 支持常见的模板变量
        template_vars = {
            "ds": context.execution_date,
            "dag_id": context.dag_id,
            "task_id": context.task_id,
            "execution_date": context.execution_date,
            "retry_number": str(context.retry_number),
            "run_id": f"{context.dag_id}__{context.execution_date}"
        }

        # 添加环境变量
        template_vars.update(context.environment_variables)

        # 模板替换
        for key, value in template_vars.items():
            command = command.replace(f"{{{{ {key} }}}}", str(value))
            command = command.replace(f"${{{key}}}", str(value))

        return command

    def _generate_script_content(self, command: str, task_config: Dict[str, Any]) -> str:
        """
        生成脚本内容

        Args:
            command: 要执行的命令
            task_config: 任务配置

        Returns:
            str: 脚本内容
        """
        shell = task_config.get("shell", self.default_shell)

        script_lines = [
            f"#!{shell}",
            "",
            "# 自动生成的Shell脚本",
            f"# 任务ID: {task_config['task_id']}",
            f"# 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "set -e  # 遇到错误立即退出",
            "",
            "# 记录开始时间",
            "echo \"任务开始执行: $(date)\"",
            "",
            "# 执行主要命令",
            command,
            "",
            "# 记录结束时间",
            "echo \"任务执行完成: $(date)\"",
        ]

        # 添加清理逻辑（如果需要）
        if task_config.get("cleanup_on_exit", False):
            script_lines.extend([
                "",
                "# 清理临时文件",
                "cleanup() {",
                "    echo \"清理临时文件...\"",
                "    # 在这里添加清理逻辑",
                "}",
                "trap cleanup EXIT"
            ])

        return "\n".join(script_lines)

    def _prepare_environment(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, str]:
        """
        准备执行环境变量

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, str]: 环境变量
        """
        # 基础环境变量
        env = os.environ.copy()

        # 添加任务相关环境变量
        env.update({
            "AIRFLOW_CTX_DAG_ID": context.dag_id,
            "AIRFLOW_CTX_TASK_ID": context.task_id,
            "AIRFLOW_CTX_EXECUTION_DATE": context.execution_date,
            "AIRFLOW_CTX_TRY_NUMBER": str(context.retry_number + 1),
            "TASK_INSTANCE_KEY_STR": f"{context.dag_id}__{context.task_id}__{context.execution_date}",
        })

        # 添加自定义环境变量
        if context.environment_variables:
            env.update(context.environment_variables)

        # 添加任务配置中的环境变量
        task_env = task_config.get("environment", {})
        if task_env:
            env.update(task_env)

        return env

    async def _execute_script(
            self,
            script_path: str,
            env: Dict[str, str],
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        执行脚本

        Args:
            script_path: 脚本路径
            env: 环境变量
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 执行结果
        """
        # 设置工作目录
        cwd = task_config.get("cwd") or context.working_directory or self.working_dir

        # 设置shell
        shell = task_config.get("shell", self.default_shell)

        # 构建命令
        cmd = [shell, script_path]

        logger.debug(f"执行命令: {' '.join(cmd)}")
        logger.debug(f"工作目录: {cwd}")

        # 创建任务标识
        task_key = f"{context.dag_id}.{context.task_id}.{context.execution_date}"

        try:
            # 启动进程
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env,
                preexec_fn=os.setsid  # 创建新的进程组
            )

            # 注册进程用于监控和取消
            if process.pid:
                self.process_registry[task_key] = psutil.Process(process.pid)

            # 监控进程执行
            start_time = datetime.now()
            stdout_data, stderr_data = await process.communicate()
            end_time = datetime.now()

            # 获取资源使用情况
            resource_usage = {"cpu_usage": 0, "memory_usage": 0}
            if task_key in self.process_registry:
                try:
                    proc = self.process_registry[task_key]
                    if proc.is_running():
                        resource_usage = self._monitor_resource_usage(proc)
                except:
                    pass
                finally:
                    self.process_registry.pop(task_key, None)

            # 处理输出
            stdout = stdout_data.decode('utf-8', errors='replace') if stdout_data else ""
            stderr = stderr_data.decode('utf-8', errors='replace') if stderr_data else ""

            # 限制输出大小
            if len(stdout) > self.max_output_size:
                stdout = stdout[:self.max_output_size] + "\n... [输出被截断] ..."
            if len(stderr) > self.max_output_size:
                stderr = stderr[:self.max_output_size] + "\n... [错误输出被截断] ..."

            result = {
                "returncode": process.returncode,
                "stdout": stdout,
                "stderr": stderr,
                "command": " ".join(cmd),
                "cwd": cwd,
                "duration": (end_time - start_time).total_seconds(),
                "cpu_usage": resource_usage["cpu_usage"],
                "memory_usage": resource_usage["memory_usage"]
            }

            logger.debug(f"脚本执行完成，退出码: {process.returncode}")
            return result

        except asyncio.CancelledError:
            # 任务被取消
            logger.warning(f"Shell任务 {context.task_id} 被取消")
            if task_key in self.process_registry:
                try:
                    proc = self.process_registry[task_key]
                    proc.terminate()
                except:
                    pass
                self.process_registry.pop(task_key, None)
            raise

        except Exception as e:
            # 执行异常
            logger.error(f"脚本执行异常: {e}")
            if task_key in self.process_registry:
                self.process_registry.pop(task_key, None)
            raise

    def _contains_dangerous_command(self, command: str) -> bool:
        """
        检查是否包含危险命令

        Args:
            command: 要检查的命令

        Returns:
            bool: 是否包含危险命令
        """
        command_lower = command.lower()

        for dangerous in self.dangerous_commands:
            if dangerous.lower() in command_lower:
                return True

        return False

    def get_supported_task_types(self) -> List[str]:
        """
        获取支持的任务类型

        Returns:
            List[str]: 支持的任务类型
        """
        return ["shell", "bash", "script", "command"]

    async def validate_script_syntax(self, script_content: str, shell: str = "/bin/bash") -> Dict[str, Any]:
        """
        验证脚本语法

        Args:
            script_content: 脚本内容
            shell: Shell类型

        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            # 创建临时脚本文件
            script_path = self.create_temp_script(script_content)

            # 使用-n选项检查语法
            cmd = [shell, "-n", script_path]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            # 清理临时文件
            self.cleanup_temp_file(script_path)

            if process.returncode == 0:
                return {"valid": True, "shell": shell}
            else:
                return {
                    "valid": False,
                    "error": stderr.decode('utf-8', errors='replace'),
                    "shell": shell
                }

        except Exception as e:
            return {"valid": False, "error": str(e), "shell": shell}

    async def dry_run(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        Shell任务的试运行（验证模式）

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 试运行结果
        """
        try:
            # 1. 验证配置
            if not self.validate_config(task_config):
                return {"success": False, "error": "任务配置无效"}

            # 2. 准备脚本（不执行）
            script_path = await self._prepare_script(task_config, context)

            # 3. 读取脚本内容用于验证
            with open(script_path, 'r', encoding='utf-8') as f:
                script_content = f.read()

            # 4. 验证脚本语法
            shell = task_config.get("shell", self.default_shell)
            syntax_result = await self.validate_script_syntax(script_content, shell)

            # 5. 检查危险命令
            dangerous_check = {
                "has_dangerous_commands": self._contains_dangerous_command(script_content),
                "dangerous_patterns": []
            }

            for pattern in self.dangerous_commands:
                if pattern.lower() in script_content.lower():
                    dangerous_check["dangerous_patterns"].append(pattern)

            # 6. 准备环境信息
            env = self._prepare_environment(task_config, context)

            # 清理临时文件
            if script_path and task_config.get("cleanup_temp_files", True):
                self.cleanup_temp_file(script_path)

            return {
                "success": True,
                "script_preview": script_content[:500] + "..." if len(script_content) > 500 else script_content,
                "script_lines": len(script_content.split('\n')),
                "shell": shell,
                "syntax_validation": syntax_result,
                "security_check": dangerous_check,
                "environment_vars": len(env),
                "working_directory": task_config.get("cwd") or context.working_directory or self.working_dir
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    async def get_script_info(self, script_path: str) -> Dict[str, Any]:
        """
        获取脚本文件信息

        Args:
            script_path: 脚本文件路径

        Returns:
            Dict[str, Any]: 脚本信息
        """
        try:
            if not os.path.exists(script_path):
                return {"success": False, "error": "脚本文件不存在"}

            stat_info = os.stat(script_path)

            with open(script_path, 'r', encoding='utf-8') as f:
                content = f.read()

            return {
                "success": True,
                "file_path": script_path,
                "file_size": stat_info.st_size,
                "permissions": oct(stat_info.st_mode)[-3:],
                "is_executable": os.access(script_path, os.X_OK),
                "line_count": len(content.split('\n')),
                "content_preview": content[:200] + "..." if len(content) > 200 else content,
                "modified_time": datetime.fromtimestamp(stat_info.st_mtime).isoformat()
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "file_path": script_path
            }

    def set_security_policy(self, allow_dangerous: bool = False, custom_patterns: List[str] = None):
        """
        设置安全策略

        Args:
            allow_dangerous: 是否允许危险命令
            custom_patterns: 自定义危险模式列表
        """
        self.allow_dangerous_commands = allow_dangerous

        if custom_patterns:
            self.dangerous_commands.extend(custom_patterns)

        logger.info(f"安全策略已更新: allow_dangerous={allow_dangerous}")


# 创建Shell执行器实例并注册
shell_executor = ShellExecutor()

# 注册到执行器注册表
from .base_executor import executor_registry

executor_registry.register("shell", shell_executor)
executor_registry.register("bash", shell_executor)
executor_registry.register("script", shell_executor)
executor_registry.register("command", shell_executor)