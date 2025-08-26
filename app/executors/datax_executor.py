"""
DataX任务执行器
支持DataX数据同步任务的执行
"""

import os
import json
import asyncio
import subprocess
import tempfile
import re
from datetime import datetime
from typing import Dict, List, Optional, Any
from loguru import logger
from pathlib import Path

from .base_executor import BaseExecutor, ExecutionContext, ExecutionResult, ExecutionStatus
from config.airflow_config import airflow_config


class DataXExecutor(BaseExecutor):
    """DataX任务执行器"""

    def __init__(self):
        super().__init__("DataXExecutor")

        # DataX配置
        self.datax_home = os.environ.get("DATAX_HOME", "/opt/datax")
        self.datax_python = os.path.join(self.datax_home, "bin", "datax.py")
        self.job_config_dir = airflow_config.DATAX_JOB_PATH
        self.log_dir = airflow_config.DATAX_LOG_PATH

        # 创建必要目录
        Path(self.job_config_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)

        # DataX参数
        self.default_jvm_params = [
            "-Xms1g", "-Xmx1g", "-XX:+HeapDumpOnOutOfMemoryError",
            "-XX:HeapDumpPath=" + self.log_dir,
            "-Dfastjson.parser.safeMode=true",
            "-Dfastjson2.parser.safeMode=true"
        ]

        logger.info(f"DataX执行器初始化完成，DataX路径: {self.datax_home}")

    def validate_config(self, task_config: Dict[str, Any]) -> bool:
        """
        验证DataX任务配置

        Args:
            task_config: 任务配置

        Returns:
            bool: 配置是否有效
        """
        required_fields = ["task_id"]

        # 检查必要字段
        for field in required_fields:
            if field not in task_config:
                logger.error(f"DataX任务配置缺少必要字段: {field}")
                return False

        # 检查配置文件或配置内容
        if not task_config.get("config") and not task_config.get("config_file"):
            logger.error("DataX任务必须指定config配置内容或config_file配置文件路径")
            return False

        # 检查DataX安装
        if not os.path.exists(self.datax_python):
            logger.error(f"DataX Python脚本不存在: {self.datax_python}")
            return False

        return True

    async def execute_task(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> ExecutionResult:
        """
        执行DataX任务

        Args:
            task_config: DataX任务配置
            context: 执行上下文

        Returns:
            ExecutionResult: 执行结果
        """
        start_time = datetime.now()
        task_id = task_config["task_id"]

        logger.info(f"开始执行DataX任务: {task_id}")

        try:
            # 1. 准备DataX配置文件
            config_file = await self._prepare_datax_config(task_config, context)

            # 2. 设置执行环境
            exec_env = self._prepare_datax_environment(task_config, context)

            # 3. 构建DataX执行命令
            datax_cmd = self._build_datax_command(config_file, task_config, context)

            # 4. 执行DataX任务
            process_result = await self._execute_datax_command(
                datax_cmd, exec_env, task_config, context
            )

            # 5. 解析DataX执行结果
            sync_stats = self._parse_datax_output(
                process_result.get("stdout", ""),
                process_result.get("stderr", "")
            )

            # 6. 清理临时文件
            if config_file and task_config.get("cleanup_temp_files", True):
                self.cleanup_temp_file(config_file)

            # 7. 构建执行结果
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
                task_result={
                    "config_file": config_file,
                    "sync_statistics": sync_stats,
                    "datax_command": " ".join(datax_cmd),
                    "total_records": sync_stats.get("total_records", 0),
                    "total_bytes": sync_stats.get("total_bytes", 0),
                    "avg_speed_records": sync_stats.get("avg_speed_records", 0),
                    "avg_speed_bytes": sync_stats.get("avg_speed_bytes", 0)
                }
            )

            if status == ExecutionStatus.SUCCESS:
                logger.info(f"DataX任务 {task_id} 执行成功，同步 {sync_stats.get('total_records', 0)} 条记录")
            else:
                logger.error(f"DataX任务 {task_id} 执行失败，退出码: {process_result['returncode']}")

            return result

        except Exception as e:
            # 构建失败结果
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            error_msg = str(e)
            logger.error(f"DataX任务 {task_id} 执行异常: {error_msg}")

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

    async def _prepare_datax_config(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> str:
        """
        准备DataX配置文件

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            str: 配置文件路径
        """
        if task_config.get("config_file"):
            # 使用现有配置文件
            config_file = task_config["config_file"]

            # 支持相对路径
            if not os.path.isabs(config_file):
                config_file = os.path.join(self.job_config_dir, config_file)

            if not os.path.exists(config_file):
                raise FileNotFoundError(f"DataX配置文件不存在: {config_file}")

            return config_file

        elif task_config.get("config"):
            # 从配置内容创建临时文件
            config_content = task_config["config"]

            # 处理配置模板变量
            config_content = self._process_config_template(config_content, context)

            # 验证配置格式
            self._validate_datax_config(config_content)

            # 创建临时配置文件
            config_json = json.dumps(config_content, indent=2, ensure_ascii=False)
            config_file = self.create_temp_file(config_json, suffix=".json")

            logger.debug(f"创建临时DataX配置文件: {config_file}")
            return config_file

        else:
            raise ValueError("未指定DataX配置内容或文件")

    def _process_config_template(
            self,
            config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        处理配置模板变量

        Args:
            config: DataX配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 处理后的配置
        """
        # 转换为JSON字符串进行模板替换
        config_json = json.dumps(config, ensure_ascii=False)

        # 支持常见的模板变量
        template_vars = {
            "ds": context.execution_date,
            "ds_nodash": context.execution_date.replace("-", ""),
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
            config_json = config_json.replace(f"{{{{ {key} }}}}", str(value))
            config_json = config_json.replace(f"${{{key}}}", str(value))

        # 转换回字典
        return json.loads(config_json)

    def _validate_datax_config(self, config: Dict[str, Any]):
        """
        验证DataX配置格式

        Args:
            config: DataX配置
        """
        # 检查基本结构
        if "job" not in config:
            raise ValueError("DataX配置缺少'job'节点")

        job = config["job"]

        if "content" not in job:
            raise ValueError("DataX配置缺少'job.content'节点")

        content = job["content"]
        if not isinstance(content, list) or len(content) == 0:
            raise ValueError("DataX配置'job.content'必须是非空数组")

        # 检查reader和writer
        for item in content:
            if "reader" not in item:
                raise ValueError("DataX配置缺少'reader'节点")
            if "writer" not in item:
                raise ValueError("DataX配置缺少'writer'节点")

    def _prepare_datax_environment(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, str]:
        """
        准备DataX执行环境变量

        Args:
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, str]: 环境变量
        """
        env = os.environ.copy()

        # DataX相关环境变量
        env.update({
            "DATAX_HOME": self.datax_home,
            "PYTHONIOENCODING": "UTF-8",
            "LANG": "en_US.UTF-8",
            "LC_ALL": "en_US.UTF-8"
        })

        # 添加任务相关环境变量
        env.update({
            "AIRFLOW_CTX_DAG_ID": context.dag_id,
            "AIRFLOW_CTX_TASK_ID": context.task_id,
            "AIRFLOW_CTX_EXECUTION_DATE": context.execution_date,
        })

        # 添加自定义环境变量
        if context.environment_variables:
            env.update(context.environment_variables)

        # 添加任务配置中的环境变量
        task_env = task_config.get("environment", {})
        if task_env:
            env.update(task_env)

        return env

    def _build_datax_command(
            self,
            config_file: str,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> List[str]:
        """
        构建DataX执行命令

        Args:
            config_file: 配置文件路径
            task_config: 任务配置
            context: 执行上下文

        Returns:
            List[str]: DataX命令
        """
        cmd = ["python", self.datax_python]

        # 添加JVM参数
        jvm_params = task_config.get("jvm_params", self.default_jvm_params)
        if jvm_params:
            for param in jvm_params:
                cmd.extend(["-j", param])

        # 添加DataX参数
        datax_params = task_config.get("datax_params", {})

        # 设置并发数
        if "channel" in datax_params:
            cmd.extend(["-p", str(datax_params["channel"])])

        # 设置日志级别
        log_level = datax_params.get("log_level", "info")
        cmd.extend(["--loglevel", log_level])

        # 添加配置文件
        cmd.append(config_file)

        return cmd

    async def _execute_datax_command(
            self,
            cmd: List[str],
            env: Dict[str, str],
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        执行DataX命令

        Args:
            cmd: DataX命令
            env: 环境变量
            task_config: 任务配置
            context: 执行上下文

        Returns:
            Dict[str, Any]: 执行结果
        """
        # 设置工作目录
        cwd = task_config.get("cwd") or self.datax_home

        logger.info(f"执行DataX命令: {' '.join(cmd)}")
        logger.debug(f"工作目录: {cwd}")

        # 创建日志文件
        log_file = os.path.join(
            self.log_dir,
            f"datax_{context.dag_id}_{context.task_id}_{context.execution_date.replace('-', '')}.log"
        )

        try:
            # 启动进程
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env
            )

            # 等待进程完成
            start_time = datetime.now()
            stdout_data, stderr_data = await process.communicate()
            end_time = datetime.now()

            # 处理输出
            stdout = stdout_data.decode('utf-8', errors='replace') if stdout_data else ""
            stderr = stderr_data.decode('utf-8', errors='replace') if stderr_data else ""

            # 保存日志到文件
            try:
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write("=== DataX执行日志 ===\n")
                    f.write(f"命令: {' '.join(cmd)}\n")
                    f.write(f"开始时间: {start_time}\n")
                    f.write(f"结束时间: {end_time}\n")
                    f.write(f"退出码: {process.returncode}\n\n")
                    f.write("=== 标准输出 ===\n")
                    f.write(stdout)
                    if stderr:
                        f.write("\n=== 错误输出 ===\n")
                        f.write(stderr)
            except Exception as e:
                logger.warning(f"保存DataX日志文件失败: {e}")

            result = {
                "returncode": process.returncode,
                "stdout": stdout,
                "stderr": stderr,
                "command": " ".join(cmd),
                "cwd": cwd,
                "duration": (end_time - start_time).total_seconds(),
                "log_file": log_file
            }

            logger.debug(f"DataX执行完成，退出码: {process.returncode}")
            return result

        except Exception as e:
            logger.error(f"DataX执行异常: {e}")
            raise

    def _parse_datax_output(self, stdout: str, stderr: str) -> Dict[str, Any]:
        """
        解析DataX输出，提取同步统计信息

        Args:
            stdout: 标准输出
            stderr: 错误输出

        Returns:
            Dict[str, Any]: 统计信息
        """
        stats = {
            "total_records": 0,
            "total_bytes": 0,
            "avg_speed_records": 0,
            "avg_speed_bytes": 0,
            "error_records": 0,
            "task_duration": 0
        }

        try:
            # 解析记录数
            records_pattern = r"共传输了(\d+)条记录"
            records_match = re.search(records_pattern, stdout)
            if records_match:
                stats["total_records"] = int(records_match.group(1))

            # 解析字节数
            bytes_pattern = r"共传输了.*?，(\d+(?:\.\d+)?)字节"
            bytes_match = re.search(bytes_pattern, stdout)
            if bytes_match:
                stats["total_bytes"] = float(bytes_match.group(1))

            # 解析平均速度（记录/秒）
            speed_records_pattern = r"(\d+(?:\.\d+)?)rec/s"
            speed_records_match = re.search(speed_records_pattern, stdout)
            if speed_records_match:
                stats["avg_speed_records"] = float(speed_records_match.group(1))

            # 解析平均速度（字节/秒）
            speed_bytes_pattern = r"(\d+(?:\.\d+)?)MB/s"
            speed_bytes_match = re.search(speed_bytes_pattern, stdout)
            if speed_bytes_match:
                stats["avg_speed_bytes"] = float(speed_bytes_match.group(1)) * 1024 * 1024  # 转换为字节

            # 解析错误记录数
            error_pattern = r"错误记录\s*(\d+)"
            error_match = re.search(error_pattern, stdout)
            if error_match:
                stats["error_records"] = int(error_match.group(1))

            # 解析任务耗时
            duration_pattern = r"任务总计耗时\s*:\s*(\d+)s"
            duration_match = re.search(duration_pattern, stdout)
            if duration_match:
                stats["task_duration"] = int(duration_match.group(1))

        except Exception as e:
            logger.warning(f"解析DataX输出失败: {e}")

        return stats

    def get_supported_task_types(self) -> List[str]:
        """
        获取支持的任务类型

        Returns:
            List[str]: 支持的任务类型
        """
        return ["datax", "sync", "data_sync", "etl"]

    async def validate_datax_config_file(self, config_file: str) -> Dict[str, Any]:
        """
        验证DataX配置文件

        Args:
            config_file: 配置文件路径

        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            if not os.path.exists(config_file):
                return {"valid": False, "error": "配置文件不存在"}

            with open(config_file, 'r', encoding='utf-8') as f:
                config_content = json.load(f)

            # 验证配置格式
            self._validate_datax_config(config_content)

            # 提取基本信息
            job_content = config_content.get("job", {}).get("content", [])
            readers = [item.get("reader", {}).get("name", "unknown") for item in job_content]
            writers = [item.get("writer", {}).get("name", "unknown") for item in job_content]

            return {
                "valid": True,
                "config_file": config_file,
                "readers": readers,
                "writers": writers,
                "job_count": len(job_content)
            }

        except json.JSONDecodeError as e:
            return {
                "valid": False,
                "error": f"JSON格式错误: {e}",
                "config_file": config_file
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e),
                "config_file": config_file
            }

    async def dry_run(
            self,
            task_config: Dict[str, Any],
            context: ExecutionContext
    ) -> Dict[str, Any]:
        """
        DataX任务的试运行（验证模式）

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

            # 2. 准备配置文件（不执行）
            config_file = await self._prepare_datax_config(task_config, context)

            # 3. 验证配置文件
            config_validation = await self.validate_datax_config_file(config_file)

            # 4. 构建命令（用于展示）
            datax_cmd = self._build_datax_command(config_file, task_config, context)

            # 5. 检查DataX环境
            datax_env_check = {
                "datax_home_exists": os.path.exists(self.datax_home),
                "datax_python_exists": os.path.exists(self.datax_python),
                "job_dir_writable": os.access(self.job_config_dir, os.W_OK),
                "log_dir_writable": os.access(self.log_dir, os.W_OK)
            }

            # 清理临时文件
            if config_file and task_config.get("cleanup_temp_files", True):
                self.cleanup_temp_file(config_file)

            return {
                "success": True,
                "config_validation": config_validation,
                "datax_command": " ".join(datax_cmd),
                "environment_check": datax_env_check,
                "datax_home": self.datax_home,
                "job_config_dir": self.job_config_dir,
                "log_dir": self.log_dir
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }

    def get_datax_info(self) -> Dict[str, Any]:
        """
        获取DataX环境信息

        Returns:
            Dict[str, Any]: DataX环境信息
        """
        return {
            "datax_home": self.datax_home,
            "datax_python": self.datax_python,
            "datax_python_exists": os.path.exists(self.datax_python),
            "job_config_dir": self.job_config_dir,
            "log_dir": self.log_dir,
            "default_jvm_params": self.default_jvm_params
        }


# 创建DataX执行器实例并注册
datax_executor = DataXExecutor()

# 注册到执行器注册表
from .base_executor import executor_registry

executor_registry.register("datax", datax_executor)
executor_registry.register("sync", datax_executor)
executor_registry.register("data_sync", datax_executor)
executor_registry.register("etl", datax_executor)