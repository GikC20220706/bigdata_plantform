# DataX集成服务
import json
import subprocess
import asyncio
import os
from typing import Dict, Any, Optional
from pathlib import Path
import tempfile
from datetime import datetime


class DataXIntegrationService:
    """DataX集成服务"""

    def __init__(self, datax_home: str = "/opt/datax"):
        self.datax_home = Path(datax_home)
        self.python_path = self.datax_home / "bin" / "datax.py"

    async def create_sync_task(self, sync_config: Dict[str, Any]) -> Dict[str, Any]:
        """创建数据同步任务"""
        try:
            # 生成DataX配置文件
            job_config = self._generate_datax_config(sync_config)

            # 创建临时配置文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(job_config, f, indent=2, ensure_ascii=False)
                config_file = f.name

            # 执行DataX任务
            result = await self._execute_datax_job(config_file, sync_config.get('task_id'))

            # 清理临时文件
            os.unlink(config_file)

            return result

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "task_id": sync_config.get('task_id')
            }

    def _generate_datax_config(self, sync_config: Dict[str, Any]) -> Dict[str, Any]:


        """生成DataX配置"""
        source = sync_config['source']
        target = sync_config['target']
        if target.get('type', '').lower() == 'hive':
            # 从环境配置或target配置中获取namenode信息
            target['namenode_host'] = target.get('namenode_host', '192.142.76.242')  # 从你的.env看到的
            target['namenode_port'] = target.get('namenode_port', '8020')

            # 动态生成HDFS路径
            database = target.get('database', 'default')
            table_name = target['table']
            base_path = target.get('base_path', '/user/hive/warehouse')

            # 生成完整的HDFS路径
            target['hdfs_path'] = f"{base_path}/{database}.db/{table_name}"

            # 设置默认文件名
            if 'file_name' not in target:
                target['file_name'] = f"{table_name}_data"

        # 根据数据源类型生成reader配置
        reader_config = self._get_reader_config(source)
        # 根据目标类型生成writer配置
        writer_config = self._get_writer_config(target)

        datax_config = {
            "job": {
                "setting": {
                    "speed": {
                        "channel": sync_config.get('parallel_jobs', 4)
                    },
                    "errorLimit": {
                        "record": sync_config.get('error_limit', 0),
                        "percentage": 0.02
                    }
                },
                "content": [{
                    "reader": reader_config,
                    "writer": writer_config
                }]
            }
        }

        return datax_config

    def _get_reader_config(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """根据数据源类型生成reader配置"""
        db_type = source['type'].lower()

        if db_type == 'mysql':
            return {
                "name": "mysqlreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:mysql://{source['host']}:{source['port']}/{source['database']}"],
                        "querySql": [source.get('query', f"SELECT * FROM {source['table']}")]
                    }]
                }
            }

        elif db_type == 'oracle':
            return {
                "name": "oraclereader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:oracle:thin:@{source['host']}:{source['port']}:{source['database']}"],
                        "querySql": [source.get('query', f"SELECT * FROM {source['table']}")]
                    }]
                }
            }

        elif db_type == 'kingbase':
            return {
                "name": "kingbaseesreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:kingbase8://{source['host']}:{source['port']}/{source['database']}"],
                        "querySql": [source.get('query', f"SELECT * FROM {source['table']}")]
                    }]
                }
            }

        else:
            raise ValueError(f"不支持的数据源类型: {db_type}")

    def _get_writer_config(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """根据目标类型生成writer配置"""
        db_type = target['type'].lower()

        if db_type == 'mysql':
            return {
                "name": "mysqlwriter",
                "parameter": {
                    "writeMode": target.get('write_mode', 'insert'),
                    "username": target['username'],
                    "password": target['password'],
                    "column": target.get('columns', ["*"]),  # 新增：字段列表
                    "connection": [{
                        "jdbcUrl": f"jdbc:mysql://{target['host']}:{target['port']}/{target['database']}?useUnicode=true&characterEncoding=utf8",
                        # 修复：完整URL格式
                        "table": [target['table']]  # 保持数组格式
                    }],
                    # 可选配置
                    "preSql": target.get('pre_sql', []),  # 新增：前置SQL
                    "postSql": target.get('post_sql', []),  # 新增：后置SQL
                    "session": target.get('session', [])  # 新增：会话设置
                }
            }

        elif db_type == 'hive':
            return {
                "name": "hdfswriter",
                "parameter": {
                    "defaultFS": f"hdfs://{target['namenode_host']}:{target['namenode_port']}",
                    "fileType": "text",
                    "path": target['hdfs_path'],
                    "fileName": target.get('file_name', 'data'),
                    "column": target.get('columns', [{"name": "*", "type": "string"}]),
                    "fieldDelimiter": "\t",
                    "writeMode": "append"
                }
            }

        elif db_type == 'kingbase':
            return {
                "name": "kingbasewriter",
                "parameter": {
                    "username": target['username'],
                    "password": target['password'],
                    "column": target.get('columns', ["*"]),
                    "connection": [{
                        "jdbcUrl": f"jdbc:postgresql://{target['host']}:{target['port']}/{target['database']}",
                        "table": [target['table']]
                    }],
                    "writeMode": target.get('write_mode', 'insert'),
                    "preSql": target.get('pre_sql', []),
                    "postSql": target.get('post_sql', [])
                }
            }

        else:
            raise ValueError(f"不支持的目标类型: {db_type}")

    async def _execute_datax_job(self, config_file: str, task_id: Optional[str] = None) -> Dict[str, Any]:
        """执行DataX任务"""
        try:
            # 构建DataX执行命令
            cmd = [
                "python3",
                str(self.python_path),
                config_file
            ]

            # 异步执行命令
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            # 解析执行结果
            if process.returncode == 0:
                # 解析DataX输出统计信息
                output = stdout.decode('utf-8')
                stats = self._parse_datax_output(output)

                return {
                    "success": True,
                    "task_id": task_id,
                    "statistics": stats,
                    "output": output
                }
            else:
                return {
                    "success": False,
                    "task_id": task_id,
                    "error": stderr.decode('utf-8'),
                    "exit_code": process.returncode
                }

        except Exception as e:
            return {
                "success": False,
                "task_id": task_id,
                "error": str(e)
            }

    def _parse_datax_output(self, output: str) -> Dict[str, Any]:
        """解析DataX输出统计信息"""
        stats = {
            "total_records": 0,
            "total_bytes": 0,
            "speed_records": 0,
            "speed_bytes": 0,
            "duration": 0
        }

        try:
            # 解析DataX的统计输出
            lines = output.split('\n')
            for line in lines:
                if "总计耗时" in line:
                    # 解析执行时间
                    pass
                elif "同步速度" in line:
                    # 解析同步速度
                    pass
                elif "读取记录数" in line:
                    # 解析记录数
                    pass

        except Exception:
            pass

        return stats


# 扩展现有的sync API
class EnhancedSyncService:
    """增强的数据同步服务"""

    def __init__(self):
        self.datax_service = DataXIntegrationService()

    async def execute_sync_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """执行数据同步任务"""
        try:
            """验证同步配置"""
            required_fields = ['source', 'target', 'id']
            for field in required_fields:
                if field not in task_config:
                    raise ValueError(f"缺少必要字段: {field}")

            # 验证数据源配置
            source = task_config['source']
            if 'type' not in source:
                raise ValueError("数据源配置不完整：缺少type字段")

            # 验证目标配置
            target = task_config['target']
            if 'type' not in target:
                raise ValueError("目标配置不完整：缺少type字段")

            # 🆕 新增：Hive目标的特殊验证
            if target.get('type', '').lower() == 'hive':
                if 'table' not in target:
                    raise ValueError("Hive目标配置缺少table字段")
            # 验证配置
            self._validate_sync_config(task_config)

            # 根据同步类型选择执行方式
            sync_type = task_config.get('sync_type', 'full')

            if sync_type == 'incremental':
                # 增量同步逻辑
                return await self._execute_incremental_sync(task_config)
            else:
                # 全量同步
                return await self._execute_full_sync(task_config)

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "task_id": task_config.get('id')
            }

    async def _execute_full_sync(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """执行全量数据同步"""
        return await self.datax_service.create_sync_task(task_config)

    async def _execute_incremental_sync(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """执行增量数据同步"""
        # 获取上次同步的水位线
        watermark = await self._get_sync_watermark(task_config['id'])

        # 修改查询条件以支持增量
        if watermark:
            incremental_column = task_config.get('incremental_column', 'updated_at')
            original_query = task_config['source'].get('query', f"SELECT * FROM {task_config['source']['table']}")

            # 添加增量条件
            if 'WHERE' in original_query.upper():
                incremental_query = f"{original_query} AND {incremental_column} > '{watermark}'"
            else:
                incremental_query = f"{original_query} WHERE {incremental_column} > '{watermark}'"

            task_config['source']['query'] = incremental_query

        result = await self.datax_service.create_sync_task(task_config)

        # 更新水位线
        if result.get('success'):
            await self._update_sync_watermark(task_config['id'], datetime.now())

        return result

    async def _get_sync_watermark(self, task_id: str) -> Optional[str]:
        """获取同步水位线"""
        # 从Redis或数据库获取上次同步的时间戳
        # 这里需要根据你的存储方式实现
        pass

    async def _update_sync_watermark(self, task_id: str, watermark: datetime):
        """更新同步水位线"""
        # 保存新的水位线到Redis或数据库
        # 这里需要根据你的存储方式实现
        pass

    def _validate_sync_config(self, config: Dict[str, Any]):
        """验证同步配置"""
        required_fields = ['source', 'target', 'id']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"缺少必要字段: {field}")

        # 验证数据源配置
        source = config['source']
        if 'type' not in source or 'host' not in source:
            raise ValueError("数据源配置不完整")

        # 验证目标配置
        target = config['target']
        if 'type' not in target:
            raise ValueError("目标配置不完整")