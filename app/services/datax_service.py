# DataX集成服务
import json
import subprocess
import asyncio
import os
import traceback
from typing import Dict, Any, Optional, List
from pathlib import Path
import tempfile
from datetime import datetime

from loguru import logger


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

            # 🔧 强制JSON序列化检查
            try:
                test_json = json.dumps(job_config, ensure_ascii=False, default=str)
                logger.info("JSON序列化检查通过")
            except Exception as json_error:
                logger.error(f"JSON序列化失败: {json_error}")
                # 输出问题配置
                logger.error(
                    f"问题配置keys: {list(job_config.keys()) if isinstance(job_config, dict) else type(job_config)}")
                raise ValueError(f"配置序列化失败: {json_error}")

            # 创建临时配置文件
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
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
        import copy
        clean_config = self._clean_config_for_serialization(sync_config)
        source = copy.deepcopy(sync_config['source'])
        target = copy.deepcopy(sync_config['target'])

        keys_to_remove = ['_sa_instance_state', 'connection', 'client', 'metadata',
                          'schema_mapping', 'column_mappings']

        for key in keys_to_remove:
            source.pop(key, None)
            target.pop(key, None)

        # 如果source是hive类型，进一步清理
        if source.get('type', '').lower() == 'hive':
            # 确保columns是简单的字符串列表
            if 'columns' in source:
                source['columns'] = [str(col) for col in source['columns']]

        if target.get('type', '').lower() == 'hive':
            # 从环境配置或target配置中获取namenode信息
            target['namenode_host'] = target.get('namenode_host', '192.142.76.242')
            target['namenode_port'] = target.get('namenode_port', '8020')

            # 🔧 修复：动态生成正确的HDFS路径，包含数据库名和分区
            database = target.get('database', 'default')
            table_name = target['table']
            base_path = target.get('base_path', '/user/hive/warehouse')

            # 生成当前日期分区
            from datetime import datetime
            current_date = datetime.now().strftime('%Y-%m-%d')
            partition_value = target.get('partition_value', current_date)

            # 生成完整的HDFS路径，包含分区
            if database and database != 'default':
                target['hdfs_path'] = f"{base_path}/{database}.db/{table_name}/dt={partition_value}"
            else:
                target['hdfs_path'] = f"{base_path}/{table_name}/dt={partition_value}"

            # 设置默认文件名
            if 'file_name' not in target:
                target['file_name'] = f"{table_name}_data"

            # 🆕 新增：Hive相关配置
            target['partition_column'] = 'dt'
            target['partition_value'] = partition_value
            target['storage_format'] = 'ORC'
            target['compression'] = 'snappy'

            logger.info(f"生成Hive HDFS路径: {target['hdfs_path']}")
            logger.info(f"分区信息: dt={partition_value}")

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
            columns = source.get('columns', [])
            if not columns:
                raise ValueError("MySQL Reader缺少字段配置")

            # 为MySQL构建SELECT语句
            columns_str = ', '.join(columns)
            select_sql = f"SELECT {columns_str} FROM {source['table']}"

            return {
                "name": "mysqlreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:mysql://{source['host']}:{source['port']}/{source['database']}"],
                        "querySql": [select_sql]
                    }]
                }
            }

        elif db_type == 'doris':
            columns = source.get('columns', [])
            if not columns:
                raise ValueError("Doris Reader缺少字段配置")

            return {
                "name": "dorisreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "column": columns,
                    "splitPk": source.get('split_pk', ''),
                    "connection": [{
                        "table": [source['table']],
                        "jdbcUrl": [f"jdbc:mysql://{source['host']}:{source['port']}/{source['database']}"]
                    }]
                }
            }
        elif db_type == 'hive':
            columns = source.get('columns', [])
            schema_mapping = source.get('schema_mapping', {})
            column_info = schema_mapping.get('columns', [])

            if not columns:
                raise ValueError("Hive Reader缺少字段配置")

            # 清理source中可能的对象引用
            clean_source = {
                'database': source.get('database', 'default'),
                'table': source.get('table'),
                'namenode_host': source.get('namenode_host', '192.142.76.242'),
                'namenode_port': source.get('namenode_port', '8020'),
                'base_path': source.get('base_path', '/user/hive/warehouse'),
                'file_type': source.get('file_type', 'orc'),
                'field_delimiter': source.get('field_delimiter', '\t'),
                'partition_filter': source.get('partition_filter'),
                'add_ods_prefix': source.get('add_ods_prefix', False)
            }

            # 🔧 生成HDFS路径
            hdfs_path = self._generate_hive_read_path(source)

            # 🔧 生成字段配置
            #column_config = self._generate_hdfs_column_config(column_info, columns)
            column_config = []
            for i, col in enumerate(columns):
                if isinstance(col, str):
                    column_config.append({
                        "index": i,
                        "type": "string"  # 统一使用string类型
                    })
                else:
                    column_config.append({
                        "index": i,
                        "type": "string"
                    })

            return {
                "name": "hdfsreader",
                "parameter": {
                    "path": hdfs_path,
                    "defaultFS": f"hdfs://{source['namenode_host']}:{source['namenode_port']}",
                    "column": column_config,
                    "fileType": source.get('file_type', 'orc'),  # 支持orc, text等
                    "encoding": "UTF-8",
                    "fieldDelimiter": source.get('field_delimiter', '\t')  # 分隔符
                }
            }

        elif db_type == 'oracle':
            columns = source.get('columns', [])
            if not columns:
                raise ValueError("Oracle Reader缺少字段配置")

            columns_str = ', '.join(columns)
            select_sql = f"SELECT {columns_str} FROM {source['table']}"

            return {
                "name": "oraclereader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:oracle:thin:@{source['host']}:{source['port']}:{source['database']}"],
                        "querySql": [select_sql]
                    }]
                }
            }

        elif db_type == 'kingbase':
            columns = source.get('columns', [])
            if not columns:
                raise ValueError("KingBase Reader缺少字段配置")

            columns_str = ', '.join(columns)
            select_sql = f"SELECT {columns_str} FROM {source['table']}"

            return {
                "name": "kingbaseesreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:kingbase8://{source['host']}:{source['port']}/{source['database']}"],
                        "querySql": [select_sql]
                    }]
                }
            }

        elif db_type == 'postgresql':
            columns = source.get('columns', [])
            if not columns:
                raise ValueError("PostgreSQL Reader缺少字段配置")

            columns_str = ', '.join(columns)
            select_sql = f"SELECT {columns_str} FROM {source['table']}"

            return {
                "name": "postgresqlreader",
                "parameter": {
                    "username": source['username'],
                    "password": source['password'],
                    "connection": [{
                        "jdbcUrl": [f"jdbc:postgresql://{source['host']}:{source['port']}/{source['database']}"],
                        "querySql": [select_sql]
                    }]
                }
            }

        else:
            raise ValueError(f"不支持的数据源类型: {db_type}")

    def _generate_hive_read_path(self, source: Dict[str, Any]) -> str:
        """生成Hive读取路径"""
        database = source.get('database', 'default')
        table_name = source['table']
        base_path = source.get('base_path', '/user/hive/warehouse')

        table_name_upper = table_name.upper()

        if table_name_upper.startswith('ODS_'):
            # 已经有ODS前缀（不管大小写），直接使用，但统一转为大写
            final_table_name = table_name_upper
        elif table_name.lower().startswith('ods_'):
            # 有小写的ods前缀，转换为大写ODS_
            final_table_name = 'ODS_' + table_name[4:]  # 去掉小写ods_，加上大写ODS_
        elif source.get('add_ods_prefix', False):
            # 没有前缀且需要添加，添加ODS_前缀
            final_table_name = f"ODS_{table_name.upper()}"
        else:
            # 不需要前缀或已有前缀，直接使用
            final_table_name = table_name

        # 构建基础路径
        if database != 'default':
            base_table_path = f"{base_path}/{database}.db/{final_table_name}"
        else:
            base_table_path = f"{base_path}/{final_table_name}"

        # 处理分区
        partition_filter = source.get('partition_filter')
        if partition_filter:
            hdfs_path = f"{base_table_path}/{partition_filter}/*"
        else:
            hdfs_path = f"{base_table_path}/*"

        logger.info(f"原始表名: {table_name}")
        logger.info(f"最终表名: {final_table_name}")
        logger.info(f"生成Hive读取路径: {hdfs_path}")
        return hdfs_path

    # def _generate_hdfs_column_config(self, column_info: List[Dict], column_names: List[str]) -> List[Dict]:
    #     """生成HDFS Reader的字段配置"""
    #     column_config = []
    #
    #     if column_info:
    #         data_columns = [col for col in column_info if col.get('name', '').lower() != 'dt']
    #         # 如果有详细的字段信息，使用类型映射
    #         for i, col in enumerate(data_columns):
    #             col_name = col.get('name', f'column_{i}')
    #             col_type = col.get('target_type', 'STRING')
    #
    #             # 转换为hdfsreader支持的类型
    #             hdfs_type = self._convert_to_hdfs_reader_type(col_type)
    #
    #             column_config.append({
    #                 "index": i,
    #                 "type": hdfs_type
    #             })
    #     else:
    #         data_column_names = [col for col in column_names if col.lower() != 'dt']
    #         # 如果只有字段名，默认都是string类型
    #         for i, col_name in enumerate(data_column_names):
    #             column_config.append({
    #                 "index": i,
    #                 "type": "string"
    #             })
    #
    #     logger.info(f"生成HDFS字段配置: {len(column_config)}个字段（已排除分区字段）")
    #     return column_config
    def _generate_hdfs_column_config(self, column_info: List[Dict], column_names: List[str]) -> List[Dict]:
        """生成HDFS Reader的字段配置"""
        column_config = []

        # 确保只使用简单类型
        if column_info:
            for i, col in enumerate(column_info):
                # 只提取必要的字段，避免其他对象引用
                if isinstance(col, dict):
                    col_name = str(col.get('name', f'column_{i}'))
                    if col_name.lower() != 'dt':  # 排除分区字段
                        column_config.append({
                            "index": len(column_config),
                            "type": "string"  # 简化类型处理
                        })
        else:
            for i, col_name in enumerate(column_names):
                if col_name.lower() != 'dt':
                    column_config.append({
                        "index": len(column_config),
                        "type": "string"
                    })

        logger.info(f"生成HDFS字段配置: {len(column_config)}个字段")
        return column_config

    def _convert_to_hdfs_reader_type(self, hive_type: str) -> str:
        """将Hive类型转换为HDFS Reader支持的类型"""
        base_type = hive_type.split('(')[0].upper()

        type_mapping = {
            'STRING': 'string',
            'VARCHAR': 'string',
            'TEXT': 'string',
            'INT': 'long',
            'INTEGER': 'long',
            'BIGINT': 'long',
            'SMALLINT': 'long',
            'TINYINT': 'long',
            'DECIMAL': 'double',
            'DOUBLE': 'double',
            'FLOAT': 'double',
            'BOOLEAN': 'boolean',
            'DATE': 'date',
            'TIMESTAMP': 'date',
            'DATETIME': 'date'
        }

        return type_mapping.get(base_type, 'string')
    def _get_writer_config(self, target: Dict[str, Any]) -> Dict[str, Any]:
        """根据目标类型生成writer配置"""
        db_type = target['type'].lower()

        if db_type == 'mysql':
            columns = target.get('columns', [])
            if not columns:
                raise ValueError("MySQL Writer缺少字段配置")

            return {
                "name": "mysqlwriter",
                "parameter": {
                    "writeMode": target.get('write_mode', 'insert'),
                    "username": target['username'],
                    "password": target['password'],
                    "column": columns,
                    "connection": [{
                        "jdbcUrl": f"jdbc:mysql://{target['host']}:{target['port']}/{target['database']}?useUnicode=true&characterEncoding=utf8",
                        "table": [target['table']]
                    }],
                    "preSql": target.get('pre_sql', []),
                    "postSql": target.get('post_sql', [])
                }
            }

        elif db_type == 'doris':
            columns = target.get('columns', [])
            if not columns:
                raise ValueError("Doris Writer缺少字段配置")

            return {
                "name": "doriswriter",
                "parameter": {
                    "loadUrl": [f"{target['host']}:{target.get('http_port', 8060)}"],
                    "column": columns,
                    "username": target['username'],
                    "password": target['password'],
                    "postSql": target.get('post_sql', []),
                    "preSql": target.get('pre_sql', []),
                    "flushInterval": target.get('flush_interval', 30000),
                    "connection": [{
                        "jdbcUrl": f"jdbc:mysql://{target['host']}:{target['port']}/{target['database']}",
                        "selectedDatabase": target['database'],
                        "table": [target['table']]
                    }],
                    "loadProps": {
                        "format": target.get('format', 'json'),
                        "strip_outer_array": target.get('strip_outer_array', True)
                    }
                }
            }

        elif db_type == 'hive':
            columns = target.get('columns', [])
            if not columns:
                raise ValueError("Hive Writer缺少字段配置")

            return {
                "name": "hdfswriter",
                "parameter": {
                    "defaultFS": f"hdfs://{target['namenode_host']}:{target['namenode_port']}",
                    "fileType": "orc",
                    "path": target['hdfs_path'],
                    "fileName": target.get('file_name', 'data'),
                    "column": [{"name": col, "type": "string"} for col in columns],
                    "fieldDelimiter": "\t",
                    "writeMode": "append",
                    "compress": target.get('compression', 'snappy'),
                    #"orcSchema": self._generate_orc_schema(columns),
                    # "hadoopConfig": {
                    #     "orc.compress": target.get('compression', 'snappy'),
                    #     "orc.create.index": "true"
                    # }
                }
            }


        elif db_type == 'kingbase':
            columns = target.get('columns', [])
            if not columns:
                raise ValueError("KingBase Writer缺少字段配置")

            return {
                "name": "kingbaseeswriter",
                "parameter": {
                    "username": target['username'],
                    "password": target['password'],
                    "column": columns,
                    "connection": [{
                        "jdbcUrl": f"jdbc:kingbase8://{target['host']}:{target['port']}/{target['database']}",
                        "table": [target['table']]
                    }],
                    "preSql": target.get('pre_sql', []),
                    "postSql": target.get('post_sql', [])
                }
            }

        elif db_type == 'postgresql':
            columns = target.get('columns', [])
            if not columns:
                raise ValueError("PostgreSQL Writer缺少字段配置")

            return {
                "name": "postgresqlwriter",
                "parameter": {
                    "username": target['username'],
                    "password": target['password'],
                    "column": columns,
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
            env = os.environ.copy()

            # 首先检查DataX是否存在
            if not self.python_path.exists():
                return {
                    "success": False,
                    "task_id": task_id,
                    "error": f"DataX脚本不存在: {self.python_path}",
                    "exit_code": -1
                }

            # 检查配置文件是否存在
            if not os.path.exists(config_file):
                return {
                    "success": False,
                    "task_id": task_id,
                    "error": f"配置文件不存在: {config_file}",
                    "exit_code": -1
                }

            # 读取并验证配置文件内容
            try:
                with open(config_file, 'r', encoding='utf-8', errors='replace') as f:
                    config_content = f.read()
                    logger.info(f"DataX配置文件内容:\n{config_content}")
                    # 验证JSON格式
                    json.loads(config_content)
            except Exception as e:
                return {
                    "success": False,
                    "task_id": task_id,
                    "error": f"配置文件格式错误: {str(e)}",
                    "exit_code": -1
                }

            # 设置JVM参数（通过环境变量）
            jvm_opts = [
                "-Xms1G",
                "-Xmx4G",
                "-XX:+UseG1GC",
                "-Dfastjson.parser.safeMode=true",
                "-Dfastjson2.parser.safeMode=true"
            ]
            env['DATAX_JVM_OPTS'] = ' '.join(jvm_opts)

            # 构建DataX执行命令 - 这个必须在if块外面！
            cmd = [
                "python",
                str(self.python_path),
                config_file
            ]

            logger.info(f"执行DataX命令: {' '.join(cmd)}")
            logger.info(f"JVM参数: {env.get('DATAX_JVM_OPTS', 'None')}")

            # 异步执行命令
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )

            stdout, stderr = await process.communicate()

            # 解码输出
            stdout_text = stdout.decode('utf-8', errors='replace')
            stderr_text = stderr.decode('utf-8', errors='replace')

            logger.info(f"DataX stdout: {stdout_text}")
            if stderr_text:
                logger.error(f"DataX stderr: {stderr_text}")
            logger.info(f"DataX退出码: {process.returncode}")

            # 解析执行结果
            if process.returncode == 0:
                # 解析DataX输出统计信息
                stats = self._parse_datax_output(stdout_text)

                return {
                    "success": True,
                    "task_id": task_id,
                    "statistics": stats,
                    "output": stdout_text,
                    "stderr": stderr_text
                }
            else:
                # 提供更详细的错误信息
                error_message = stderr_text or stdout_text or f"DataX执行失败，退出码: {process.returncode}"

                # 检查是否是StackOverflowError
                if "StackOverflowError" in error_message:
                    logger.error("检测到StackOverflowError，这是FastJSON序列化问题")
                    error_message = "FastJSON序列化失败（StackOverflowError），请检查Hive reader配置"

                return {
                    "success": False,
                    "task_id": task_id,
                    "error": error_message,
                    "stdout": stdout_text,
                    "stderr": stderr_text,
                    "exit_code": process.returncode
                }

        except FileNotFoundError as e:
            return {
                "success": False,
                "task_id": task_id,
                "error": f"命令未找到: {str(e)}。请检查DataX是否正确安装",
                "exit_code": -1
            }
        except Exception as e:
            logger.error(f"执行DataX任务异常: {str(e)}")
            return {
                "success": False,
                "task_id": task_id,
                "error": f"执行异常: {str(e)}",
                "exit_code": -1
            }

    def _generate_orc_schema(self, columns: List[str]) -> str:
        """生成ORC Schema"""
        # 简化版本，实际应该根据字段类型生成
        orc_fields = [f"{col}:string" for col in columns]
        return f"struct<{','.join(orc_fields)}>"

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

    def _clean_config_for_serialization(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """清理配置中可能导致循环引用的对象"""
        import copy
        clean_config = {}

        for key, value in config.items():
            if isinstance(value, dict):
                clean_config[key] = self._clean_dict(value)
            elif isinstance(value, list):
                clean_config[key] = [self._clean_dict(item) if isinstance(item, dict) else item for item in value]
            else:
                clean_config[key] = value

        return clean_config

    def _clean_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """清理字典中的不可序列化对象"""
        clean = {}
        exclude_keys = {'class', 'classLoader', 'module', '__class__', '__dict__'}

        for k, v in data.items():
            if k not in exclude_keys and not callable(v):
                try:
                    json.dumps(v)  # 测试是否可序列化
                    clean[k] = v
                except (TypeError, ValueError):
                    clean[k] = str(v)  # 转为字符串

        return clean


# 扩展现有的sync API
class EnhancedSyncService:
    """增强的数据同步服务"""

    def __init__(self):
        self.datax_service = DataXIntegrationService()

    async def execute_sync_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """执行数据同步任务"""
        try:
            logger.info(f"验证同步配置: {task_config}")
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
            logger.info("配置验证通过")
            # 根据同步类型选择执行方式
            sync_type = task_config.get('sync_type', 'full')
            logger.info(f"同步类型: {sync_type}")
            if sync_type == 'incremental':
                # 增量同步逻辑
                return await self._execute_incremental_sync(task_config)
            else:
                # 全量同步
                return await self._execute_full_sync(task_config)

        except Exception as e:
            error_msg = f"同步任务执行失败: {str(e)}"
            logger.error(f"{error_msg}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            return {
                "success": False,
                "error": error_msg,
                "error_type": "sync_task_error",
                "task_id": task_config.get('id'),
                "traceback": traceback.format_exc()
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