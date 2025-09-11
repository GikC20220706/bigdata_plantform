"""
DAG生成服务
动态生成Airflow DAG文件，支持SQL、Shell、DataX等任务类型
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from jinja2 import Template, Environment, FileSystemLoader
from loguru import logger

from config.airflow_config import airflow_config
from app.utils.airflow_client import airflow_client
from app.models.task import TaskDefinition, TaskSchedule


class DAGGeneratorService:
    """DAG动态生成服务"""

    def __init__(self):
        self.dag_folder = Path(airflow_config.DAG_FOLDER)
        self.templates_folder = Path(airflow_config.DAG_TEMPLATES_FOLDER)
        self.generated_folder = self.dag_folder / "generated"

        # 确保目录存在
        self.dag_folder.mkdir(parents=True, exist_ok=True)
        self.templates_folder.mkdir(parents=True, exist_ok=True)
        self.generated_folder.mkdir(parents=True, exist_ok=True)

        # 初始化Jinja2环境
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.templates_folder)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True
        )

        # 创建DAG模板
        self._create_dag_templates()

    def _create_dag_templates(self):
        """创建DAG模板文件"""
        templates = {
            "sql_dag.py.j2": self._get_sql_dag_template(),
            "shell_dag.py.j2": self._get_shell_dag_template(),
            "datax_dag.py.j2": self._get_datax_dag_template(),
            "multi_task_dag.py.j2": self._get_multi_task_dag_template()
        }

        for filename, content in templates.items():
            template_file = self.templates_folder / filename
            if not template_file.exists():
                template_file.write_text(content, encoding='utf-8')
                logger.info(f"创建DAG模板: {filename}")

    async def generate_dag(
            self,
            task_config: Dict[str, Any],
            schedule_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        生成DAG文件

        Args:
            task_config: 任务配置
                {
                    "dag_id": "data_sync_daily",
                    "display_name": "每日数据同步",
                    "description": "同步业务数据到数据仓库",
                    "task_type": "datax",  # sql, shell, datax, multi
                    "tasks": [...],  # 任务列表
                    "owner": "bigdata-platform"
                }
            schedule_config: 调度配置
                {
                    "schedule_interval": "0 2 * * *",
                    "start_date": "2024-01-01",
                    "catchup": False,
                    "max_active_runs": 1
                }

        Returns:
            生成结果
        """
        try:
            # 1. 验证配置
            validation_result = self._validate_config(task_config)
            if not validation_result["valid"]:
                return {
                    "success": False,
                    "error": f"配置验证失败: {validation_result['error']}"
                }

            # 2. 准备DAG参数
            dag_params = self._prepare_dag_params(task_config, schedule_config)

            # 3. 选择并渲染模板
            template_name = self._get_template_name(task_config["task_type"])
            dag_content = self._render_dag_template(template_name, dag_params)

            # 4. 生成DAG文件
            dag_file_path = self._write_dag_file(dag_params["dag_id"], dag_content)

            # 5. 验证生成的DAG
            validation_result = await self._validate_generated_dag(dag_file_path)

            return {
                "success": True,
                "dag_id": dag_params["dag_id"],
                "dag_file_path": str(dag_file_path),
                "template_used": template_name,
                "validation_result": validation_result,
                "generated_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"生成DAG失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "task_config": task_config
            }

    async def generate_multi_task_dag(
            self,
            dag_config: Dict[str, Any],
            tasks: List[Dict[str, Any]],
            dependencies: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        生成包含多个任务的复杂DAG

        Args:
            dag_config: DAG基础配置
            tasks: 任务列表
            dependencies: 任务依赖关系 [{"upstream": "task1", "downstream": "task2"}]
        """
        try:
            dag_id = dag_config["dag_id"]

            # 构建完整的DAG配置
            full_config = {
                **dag_config,
                "task_type": "multi",
                "tasks": tasks,
                "dependencies": dependencies or []
            }

            # 生成DAG
            result = await self.generate_dag(full_config)

            if result["success"]:
                logger.info(f"多任务DAG生成成功: {dag_id}, 包含 {len(tasks)} 个任务")

            return result

        except Exception as e:
            logger.error(f"生成多任务DAG失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def update_dag(
            self,
            dag_id: str,
            task_config: Dict[str, Any],
            schedule_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """更新现有DAG"""
        try:
            # 先暂停DAG
            pause_result = await airflow_client.pause_dag(dag_id, is_paused=True)

            # 重新生成DAG
            result = await self.generate_dag(task_config, schedule_config)

            if result["success"]:
                # 等待一段时间让Airflow检测到文件变化
                await asyncio.sleep(5)

                # 重新启用DAG
                await airflow_client.pause_dag(dag_id, is_paused=False)

                result["updated"] = True
                logger.info(f"DAG更新成功: {dag_id}")

            return result

        except Exception as e:
            logger.error(f"更新DAG失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def delete_dag(self, dag_id: str) -> Dict[str, Any]:
        """删除DAG文件和Airflow中的DAG"""
        try:
            # 1. 从Airflow中删除DAG
            airflow_delete_result = await airflow_client.delete_dag(dag_id)

            # 2. 删除DAG文件
            dag_file_path = self.generated_folder / f"{dag_id}.py"
            if dag_file_path.exists():
                dag_file_path.unlink()
                logger.info(f"DAG文件已删除: {dag_file_path}")

            return {
                "success": True,
                "dag_id": dag_id,
                "airflow_deleted": airflow_delete_result,
                "file_deleted": True,
                "deleted_at": datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"删除DAG失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    async def list_generated_dags(self) -> Dict[str, Any]:
        """列出所有生成的DAG"""
        try:
            generated_dags = []

            for dag_file in self.generated_folder.glob("*.py"):
                if dag_file.name.startswith("_"):  # 跳过私有文件
                    continue

                dag_info = {
                    "dag_id": dag_file.stem,
                    "file_path": str(dag_file),
                    "file_size": dag_file.stat().st_size,
                    "created_at": datetime.fromtimestamp(dag_file.stat().st_ctime),
                    "modified_at": datetime.fromtimestamp(dag_file.stat().st_mtime)
                }

                generated_dags.append(dag_info)

            return {
                "success": True,
                "total": len(generated_dags),
                "dags": generated_dags
            }

        except Exception as e:
            logger.error(f"列出生成的DAG失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    # ==================== 私有方法 ====================

    def _validate_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """验证任务配置"""
        required_fields = ["dag_id", "task_type"]
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            return {
                "valid": False,
                "error": f"缺少必要字段: {missing_fields}"
            }

        # 验证任务类型
        valid_types = ["sql", "shell", "datax", "multi"]
        if config["task_type"] not in valid_types:
            return {
                "valid": False,
                "error": f"不支持的任务类型: {config['task_type']}, 支持的类型: {valid_types}"
            }

        # 验证DAG ID格式
        dag_id = config["dag_id"]
        if not dag_id.replace("_", "").replace("-", "").isalnum():
            return {
                "valid": False,
                "error": "DAG ID只能包含字母、数字、下划线和短横线"
            }

        return {"valid": True}

    def _prepare_dag_params(
            self,
            task_config: Dict[str, Any],
            schedule_config: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """准备DAG渲染参数"""
        # 默认调度配置
        default_schedule = {
            "schedule_interval": None,  # 手动触发
            "start_date": datetime.now().strftime("%Y-%m-%d"),
            "catchup": False,
            "max_active_runs": 1,
            "max_active_tasks": 16,
            "retries": airflow_config.DEFAULT_RETRY_COUNT,
            "retry_delay_seconds": airflow_config.DEFAULT_RETRY_DELAY
        }

        if schedule_config:
            default_schedule.update(schedule_config)

        # 构建完整参数
        dag_params = {
            "dag_id": task_config["dag_id"],
            "display_name": task_config.get("display_name", task_config["dag_id"]),
            "description": task_config.get("description", ""),
            "owner": task_config.get("owner", airflow_config.DEFAULT_DAG_ARGS["owner"]),
            "task_type": task_config["task_type"],
            "tasks": task_config.get("tasks", []),
            "dependencies": task_config.get("dependencies", []),
            "schedule": default_schedule,
            "dag_args": airflow_config.DEFAULT_DAG_ARGS,
            "generated_at": datetime.now().isoformat(),
            "bigdata_platform_api": airflow_config.BIGDATA_PLATFORM_API
        }

        return dag_params

    def _get_template_name(self, task_type: str) -> str:
        """根据任务类型获取模板名称"""
        template_mapping = {
            "sql": "sql_dag.py.j2",
            "shell": "shell_dag.py.j2",
            "datax": "datax_dag.py.j2",
            "multi": "multi_task_dag.py.j2"
        }
        return template_mapping.get(task_type, "multi_task_dag.py.j2")

    def _render_dag_template(self, template_name: str, params: Dict[str, Any]) -> str:
        """渲染DAG模板"""
        template = self.jinja_env.get_template(template_name)
        return template.render(**params)

    def _write_dag_file(self, dag_id: str, content: str) -> Path:
        """写入DAG文件并确保Airflow能访问"""
        dag_file_path = self.generated_folder / f"{dag_id}.py"

        # 添加文件头注释
        header = f'''"""
    自动生成的Airflow DAG文件
    DAG ID: {dag_id}
    生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    生成器: 数据底座DAG生成服务

    警告: 此文件由系统自动生成，请勿手动修改！
    如需修改，请通过数据底座平台的调度管理界面进行。
    """

    '''

        full_content = header + content

        # 写入到generated子目录
        dag_file_path.write_text(full_content, encoding='utf-8')

        # 同时复制到Airflow能访问的主DAG目录
        main_dag_path = self.dag_folder / f"{dag_id}.py"
        main_dag_path.write_text(full_content, encoding='utf-8')

        logger.info(f"DAG文件已生成: {dag_file_path}")
        logger.info(f"DAG文件已同步到Airflow目录: {main_dag_path}")

        return dag_file_path

    async def _validate_generated_dag(self, dag_file_path: Path) -> Dict[str, Any]:
        """验证生成的DAG文件"""
        try:
            # 基础语法检查
            with open(dag_file_path, 'r', encoding='utf-8') as f:
                code = f.read()

            # 编译检查
            compile(code, str(dag_file_path), 'exec')

            return {
                "valid": True,
                "file_size": dag_file_path.stat().st_size,
                "line_count": len(code.split('\n'))
            }

        except SyntaxError as e:
            return {
                "valid": False,
                "error": f"语法错误: {e}",
                "line": e.lineno
            }
        except Exception as e:
            return {
                "valid": False,
                "error": str(e)
            }

    # ==================== DAG模板定义 ====================

    def _get_sql_dag_template(self) -> str:
        """SQL任务DAG模板"""
        return '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

# DAG默认参数
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {{ schedule.retries }},
    'retry_delay': timedelta(seconds={{ schedule.retry_delay_seconds }}),
    'start_date': datetime.strptime('{{ schedule.start_date }}', '%Y-%m-%d'),
}

# 创建DAG
dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule.schedule_interval }}' if '{{ schedule.schedule_interval }}' != 'None' else None,
    catchup={{ schedule.catchup|lower }},
    max_active_runs={{ schedule.max_active_runs }},
    max_active_tasks={{ schedule.max_active_tasks }},
    tags=['sql', 'bigdata-platform', '{{ task_type }}']
)

def notify_platform(task_id, status, **context):
    """通知数据底座平台任务状态"""
    try:
        api_url = "{{ bigdata_platform_api }}/api/v1/scheduler/task-status"
        data = {
            "dag_id": "{{ dag_id }}",
            "task_id": task_id,
            "status": status,
            "execution_date": context.get('ds'),
            "log_url": context.get('task_instance').log_url
        }
        requests.post(api_url, json=data, timeout=10)
    except Exception as e:
        print(f"通知平台失败: {e}")

{% for task in tasks %}
# SQL任务: {{ task.task_id }}
sql_task_{{ loop.index }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "执行SQL任务: {{ task.task_id }}"
    {% if task.sql_file %}
    # 执行SQL文件
    mysql -h{{ task.connection.host }} -P{{ task.connection.port }} -u{{ task.connection.username }} -p{{ task.connection.password }} {{ task.connection.database }} < {{ task.sql_file }}
    {% else %}
    # 执行SQL语句
    mysql -h{{ task.connection.host }} -P{{ task.connection.port }} -u{{ task.connection.username }} -p{{ task.connection.password }} {{ task.connection.database }} -e "{{ task.sql }}"
    {% endif %}
    echo "SQL任务执行完成"
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% endfor %}

# 设置任务依赖关系
{% for dep in dependencies %}
{{ dep.upstream }} >> {{ dep.downstream }}
{% endfor %}
'''

    def _get_shell_dag_template(self) -> str:
        """Shell任务DAG模板"""
        return '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import requests

# DAG默认参数
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {{ schedule.retries }},
    'retry_delay': timedelta(seconds={{ schedule.retry_delay_seconds }}),
    'start_date': datetime.strptime('{{ schedule.start_date }}', '%Y-%m-%d'),
}

# 创建DAG
dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule.schedule_interval }}' if '{{ schedule.schedule_interval }}' != 'None' else None,
    catchup={{ schedule.catchup|lower }},
    max_active_runs={{ schedule.max_active_runs }},
    max_active_tasks={{ schedule.max_active_tasks }},
    tags=['shell', 'bigdata-platform', '{{ task_type }}']
)

def notify_platform(task_id, status, **context):
    """通知数据底座平台任务状态"""
    try:
        api_url = "{{ bigdata_platform_api }}/api/v1/scheduler/task-status"
        data = {
            "dag_id": "{{ dag_id }}",
            "task_id": task_id,
            "status": status,
            "execution_date": context.get('ds'),
            "log_url": context.get('task_instance').log_url
        }
        requests.post(api_url, json=data, timeout=10)
    except Exception as e:
        print(f"通知平台失败: {e}")

{% for task in tasks %}
# Shell任务: {{ task.task_id }}
shell_task_{{ loop.index }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "开始执行Shell任务: {{ task.task_id }}"
    {% if task.script_file %}
    # 执行脚本文件
    chmod +x {{ task.script_file }}
    {{ task.script_file }}
    {% else %}
    # 执行Shell命令
    {{ task.command }}
    {% endif %}
    echo "Shell任务执行完成"
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% endfor %}

# 设置任务依赖关系
{% for dep in dependencies %}
{{ dep.upstream }} >> {{ dep.downstream }}
{% endfor %}
'''

    def _get_datax_dag_template(self) -> str:
        """DataX任务DAG模板"""
        return '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import requests
import json

# DAG默认参数
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {{ schedule.retries }},
    'retry_delay': timedelta(seconds={{ schedule.retry_delay_seconds }}),
    'start_date': datetime.strptime('{{ schedule.start_date }}', '%Y-%m-%d'),
}

# 创建DAG
dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule.schedule_interval }}' if '{{ schedule.schedule_interval }}' != 'None' else None,
    catchup={{ schedule.catchup|lower }},
    max_active_runs={{ schedule.max_active_runs }},
    max_active_tasks={{ schedule.max_active_tasks }},
    tags=['datax', 'sync', 'bigdata-platform']
)

def notify_platform(task_id, status, **context):
    """通知数据底座平台任务状态"""
    try:
        api_url = "{{ bigdata_platform_api }}/api/v1/scheduler/task-status"
        data = {
            "dag_id": "{{ dag_id }}",
            "task_id": task_id,
            "status": status,
            "execution_date": context.get('ds'),
            "log_url": context.get('task_instance').log_url
        }
        requests.post(api_url, json=data, timeout=10)
    except Exception as e:
        print(f"通知平台失败: {e}")

{% for task in tasks %}
# DataX同步任务: {{ task.task_id }}
datax_task_{{ loop.index }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "开始执行DataX同步任务: {{ task.task_id }}"

    # 设置DataX环境变量
    export DATAX_HOME=/opt/datax
    export PATH=$DATAX_HOME/bin:$PATH

    # 执行DataX任务
    {% if task.config_file %}
    python $DATAX_HOME/bin/datax.py {{ task.config_file }}
    {% else %}
    # 动态生成DataX配置文件
    cat > /tmp/datax_{{ task.task_id }}_$.json << 'EOF'
{{ task.config | tojson }}
EOF

    python $DATAX_HOME/bin/datax.py /tmp/datax_{{ task.task_id }}_$.json

    # 清理临时文件
    rm -f /tmp/datax_{{ task.task_id }}_$.json
    {% endif %}

    echo "DataX同步任务执行完成"
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% endfor %}

# 设置任务依赖关系
{% for dep in dependencies %}
{{ dep.upstream }} >> {{ dep.downstream }}
{% endfor %}
'''

    def _get_multi_task_dag_template(self) -> str:
        """多任务混合DAG模板"""
        return '''
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import requests
import json

# DAG默认参数
default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {{ schedule.retries }},
    'retry_delay': timedelta(seconds={{ schedule.retry_delay_seconds }}),
    'start_date': datetime.strptime('{{ schedule.start_date }}', '%Y-%m-%d'),
}

# 创建DAG
dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule.schedule_interval }}' if '{{ schedule.schedule_interval }}' != 'None' else None,
    catchup={{ schedule.catchup|lower }},
    max_active_runs={{ schedule.max_active_runs }},
    max_active_tasks={{ schedule.max_active_tasks }},
    tags=['multi-task', 'bigdata-platform', 'workflow']
)

def notify_platform(task_id, status, **context):
    """通知数据底座平台任务状态"""
    try:
        api_url = "{{ bigdata_platform_api }}/api/v1/scheduler/task-status"
        data = {
            "dag_id": "{{ dag_id }}",
            "task_id": task_id,
            "status": status,
            "execution_date": context.get('ds'),
            "log_url": context.get('task_instance').log_url
        }
        requests.post(api_url, json=data, timeout=10)
    except Exception as e:
        print(f"通知平台失败: {e}")

# 开始任务
start_task = DummyOperator(
    task_id='start',
    dag=dag
)

# 结束任务
end_task = DummyOperator(
    task_id='end',
    dag=dag
)

{% for task in tasks %}
{% if task.type == 'sql' %}
# SQL任务: {{ task.task_id }}
{{ task.task_id }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "执行SQL任务: {{ task.task_id }}"
    {% if task.sql_file %}
    mysql -h{{ task.connection.host }} -P{{ task.connection.port }} -u{{ task.connection.username }} -p{{ task.connection.password }} {{ task.connection.database }} < {{ task.sql_file }}
    {% else %}
    mysql -h{{ task.connection.host }} -P{{ task.connection.port }} -u{{ task.connection.username }} -p{{ task.connection.password }} {{ task.connection.database }} -e "{{ task.sql }}"
    {% endif %}
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% elif task.type == 'shell' %}
# Shell任务: {{ task.task_id }}
{{ task.task_id }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "执行Shell任务: {{ task.task_id }}"
    {% if task.script_file %}
    chmod +x {{ task.script_file }}
    {{ task.script_file }}
    {% else %}
    {{ task.command }}
    {% endif %}
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% elif task.type == 'datax' %}
# DataX同步任务: {{ task.task_id }}
{{ task.task_id }} = BashOperator(
    task_id='{{ task.task_id }}',
    bash_command="""
    echo "执行DataX同步任务: {{ task.task_id }}"
    export DATAX_HOME=/opt/datax
    export PATH=$DATAX_HOME/bin:$PATH
    {% if task.config_file %}
    python $DATAX_HOME/bin/datax.py {{ task.config_file }}
    {% else %}
    cat > /tmp/datax_{{ task.task_id }}_$.json << 'EOF'
{{ task.config | tojson }}
EOF
    python $DATAX_HOME/bin/datax.py /tmp/datax_{{ task.task_id }}_$.json
    rm -f /tmp/datax_{{ task.task_id }}_$.json
    {% endif %}
    """,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% elif task.type == 'python' %}
def {{ task.task_id }}_function(**context):
    """{{ task.description or task.task_id }}"""
    {{ task.python_code | indent(4) }}

# Python任务: {{ task.task_id }}
{{ task.task_id }} = PythonOperator(
    task_id='{{ task.task_id }}',
    python_callable={{ task.task_id }}_function,
    dag=dag,
    on_success_callback=lambda context: notify_platform('{{ task.task_id }}', 'success', **context),
    on_failure_callback=lambda context: notify_platform('{{ task.task_id }}', 'failed', **context)
)

{% else %}
# 默认任务: {{ task.task_id }}
{{ task.task_id }} = DummyOperator(
    task_id='{{ task.task_id }}',
    dag=dag
)

{% endif %}
{% endfor %}

# 设置任务依赖关系
start_task >> [{% for task in tasks %}{{ task.task_id }}{{ "," if not loop.last }}{% endfor %}]

{% for dep in dependencies %}
{{ dep.upstream }} >> {{ dep.downstream }}
{% endfor %}

[{% for task in tasks %}{{ task.task_id }}{{ "," if not loop.last }}{% endfor %}] >> end_task
'''


# 创建全局DAG生成服务实例
dag_generator_service = DAGGeneratorService()


# ==================== 便捷函数 ====================

async def create_sql_dag(
        dag_id: str,
        sql_tasks: List[Dict[str, Any]],
        schedule_interval: Optional[str] = None,
        **kwargs
) -> Dict[str, Any]:
    """创建SQL任务DAG的便捷函数"""
    task_config = {
        "dag_id": dag_id,
        "task_type": "sql",
        "tasks": sql_tasks,
        **kwargs
    }

    schedule_config = {}
    if schedule_interval:
        schedule_config["schedule_interval"] = schedule_interval

    return await dag_generator_service.generate_dag(task_config, schedule_config)


async def create_datax_dag(
        dag_id: str,
        sync_tasks: List[Dict[str, Any]],
        schedule_interval: Optional[str] = None,
        **kwargs
) -> Dict[str, Any]:
    """创建DataX同步DAG的便捷函数"""
    task_config = {
        "dag_id": dag_id,
        "task_type": "datax",
        "tasks": sync_tasks,
        **kwargs
    }

    schedule_config = {}
    if schedule_interval:
        schedule_config["schedule_interval"] = schedule_interval

    return await dag_generator_service.generate_dag(task_config, schedule_config)


async def create_shell_dag(
        dag_id: str,
        shell_tasks: List[Dict[str, Any]],
        schedule_interval: Optional[str] = None,
        **kwargs
) -> Dict[str, Any]:
    """创建Shell任务DAG的便捷函数"""
    task_config = {
        "dag_id": dag_id,
        "task_type": "shell",
        "tasks": shell_tasks,
        **kwargs
    }

    schedule_config = {}
    if schedule_interval:
        schedule_config["schedule_interval"] = schedule_interval

    return await dag_generator_service.generate_dag(task_config, schedule_config)
