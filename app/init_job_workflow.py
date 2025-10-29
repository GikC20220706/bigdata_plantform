"""
作业流系统初始化
在应用启动时调用此脚本初始化作业流相关组件
"""
from loguru import logger


def init_job_workflow_system():
    """初始化作业流系统"""
    try:
        logger.info("=" * 50)
        logger.info("开始初始化作业流系统...")

        # 1. 注册所有执行器
        from app.services.executors import init_executors
        init_executors()
        logger.info("✓ 执行器注册完成")

        # 2. TODO: 初始化调度器（如果有定时任务）
        # from app.services.scheduler_service import init_scheduler
        # init_scheduler()

        # 3. TODO: 加载持久化的作业流配置

        logger.info("✓ 作业流系统初始化完成")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"作业流系统初始化失败: {e}")
        raise


if __name__ == "__main__":
    init_job_workflow_system()