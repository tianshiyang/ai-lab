from celery.utils.log import get_task_logger
from celery_app import final_app

logger = get_task_logger(__name__)


@final_app.task(name="final_app.final_app_add")
def final_app_add(a: int, b: int) -> int:
    """任务1"""
    logger.info("开始计算 %s + %s", a, b)
    result = a + b
    logger.info("计算完成,结果 %s", result)
    return result


@final_app.task(name="final_app.final_app_sub")
def final_app_sub(a: int, b: int) -> int:
    """任务1"""
    logger.info("开始计算 %s - %s", a, b)
    result = a - b
    logger.info("计算完成,结果 %s", result)
    return result


@final_app.task(name="final_app.scheduler_final_mul")
def final_app_scheduler_final_mul():
    logger.info("每嗝5秒执行一次")
