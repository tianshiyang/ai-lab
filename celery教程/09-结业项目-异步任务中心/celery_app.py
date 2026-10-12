import os
from pathlib import Path

import dotenv
from celery import Celery
from celery.schedules import crontab

dotenv.load_dotenv(Path(__file__).parents[2] / ".env")

final_app = Celery(
    "final",
    broker=os.getenv("CELERY_BROKER_URL"),
    include=["tasks"],
)

final_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    task_default_queue="celery_final",
    task_routes={
        "sub.*": {"queue": "final_sub"},
        "add.*": {"queue": "final_add"},
    },
)

final_app.conf.beat_schedule = {
    "mul-every-30s": {
        "task": "scheduler.scheduler_final_mul",
        "schedule": 30,
    },
    "mul-every-year-0101": {  # 每年 1 月 1 日凌晨 3 点执行一次
        "task": "scheduler.scheduler_final_mul",
        "schedule": crontab(hour=3, minute=0, day_of_month=1, month_of_year=1),
    },
}
