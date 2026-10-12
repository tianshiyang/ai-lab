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
    "zhouqirenwu5miao": {
        "task": "scheduler.scheduler_final_mul",
        "schedule": 30,
    },
    "zhouqirenwu5miao": {
        "task": "scheduler.scheduler_final_mul",
        "schedule": crontab(hour=3, minute=0),
    },
}
