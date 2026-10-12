import os
from pathlib import Path

import dotenv
from celery import Celery

dotenv.load_dotenv(Path(__file__).parents[2] / ".env")

final_app = Celery(
    "final",
    broker=os.getenv("CELERY_BROKER_URL"),
    include=["celery教程.09-结业项目-异步任务中心.tasks"],
)

final_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    task_default_queue="celery_final",
)
