import os
from pathlib import Path

import dotenv
from celery import Celery

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
        "final_app.final_app_sub": {"queue": "final_sub"},
        "final_app.final_app_add": {"queue": "final_add"},
    },
)
