import os
from pathlib import Path

from celery import Celery
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

app = Celery(
    "demo",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_BACKEND"],
    include=["tasks"],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
    task_default_queue="celery_demo",
)
