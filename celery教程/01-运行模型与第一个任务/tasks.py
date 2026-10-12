from celery教程.celery_app import app


@app.task(name="demo.add")
def add(left: int, right: int) -> int:
    return left + right
