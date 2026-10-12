from celery_app import final_app


@final_app.task(name="final_app.final_app_add")
async def final_app_add(a: int, b: int) -> int:
    """任务1"""
    return a + b
