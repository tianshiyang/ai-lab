from celery教程.tasks import add

result = add.delay(2, 3)

print(f"task_id={result.id}")
print(f"state={result.state}")
