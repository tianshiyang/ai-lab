from celery教程.tasks import add

result = add.apply_async(args=(2, 3), countdown=1, expires=60)

# print(result.id)

try:
    value = result.get(timeout=5)
except TimeoutError:
    print("调用端等待超时；这不等于 worker 已取消任务")
else:
    print(value)
