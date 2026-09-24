import asyncio


async def demo_task_group() -> None:
    async def success() -> str:
        await asyncio.sleep(0.05)
        return "A"

    async def fail() -> None:
        await asyncio.sleep(0.1)
        raise ValueError("B 失败")

    async def slow() -> None:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            print("C 被取消")
            raise

    try:
        async with asyncio.TaskGroup() as group:
            success_task = group.create_task(success())
            group.create_task(fail())
            group.create_task(slow())
    except* ValueError as errors:
        print(success_task.result())
        print(errors.exceptions[0])


asyncio.run(demo_task_group())
