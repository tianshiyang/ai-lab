import asyncio


async def demo_create_task() -> None:
    async def work() -> str:
        await asyncio.sleep(0.1)
        return "任务结果"
    task = asyncio.create_task(work(), name="my-task")
    print(task.get_name(), task.done())
    print(await task)
    print(task.done())

async def main() -> None:
    await demo_create_task()


if __name__ == "__main__":
    asyncio.run(main())