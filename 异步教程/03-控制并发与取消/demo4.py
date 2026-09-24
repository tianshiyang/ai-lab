import asyncio


async def demo_task_group() -> None:
    async def long_running(name: str) -> None:
        try:
            await asyncio.sleep(1)
            print("执行成功")
        except asyncio.CancelledError:
            print(f"{name}被取消")
            raise

    async def failing() -> None:
        await asyncio.sleep(2)
        raise ValueError("故意失败")

    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(long_running("A"))
            group.create_task(long_running("B"))
            group.create_task(failing())

    except* ValueError as errors:
        print(errors.exceptions[0])

async def main() -> None:
    await demo_task_group()


if __name__ == "__main__":
    asyncio.run(main())
