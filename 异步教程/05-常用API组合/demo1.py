import asyncio


async def demo_queue() -> None:
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=2)

    async def producer() -> None:
        for item in range(3):
            await queue.put(item)
            print(f"放入 {item}")
        await queue.put(None)  # 本 demo 用 None 表示停止

    async def consumer() -> None:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    return
                print(f"取出 {item}")
            finally:
                queue.task_done()

    producer_task = asyncio.create_task(producer())
    consumer_task = asyncio.create_task(consumer())
    await producer_task
    # await queue.join()
    await consumer_task


async def main() -> None:
    await demo_queue()


if __name__ == "__main__":
    asyncio.run(main())