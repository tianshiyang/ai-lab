import asyncio


async def main():
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)

    async def producer():
        for i in range(3):
            await queue.put(i)
            print(f"放入{i}")
        await queue.put(None)

    async def consumer():
        while True:
            data = await queue.get()
            try:
                if data is None:
                    return
                print(f"数据：{data}")
            finally:
                queue.task_done()

    p = asyncio.create_task(producer())
    c = asyncio.create_task(consumer())
    await p
    await queue.join()
    await c


if __name__ == "__main__":
    asyncio.run(main())
