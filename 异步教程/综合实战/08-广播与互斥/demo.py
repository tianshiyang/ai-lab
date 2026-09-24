import asyncio


async def main():
    data = {"num": 0}
    lock = asyncio.Lock()

    async def task():
        async with lock:
            num = data["num"]
            await asyncio.sleep(0.1)
            data["num"] = num + 1

    await asyncio.gather(*(task() for i in range(10)))

    print(data["num"])


if __name__ == "__main__":
    asyncio.run(main())
