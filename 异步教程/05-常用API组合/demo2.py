import asyncio


async def demo_event() -> None:
    ready = asyncio.Event()

    async def waiter(name: str) -> None:
        await ready.wait()
        print(f"{name}被唤醒")

    tasks = [asyncio.create_task(waiter("A")), asyncio.create_task(waiter("B"))]
    await asyncio.sleep(0)
    ready.set()
    await asyncio.gather(*tasks)


async def main() -> None:
    await demo_event()


if __name__ == "__main__":
    asyncio.run(main())
