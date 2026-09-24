import asyncio


async def demo_lock() -> None:
    lock = asyncio.Lock()

    state = {"count": 0}

    async def increment() -> None:
        async with lock:
            old_count = state["count"]
            await asyncio.sleep(1)
            state["count"] = old_count + 1

    await asyncio.gather(*(increment() for _ in range(3)))

    print(state["count"])


async def main() -> None:
    await demo_lock()


if __name__ == "__main__":
    asyncio.run(main())
