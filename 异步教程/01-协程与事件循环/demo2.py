import asyncio
import threading


async def demo_await_yields_control() -> None:
    async def worker(name: str, delay: float) -> None:
        for index in range(2):
            print(f"{name}: {index}, thread={threading.get_ident()}")
            await asyncio.sleep(delay)

    await asyncio.gather(worker("A", 3), worker("B", 5))


async def main() -> None:
    await demo_await_yields_control()


if __name__ == "__main__":
    asyncio.run(main())
