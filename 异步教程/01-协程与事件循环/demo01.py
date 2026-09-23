import asyncio


async def demo_await_returns_value() -> None:
    async def get_value() -> str:
        await asyncio.sleep(0.1)
        return "拿到结果了"

    result = await get_value()
    print(result)


if __name__ == "__main__":
    asyncio.run(demo_await_returns_value())
