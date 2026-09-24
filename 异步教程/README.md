# Python 异步：自己动手的 API 实验册

这里没有配套 `.py` 文件。每个案例都是讲义里的一个完整代码块：复制**一个块**到你自己的文件中运行即可。案例之间不共享变量、不依赖上一个案例的执行结果。

统一的阅读方式：

1. 复制一个代码块，运行其中唯一的 `demo_xxx()`；
2. 读输出，再改一个参数自己观察；
3. 想在同一个文件连续体验时，复制多个 `demo_xxx` 函数，然后在自己的 `main()` 中分别 `await` 它们；
4. 不理解时回到该代码块旁的说明。

建议 Python 3.11+；所有代码仅依赖标准库。

| 章节 | 体验的 API |
| --- | --- |
| [01 协程与事件循环](01-协程与事件循环/讲义.md) | `async def`、`await`、`asyncio.run`、`sleep` |
| [02 任务与结果收集](02-任务与结果收集/讲义.md) | `create_task`、`gather`、`as_completed` |
| [03 控制并发与取消](03-控制并发与取消/讲义.md) | `Semaphore`、`timeout`、`wait_for`、`TaskGroup` |
| [04 线程、进程与阻塞代码](04-线程、进程与阻塞代码/讲义.md) | `to_thread`、线程池、进程池 |
| [05 常用 API 组合](05-常用API组合/讲义.md) | `Queue`、`Event`、`Lock`、`wait` |
| [综合实战](综合实战/README.md) | 九道独立练习：从并发启动到进程池 |

一句总原则：**等待 I/O 时让出事件循环；同步 I/O 用线程隔离；CPU 密集计算交给进程；创建的 Task 必须有人负责等待、取消和处理异常。**

## 学习优先级标记

讲义中的 API 都会带上以下标记：

| 标记 | 含义 | 你的学习目标 |
| --- | --- | --- |
| **⭐⭐⭐ 企业常用** | 在异步 Web 服务、爬虫、批处理、SDK 调用中经常直接写到 | 能独立写出、解释异常与取消语义 |
| **⭐⭐ 条件常用** | 常见，但只在特定架构或任务形态出现 | 知道何时该选它，何时不该选 |
| **⭐ 补充了解** | 老项目兼容、特定场景或底层协调工具 | 看懂即可；遇到场景再回来看 |

最先掌握这一组：`async def` / `await`、`asyncio.run`、`create_task`、`gather`、`Semaphore`、`timeout`、`TaskGroup`、`to_thread`。它们覆盖了绝大多数现代 Python 异步服务的日常代码。

## 一张表分清总耗时、返回值与失败后果

先固定同一个实验，下面所有 API 都拿它来比较：三个任务在 `0s` 一起开始，A 在 `1s` 返回 `"A"`，B 在 `2s` 抛出 `ValueError`，C 在 `3s` 返回 `"C"`。表中假设调用方处理了异常，事件循环会继续运行。

| 写法 | 当前这句何时结束 | 当前这句得到什么 | B 在 2s 失败后，C 怎么办 | 适用场景 |
| --- | --- | --- | --- | --- |
| `await task_a` | 1s | `"A"` | 不影响 C | 只等一个 Task |
| `await gather(A, B, C)` | 2s | 直接抛 `ValueError` | **继续，到 3s 成功** | 独立任务；全部成功才要结果 |
| `await gather(..., return_exceptions=True)` | 3s | `["A", ValueError(...), "C"]` | 继续 | 允许部分失败，自己逐项处理 |
| `async with TaskGroup()` | 约 2s + 取消清理 | 不直接返回结果；退出时抛 `ExceptionGroup` | **取消 C** | 子任务必须同生共死 |
| `as_completed(...)` 并逐项捕获异常 | 1s、2s、3s 分别交付 | 依次得到 `"A"`、B 的异常、`"C"` | 继续 | 谁先完成就先处理谁 |
| `wait(..., FIRST_COMPLETED)` | 1s | `(done={A}, pending={B,C})` | 继续 | 只要第一个结束者，后续自己决定 |
| `wait(..., FIRST_EXCEPTION)` | 2s | `(done={A,B}, pending={C})` | 继续 | 等首次失败，后续自己决定 |
| `wait(..., ALL_COMPLETED)` | 3s | `(done={A,B,C}, pending=set())` | 已结束 | 只想自己检查每个 Task 状态 |

`gather`、`TaskGroup`、完整消费 `as_completed` 在**全部成功**时，整体耗时都接近最慢任务：本例为 `3s`，不是 `1 + 2 + 3 = 6s`。区别只在失败后的策略。

| API / 写法 | 成功时返回什么 | 失败时 |
| --- | --- | --- |
| `await task` | 协程的 `return` 值 | 在这行抛该 Task 的异常 |
| `await gather(...)` | 按输入顺序的结果列表 | 默认在这行抛第一个异常 |
| `await completed`（来自 `as_completed`） | 下一个完成 Task 的结果 | 在这一次 `await` 抛该 Task 的异常 |
| `await wait(...)` | `(done, pending)` | 不替你抛子任务异常；读 `task.result()` 才会抛 |
| `async with TaskGroup()` | **没有总结果列表**；从各个 Task 取 `.result()` | 普通异常时退出块并抛 `ExceptionGroup` |

`TaskGroup` 不能写成 `await asyncio.TaskGroup()`；它是 `async with` 管理的一组 Task。一个子任务提前成功，不会让组提前成功；只有其他任务也正常完成，才能正常离开代码块。

选择时只问一句：**失败后其他任务还要不要继续？** 要继续，用 `gather` / `as_completed` / `wait`；不要继续、要一起收尾，用 `TaskGroup`。

## 先抓住一条执行主线

代码中的 `async def main()` 不是特殊魔法，只是一个协程函数。最下方的 `asyncio.run(main())` 才负责创建事件循环、运行 `main`、等待它结束后关闭循环。

在事件循环的概念模型中，反复发生的是下面四步：

```text
1. 取出“现在可以继续”的 Task / 回调
2. 让它一直运行，直到 return、抛异常，或遇到 await
3. 若 await 的对象尚未完成：把该 Task 挂起，登记它的唤醒条件
4. 处理计时器和 I/O 就绪事件；条件满足时，把对应 Task 放回就绪队列
```

`await` 不是“另起线程”，也不保证立刻切换到某个特定协程；它只是给事件循环一次调度其他就绪工作的机会。事件循环通常运行在一个线程中，所以每个时刻只有一个 Python 协程片段在执行。

如果以后你把多个 demo 放到同一个文件，请在自己的 `main()` 中逐个 `await`：

```python
async def main() -> None:
    await demo_one()
    await demo_two()
```

这表示“第一个 demo 彻底结束，再开始第二个”。要让两个 demo 同时推进，则需要第 02 章的 `gather` 或 `create_task`。
