import time
from contextlib import contextmanager


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


class Timer:
    """类版:__enter__ 进门,__exit__ 出门,中间出事也照样出门。"""

    def __enter__(self):
        self.start = time.monotonic()
        log("进门：开始计时")
        return self

    def __exit__(self, exc_type, exc, tb):
        cost = time.monotonic() - self.start
        log(f"出门:耗时 {cost:.2f} 秒(中途{'出了事:' + str(exc) if exc else '平安'})")
        return False


@contextmanager
def db_connection(name: str):
    """装饰器版:yield 前是进门,yield 后是出门——两段式拍平成一个函数。"""
    log(f"连接 {name} 建立了")
    try:
        yield name
    finally:
        log(f"连接 {name} 关闭了(finally 保证)")


@contextmanager
def swallow_all():
    """反面教材:__exit__ 返回 True = 异常就地吞掉,外面毫无感知。"""
    try:
        yield
    except Exception as e:  # noqa: BLE001 —— 故意裸抓,这是反面教材
        log(f"我把 {e} 吞了,外面啥也不知道")
        return True  # contextmanager 里 return True 等价于 __exit__ 返回 True


if __name__ == "__main__":
    # log("=== 第一幕:Timer 类版,平安出门 ===")
    # with Timer():
    #     time.sleep(0.5)
    #
    # log("=== 第二幕:中途抛异常,门还是出了,异常照常往上抛 ===")
    # try:
    #     with Timer():
    #         time.sleep(0.2)
    #         raise RuntimeError("业务炸了")
    # except RuntimeError as e:
    #     log(f"外面接到了:{e}")
    log("=== 第三幕:contextmanager 装饰器版,异常也关连接 ===")
    try:
        with db_connection("订单库") as conn:
            log(f"拿到连接 {conn},干活")
            raise RuntimeError("查询失败")
    except RuntimeError as e:
        log(f"外面接到了:{e}")

    log("=== 第四幕:吞异常的反面教材,外面毫无感知 ===")
    with swallow_all():
        raise RuntimeError("这个异常消失了")
    log("程序若无其事地走到了这里——bug 从此无迹可寻")
