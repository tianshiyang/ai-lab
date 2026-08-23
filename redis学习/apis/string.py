from redis学习.config import redis_client as r


def exercise_set_notice(content: str) -> bool:
    """保存一条网站公告"""
    return r.set("site:notice", content)


def exercise_set_verify_code(phone: str, code: str) -> bool:
    """五分钟后失效的验证码"""
    return r.set(f"verify:code:{phone}", code, ex=5 * 60)


def exercise_try_order_lock(order_id: str) -> bool:
    """订单号尝试创建一把 10 秒锁"""
    result = r.set(f"lock:order:{order_id}", "practice-worker", nx=True, ex=10)
    print(f"抢到锁的结果：{result}")
    return result


def exercise_get_notice() -> str:
    """读取网站公告"""
    notice = r.get("site:notice")
    print(notice)
    return notice


def exercise_mget_products(product_ids: list[str]) -> list[str | None]:
    """批量读取三个商品的缓存"""
    keys = [f"product:detail:{product_id}" for product_id in product_ids]
    print(r.mget(keys))
    return r.mget(keys)


if __name__ == "__main__":
    # exercise_set_notice("通知")

    # exercise_set_verify_code(phone="1718421515", code="123123")

    # exercise_try_order_lock("123")
    # exercise_try_order_lock("123")

    # exercise_get_notice()

    exercise_mget_products(["product-1", "product-2", "product-3"])