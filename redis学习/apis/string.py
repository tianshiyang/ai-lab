import json

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
    return notice


def exercise_mget_products(product_ids: list[str]) -> list[str | None]:
    """批量读取三个商品的缓存"""
    keys = [f"product:detail:{product_id}" for product_id in product_ids]
    return r.mget(keys)


def exercise_delete_product_cache(product_id: str) -> bool:
    """删除某个商品详情缓存"""
    return r.delete(f"product:{product_id}") == 1


def exercise_session_exists(session_id: str) -> bool:
    """判断一个 Session 是否还有效"""
    return r.exists(f"session:{session_id}") == 1


def exercise_refresh_cart_expire(user_id: str) -> bool:
    """把指定用户的购物车续期 30 天。"""
    return r.expire(f"cart:{user_id}", 30 * 24 * 60 * 60)


def exercise_read_ttl(user_id: str):
    """获取过期时间"""
    seconds = r.ttl(f"cart:{user_id}")
    if seconds == -2:
        print("不存在")
    elif seconds == -1:
        print("未设置过期时间")
    else:
        print(f"还剩{seconds}秒")


def exercise_claim_invite_code(invite_id: str) -> str | None:
    """领取一个只能使用一次的邀请码"""
    value = r.getdel(f"invite:once:{invite_id}")
    print(f"值{value}")
    return value


def exercise_incr_article_views(article_id: int) -> int:
    """记录一篇文章被阅读一次"""
    return r.incr(f"article:views:{article_id}")


def exercise_incrby_article_count(article_id: int) -> int:
    """记录一篇文章被阅读一次"""
    return r.incrby(f"article:views:{article_id}", 10)


def practice_product_cache(product_id: str, product: dict) -> dict:
    """综合"""
    key = f"product:detail:{product_id}"
    cached = r.get(key)
    if r.get(key) is not None:
        print(f"json.loads(cached): {json.loads(cached)}")
        return json.loads(cached)
    r.set(key, json.dumps(product), ex=30 * 60)
    return product


if __name__ == "__main__":
    # exercise_set_notice("通知")

    # exercise_set_verify_code(phone="1718421515", code="123123")

    # exercise_try_order_lock("123")
    # exercise_try_order_lock("123")

    # exercise_get_notice()

    # exercise_mget_products(["product-1", "product-2", "product-3"])

    # exercise_delete_product_cache("product-1")

    # exercise_session_exists("session-1")

    # exercise_refresh_cart_expire("user-2")

    # exercise_read_ttl("user-2")

    # exercise_claim_invite_code("invite-1")

    # exercise_incr_article_views(1)

    # exercise_incrby_article_count(1)

    practice_product_cache("product-1", {"product_id": 1, "product_name": "商品"})
