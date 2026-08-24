from redis学习.config import redis_client as r


def exercise_hset_profile(user_id: int, name: str, city: str) -> int:
    """保存用户昵称和所在城市"""
    return r.hset(f"user:profile:{user_id}", mapping={"name": name, "city": city})


def exercise_hget_name(user_id: int, name: str) -> int:
    """读取用户昵称"""
    return r.hget(f"user:profile:{user_id}", name)


def exercise_hgetall_profile(user_id: int) -> dict:
    """读取用户全部信息"""
    return r.hgetall(f"user:profile:{user_id}")


def exercise_hincrby_cart(user_id: int, sku_id: str) -> dict:
    """给购物车里的某个 SKU 加一件"""
    return r.hincrby(f"cart:{user_id}", f"sku:{sku_id}", 1)


def exercise_hdel_cart_sku(user_id: int, sku_id: str) -> dict:
    """从购物车删除一个 SKU"""
    return r.hdel(f"cart:{user_id}", f"sku:{sku_id}")


def practice_add_to_cart(user_id: int, sku_id: int, quantity: int = 1):
    """购物车"""
    key = f"cart:{user_id}"
    r.hincrby(key, f"sku:{sku_id}", quantity)
    r.expire(key, 30)
    return r.hgetall(key)


if __name__ == "__main__":
    # print(exercise_hset_profile(user_id=1, name="张三", city="北京"))
    # print(exercise_hget_name(user_id=1, name="city"))
    # print(exercise_hgetall_profile(user_id=1))
    # exercise_hincrby_cart(user_id=1, sku_id="1")
    # exercise_hdel_cart_sku(user_id=1, sku_id="1")
    print(practice_add_to_cart(user_id=3, sku_id=3))
