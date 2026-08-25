import json

from redis学习.config import redis_client as r


def exercise_pipeline_read_home(user_id: int) -> dict:
    """用一个管道同时读取用户资料、是否点赞和榜单前十。"""
    with r.pipeline(transaction=True) as pipe:
        pipe.hgetall(f"user:profile:{user_id}")
        pipe.sismember("article:like:9527", str(user_id))
        pipe.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)
        profile, liked, top_ten = pipe.execute()
    return {"profile": profile, "liked": liked, "top_ten": top_ten}


def practice_batch_get_products(product_ids: list[str]) -> dict[int, dict | None]:
    """一次拿到多个商品缓存，并把命中的 JSON 转成字典。"""
    keys = [f"product:detail:{product_id}" for product_id in product_ids]
    with r.pipeline() as pipe:
        for key in keys:
            pipe.get(key)
        cached_values = pipe.execute()
    return {
        product_id: json.loads(value) if value is not None else None
        for product_id, value in zip(product_ids, cached_values, strict=True)
    }


if __name__ == "__main__":
    exercise_pipeline_read_home(1)