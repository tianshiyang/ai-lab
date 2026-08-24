from redis学习.config import redis_client as r


def exercise_zadd_rank(user_id: int, score: int) -> int:
    """用户的初始积分放进周榜"""
    return r.zadd("rank:weekly:2026-W34", {str(user_id): score})


def exercise_zincrby_points(user_id: int, points: int) -> float:
    """用户完成任务后加积分"""
    return r.zincrby("rank:weekly:2026-W34", points, str(user_id))


def exercise_zrevrange_top_ten():
    """获取周榜前十名和积分。"""
    return r.zrevrange("rank:weekly:2026-W34", 0, 9, withscores=True)


def exercise_zrevrank_display(user_id: int) -> int | None:
    """获取某成员名次"""
    rank = r.zrevrank("rank:weekly:2026-W34", str(user_id))
    return rank + 1 if rank is not None else None


def exercise_zrange_due_orders() -> list[str]:
    """按分数范围读取成员"""
    import time

    return r.zrangebyscore("rank:weekly:2026-W34", 0, time.time(), 0, 10)


def exercise_zrem_due_order(user_id: int):
    """删除人员"""
    return r.zrem("rank:weekly:2026-W34", str(user_id))


def practice_add_score_and_rank(user_id: int, points: int) -> dict:
    """积分排行榜"""
    key = "rank:weekly:2026-W34"
    score = r.zincrby(key, points, str(user_id))
    rank = r.zrevrank(key, str(user_id))
    top_ten = r.zrevrange(key, 0, 9, withscores=True)
    return {
        "score": score,
        "rank": rank + 1 if rank is not None else None,
        "top_ten": top_ten,
    }


if __name__ == "__main__":
    # print(exercise_zadd_rank(3, 7))
    # print(exercise_zincrby_points(3, 7))

    # print(exercise_zrevrange_top_ten())

    # print(exercise_zrevrank_display(1))

    # print(exercise_zrange_due_orders())

    # print(exercise_zrem_due_order(1))

    print(practice_add_score_and_rank(1, 10))
