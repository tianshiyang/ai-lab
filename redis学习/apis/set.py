from redis学习.config import redis_client as r


def exercise_sadd_like(article_id: int, user_id: int):
    """为文章添加一个点赞，并返回是否是第一次点赞"""
    added_count = r.sadd(f"article:{article_id}", str(user_id))
    r.hincrby("article", article_id, added_count)
    return added_count


def exercise_sismember_activity(article_id: int, user_id: int):
    """判断用户是否已经报名活动"""
    return r.sismember(f"article:{article_id}", str(user_id))


def exercise_srem_like(article_id: int, user_id: int):
    """取消文章点赞并返回是否成功"""
    return r.srem(f"article:{article_id}", str(user_id))


def exercise_scard_likes(article_id: int):
    """获取指定文章的点赞数"""
    return r.scard(f"article:{article_id}")


def exercise_smembers_activity(article_id: int):
    """读取一场小型活动的报名用户"""
    return r.smembers(f"article:{article_id}")


def practice_like_article(article_id: int, user_id: int) -> dict[str, int | bool]:
    """文章点赞"""
    key = f"article:like:{article_id}"
    added_count = r.sadd(key, str(user_id))
    like_count = r.scard(key)
    r.expire(key, 10)
    return {"is_new_like": added_count == 1, "like_count": like_count}


if __name__ == "__main__":
    # print(exercise_sadd_like(1, 2))

    # print(exercise_sismember_activity(1, 10))

    # print(exercise_srem_like(1, 7))

    # print(exercise_scard_likes(1))

    # print(exercise_smembers_activity(1))

    print(practice_like_article(10, 10))
