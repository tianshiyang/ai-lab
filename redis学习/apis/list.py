from redis学习.config import redis_client as r


def init_list():
    """初始化"""
    arr = [f"keyword_{i}" for i in range(10)]
    return r.lpush(f"search:history:1", *arr)


def exercise_lpush_search_history(user_id: str, keyword) -> int:
    """向用户最近搜索记录中放入一个关键词"""
    return r.lpush(f"search:history:{user_id}", keyword)


def exercise_rpop_email_task(user_id: str) -> str:
    """从邮件队列中取一个任务。"""
    return r.rpop(f"search:history:{user_id}")


def exercise_lpop_undo(user_id: str) -> str:
    """取出最近的一条撤销操作"""
    return r.lpop(f"search:history:{user_id}")


def exercise_lrange_search_history(user_id: str) -> list[int]:
    """读取用户最近 10 条搜索记录"""
    return r.lrange(f"search:history:{user_id}", 0, 4)


def exercise_lrem_search_history(user_id: str, keyword: str) -> int:
    """从搜索记录中清除指定关键词的所有旧记录"""
    return r.lrem(f"search:history:{user_id}", 0, keyword)


def exercise_ltrim_search_history(user_id: str) -> int:
    """把用户搜索记录限制为最近 10 条"""
    return r.ltrim(f"search:history:{user_id}", 0, 2)


def exercise_brpop_email_task(user_id: str) -> tuple[str, str]:
    """等待最多 2 秒，取一条邮件任务"""
    return r.brpop(f"search:history:{user_id}", timeout=2)


def practice_add_browse_history(user_id: int, product_id: int) -> list[str]:
    """同一商品只保留一条、最新浏览放最前面、最多保存 20 条、30 天过期"""
    key = f"history:{user_id}"
    value = str(product_id)

    with r.pipeline(transaction=True) as pipe:
        pipe.lrem(key, 0, value)
        pipe.lpush(key, value)
        pipe.ltrim(key, 0, 19)
        pipe.expire(key, 30 * 24 * 60 * 60)
        pipe.lrange(key, 0, 19)
        results = pipe.execute()

    return results[-1]


if __name__ == "__main__":
    # init_list()

    # exercise_lpush_search_history("1", "keyword_1")

    # print(exercise_rpop_email_task("1"))

    # print(exercise_lpop_undo("1"))

    # print(exercise_lrange_search_history("1"))

    # print(exercise_lrem_search_history("1", "keyword_4"))

    # print(exercise_ltrim_search_history("1"))
    
    print(exercise_brpop_email_task("1"))