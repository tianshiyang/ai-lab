import os

import redis

redis_client = redis.Redis.from_url(url=os.getenv("REDIS_URL", ""), decode_responses=True)

if __name__ == "__main__":
    print(redis_client.ping())