import os

import redis
from dotenv import load_dotenv

load_dotenv()

redis_client = redis.Redis.from_url(url=os.getenv("REDIS_URL", ""), decode_responses=True)

if __name__ == "__main__":
    print(redis_client.ping())