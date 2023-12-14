import redis
    
def redis_clear_all():
    redis_client = redis.StrictRedis(host="redis",port=6379, db = 1)
    redis_client.flushdb()
    print("Redis flushed.")