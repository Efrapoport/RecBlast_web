import redis

REDIS_HOST = 'localhost'

pool = redis.ConnectionPool(host=REDIS_HOST)
redis_pool = redis.Redis(connection_pool=pool)
