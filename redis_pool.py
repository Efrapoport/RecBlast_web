import redis

REDIS_HOST = 'redis.internal.reciprocalblast.com'

pool = redis.ConnectionPool(host=REDIS_HOST)
redis_pool = redis.Redis(connection_pool=pool)
