"""
Cache moudle
So far, there is redis cache supported
Usage:

    from dtable_events.cache import redis_cache

"""


from dtable_events.app import config
from dtable_events.app.event_redis import RedisClient
from dtable_events.cache.clients import RedisCacheClient


redis_client, _redis_cache = None, None

def _init_redis():
    global redis_client, _redis_cache
    if not redis_client and config.global_config:
        redis_client = RedisClient(config.global_config)
        _redis_cache = RedisCacheClient(redis_client)

class RedisCacheProxy(object):

    def __getattr__(self, name):
        _init_redis()
        return getattr(_redis_cache, name)

    def __setattr__(self, name, value):
        _init_redis()
        return setattr(_redis_cache, name, value)

    def __delattr__(self, name):
        _init_redis()
        return delattr(_redis_cache, name)


redis_cache = RedisCacheProxy()
