"""
Cache moudle
So far, there is redis cache supported
Usage:

    from dtable_events.cache import redis_cache

"""


from dtable_events.cache.clients import RedisCacheClient


redis_cache = RedisCacheClient()
