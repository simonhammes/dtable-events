from dtable_events.app.event_redis import RedisClient


class BaseCacheClient:

    def set(self, key, value, timeout=None):
        raise NotImplementedError('.set() must be overridden')

    def get(self, key):
        raise NotImplementedError('.get() must be overridden')

    def delete(self, key):
        raise NotImplementedError('.delete() must be overridden')


class RedisCacheClient(BaseCacheClient):

    def __init__(self):
        self._redis_client = None

    def _init_redis(self, config):
        self._redis_client = RedisClient(config)

    def set(self, key, value, timeout=None):
        return self._redis_client.set(key, value, timeout=timeout)

    def get(self, key):
        return self._redis_client.get(key)

    def delete(self, key):
        return self._redis_client.delete(key)
