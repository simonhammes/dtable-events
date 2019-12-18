# -*- coding: utf-8 -*-
import redis


class EventRedis(object):
    def __init__(self):
        self.redis_connection = None
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None

    def _parse_config(self, config):
        if config.has_option('REDIS', 'host'):
            self._host = config.get('REDIS', 'host')

        if config.has_option('REDIS', 'port'):
            self._port = config.getint('REDIS', 'port')

        if config.has_option('REDIS', 'password'):
            self._password = config.get('REDIS', 'password')

    def get_connection(self, config):
        self._parse_config(config)

        redis_pool = redis.ConnectionPool(
            host=self._host, port=self._port, password=self._password
        )

        self.redis_connection = redis.Redis(connection_pool=redis_pool)

        return self.redis_connection


event_redis = EventRedis()
