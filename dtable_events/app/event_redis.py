# -*- coding: utf-8 -*-
import time
import logging
import redis

logger = logging.getLogger(__name__)

class RedisClient(object):

    def __init__(self, config):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self._socket_timeout = 20
        self._socket_connect_timeout = 20

        self.get_msg_timeout = 20
        self._parse_config(config)

        """
        By default, each Redis instance created will in turn create its own connection pool.
        Every caller using redis client will has it's own pool with config caller passed.
        """
        self.connection = redis.Redis(
            host=self._host, port=self._port, password=self._password,
            socket_timeout=self._socket_timeout, socket_connect_timeout=self._socket_connect_timeout
            )


    def _parse_config(self, config):
        if config.has_option('REDIS', 'host'):
            self._host = config.get('REDIS', 'host')

        if config.has_option('REDIS', 'port'):
            self._port = config.getint('REDIS', 'port')

        if config.has_option('REDIS', 'password'):
            self._password = config.get('REDIS', 'password')

        if config.has_option('REDIS', 'socket_timeout'):
            self._socket_timeout = config.get('REDIS', 'socket_timeout')

        if config.has_option('REDIS', 'socket_connect_timeout'):
            self._socket_connect_timeout = config.get('REDIS', 'socket_connect_timeout')

        if config.has_option('REDIS', 'get_msg_timeout'):
            self.get_msg_timeout = config.get('REDIS', 'get_msg_timeout')


    def get_subscriber(self, channel_name):
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber
