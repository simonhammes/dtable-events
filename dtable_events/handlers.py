# -*- coding: utf-8 -*-
import logging
import time
import json
from threading import Thread, Event

from dtable_events.event_redis import event_redis
from dtable_events.utils import save_user_activities
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def _redis_connection(config):
    while True:
        try:
            connection = event_redis.get_connection(config)
            connection.ping()
        except Exception as e:
            logger.error('redis error: %s, reconnecting', e)
            time.sleep(5)
        else:
            return connection


class MessageHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_connection = _redis_connection(config)
        self._subscriber = self._redis_connection.pubsub(ignore_subscribe_messages=True)
        self._subscriber.subscribe('table-events')

    def run(self):
        logger.info('Starting handle message...')
        while not self._finished.is_set():
            try:
                message = self._subscriber.get_message()
                if message is not None:
                    event = message['data']
                    self._redis_connection.rpush('table_event_queue', event)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)


class EventHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_connection = _redis_connection(config)
        self._db_session_class = init_db_session_class(config)

    def run(self):
        logger.info('Starting handle event...')
        while not self._finished.is_set():
            try:
                event_tuple = self._redis_connection.blpop('table_event_queue', 1)
                if event_tuple is not None:
                    key, value = event_tuple
                    event = json.loads(value)

                    session = self._db_session_class()
                    try:
                        save_user_activities(session, event)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
            except Exception as e:
                logger.error('Failed handle message: %s' % e)
