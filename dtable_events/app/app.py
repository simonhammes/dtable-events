# -*- coding: utf-8 -*-
from dtable_events.activities.handlers import MessageHandler
from dtable_events.statistics.counter import UserActivityCounter


class App(object):
    def __init__(self, config):
        self._message_handler = MessageHandler(config)
        self._user_activity_counter = UserActivityCounter(config)

    def serve_forever(self):
        self._message_handler.start()
        self._user_activity_counter.start()
