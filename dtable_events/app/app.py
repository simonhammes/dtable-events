# -*- coding: utf-8 -*-
from dtable_events.activities.handlers import MessageHandler


class App(object):
    def __init__(self, config):
        self._message_handler = MessageHandler(config)

    def serve_forever(self):
        self._message_handler.start()
