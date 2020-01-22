# -*- coding: utf-8 -*-
from dtable_events.activities.handlers import MessageHandler
from dtable_events.statistics.counter import UserActivityCounter
from dtable_events.tasks.work_weixin_notices_sender import WorkWinxinNoticeSender


class App(object):
    def __init__(self, config):
        self._message_handler = MessageHandler(config)
        self._user_activity_counter = UserActivityCounter(config)
        self._work_weixin_notices_sender = WorkWinxinNoticeSender(config)

    def serve_forever(self):
        self._message_handler.start()
        self._user_activity_counter.start()
        self._work_weixin_notices_sender.start()
