# -*- coding: utf-8 -*-
from dtable_events.activities.handlers import MessageHandler, NotificationRuleHandler
from dtable_events.statistics.counter import UserActivityCounter
from dtable_events.dtable_io.dtable_io_server import DTableIOServer
from dtable_events.tasks.instant_notices_sender import InstantNoticeSender
from dtable_events.tasks.email_notices_sender import EmailNoticesSender
from dtable_events.tasks.dtables_cleaner import DTablesCleaner
from dtable_events.tasks.dtable_updates_sender import DTableUpdatesSender
from dtable_events.tasks.dtable_notification_rules_scanner import DTableNofiticationRulesScanner
from dtable_events.tasks.dtable_real_time_rows_counter import DTableRealTimeRowsCounter
from dtable_events.tasks.ldap_syncer import LDAPSyncer
from dtable_events.webhook.webhook import Webhooker


class App(object):
    def __init__(self, config, dtable_server_config, task_mode):
        self._enable_foreground_tasks = task_mode.enable_foreground_tasks
        self._enable_background_tasks = task_mode.enable_background_tasks

        if self._enable_foreground_tasks:
            self._dtable_io_server = DTableIOServer(config, dtable_server_config)

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler = MessageHandler(config)
            self._notification_rule_handler = NotificationRuleHandler(config)
            self._user_activity_counter = UserActivityCounter(config)
            self._dtable_real_time_rows_counter = DTableRealTimeRowsCounter(config)
            self._webhooker = Webhooker(config)
            # cron jobs
            self._instant_notices_sender = InstantNoticeSender(config)
            self._email_notices_sender = EmailNoticesSender(config)
            self._dtables_cleaner = DTablesCleaner(config)
            self._dtable_updates_sender = DTableUpdatesSender(config)
            self._dtable_notification_rules_scanner = DTableNofiticationRulesScanner(config)
            self._ldap_syncer = LDAPSyncer(config)

    def serve_forever(self):
        if self._enable_foreground_tasks:
            self._dtable_io_server.start()

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler.start()                    # always True
            self._notification_rule_handler.start()          # always True
            self._user_activity_counter.start()              # always True
            self._dtable_real_time_rows_counter.start()      # default True
            self._webhooker.start()                          # always True
            # cron jobs
            self._instant_notices_sender.start()             # default True
            self._email_notices_sender.start()               # default True
            self._dtables_cleaner.start()                    # default True
            self._dtable_updates_sender.start()              # default True
            self._dtable_notification_rules_scanner.start()  # default True
            self._ldap_syncer.start()                        # default False
