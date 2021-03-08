import os
import logging
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.activities.notification_rules_utils import check_near_deadline_notification_rule
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, get_python_executable, run


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

__all__ = [
    'DTableNofiticationRulesScanner',
]


class DTableNofiticationRulesScanner(object):

    def __init__(self, config):
        self._enabled = False
        self._logfile = None
        self._timezone = 'UTC'
        self._parse_config(config)
        self._prepare_logfile()
        self._db_session_class = init_db_session_class(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'dtables_notification_rule_scanner.log')

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'NOTIFY-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'NOTIFY SCANNER'
            if not config.has_section(section_name):
                return

        if config.has_section('DTABLE-WEB') and config.has_option('DTABLE-WEB', 'TIME_ZONE'):
            self._timezone = config.get('DTABLE-WEB', 'TIME_ZONE')

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self._enabled = True


    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable notification rules scanner: it is not enabled!')
            return

        logging.info('Start dtable notification rules scanner')

        DTableNofiticationRulesScannerTimer(self._logfile, self._db_session_class, self._timezone).start()

    def is_enabled(self):
        return self._enabled


def scan_dtable_notification_rules(db_session, timezone):
    sql = '''
            SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules
            WHERE (run_condition='per_day' AND last_trigger_time<:per_day_check_time)
            OR (run_condition='per_week' AND last_trigger_time<:per_week_check_time)
            OR last_trigger_time is null
        '''
    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    per_week_check_time = datetime.utcnow() - timedelta(days=6)
    rules = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time
    })

    for rule in rules:
        try:
            check_near_deadline_notification_rule(rule, db_session, timezone)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')
        db_session.commit()


class DTableNofiticationRulesScannerTimer(Thread):

    def __init__(self, logfile, db_session_class, timezone):
        super(DTableNofiticationRulesScannerTimer, self).__init__()
        self._logfile = logfile
        self.db_session_class = db_session_class
        self.timezone = timezone

        # db_session = self.db_session_class()
        # try:
        #     scan_dtable_notification_rules(db_session, self.timezone)
        # except Exception as e:
        #     logging.exception('error when scanning dtable notification rules: %s', e)
        # finally:
        #     db_session.close()

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan notification rules...')

            db_session = self.db_session_class()
            try:
                scan_dtable_notification_rules(db_session, self.timezone)
            except Exception as e:
                logging.exception('error when scanning dtable notification rules: %s', e)
            finally:
                db_session.close()

        sched.start()

