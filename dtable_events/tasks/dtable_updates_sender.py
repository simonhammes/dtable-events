# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_python_executable, run


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

__all__ = [
    'DTableUpdatesSender',
]


class DTableUpdatesSender(object):

    def __init__(self):
        self._interval = 60
        self._logfile = None

        self._prepare_logfile()

    def _prepare_logfile(self):
        log_dir = os.environ.get('DTABLE_EVENTS_LOG_DIR', '')
        self._logfile = os.path.join(log_dir, 'dtable_updates_sender.log')

    def start(self):
        logging.info('Start dtable updates sender, interval = %s sec', self._interval)
        DTableUpdatesSenderTimer(self._interval, self._logfile).start()


class DTableUpdatesSenderTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'send_dtable_updates',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('send dtable updates email error: %s', e)

    def cancel(self):
        self.finished.set()
