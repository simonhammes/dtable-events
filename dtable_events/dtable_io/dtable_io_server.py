import time
import logging
import os
import sys
from http.server import HTTPServer
from threading import Thread

from dtable_events.dtable_io.request_handler import DTableIORequestHandler
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager


class DTableIOServer(Thread):

    def __init__(self, config, dtable_server_config):
        Thread.__init__(self)
        self._parse_config(config, dtable_server_config)
        task_manager.init(
            self._workers, self._dtable_private_key, self._dtable_web_service_url,
            self._file_server_port, self._dtable_server_url,
            self._io_task_timeout, config
        )
        message_task_manager.init(
            self._workers, self._dtable_private_key, self._dtable_web_service_url,
            self._file_server_port, self._dtable_server_url,
            self._io_task_timeout, config
        )
        task_manager.run()
        message_task_manager.run()
        self._server = HTTPServer((self._host, int(self._port)), DTableIORequestHandler)

    def _parse_config(self, config, dtable_server_config):
        if config.has_option('DTABLE-IO', 'host'):
            self._host = config.get('DTABLE-IO', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('DTABLE-IO', 'port'):
            self._port = config.getint('DTABLE-IO', 'port')
        else:
            self._port = '6000'

        if config.has_option('DTABLE-IO', 'workers'):
            self._workers = config.getint('DTABLE-IO', 'workers')
        else:
            self._workers = 3

        if config.has_option('DTABLE-IO', 'io_task_timeout'):
            self._io_task_timeout = config.getint('DTABLE-IO', 'io_task_timeout')
        else:
            self._io_task_timeout = 3600

        if config.has_option('DTABLE-IO', 'file_server_port'):
            self._file_server_port = config.getint('DTABLE-IO', 'file_server_port')
        else:
            self._file_server_port = 8082

        central_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', '')
        self._dtable_web_service_url = "http://127.0.0.1:8000"
        self._dtable_server_url = "http://127.0.0.1:5000"
        if central_conf_dir:
            try:
                if os.path.exists(central_conf_dir):
                    sys.path.insert(0, central_conf_dir)
                    from dtable_web_settings import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
                    self._dtable_web_service_url = DTABLE_WEB_SERVICE_URL
                    self._dtable_private_key = DTABLE_PRIVATE_KEY
                    self._dtable_server_url = DTABLE_SERVER_URL
            except ImportError:
                dtable_web_seahub_dir = os.path.join(os.environ.get('DTABLE_WEB_DIR', ''), 'seahub')
                if os.path.exists(dtable_web_seahub_dir):
                    sys.path.insert(0, dtable_web_seahub_dir)
                    from local_settings import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
                    self._dtable_web_service_url = DTABLE_WEB_SERVICE_URL
                    self._dtable_private_key = DTABLE_PRIVATE_KEY 
                    self._dtable_server_url = DTABLE_SERVER_URL
            except Exception as e:
                logging.error(f'import settings from SEAFILE_CENTRAL_CONF_DIR/dtable_web_settings.py failed {e}')

    def run(self):
        while 1:
            try:
                self._server.serve_forever()
            except Exception as e:
                logging.error(e)
                time.sleep(5)
                self._server.server_close()
                self._server = HTTPServer((self._host, int(self._port)), DTableIORequestHandler)
