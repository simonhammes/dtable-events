import logging
from http.server import HTTPServer
from threading import Thread

from dtable_events.dtable_io.request_handler import DTableIORequestHandler
from dtable_events.dtable_io.task_manager import task_manager

class DTableIOServer(Thread):

    def __init__(self, config, dtable_server_config):
        Thread.__init__(self)
        self._parse_config(config, dtable_server_config)
        task_manager.init(
            self._workers, self._dtable_private_key, self._dtable_web_service_url, self._file_server_port,
            self._io_task_timeout, config
        )
        self._server= HTTPServer((self._host, int(self._port)), DTableIORequestHandler)

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

        self._dtable_private_key = dtable_server_config['private_key']
        try:
            self._dtable_web_service_url = dtable_server_config['dtable_web_service_url']
        except KeyError:
            self._dtable_web_service_url = "http://127.0.0.1:8000"

    def run(self):
        while 1:
            try:
                self._server.serve_forever()
            except Exception as e:
                logging.error(e)
                self._server.server_close()
                self._server = HTTPServer((self._host, int(self._port)), DTableIORequestHandler)
