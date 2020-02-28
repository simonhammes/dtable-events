from http.server import HTTPServer

from dtable_events.dtable_io.request_handler import DTableIORequestHandler
from dtable_events.dtable_io.TaskManager import task_manager


class DTableIOServer(object):

    def __init__(self, config):
        self._parse_config(config)
        self._server= HTTPServer((self._host, int(self._port)), DTableIORequestHandler)
        task_manager.init(
            workers=self._workers
        )


    def _parse_config(self, config):
        if config.has_option('DTABLE-IO', 'host'):
            self._host = config.get('DTABLE-IO', 'host')

        if config.has_option('DTABLE-IO', 'port'):
            self._port = config.getint('DTABLE-IO', 'port')

        if config.has_option('DTABLE-IO', 'workers'):
            self._workers = config.getint('DTABLE-IO', 'workers')
        else:
            self._workers = 3


    def start(self):
        self._server.serve_forever()


