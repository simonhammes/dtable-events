import time
import os
import queue
import threading
import logging


from seaserv import seafile_api


class TaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, io_task_timeout, config):
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'io_task_timeout': io_task_timeout,
            'workers': workers,
        }
        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()

    def add_export_task(self, username, repo_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import get_dtable_export_content

        dtable_file_id = seafile_api.get_file_id_by_path(repo_id, '/' + dtable_name + '.dtable')
        asset_dir_path = os.path.join('/asset', dtable_uuid)
        asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)

        task_id = str(int(time.time()*1000))
        task = (get_dtable_export_content,
                (username, repo_id, dtable_name, dtable_uuid, dtable_file_id, asset_dir_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name):
        from dtable_events.dtable_io import post_dtable_import_files

        task_id = str(int(time.time()*1000))
        task = (post_dtable_import_files,
                (username, repo_id, workspace_id, dtable_uuid, dtable_file_name, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_export_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files):
        from dtable_events.dtable_io import get_dtable_export_asset_files

        task_id = str(int(time.time()*1000))
        task = (get_dtable_export_asset_files,
                (username, repo_id, dtable_uuid, files, task_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def query_status(self, task_id):
        task = self.tasks_map[task_id]
        if task == 'success':
            self.tasks_map.pop(task_id, None)
            return True
        return False

    def handle_task(self):
        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            else:
                task = self.tasks_map[task_id]
                try:
                    task[0](*task[1])
                    self.tasks_map[task_id] = 'success'
                except Exception as e:
                    logging.error('Failed to handle task %s, error: %s' % (task_id, e))
                    self.tasks_map.pop(task_id, None)

    def run(self):
        t = threading.Thread(target=self.handle_task)
        t.setDaemon(True)
        t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


task_manager = TaskManager()
