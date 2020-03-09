import time
import os
from concurrent.futures import ThreadPoolExecutor

from seaserv import seafile_api


class TaskManager:

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, io_task_timeout):
        self._future_pool = {}
        self._executor = ThreadPoolExecutor(max_workers=workers)
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'io_task_timeout': io_task_timeout,
        }

    def is_valid_task_id(self, task_id):
        return task_id in self._future_pool.keys()

    def add_export_task(self, username, repo_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import get_dtable_export_content

        dtable_file_dir_id = seafile_api.get_file_id_by_path(repo_id, '/' + dtable_name + '.dtable/')
        asset_dir_path = os.path.join('/asset', dtable_uuid)
        asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)
        future = self._executor.submit(get_dtable_export_content, username, repo_id, dtable_name, dtable_uuid,
                                       dtable_file_dir_id, asset_dir_id)
        future_id = str(int(time.time()*1000))
        self._future_pool[future_id] = future
        return future_id

    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name, uploaded_temp_path):
        from dtable_events.dtable_io import post_dtable_import_files

        future = self._executor.submit(post_dtable_import_files,
                                       username, repo_id, workspace_id, dtable_uuid, dtable_file_name, uploaded_temp_path)
        future_id = str(int(time.time()*1000))
        self._future_pool[future_id] = future
        return future_id

    def query_status(self, task_id):
        future = self._future_pool[task_id]

        # if future state is not running, done will return true
        if future.done():
            self._future_pool.pop(task_id, None)
            try:
                future.result(timeout=self.conf['io_task_timeout'])
            except Exception:
                raise Exception
            return True
        return False

task_manager = TaskManager()
