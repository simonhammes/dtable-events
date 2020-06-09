import time
import os
import multiprocessing

from seaserv import seafile_api


class TaskManager:

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, io_task_timeout):
        self.task_pool = {}
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'io_task_timeout': io_task_timeout,
            'workers': workers,
        }

    def is_valid_task_id(self, task_id):
        return task_id in self.task_pool.keys()

    def is_workers_maxed(self):
        self.clean_pool()
        return len(self.task_pool) >= self.conf['workers']

    def clean_pool(self):
        self.task_pool = {task_id: task for (task_id,task) in self.task_pool.items() if task.is_alive()}

    def add_export_task(self, username, repo_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import get_dtable_export_content

        dtable_file_id = seafile_api.get_file_id_by_path(repo_id, '/' + dtable_name + '.dtable')
        asset_dir_path = os.path.join('/asset', dtable_uuid)
        asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)

        task = multiprocessing.Process(target=get_dtable_export_content,
                                                 args=(username, repo_id, dtable_name, dtable_uuid,
                                                 dtable_file_id, asset_dir_id))
        task.start()
        task_id = str(int(time.time()*1000))
        self.task_pool[task_id] = task

        return task_id

    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name):
        from dtable_events.dtable_io import post_dtable_import_files

        task = multiprocessing.Process(target=post_dtable_import_files, args=(username, repo_id, workspace_id, dtable_uuid, dtable_file_name))
        task.start()
        task_id = str(int(time.time()*1000))
        self.task_pool[task_id] = task

        return task_id

    def query_status(self, task_id):
        task = self.task_pool[task_id]
        if not task.is_alive():
            self.task_pool.pop(task_id, None)
            return True
        return False

    def cancel_task(self, task_id):
        task = self.task_pool[task_id]

        task.terminate()
        while task.is_alive():
            task.join(0.1)

        self.task_pool.pop(task_id, None)


task_manager = TaskManager()
