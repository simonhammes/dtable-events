import time
import os
import queue
import threading


from seaserv import seafile_api


class TaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None
        self.current_task_info = None
        self.t = None

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, dtable_server_url, io_task_timeout, config):
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'dtable_server_url': dtable_server_url,
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
                (username, repo_id, dtable_uuid, asset_dir_id, self.config))
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

    def add_export_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map=None):
        from dtable_events.dtable_io import get_dtable_export_asset_files

        task_id = str(int(time.time()*1000))
        task = (get_dtable_export_asset_files,
                (username, repo_id, dtable_uuid, files, task_id, files_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_transfer_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map, parent_dir, relative_path, replace, repo_api_token, seafile_server_url):
        from dtable_events.dtable_io import get_dtable_transfer_asset_files
        task_id = str(int(time.time() * 1000))
        task = (get_dtable_transfer_asset_files,
                (username,
                 repo_id,
                 dtable_uuid,
                 files,
                 task_id,
                 files_map,
                 parent_dir,
                 relative_path,
                 replace,
                 repo_api_token,
                 seafile_server_url))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_parse_excel_task(self, username, repo_id, workspace_id, dtable_name, custom):
        from dtable_events.dtable_io import parse_excel

        task_id = str(int(time.time()*1000))
        task = (parse_excel,
                (username, repo_id, workspace_id, dtable_name, custom, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import import_excel

        task_id = str(int(time.time()*1000))
        task = (import_excel,
                (username, repo_id, workspace_id, dtable_uuid, dtable_name, self.config))
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
        from dtable_events.dtable_io import dtable_io_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_io_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                self.current_task_info = task_id + ' ' + str(task[0])
                dtable_io_logger.info('Run task: %s' % self.current_task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_io_logger.info('Run task success: %s cost %ds \n' % (self.current_task_info, int(finish_time - start_time)))
                self.current_task_info = None
            except Exception as e:
                dtable_io_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info = None

    def run(self):
        self.t = threading.Thread(target=self.handle_task)
        self.t.setDaemon(True)
        self.t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


task_manager = TaskManager()
