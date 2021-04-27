import time
import queue
import threading

class TaskMessageManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None
        self.current_task_info = None
        self.t = None
        self.err_msg = None

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
    
    def add_email_sending_task(self, auth_info, send_info):
        from dtable_events.dtable_io import send_email_msg
        task_id = str(int(time.time() * 1000))
        task = (send_email_msg,(auth_info, send_info))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_wechat_sending_task(self, webhook_url, msg ):
        from dtable_events.dtable_io import send_wechat_msg
        task_id = str(int(time.time() * 1000))
        task = (send_wechat_msg, (webhook_url, msg))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def query_status(self, task_id):
        err_msg = self.err_msg
        task = self.tasks_map[task_id]
        if task == 'success':
            self.tasks_map.pop(task_id, None)
            self.err_msg = None
            return True, err_msg
        return False, err_msg

    def handle_task(self):
        from dtable_events.dtable_io import dtable_message_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_message_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                self.current_task_info = task_id + ' ' + str(task[0])
                dtable_message_logger.info('Run task: %s' % self.current_task_info)
                start_time = time.time()

                # run
                self.err_msg = task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_message_logger.info('Run task success: %s cost %ds \n' % (self.current_task_info, int(finish_time - start_time)))
                self.current_task_info = None
            except Exception as e:
                dtable_message_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info = None

    def run(self):
        self.t = threading.Thread(target=self.handle_task)
        self.t.setDaemon(True)
        self.t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


message_task_manager = TaskMessageManager()
