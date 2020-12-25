import json
import time
import logging
from datetime import datetime
from threading import Thread
from queue import Queue, Empty

import requests
from sqlalchemy.orm import scoped_session

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.webhook.models import Webhooks, WebhookJobs, PENDING, SENDING, SUCCESS, FAILURE, DB

logger = logging.getLogger(__name__)


class Webhooker:
    """
    Webhooker is used to trigger webhooks, generate webhook jobs
    There are a few steps in this program
    1. subscribe events from redis
    2. query webhooks added in dtable-web about these events
    3. generate webhook jobs according to webhooks, including request body, headers...etc
    4. trigger jobs one by one

    Each WebhookJob instance has 4 states, PENDING, SENDING, SUCCESS and FAILURE.
    Once a job instance generated, it's in PENDING state and it will be in SENDING when being sent.
    It is only 20X that the status code of response from hook triggered is, job's state is SUCCESS.
    In situation where request failed or response status isn't 20X, state is FAILURE.

    Well, when restart program, it also needs to recover PENDING jobs. So the 0th step:
    0. recover PENGING jobs from db.

    Steps above run in multi-threads to improve efficiency and note that all sqlalchemy sessions are scoped sessions.
    """

    def __init__(self, config):
        self._db_session_class = scoped_session(init_db_session_class(config))
        self._redis_client = RedisClient(config)
        self._subscriber = self._redis_client.get_subscriber('table-events')
        self.post_queue = Queue()

    def start(self):
        logger.info('Starting handle webhook jobs...')
        self.recover_from_db()
        tds = [Thread(target=self.sub_from_redis, daemon=True)]
        tds.extend([Thread(target=self.post_webhook_jobs, daemon=True) for i in range(2)])
        [td.start() for td in tds]

    def recover_from_db(self):
        with DB(self._db_session_class()) as db_session:
            webhook_jobs = db_session.query(WebhookJobs).filter(WebhookJobs.status==PENDING).all()
            [self.post_queue.put(job) for job in webhook_jobs]

    def sub_from_redis(self):
        """
        all events from redis are kind of update so far
        """
        while True:
            try:
                for item in self._subscriber.listen():
                    if item['type'] == 'message':
                        data = item['data'].decode('utf-8')
                        try:
                            data = json.loads(data)
                        except:
                            continue

                        self.checkout_webhook_jobs({
                            'data': data,
                            'event': 'update'
                        })
            except:
                self._subscriber = self._redis_client.get_subscriber('table-events')

    def checkout_webhook_jobs(self, event):
        data = event['data']
        # get dtable_uuid
        dtable_uuid = data.get('dtable_uuid')
        # get dtable_uuid all webhooks
        with DB(self._db_session_class()) as db_session:
            hooks = db_session.query(Webhooks).filter(Webhooks.dtable_uuid==dtable_uuid).all()
            # validate webhooks one by one and generate / put webhook_jobs
            for hook in hooks:
                request_body = hook.gen_request_body(event)
                request_headers = hook.gen_request_headers()
                hook_job = WebhookJobs(hook.id, request_body, hook.url, request_headers=request_headers)
                # add and commit
                db_session.add(hook_job)
                db_session.commit()
                self.post_queue.put(hook_job)

    def post_webhook_jobs(self):
        while True:
            hook_job = self.post_queue.get()
            with DB(self._db_session_class()) as db_session:
                hook_job = db_session.merge(hook_job)
                # update status
                hook_job.status = SENDING
                db_session.commit()
                # post
                try:
                    body = json.loads(hook_job.request_body) if hook_job.request_body else None
                    headers = json.loads(hook_job.request_headers) if hook_job.request_headers else None
                    hook_job.trigger_at = datetime.now()
                    response = requests.post(hook_job.url, json=body, headers=headers)
                except Exception as e:
                    logger.error('post error: %s', e)
                    hook_job.status = FAILURE
                else:
                    hook_job.response_status = response.status_code
                    hook_job.response_body = response.text
                    hook_job.status = SUCCESS if 200 <= response.status_code < 300 else FAILURE
                finally:
                    db_session.commit()
