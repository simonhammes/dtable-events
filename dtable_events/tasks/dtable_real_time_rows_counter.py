# -*- coding: utf-8 -*-
import logging
import time
import json
from datetime import datetime
from threading import Thread, Event

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def count_rows_by_uuids(session, dtable_uuids):
    dtable_uuids = [uuid.replace('-', '') for uuid in dtable_uuids]
    # select user and org
    sql = '''
    SELECT w.owner, w.org_id FROM workspaces w
    JOIN dtables d ON w.id=d.workspace_id
    WHERE d.uuid in :dtable_uuids
    '''
    results = session.execute(sql, {'dtable_uuids': dtable_uuids}).fetchall()
    usernames, org_ids = set(), set()
    for owner, org_id in results:
        if org_id != -1:
            org_ids.add(org_id)
        else:
            if '@seafile_group' not in owner:
                usernames.add(owner)
    # count user and org
    if usernames:
        sql = '''
        SELECT w.owner, d.uuid FROM dtables d
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE w.owner IN :usernames AND d.deleted=0 AND org_id=-1
        '''
        user_dtable_uuids = session.execute(sql, {'usernames': list(usernames)}).fetchall()
        user_dtable_uuids_dict = {}  # {username: [dtable_uuid...]}
        for username, dtable_uuid in user_dtable_uuids:
            if user_dtable_uuids_dict.get(username):
                user_dtable_uuids_dict[username].append(dtable_uuid)
            else:
                user_dtable_uuids_dict[username] = [dtable_uuid]
        for username, dtable_uuids in user_dtable_uuids_dict.items():
            user_sql = '''
            INSERT INTO user_rows_count(username, rows_count, rows_count_update_at)
            SELECT :username, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count drc
            WHERE drc.dtable_uuid in :dtable_uuids
            ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
            '''
            try:
                session.execute(user_sql, {
                    'username': username,
                    'update_at': datetime.utcnow(),
                    'dtable_uuids': dtable_uuids
                })
                session.commit()
            except Exception as e:
                logger.error('update user rows count: %s error: %s', username, e)

    if org_ids:
        sql = '''
        SELECT w.org_id, d.uuid FROM dtables d
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE d.deleted=0 AND w.org_id in :org_ids
        '''
        org_dtable_uuids = session.execute(sql, {'org_ids': list(org_ids)})
        org_dtable_uuids_dict = {}  # {org_id: [dtable_uuids...]}
        for org_id, dtable_uuid in org_dtable_uuids:
            if org_dtable_uuids_dict.get(org_id):
                org_dtable_uuids_dict[org_id].append(dtable_uuid)
            else:
                org_dtable_uuids_dict[org_id] = [dtable_uuid]
        for org_id, dtable_uuids in org_dtable_uuids_dict.items():
            org_sql = '''
            INSERT INTO org_rows_count(org_id, rows_count, rows_count_update_at)
            SELECT :org_id, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count drc
            WHERE drc.dtable_uuid in :dtable_uuids
            ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
            '''
            try:
                session.execute(org_sql, {
                    'org_id': org_id,
                    'update_at': datetime.utcnow(),
                    'dtable_uuids': dtable_uuids
                })
                session.commit()
            except Exception as e:
                logger.error('update org rows count: %s, error: %s', org_id, e)


class DTableRealTimeRowsCounter(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)


    def run(self):
        logger.info('Starting handle table rows count...')
        subscriber = self._redis_client.get_subscriber('count-rows')
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    dtable_uuids = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        count_rows_by_uuids(session, dtable_uuids)
                    except Exception as e:
                        logger.error('Handle table rows count: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('count-rows')
