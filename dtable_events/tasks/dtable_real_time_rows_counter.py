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
    SELECT owner, org_id FROM dtable_rows_count
    WHERE dtable_uuid IN :dtable_uuids
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
        user_sql = '''
        INSERT INTO user_rows_count(username, rows_count, rows_count_update_at)
        SELECT drc.owner AS username, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count drc
        JOIN dtables d ON drc.dtable_uuid=d.uuid
        WHERE drc.owner IN :usernames AND d.deleted=0
        GROUP BY drc.owner
        ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
        '''
        try:
            session.execute(user_sql, {
                'usernames': list(usernames),
                'update_at': datetime.utcnow()
            })
            session.commit()
        except Exception as e:
            logger.error('update users rows error: %s', e)

    if org_ids:
        org_sql = '''
        INSERT INTO org_rows_count(org_id, rows_count, rows_count_update_at)
        SELECT drc.org_id, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count as drc
        JOIN dtables d ON drc.dtable_uuid=d.uuid
        WHERE drc.org_id IN :org_ids AND d.deleted=0
        GROUP BY drc.org_id
        ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
        '''
        try:
            session.execute(org_sql, {
                'org_ids': list(org_ids),
                'update_at': datetime.utcnow()
            })
            session.commit()
        except Exception as e:
            logger.error('update orgs rows error: %s', e)


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
