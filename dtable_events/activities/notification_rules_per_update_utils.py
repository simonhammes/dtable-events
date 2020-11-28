# -*- coding: utf-8 -*-
import logging
import time
import json
import os
import datetime
from datetime import date, timedelta, datetime
import requests
import jwt
import sys

logger = logging.getLogger(__name__)


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

sys.path.insert(0, dtable_web_dir)
try:
    from seahub.settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)


CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'


def is_trigger_time_satisfy(last_trigger_time):
    if last_trigger_time == None:
        return True
    if (datetime.utcnow() - last_trigger_time).total_seconds() >= 60 * 5:
        return True


def update_rule_last_trigger_time(rule_id, db_session):

    cmd = "UPDATE dtable_notification_rules SET last_trigger_time=:new_time WHERE id=:rule_id"
    db_session.execute(cmd, {'new_time': datetime.utcnow(), 'rule_id': rule_id})


def scan_notifications_rules_per_update(event_data, db_session):
    row_id = event_data.get('row_id', '')
    message_dtable_uuid = event_data.get('dtable_uuid', '')
    table_id = event_data.get('table_id', '')
    row_data = event_data.get('row_data', [])
    column_keys = [cell.get('column_key') for cell in row_data if 'column_key' in cell]

    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules WHERE run_condition='per_update'" \
          "AND dtable_uuid=:dtable_uuid"
    rules = db_session.execute(sql, {'dtable_uuid': message_dtable_uuid})

    for rule in rules:
        try:
            check_notification_rule(rule, table_id, row_id, column_keys, db_session)
        except Exception as e:
            logger.error(f'check rule failed. {rule}, error: {e}')
    db_session.commit()


def get_dtable_server_token(dtable_uuid):
    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': 'dtable-web',
        'permission': 'rw',
    }
    try:
        access_token = jwt.encode(
            payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
        )
    except Exception as e:
        logger.error(e)
        return
    return access_token


def send_notification(dtable_uuid, user, detail):
    access_token = get_dtable_server_token(dtable_uuid)
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/notifications/'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    body = {
        'to_user': user,
        'msg_type': 'notification_rules',
        'detail': detail,
    }
    res = requests.post(url, headers=headers, json=body)

    if res.status_code != 200:
        logger.error(res)


def is_row_in_view(row_id, view_id, dtable_uuid, table_id):
    access_token = get_dtable_server_token(dtable_uuid)
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-in-view/'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    params = {
        'row_id': row_id,
        'view_id': view_id,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_in_view')


def is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id):
    access_token = get_dtable_server_token(dtable_uuid)
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-satisfy-filters/'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    data = {
        'row_id': row_id,
        'filters': filters,
        'filter_conjunction': filter_conjuntion
    }
    res = requests.get(url, headers=headers, json=data)

    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_satisfy_filters')


def check_notification_rule(rule, message_table_id, row_id='', column_keys = [], db_session=None):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    creator = rule[3]
    last_trigger_time = rule[4]
    dtable_uuid = rule[5]

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')
    table_id = trigger['table_id']
    view_id = trigger['view_id']

    if message_table_id != table_id:
        return

    if not is_row_in_view(row_id, view_id, dtable_uuid, message_table_id):
        return

    if trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not row_id:
            return
        if not is_trigger_time_satisfy(last_trigger_time):
            return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': msg,
            'row_id_list': [row_id],
        }
        for user in users:
            send_notification(dtable_uuid, user, detail)

    elif trigger['condition'] == CONDITION_FILTERS_SATISFY:
        filters = trigger.get('filters', [])
        if not filters:
            return

        target_column_keys = trigger.get('column_keys', [])
        watch_all_columns = trigger.get('watch_all_columns', False)

        if not watch_all_columns:
            has_msg_key_in_target_keys = False
            for msg_key in column_keys:
                if msg_key in target_column_keys:
                    has_msg_key_in_target_keys = True
                    break
            if not has_msg_key_in_target_keys:
                return

        filter_conjuntion = trigger.get('filter_conjunction', 'And')
        if is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id):
            detail = {
                'table_id': table_id,
                'view_id': view_id,
                'condition': CONDITION_FILTERS_SATISFY,
                'rule_id': rule.id,
                'rule_name': rule_name,
                'msg': msg,
                'row_id_list': [row_id],
            }
            for user in users:
                send_notification(dtable_uuid, user, detail)

    else:
        return

    update_rule_last_trigger_time(rule_id, db_session)
