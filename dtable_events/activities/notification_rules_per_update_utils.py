# -*- coding: utf-8 -*-
import logging
import time
import json
import os
import datetime
from datetime import date, timedelta, datetime
import requests
import jwt

dtable_server_config_path = os.path.join(os.environ['CONF_PATH'], 'dtable_web_settings.py')
import sys
sys.path.insert(0, dtable_server_config_path)
from dtable_web_settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL


CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_VIEW_NOT_EMPTY = 'view_not_empty'
CONDITION_NEAR_DEADLINE = 'near_deadline'

logger = logging.getLogger(__name__)


def is_trigger_time_satisfy(last_trigger_time):
    if (datetime.utcnow() - last_trigger_time).total_seconds() >= 60 * 5:
        return True


def get_dtable_uuid_by_id(dtable_id, db_session):
    cmd = "SELECT `uuid` FROM dtables WHERE id=:dtable_id"
    results = db_session.execute(cmd, {'dtable_id': dtable_id})
    if not results:
        logger.error('get_dtable_uuid_by_id get data from database failed.')
    for res in results:
        return res[0]


def update_rule_last_trigger_time(rule_id, db_session):

    cmd = "UPDATE dtable_notification_rules SET last_trigger_time=:new_time WHERE id=:rule_id"
    db_session.execute(cmd, {'new_time': datetime.utcnow(), 'rule_id': rule_id})


def scan_notifications_rules_per_update(row_id, db_session):
    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_id` FROM dtable_notification_rules WHERE run_condition='per_update'"
    rules = db_session.execute(sql)
    for rule in rules:
        check_notification_rule(rule, row_id, db_session)
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


def is_view_not_empty(dtable_uuid, table_id, view_id):
    access_token = get_dtable_server_token(dtable_uuid)
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    query_param = {
        'table_id': table_id,
        'view_id': view_id
    }
    try:
        res = requests.get(url, headers=headers, params=query_param)
    except requests.HTTPError as e:
        logger.error(e)
    rows = json.loads(res.content).get('rows', [])
    return len(rows) != 0


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


def list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days):
    access_token = get_dtable_server_token(dtable_uuid)
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    query_param = {
        'table_id': table_id,
        'view_id': view_id
    }
    try:
        res = requests.get(url, headers=headers, params=query_param)
    except requests.HTTPError as e:
        logger.error(e)
    rows = json.loads(res.content).get('rows', [])
    rows_near_deadline = []
    for row in rows:
        deadline_date_date_str = row.get(date_column_name, '')
        if not deadline_date_date_str:
            continue
        if ' ' in deadline_date_date_str:
            deadline_date_date_str = deadline_date_date_str.split(' ')[0]
        deadline_date = datetime.strptime(deadline_date_date_str, '%Y-%m-%d').date()
        now_plus_alarm_date = date.today() + timedelta(days=int(alarm_days))
        if date.today() <= deadline_date <= now_plus_alarm_date:
            rows_near_deadline.append(row)
    return rows_near_deadline


def check_notification_rule(rule, row_id='', db_session=None):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    creator = rule[3]
    last_trigger_time = rule[4]
    dtable_id = rule[5]

    if not is_trigger_time_satisfy(last_trigger_time):
        return

    dtable_uuid = get_dtable_uuid_by_id(dtable_id, db_session)

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')

    # send notification
    if trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not row_id:
            return

        table_id = trigger['table_id']
        view_id = trigger['view_id']

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

    elif trigger['condition'] == CONDITION_VIEW_NOT_EMPTY:
        table_id = trigger['table_id']
        view_id = trigger['view_id']
        if is_view_not_empty(dtable_uuid, table_id, view_id):
            detail = {
                'table_id': table_id,
                'view_id': view_id,
                'condition': CONDITION_VIEW_NOT_EMPTY,
                'rule_id': rule.id,
                'rule_name': rule_name,
                'msg': msg
            }
            for user in users:
                send_notification(dtable_uuid, user, detail)

    elif trigger['condition'] == CONDITION_NEAR_DEADLINE:
        date_column_name = trigger['date_column_name']
        alarm_days = trigger['alarm_days']
        table_id = trigger['table_id']
        view_id = trigger['view_id']
        rows_near_deadline = list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days)
        if not rows_near_deadline:
            return

        row_id_list = [row['_id'] for row in rows_near_deadline]
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_NEAR_DEADLINE,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': msg,
            'row_id_list': row_id_list,
        }
        for user in users:
            send_notification(dtable_uuid, user, detail)

    update_rule_last_trigger_time(rule_id, db_session)
