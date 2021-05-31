# -*- coding: utf-8 -*-
import logging
import time
import json
import os
from datetime import datetime, date, timedelta
import requests
import jwt
import sys
import re
import pytz

from dtable_events.utils.constants import ColumnTypes
from dtable_events.cache import redis_cache as cache

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
CONDITION_NEAR_DEADLINE = 'near_deadline'



def is_trigger_time_satisfy(last_trigger_time):
    if last_trigger_time == None:
        return True
    if (datetime.utcnow() - last_trigger_time).total_seconds() >= 60 * 5:
        return True


def update_rule_last_trigger_time(rule_id, db_session):

    cmd = "UPDATE dtable_notification_rules SET last_trigger_time=:new_time WHERE id=:rule_id"
    db_session.execute(cmd, {'new_time': datetime.utcnow(), 'rule_id': rule_id})


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


def scan_notifications_rules_per_update(event_data, db_session):
    row_id = event_data.get('row_id', '')
    message_dtable_uuid = event_data.get('dtable_uuid', '')
    table_id = event_data.get('table_id', '')
    row_data = event_data.get('row_data', [])
    column_keys = [cell.get('column_key') for cell in row_data if 'column_key' in cell]
    if not row_id or not message_dtable_uuid or not table_id:
        logger.error(f'redis event data not valid, event_data = {event_data}')
        return

    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules WHERE run_condition='per_update'" \
          "AND dtable_uuid=:dtable_uuid AND is_valid=1"
    rules = db_session.execute(sql, {'dtable_uuid': message_dtable_uuid})

    dtable_server_access_token = get_dtable_server_token(message_dtable_uuid)

    for rule in rules:
        try:
            check_notification_rule(rule, table_id, row_id, column_keys, dtable_server_access_token, db_session)
        except Exception as e:
            logger.error(f'check rule failed. {rule}, error: {e}')
    db_session.commit()


def list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, column_key, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/' + row_id + '/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {
        'table_id': table_id,
        'view_id': view_id,
        'convert': False,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        logger.error(f'failed to list_users_by_column_key {res.text}')

    rowdict = json.loads(res.content)
    user_list = rowdict.get(column_key, [])
    if isinstance(user_list, str):
        return [user_list]
    return user_list


def send_notification(dtable_uuid, user_msg_list, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/notifications-batch/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    body = {
        'user_messages': user_msg_list,
    }
    res = requests.post(url, headers=headers, json=body)

    if res.status_code != 200:
        logger.error(f'failed to send_notification {res.text}')


def deal_invalid_rule(rule_id, db_session):
    sql = "UPDATE dtable_notification_rules SET is_valid=:is_valid WHERE id=:rule_id"
    try:
        db_session.execute(sql, {'is_valid': 0, 'rule_id': rule_id})
    except Exception as e:
        logger.error(e)


def is_view_in_table(view_id, dtable_uuid, table_id, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/metadata/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    res = requests.get(url, headers=headers)
    # dtable not found
    if res.status_code == 404:
        return False
    if res.status_code != 200:
        return True
    tables = json.loads(res.content).get('metadata', {}).get('tables', {})
    for table in tables:
        if table['_id'] == table_id:
            for view in table['views']:
                if view['_id'] == view_id:
                    return True
    return False


def is_row_in_view(row_id, view_id, dtable_uuid, table_id, dtable_server_access_token, rule_id=None, db_session=None):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-in-view/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {
        'row_id': row_id,
        'view_id': view_id,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code == 404:
        # perhaps 404 is reason for row_id, we only deal with 'view not found','table not found' and 'dtable not found'
        if not is_view_in_table(view_id, dtable_uuid, table_id, dtable_server_access_token):
            deal_invalid_rule(rule_id, db_session)
    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_in_view')


def is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id, dtable_server_access_token, rule_id=None, db_session=None):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-satisfy-filters/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    data = {
        'row_id': row_id,
        'filters': filters,
        'filter_conjunction': filter_conjuntion
    }
    res = requests.get(url, headers=headers, json=data)

    if res.status_code == 404:
        deal_invalid_rule(rule_id, db_session)
    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_satisfy_filters')


def list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token, rule_id=None, db_session=None):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    query_params = {
        'table_id': table_id,
        'view_id': view_id,
        'convert_link_id': True
    }
    try:
        res = requests.get(url, headers=headers, params=query_params)
    except Exception as e:
        logger.error(e)
        return []

    if res.status_code == 404:
        deal_invalid_rule(rule_id, db_session)
    if res.status_code != 200:
        logger.error(res.text)
        return []

    rows = json.loads(res.content).get('rows', [])
    rows_near_deadline = []
    for row in rows:
        deadline_date_date_str = row.get(date_column_name, '')
        if not deadline_date_date_str:
            continue
        if ' ' in deadline_date_date_str:
            deadline_date_date_str = deadline_date_date_str.split(' ')[0]
        try:
            deadline_date = datetime.strptime(deadline_date_date_str, '%Y-%m-%d').date()
        except Exception as e:
            # perhaps result-type of fomular column has been changed to non-date
            logger.warning('date_column_name: %s value: %s, transfer to date error: %s', date_column_name, deadline_date_date_str, e)
            continue
        now_plus_alarm_date = date.today() + timedelta(days=int(alarm_days))
        if date.today() <= deadline_date <= now_plus_alarm_date:
            rows_near_deadline.append(row)
    return rows_near_deadline


def get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/columns/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    query_params = {
        'table_id': table_id,
        'view_id': view_id
    }
    try:
        response = requests.get(url, params=query_params, headers=headers)
        columns = response.json()['columns']
    except Exception as e:
        logger.error('dtable_uuid: %s, table_id: %s, view_id: %s request columns error: %s', dtable_uuid, table_id, view_id, e)
        return []
    return columns


def get_nickname_by_usernames(usernames, db_session):
    """
    fetch nicknames by usernames from db / cache
    return: {username0: nickname0, username1: nickname1...}
    """
    if not usernames:
        return {}
    cache_timeout = 60*60*24
    key_format = 'user:nickname:%s'
    users_dict, miss_users = {}, []

    for username in usernames:
        nickname = cache.get(key_format % username)
        if nickname is None:
            miss_users.append(username)
        else:
            users_dict[username] = nickname
            cache.set(key_format % username, nickname, timeout=cache_timeout)

    if not miss_users:
        return users_dict

    # miss_users is not empty
    sql = "SELECT user, nickname FROM profile_profile WHERE user in :users"
    try:
        for username, nickname in db_session.execute(sql, {'users': usernames}).fetchall():
            users_dict[username] = nickname
            cache.set(key_format % username, nickname, timeout=cache_timeout)
    except Exception as e:
        logger.error('check nicknames error: %s', e)

    return users_dict


def _get_dtable_metadata(dtable_uuid):
    access_token = get_dtable_server_token(dtable_uuid)
    metadata_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/metadata/'
    headers = {'Authorization': 'Token ' + access_token.decode()}
    response = requests.get(metadata_url, headers=headers)
    return response.json().get('metadata')


def _fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata=None):
    for blank in column_blanks:
        if col_name_dict[blank]['type'] in [
            ColumnTypes.TEXT,
            ColumnTypes.DATE,
            ColumnTypes.LONG_TEXT,
            ColumnTypes.SINGLE_SELECT,
            ColumnTypes.URL,
            ColumnTypes.DURATION,
            ColumnTypes.NUMBER,
            ColumnTypes.EMAIL,
            ColumnTypes.AUTO_NUMBER,
            ColumnTypes.CTIME,
            ColumnTypes.MTIME
        ]:
            value = row.get(blank, '')
            msg = msg.replace('{' + blank + '}', str(value) if value else '')  # maybe value is None and str(None) is 'None'

        elif col_name_dict[blank]['type'] in [
            ColumnTypes.IMAGE,
            ColumnTypes.MULTIPLE_SELECT,
            ColumnTypes.LINK,
        ]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join(value) + ']') if value else '[]')  # maybe value is None

        elif col_name_dict[blank]['type'] in [ColumnTypes.FILE]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join([f['name'] for f in value]) + ']') if value else '[]')

        elif col_name_dict[blank]['type'] in [ColumnTypes.COLLABORATOR]:
            users = row.get(blank, [])
            if users is None:
                names = []
            else:
                names_dict = get_nickname_by_usernames(users, db_session)
                names = [names_dict.get(user) for user in users if user in names_dict]
            msg = msg.replace('{' + blank + '}', '[' + ', '.join(names) + ']')

        elif col_name_dict[blank]['type'] in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            value = row.get(blank, '')
            if value is None:
                name = ''
            else:
                name = get_nickname_by_usernames([value], db_session).get(value, '')
            msg = msg.replace('{' + blank + '}', name)

        elif col_name_dict[blank]['type'] in [ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA]:
            # Fill formula blanks
            # If result_type of formula is 'column', which indicates that real result_type could be like collaborator
            # we need to fill blanks with nicknames,
            # else just transfer value to str to fill blanks.
            # Judge whether value is like collaborator or not is base on metadata of dtable
            value = row.get(blank)

            # If result of formula is a string or None, just replace directly
            if isinstance(value, str) or value is None:
                msg = msg.replace('{' + blank + '}', value if value else '')
                continue

            formula_data = col_name_dict[blank].get('data')
            if not formula_data:
                continue
            # If not result_type is not 'column', just return str(value)
            if formula_data.get('result_type') != 'column':
                msg = msg.replace('{' + blank + '}', str(value) if value else '')
                continue

            # According `display_column_key`, `linked_table_id` and metadata of dtable,
            # judge whether result_type of formula is like collaborator or not.
            display_column_key, linked_table_id = formula_data.get('display_column_key'), formula_data.get('linked_table_id')
            target_column_type = None
            if not dtable_metadata:
                try:
                    dtable_metadata = _get_dtable_metadata(dtable_uuid)
                except Exception as e:
                    logger.error('request dtable metadata in fill msg error: %s', e)
                    continue
            for table in dtable_metadata.get('tables', []):
                if table.get('_id') == linked_table_id:
                    columns = table.get('columns', [])
                    for col in columns:
                        if col.get('key') == display_column_key:
                            target_column_type = col.get('type')
                            break
                if target_column_type:
                    break

            # If result_type is like collaborator, fill blanks with nicknames
            if target_column_type in [ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
                if value is None:
                    msg = msg.replace('{' + blank + '}', '[]')
                    continue
                names_dict = get_nickname_by_usernames(value, db_session)
                names = [names_dict.get(user) for user in value if user in names_dict]
                msg = msg.replace('{' + blank + '}', '[' + ', '.join(names) + ']')

            # else just fill str(value)
            else:
                if value is None:
                    msg = msg.replace('{' + blank + '}', '')
                elif isinstance(value, list):
                    msg = msg.replace('{' + blank + '}', '[' + ', '.join([str(v) for v in value]) + ']')
                else:
                    msg = msg.replace('{' + blank + '}', str(value))

    return msg


def _get_column_blanks(blanks, columns):
    col_name_dict = {col['name']: col for col in columns}

    column_blanks = []
    for blank in blanks:
        if blank in col_name_dict:
            column_blanks.append(blank)
    if not column_blanks:
        return [], col_name_dict

    return column_blanks, col_name_dict


def gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token, db_session):
    if not msg:
        return msg

    # checkout all blanks to fill in
    # if no blanks, just return msg
    blanks = set(re.findall(r'\{([^{]*?)\}', msg))
    if not blanks:
        return msg

    columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)

    column_blanks, col_name_dict = _get_column_blanks(blanks, columns)

    if not column_blanks:
        return msg

    # get row of table-view-row
    row_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/{dtable_uuid}/rows/{row_id}/'.format(dtable_uuid=dtable_uuid, row_id=row_id)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {'table_id': table_id, 'convert_link_id': True}
    try:
        response = requests.get(row_url, params=params, headers=headers)
        row = response.json()
    except Exception as e:
        logger.error('dtable_uuid: %s, table_id: %s, row_id: %s, request row error: %s', dtable_uuid, table_id, row_id, e)
        return msg, {}

    msg = _fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session)

    return msg


def gen_notification_msg_with_row(dtable_uuid, msg, row, column_blanks, col_name_dict, db_session, dtable_metadata=None):
    if not msg:
        return msg
    if not column_blanks:
        return msg

    return _fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata=dtable_metadata)


def check_notification_rule(rule, message_table_id, row_id, column_keys, dtable_server_access_token, db_session):

    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    creator = rule[3]
    last_trigger_time = rule[4]
    dtable_uuid = rule[5]

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    users_column_key = action.get('users_column_key')
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')
    table_id = trigger['table_id']
    view_id = trigger['view_id']

    if message_table_id != table_id:
        return

    if not is_row_in_view(row_id, view_id, dtable_uuid, message_table_id, dtable_server_access_token, rule_id, db_session):
        return
    user_msg_list = []

    if trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not is_trigger_time_satisfy(last_trigger_time):
            return

        target_column_keys = trigger.get('column_keys', [])
        watch_all_columns = trigger.get('watch_all_columns')
        if watch_all_columns is not None and not isinstance(watch_all_columns, bool):
            watch_all_columns = False

        # For compatibility with old code, there is no need to judge whether updated column_keys in target_column_keys when watch_all_columns is None
        # Only watch_all_columns is not None, need to judge whether send notification or not
        if watch_all_columns is not None and not watch_all_columns:
            has_msg_key_in_target_keys = False
            for msg_key in column_keys:
                if msg_key in target_column_keys:
                    has_msg_key_in_target_keys = True
                    break
            if not has_msg_key_in_target_keys:
                return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token, db_session),
            'row_id_list': [row_id],
        }

        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            users = list(set(users + users_from_cell))

        for user in users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

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
        if not is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id, dtable_server_access_token, rule_id, db_session):
            return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_FILTERS_SATISFY,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token, db_session),
            'row_id_list': [row_id],
        }
        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            users = list(set(users + users_from_cell))

        for user in users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    else:
        return

    update_rule_last_trigger_time(rule_id, db_session)


def check_near_deadline_notification_rule(rule, db_session, timezone):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    dtable_uuid = rule[5]

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    users_column_key = action.get('users_column_key')
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')
    table_id = trigger['table_id']
    view_id = trigger['view_id']

    if trigger['condition'] != CONDITION_NEAR_DEADLINE:
        return

    date_column_name = trigger['date_column_name']
    alarm_days = trigger['alarm_days']
    table_id = trigger['table_id']
    view_id = trigger['view_id']
    notify_hour = trigger.get('notify_hour')

    try:
        cur_hour = int((datetime.utcnow() + pytz.timezone(timezone)._utcoffset).strftime('%H'))
    except Exception as e:
        logger.error('timezone: %s parse error: %s', timezone, e)
        cur_hour = int(time.strftime('%H'))

    if notify_hour != None:
        if int(notify_hour) != cur_hour:
            return
    else:
        if cur_hour != 12:
            return

    dtable_server_access_token = get_dtable_server_token(dtable_uuid)
    try:
        rows_near_deadline = list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token, rule_id, db_session)
    except Exception as e:
        logger.error('list rows_near_deadline failed. error: {}'.format(e))
        return

    if not rows_near_deadline:
        return

    try:
        dtable_metadata = _get_dtable_metadata(dtable_uuid)
    except Exception as e:
        logger.error('request dtable metadata error: %s', e)
        return

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    if blanks:
        columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)
        column_blanks, col_name_dict = _get_column_blanks(blanks, columns)

    for row in rows_near_deadline[:25]:
        row_id = row['_id']

        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            to_users = list(set(users + users_from_cell))
        else:
            to_users = users

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_NEAR_DEADLINE,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row(dtable_uuid, msg, row, column_blanks, col_name_dict, db_session, dtable_metadata=dtable_metadata),
            'row_id_list': [row_id],
        }

        user_msg_list = []
        for user in to_users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    update_rule_last_trigger_time(rule_id, db_session)
