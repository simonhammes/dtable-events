import json
import logging
import re
import time
import os
from datetime import datetime, date, timedelta

import jwt
import requests

from dtable_events.automations.models import BoundThirdPartyAccounts
from dtable_events.dtable_io import send_wechat_msg, send_email_msg
from dtable_events.notification_rules.notification_rules_utils import _fill_msg_blanks as fill_msg_blanks, \
    send_notification
from dtable_events.utils import utc_to_tz, uuid_str_to_36_chars
from dtable_events.utils.constants import ColumnTypes


logger = logging.getLogger(__name__)

# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')
try:
    # from seahub.settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
    import seahub.settings as seahub_settings
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    TIME_ZONE = getattr(seahub_settings, 'TIME_ZONE', 'UTC')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'
PER_MONTH = 'per_month'

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_ROWS_ADDED = 'rows_added'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'
CONDITION_PERIODICALLY = 'run_periodically'
CONDITION_PERIODICALLY_BY_CONDITION = 'run_periodically_by_condition'

MESSAGE_TYPE_AUTOMATION_RULE = 'automation_rule'

EMAIL_RE = re.compile(
        r"(^[-!#$%&*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&*+/=?^_`{}|~0-9A-Z]+)*"  # dot-atom
        # quoted-string, see also http://tools.ietf.org/html/rfc2822#section-3.2.5
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"'
        r')@((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)$)'  # domain
        r'|\[(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\]$',
        re.IGNORECASE)  # literal form, ipv4 address (SMTP 4.1.3)

def get_third_party_account(session, account_id):
    account_query = session.query(BoundThirdPartyAccounts).filter(
        BoundThirdPartyAccounts.id == account_id
    )
    if account_query:
        account = account_query.first()
        return account.to_dict()
    return None

def email2list(email_str, split_pattern='[,，]'):
    email_list = [value.strip() for value in re.split(split_pattern, email_str) if value.strip()]
    return email_list


class BaseAction:

    def __init__(self, auto_rule, data=None):
        self.auto_rule = auto_rule
        self.action_type = 'base'
        self.data = data

    def do_action(self):
        pass

    def parse_column_value(self, column, value):
        if column.get('type') == ColumnTypes.SINGLE_SELECT:
            select_options = column.get('data', {}).get('options', [])
            for option in select_options:
                if value == option.get('id'):
                    return option.get('name')

        elif column.get('type') == ColumnTypes.MULTIPLE_SELECT:
            m_select_options = column.get('data', {}).get('options', [])
            if isinstance(value, list):
                parse_value_list = []
                for option in m_select_options:
                    if option.get('id') in value:
                        option_name = option.get('name')
                        parse_value_list.append(option_name)
                return parse_value_list
        else:
            return value


class UpdateAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, data, updates):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, data)
        self.action_type = 'update'
        self.updates = updates
        self.update_data = {
            'row': {},
            'table_name': self.auto_rule.table_name,
            'row_id':''
        }
        self._init_updates()

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        if self.auto_rule.run_condition == PER_UPDATE:
            for col in self.auto_rule.view_columns:
                if 'key' in col and col.get('type') in self.VALID_COLUMN_TYPES:
                    col_name = col.get('name')
                    col_key = col.get('key')
                    if col_key in self.updates.keys():
                        filtered_updates[col_name] = self.parse_column_value(col, self.updates.get(col_key))
            row_id = self.data['row']['_id']
            self.update_data['row'] = filtered_updates
            self.update_data['row_id'] = row_id

    def _can_do_action(self):
        if not self.update_data.get('row') or not self.update_data.get('row_id'):
            return False
        if self.auto_rule.run_condition == PER_UPDATE:
            # if columns in self.updates was updated, forbidden action!!!
            updated_column_keys = self.data.get('updated_column_keys', [])
            to_update_keys = [col['key'] for col in self.auto_rule.view_columns if col['name'] in self.updates]
            for key in updated_column_keys:
                if key in to_update_keys:
                    return False
        if self.auto_rule.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
        row_update_url = api_url.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/rows/?from=dtable-events'
        try:
            response = requests.put(row_update_url, headers=self.auto_rule.headers, json=self.update_data)
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        if response.status_code != 200:
            logger.error('update dtable: %s error response status code: %s', self.auto_rule.dtable_uuid, response.status_code)
        else:
            self.auto_rule.set_done_actions()

class LockRowAction(BaseAction):


    def __init__(self, auto_rule, data, trigger):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, data)
        self.action_type = 'lock'
        self.update_data = {
            'table_name': self.auto_rule.table_name,
            'row_ids':[],
        }
        self.trigger = trigger
        self._init_updates()

    def _check_row_conditions(self):
        filters = self.trigger.get('filters', [])
        filter_conjunction = self.trigger.get('filter_conjunciton', 'And')
        table_id = self.auto_rule.table_id
        view_id = self.auto_rule.view_id
        api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
        client_url = api_url.rstrip('/') + '/api/v1/internal/dtables/' + self.auto_rule.dtable_uuid + '/filter-rows/'
        json_data = {
            'table_id': table_id,
            'view_id': view_id,
            'filter_conditions': {
                'filters': filters,
                'filter_conjunction': filter_conjunction
            }
        }
        try:
            response = requests.post(client_url, headers=self.auto_rule.headers, json=json_data)
            rows_data = response.json().get('rows')
            return rows_data or []
        except Exception as e:
            logger.error('lock dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return


    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        if self.auto_rule.run_condition == PER_UPDATE:
            row_id = self.data['row']['_id']
            self.update_data['row_ids'].append(row_id)

        if self.auto_rule.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            rows_data = self._check_row_conditions()
            for row in rows_data:
                self.update_data['row_ids'].append(row.get('_id'))


    def _can_do_action(self):
        if not self.update_data.get('row_ids'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return

        api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
        row_update_url = api_url.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/lock-rows/?from=dtable-events'
        try:
            response = requests.put(row_update_url, headers=self.auto_rule.headers, json=self.update_data)
        except Exception as e:
            logger.error('lock dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        if response.status_code != 200:
            logger.error('lock dtable: %s error response status code: %s', self.auto_rule.dtable_uuid, response.status_code)
        else:
            self.auto_rule.set_done_actions()

class AddRowAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, row):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        row: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule)
        self.action_type = 'add'
        self.row = row
        self.row_data = {
            'row': {},
            'table_name': self.auto_rule.table_name
        }
        self._init_updates()

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = utc_to_tz(datetime.utcnow(), TIME_ZONE)
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        for col in self.auto_rule.view_columns:
            if 'key' in col and col.get('type') in self.VALID_COLUMN_TYPES:
                col_name = col.get('name')
                col_type = col.get('type')
                col_key = col.get('key')
                if col_key in self.row.keys():
                    if col_type == ColumnTypes.DATE:
                        time_format = col.get('data', {}).get('format', '')
                        format_length = len(time_format.split(" "))
                        try:
                            time_dict = self.row.get(col_key)
                            set_type = time_dict.get('set_type')
                            if set_type == 'specific_value':
                                time_value = time_dict.get('value')
                                filtered_updates[col_name] = time_value
                            elif set_type == 'relative_date':
                                offset = time_dict.get('offset')
                                filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                        except Exception as e:
                            logger.error(e)
                            filtered_updates[col_name] = self.row.get(col_key)
                    else:
                        filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))
        self.row_data['row'] = filtered_updates

    def _can_do_action(self):
        if not self.row_data.get('row'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
        row_add_url = api_url.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/rows/?from=dtable-events'
        try:
            response = requests.post(row_add_url, headers=self.auto_rule.headers, json=self.row_data)
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        if response.status_code != 200:
            logger.error('update dtable: %s error response status code: %s', self.auto_rule.dtable_uuid, response.status_code)
        else:
            self.auto_rule.set_done_actions()

class NotifyAction(BaseAction):

    def __init__(self, auto_rule, data, msg, users, users_column_key):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        msg: message set in action
        users: who will receive notification(s)
        """
        super().__init__(auto_rule, data)
        self.action_type = 'notify'
        self.msg = msg
        self.users = users
        self.users_column_key = users_column_key

        self.column_blanks = []
        self.col_name_dict = {}


        self._init_notify(msg)

    def _init_notify(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.view_columns}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        dtable_uuid, row, raw_row = self.auto_rule.dtable_uuid, self.data['converted_row'], self.data['row']
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        msg = self.msg
        if self.column_blanks:
            msg = self._fill_msg_blanks(row)

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': self.auto_rule.trigger.get('condition'),
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': msg,
            'row_id_list': [row['_id']],
        }

        user_msg_list = []
        users = self.users
        if self.users_column_key:
            users_from_column = raw_row.get(self.users_column_key, [])
            if not isinstance(users_from_column, list):
                users_from_column = [users_from_column, ]
            users = list(set(self.users + users_from_column))
        for user in users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def cron_notify(self):
        dtable_uuid = self.auto_rule.dtable_uuid
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_PERIODICALLY,
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': self.msg,
            'row_id_list': []
        }
        user_msg_list = []
        for user in self.users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK, PER_MONTH]:
            self.cron_notify()
            self.auto_rule.set_done_actions()

class SendWechatAction(BaseAction):

    def __init__(self, auto_rule, data, msg, account_id):

        super().__init__(auto_rule, data)
        self.action_type = 'send_wechat'
        self.msg = msg
        self.account_id = account_id

        self.webhook_url = ''
        self.column_blanks = []
        self.col_name_dict = {}

        self._init_notify(msg)


    def _init_notify(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.view_columns}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if account_dict:
            self.webhook_url = account_dict.get('detail', {}).get('webhook_url', '')


    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.msg
        if self.column_blanks:
            msg = self._fill_msg_blanks(row)
        try:
            send_wechat_msg(self.webhook_url, msg)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def cron_notify(self):
        try:
            send_wechat_msg(self.webhook_url, self.msg)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()


class SendEmailAction(BaseAction):



    def is_valid_email(self, email):
        """A heavy email format validation.
        """
        return True if EMAIL_RE.match(email) is not None else False

    def __init__(self,
                 auto_rule,
                 data,
                 send_info,
                 account_id,
                 ):

        super().__init__(auto_rule, data)
        self.action_type = 'send_email'
        self.account_id = account_id

        # send info
        self.send_info = send_info

        # auth info
        self.auth_info = {}


        self.column_blanks = []
        self.column_blanks_send_to = []
        self.column_blanks_copy_to = []
        self.col_name_dict = {}

        self._init_notify()

    def _init_notify_msg(self):
        msg = self.send_info.get('message')
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify_send_to(self):
        send_to_list = self.send_info.get('send_to')
        blanks = []
        for send_to in send_to_list:
            res = re.findall(r'\{([^{]*?)\}', send_to)
            if res:
                blanks.extend(res)
        self.column_blanks_send_to = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify_copy_to(self):
        copy_to_list = self.send_info.get('copy_to')
        blanks = []
        for copy_to in copy_to_list:
            res = re.findall(r'\{([^{]*?)\}', copy_to)
            if res:
                blanks.extend(res)
        self.column_blanks_copy_to = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify(self):
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.view_columns}
        self._init_notify_msg()
        self._init_notify_send_to()
        self._init_notify_copy_to()
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if account_dict:
            account_detail = account_dict.get('detail', {})

            email_host = account_detail.get('email_host', '')
            email_port = account_detail.get('email_port', 0)
            host_user = account_detail.get('host_user', '')
            password = account_detail.get('password', '')
            self.auth_info = {
                'email_host': email_host,
                'email_port': int(email_port),
                'host_user': host_user,
                'password' : password
            }

    def _fill_msg_blanks(self, row, text, blanks):

        col_name_dict = self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, text, blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.send_info.get('message', '')
        send_to_list = self.send_info.get('send_to', [])
        copy_to_list = self.send_info.get('copy_to', [])
        if self.column_blanks:
            msg = self._fill_msg_blanks(row, msg, self.column_blanks)
        if self.column_blanks_send_to:
            send_to_list = [self._fill_msg_blanks(row, send_to, self.column_blanks_send_to) for send_to in send_to_list]
        if self.column_blanks_copy_to:
            copy_to_list = [self._fill_msg_blanks(row, copy_to, self.column_blanks_copy_to) for copy_to in copy_to_list]
        self.send_info.update({
            'message': msg,
            'send_to': [send_to for send_to in send_to_list if self.is_valid_email(send_to)],
            'copy_to': [copy_to for copy_to in copy_to_list if self.is_valid_email(copy_to)],
        })
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=self.send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def cron_notify(self):
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=self.send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()

class RuleInvalidException(Exception):
    """
    Exception which indicates rule need to be set is_valid=Fasle
    """
    pass


class AutomationRule:

    def __init__(self, data, db_session, raw_trigger, raw_actions, options):
        self.rule_id = options.get('rule_id', None)
        self.rule_name = ''
        self.run_condition = options.get('run_condition', None)
        self.dtable_uuid = options.get('dtable_uuid', None)
        self.trigger = None
        self.action_infos = []
        self.last_trigger_time = options.get('last_trigger_time', None)
        self.trigger_count = options.get('trigger_count', None)
        self.org_id = options.get('org_id', None)
        self.creator = options.get('creator', None)
        self.data = data
        self.db_session = db_session

        self.table_id = None
        self.view_id = None

        self._table_name = ''
        self._dtable_metadata = None
        self._access_token = None
        self._view_columns = None

        self.done_actions = False
        self._load_trigger_and_actions(raw_trigger, raw_actions)

    def _load_trigger_and_actions(self, raw_trigger, raw_actions):
        self.trigger = json.loads(raw_trigger)

        self.table_id = self.trigger.get('table_id')
        if self.run_condition == PER_UPDATE:
            self._table_name = self.data.get('table_name', '')
        self.view_id = self.trigger.get('view_id')

        self.rule_name = self.trigger.get('rule_name', '')
        self.action_infos = json.loads(raw_actions)

    @property
    def access_token(self):

        if not self._access_token:
            self._access_token = jwt.encode(
                payload={
                    'exp': int(time.time()) + 300,
                    'dtable_uuid': uuid_str_to_36_chars(self.dtable_uuid),
                    'username': 'Automation Rule',
                    'permission': 'rw',
                },
                key=DTABLE_PRIVATE_KEY
            )
        return self._access_token

    @property
    def headers(self):
        return {'Authorization': 'Token ' + self.access_token.decode()}

    @property
    def dtable_metadata(self):
        if not self._dtable_metadata:
            api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
            url = api_url.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'
            response = requests.get(url, headers=self.headers)
            if response.status_code == 404:
                raise RuleInvalidException('request metadata 404')
            self._dtable_metadata = response.json().get('metadata')
        return self._dtable_metadata

    @property
    def view_columns(self):
        """
        columns of the view defined in trigger
        """
        if not self._view_columns:
            table_id, view_id = self.table_id, self.view_id
            api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
            url = api_url.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'
            response = requests.get(url, params={'table_id': table_id, 'view_id': view_id}, headers=self.headers)
            if response.status_code == 404:
                raise RuleInvalidException('request view columns 404')
            self._view_columns = response.json().get('columns')
        return self._view_columns

    @property
    def table_name(self):
        """
        name of table defined in rule
        """
        if not self._table_name and self.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            dtable_metadata = self.dtable_metadata
            tables = dtable_metadata.get('tables', [])
            for table in tables:
                if table.get('_id') == self.table_id:
                    self._table_name = table.get('name')
                    break
        return self._table_name

    def can_do_actions(self):
        if self.trigger.get('condition') not in (CONDITION_FILTERS_SATISFY, CONDITION_PERIODICALLY, CONDITION_ROWS_ADDED, CONDITION_PERIODICALLY_BY_CONDITION):
            return False

        if self.trigger.get('condition') == CONDITION_ROWS_ADDED:
            if self.data.get('op_type') not in ['insert_row', 'append_rows']:
                return False

        if self.trigger.get('condition') in [CONDITION_FILTERS_SATISFY, CONDITION_ROWS_MODIFIED]:
            if self.data.get('op_type') not in ['modify_row', 'modify_rows']:
                return False

        if self.run_condition == PER_UPDATE:
            return True

        elif self.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            cur_hour = int(utc_to_tz(datetime.utcnow(), TIME_ZONE).strftime('%H'))
            cur_datetime = utc_to_tz(datetime.utcnow(), TIME_ZONE)
            cur_week_day = cur_datetime.isoweekday()
            cur_month_day = cur_datetime.day
            if self.run_condition == PER_DAY:
                trigger_hour = self.trigger.get('notify_hour', 12)
                if cur_hour != trigger_hour:
                    return False
            elif self.run_condition == PER_WEEK:
                trigger_hour = self.trigger.get('notify_week_hour', 12)
                trigger_day = self.trigger.get('notify_week_day', 7)
                if cur_hour != trigger_hour or cur_week_day != trigger_day:
                    return False
            else:
                trigger_hour = self.trigger.get('notify_month_hour', 12)
                trigger_day = self.trigger.get('notify_month_day', 1)
                if cur_hour != trigger_hour or cur_month_day != trigger_day:
                    return False
            return True

        return False


    def do_actions(self):
        if not self.can_do_actions():
            return
        try:
            for action_info in self.action_infos:
                if action_info.get('type') == 'update_record':
                    updates = action_info.get('updates')
                    UpdateAction(self, self.data, updates).do_action()

                if action_info.get('type') == 'add_record':
                    row = action_info.get('row')
                    AddRowAction(self, row).do_action()

                elif action_info.get('type') == 'notify':
                    default_msg = action_info.get('default_msg', '')
                    users = action_info.get('users', [])
                    users_column_key = action_info.get('users_column_key', '')
                    NotifyAction(self, self.data, default_msg, users, users_column_key).do_action()

                elif action_info.get('type') == 'lock_record':
                    LockRowAction(self, self.data, self.trigger).do_action()

                elif action_info.get('type') == 'send_wechat':
                    account_id = int(action_info.get('account_id'))
                    default_msg = action_info.get('default_msg', '')
                    SendWechatAction(self, self.data, default_msg, account_id).do_action()

                elif action_info.get('type') == 'send_email':
                    account_id = int(action_info.get('account_id'))
                    msg = action_info.get('default_msg', '')
                    subject = action_info.get('subject', '')
                    send_to_list = email2list(action_info.get('send_to', ''))
                    copy_to_list = email2list(action_info.get('copy_to', ''))

                    send_info = {
                        'message': msg,
                        'send_to': send_to_list,
                        'copy_to': copy_to_list,
                        'subject': subject
                    }
                    SendEmailAction(self, self.data, send_info, account_id).do_action()

        except RuleInvalidException as e:
            logger.error('auto rule: %s, invalid error: %s', self.rule_id, e)
            self.set_invalid()
        except Exception as e:
            logger.exception(e)
            logger.error('rule: %s, do actions error: %s', self.rule_id, e)
        finally:
            if self.done_actions:
                self.update_last_trigger_time()

    def set_done_actions(self, done=True):
        self.done_actions = done

    def update_last_trigger_time(self):
        try:
            set_statistic_sql_user = '''
                INSERT INTO user_auto_rules_statistics (username, trigger_date, trigger_count, update_at) VALUES 
                (:username, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''

            set_statistic_sql_org = '''
                INSERT INTO org_auto_rules_statistics (org_id, trigger_date, trigger_count, update_at) VALUES
                (:org_id, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''
            set_last_trigger_time_sql = '''
                UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time, trigger_count=:trigger_count WHERE id=:rule_id;
            '''

            org_id = self.org_id
            if not org_id:
                sql = set_last_trigger_time_sql
            else:
                sql = "%s%s" % (set_last_trigger_time_sql, set_statistic_sql_user if self.org_id == -1 else set_statistic_sql_org)

            cur_date = datetime.now().date()
            cur_year, cur_month = cur_date.year, cur_date.month
            trigger_date = date(year=cur_year, month=cur_month, day=1)
            self.db_session.execute(sql, {
                'rule_id': self.rule_id,
                'trigger_time': datetime.utcnow(),
                'trigger_date': trigger_date,
                'trigger_count': self.trigger_count + 1,
                'username': self.creator,
                'org_id': self.org_id
            })
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)

    def set_invalid(self):
        try:
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET is_valid=0 WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id})
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)
