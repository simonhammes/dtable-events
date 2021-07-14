import json
import logging
import re
import time
import os
from datetime import datetime, date, timedelta

import jwt
import requests

from dtable_events.activities.notification_rules_utils import _fill_msg_blanks as fill_msg_blanks, \
    send_notification
from dtable_events.utils import utc_to_tz
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
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'
CONDITION_PERIODICALLY = 'run_periodically'

MESSAGE_TYPE_AUTOMATION_RULE = 'automation_rule'


class BaseAction:

    def __init__(self, auto_rule, data=None):
        self.auto_rule = auto_rule
        self.action_type = 'base'
        self.data = data

    def do_action(self):
        pass


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
        valid_view_column_names = [col.get('name') for col in self.auto_rule.view_columns if 'name' in col and col.get('type') in self.VALID_COLUMN_TYPES]
        filtered_updates = {key: value for key, value in self.updates.items() if key in valid_view_column_names}

        if self.auto_rule.run_condition == PER_UPDATE:
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
        if self.auto_rule.run_condition in (PER_DAY, PER_WEEK):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        row_update_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/rows/'
        try:
            response = requests.put(row_update_url, headers=self.auto_rule.headers, json=self.update_data)
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        if response.status_code != 200:
            logger.error('update dtable: %s error response status code: %s', self.auto_rule.dtable_uuid, response.status_code)
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

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        valid_view_column_names = [col.get('name') for col in self.auto_rule.view_columns if 'name' in col and col.get('type') in self.VALID_COLUMN_TYPES]
        filtered_updates = {key: value for key, value in self.row.items() if key in valid_view_column_names}
        self.row_data['row'] = filtered_updates


    def _can_do_action(self):
        if not self.row_data.get('row'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        row_add_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/rows/'
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

    def __init__(self, auto_rule, data, msg, users):
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

        self.column_blanks = []
        self.col_name_dict = {}

        self.interval_valid = True

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
        dtable_uuid, row = self.auto_rule.dtable_uuid, self.data['converted_row']
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
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()

class RuleInvalidException(Exception):
    """
    Exception which indicates rule need to be set is_valid=Fasle
    """
    pass


class AutomationRule:

    def __init__(self, rule_id, run_condition, dtable_uuid, raw_trigger, raw_actions, last_trigger_time, data, db_session):
        self.rule_id = rule_id
        self.rule_name = ''
        self.run_condition = run_condition
        self.dtable_uuid = dtable_uuid
        self.trigger = None
        self.action_infos = []
        self.last_trigger_time = last_trigger_time
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
                    'dtable_uuid': self.dtable_uuid,
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
            url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'
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
            url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'
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
        if not self._table_name and self.run_condition in (PER_DAY, PER_WEEK):
            dtable_metadata = self.dtable_metadata
            tables = dtable_metadata.get('tables', [])
            for table in tables:
                if table.get('_id') == self.table_id:
                    self._table_name = table.get('name')
                    break
        return self._table_name

    def can_do_actions(self):
        if self.trigger.get('condition') not in (CONDITION_FILTERS_SATISFY, CONDITION_PERIODICALLY):
            return False

        if self.run_condition == PER_UPDATE:
            return True

        elif self.run_condition in (PER_DAY, PER_WEEK):
            cur_hour = int(utc_to_tz(datetime.utcnow(), TIME_ZONE).strftime('%H'))
            cur_datetime = utc_to_tz(datetime.utcnow(), TIME_ZONE)
            cur_week_day = cur_datetime.isoweekday()
            if self.run_condition == PER_DAY:
                trigger_hour = self.trigger.get('notify_hour', 12)
                if cur_hour != trigger_hour:
                    return False
            else:
                trigger_hour = self.trigger.get('notify_week_hour', 12)
                trigger_day = self.trigger.get('notify_week_day', 7)
                if cur_hour != trigger_hour or cur_week_day != trigger_day:
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
                    NotifyAction(self, self.data, default_msg, users).do_action()
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
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id, 'trigger_time': datetime.utcnow()})
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
