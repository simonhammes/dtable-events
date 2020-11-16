# -*- coding: utf-8 -*-
import json
import logging
from hashlib import md5
from datetime import datetime, timedelta

from sqlalchemy import desc, func, case
from seaserv import ccnet_api

from dtable_events.activities.models import Activities, UserDTables

logger = logging.getLogger(__name__)


class TableActivityDetail(object):
    def __init__(self, activity):
        self.id = activity.id
        self.dtable_uuid = activity.dtable_uuid
        self.row_id = activity.row_id
        self.op_user = activity.op_user
        self.op_type = activity.op_type
        self.op_time = activity.op_time
        self.op_app = activity.op_app

        detail_dict = json.loads(activity.detail)
        for key in detail_dict:
            self.__dict__[key] = detail_dict[key]

    def __getitem__(self, key):
        return self.__dict__[key]


def save_or_update_or_delete(session, event):
    if event['op_type'] == 'modify_row':
        op_time = datetime.utcfromtimestamp(event['op_time'])
        _timestamp = op_time - timedelta(minutes=5)
        # If a row was edited many times by same user in 5 minutes, just update record.
        q = session.query(Activities).filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user'],
            Activities.op_time > _timestamp
        ).order_by(desc(Activities.id))
        row = q.first()
        if row:
            if row.op_type == 'insert_row':
                detail = json.loads(row.detail)
                cells_data = event['row_data']
                # Update cells values.
                for cell_data in cells_data:
                    for i in detail['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            if i['column_type'] != cell_data['column_type']:
                                i['column_type'] = cell_data['column_type']
                                i['column_data'] = cell_data['column_data']
                            break
                    else:
                        del cell_data['old_value']
                        detail['row_data'].append(cell_data)
                detail['row_name'] = event['row_name']
            else:
                detail = json.loads(row.detail)
                cells_data = event['row_data']
                # Update cells values and keep old_values unchanged.
                for cell_data in cells_data:
                    for i in detail['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            if i['column_type'] != cell_data['column_type']:
                                i['column_type'] = cell_data['column_type']
                                i['column_data'] = cell_data['column_data']
                                i['old_value'] = cell_data['old_value']
                            break
                    else:
                        detail['row_data'].append(cell_data)
                detail['row_name'] = event['row_name']

            detail = json.dumps(detail)
            update_activity_timestamp(session, row.id, op_time, detail)
        else:
            save_user_activities(session, event)
    elif event['op_type'] == 'delete_row':
        op_time = datetime.utcfromtimestamp(event['op_time'])
        _timestamp = op_time - timedelta(minutes=5)
        # If a row was inserted by same user in 5 minutes, just delete this record.
        q = session.query(Activities).filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user'],
            Activities.op_time > _timestamp
        ).order_by(desc(Activities.id))
        row = q.first()
        if row and row.op_type == 'insert_row':
            session.query(Activities).filter(Activities.id == row.id).delete()
            session.commit()
        else:
            save_user_activities(session, event)
    else:
        save_user_activities(session, event)


def update_activity_timestamp(session, activity_id, op_time, detail):
    activity = session.query(Activities).filter(Activities.id == activity_id)
    activity.update({"op_time": op_time, "detail": detail})
    session.commit()


def get_table_activities(session, username, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    table_activities = list()
    try:
        uuid_list = session.query(UserDTables.dtable_uuid).filter(UserDTables.username == username)
        q = session.query(
            Activities.dtable_uuid, Activities.op_time.label('op_date'),
            func.date_format(Activities.op_time, '%Y-%m-%d 00:00:00').label('date'),
            func.count(case([(Activities.op_type == 'insert_row', 1)])).label('insert_row'),
            func.count(case([(Activities.op_type == 'modify_row', 1)])).label('modify_row'),
            func.count(case([(Activities.op_type == 'delete_row', 1)])).label('delete_row'))
        q = q.filter(
            Activities.op_time > (datetime.utcnow() - timedelta(days=7)),
            Activities.dtable_uuid.in_(uuid_list)).group_by(Activities.dtable_uuid, 'date')
        table_activities = q.order_by(desc(Activities.op_time)).slice(start, start + limit).all()
    except Exception as e:
        logger.error('Get table activities failed: %s' % e)

    return table_activities


def get_activities_detail(session, dtable_uuid, start_time, end_time, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    activities = list()
    try:
        q = session.query(Activities).filter(Activities.dtable_uuid == dtable_uuid).\
            filter(Activities.op_time.between(start_time, end_time))
        activities = q.order_by(desc(Activities.op_time)).slice(start, start + limit).all()
    except Exception as e:
        logger.error('Get table activities detail failed: %s' % e)

    activities_detail = list()
    for activity in activities:
        try:
            activity_detail = TableActivityDetail(activity)
            activities_detail.append(activity_detail)
        except Exception as e:
            logger.warning(e)
            continue

    return activities_detail


def save_user_activities(session, event):
    dtable_uuid = event['dtable_uuid']
    row_id = event['row_id']
    op_user = event['op_user']
    op_type = event['op_type']
    op_time = datetime.utcfromtimestamp(event['op_time'])
    op_app = event.get('op_app')

    table_id = event['table_id']
    table_name = event['table_name']
    row_name = event['row_name']
    row_data = event['row_data']

    detail_dict = dict()
    detail_dict["table_id"] = table_id
    detail_dict["table_name"] = table_name
    detail_dict["row_name"] = row_name
    detail_dict["row_data"] = row_data
    detail = json.dumps(detail_dict)

    activity = Activities(dtable_uuid, row_id, op_user, op_type, op_time, detail, op_app)
    session.add(activity)
    session.commit()

    op_date = op_time.replace(hour=0, minute=0, second=0, microsecond=0)
    op_date_str = op_date.strftime('%Y-%m-%d 00:00:00')

    cmd = "SELECT to_user FROM dtable_share WHERE dtable_id=(SELECT id FROM dtables WHERE uuid=:dtable_uuid)"
    user_list = [res[r'to_user'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})]

    cmd = "SELECT owner FROM workspaces WHERE id=(SELECT workspace_id FROM dtables WHERE uuid=:dtable_uuid)"
    owner = [res[r'owner'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})][0]

    if '@seafile_group' not in owner:
        user_list.append(owner)
    else:
        group_id = int(owner.split('@')[0])
        members = ccnet_api.get_group_members(group_id)
        for member in members:
            if member.user_name not in user_list:
                user_list.append(member.user_name)

    for user in user_list:
        user_uuid_date_md5 = md5((user + dtable_uuid + op_date_str).encode('utf-8')).hexdigest()
        cmd = "REPLACE INTO user_dtables (user_uuid_date_md5, username, dtable_uuid, op_date)" \
              "values(:user_uuid_date_md5, :username, :dtable_uuid, :op_date)"
        session.execute(cmd, {"user_uuid_date_md5": user_uuid_date_md5, "username": user,
                              "dtable_uuid": dtable_uuid, "op_date": op_date})
    session.commit()
