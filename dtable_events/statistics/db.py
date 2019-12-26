# -*- coding: utf-8 -*-
import logging
from hashlib import md5
from datetime import datetime

from sqlalchemy import func

from dtable_events.statistics.models import UserActivityStatistics

logger = logging.getLogger(__name__)


def save_user_activity_stat(session, msg):
    username = msg['username']
    timestamp = msg['timestamp']

    user_time_md5 = md5((username + timestamp).encode('utf-8')).hexdigest()
    msg['user_time_md5'] = user_time_md5

    cmd = "REPLACE INTO user_activity_statistics (user_time_md5, username, timestamp, org_id)" \
          "values(:user_time_md5, :username, :timestamp, :org_id)"

    session.execute(cmd, msg)
    session.commit()


def get_user_activity_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    try:
        q = session.query(
            func.date(func.convert_tz(UserActivityStatistics.timestamp, '+00:00', offset)).label("timestamp"),
            func.count(UserActivityStatistics.user_time_md5).label("number")
        )
        q = q.filter(UserActivityStatistics.timestamp.between(
            func.convert_tz(start_at_0, offset, '+00:00'), func.convert_tz(end_at_23, offset, '+00:00')
        ))
        rows = q.group_by(func.date(func.convert_tz(UserActivityStatistics.timestamp, '+00:00', offset))).\
            order_by("timestamp").all()
    except Exception as e:
        logger.error('Get user activity statistics failed:', e)
        rows = list()

    res = list()
    for row in rows:
        res.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.number))
    return res
