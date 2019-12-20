# -*- coding: utf-8 -*-
from hashlib import md5


def save_user_activity_stat(session, msg):
    username = msg['username']
    timestamp = msg['timestamp']

    user_time_md5 = md5((username + timestamp).encode('utf-8')).hexdigest()
    msg['user_time_md5'] = user_time_md5

    cmd = "REPLACE INTO user_activity_statistics (user_time_md5, username, timestamp, org_id)" \
          "values(:user_time_md5, :username, :timestamp, :org_id)"

    session.execute(cmd, msg)
    session.commit()
