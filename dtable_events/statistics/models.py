# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text

from dtable_events.db import Base


class UserActivityStatistics(Base):
    __tablename__ = 'user_activity_statistics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_time_md5 = Column(String(length=32), unique=True)
    username = Column(String(length=255), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    org_id = Column(Integer, nullable=False)

    def __init__(self, user_time_md5, username, timestamp, org_id):
        self.user_time_md5 = user_time_md5
        self.username = username
        self.timestamp = timestamp
        self.org_id = org_id


class EmailSendingLog(Base):
    __tablename__ = 'email_sending_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(length=255), nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    host = Column(String(length=255), nullable=False)
    success = Column(Boolean, nullable=False, default=False)

    def __init__(self, username, timestamp, host, success):
        self.username = username
        self.timestamp = timestamp
        self.host = host
        self.success = success
