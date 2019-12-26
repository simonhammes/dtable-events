# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, DateTime

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
