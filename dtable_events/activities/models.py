# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, DateTime, Text, Index

from dtable_events.db import Base


class Activities(Base):
    __tablename__ = 'activities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dtable_uuid = Column(String(length=36), nullable=False, index=True)
    row_id = Column(String(length=36), nullable=False, index=True)
    op_user = Column(String(length=255), nullable=False)
    op_type = Column(String(length=128), nullable=False)
    op_time = Column(DateTime, nullable=False, index=True)
    detail = Column(Text, nullable=False)
    op_app = Column(String(length=255))

    __table_args__ = (Index('ix_activities_op_time_dtable_uuid', 'op_time', 'dtable_uuid'),)

    def __init__(self, dtable_uuid, row_id, op_user, op_type, op_time, detail, op_app):
        self.dtable_uuid = dtable_uuid
        self.row_id = row_id
        self.op_user = op_user
        self.op_type = op_type
        self.op_time = op_time
        self.detail = detail
        self.op_app = op_app
