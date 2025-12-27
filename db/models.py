import uuid

from sqlalchemy import (
    Column, Integer, String, DateTime,
    Text, Boolean, func
)

from .connector import Base


class LogEntryModel(Base):
    __tablename__ = "log_entries"

    id = Column(Integer, primary_key=True, index=True)
    server_name = Column(String(255), nullable=False)
    log_text = Column(Text, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=func.now())
    is_sent = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    sent_at = Column(DateTime, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'server_name': self.server_name,
            'log_text': self.log_text,
            'timestamp': self.timestamp,
            'is_sent': self.is_sent,
            'sent_at': self.sent_at,
            'created_at': self.created_at
        }

    def get_dict_to_send(self):
        return {
            'uuid': str(uuid.uuid4()),
            'server_name': self.server_name,
            'log_text': self.log_text,
            'timestamp': self.timestamp,
            'sent_at': self.sent_at,
        }