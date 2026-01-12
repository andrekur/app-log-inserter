from pydantic import BaseModel
from datetime import datetime


class LogEntryDataIn(BaseModel):
    log_text: str
    
    class Config:
        from_attributes = True


class LogEntryDataOut(BaseModel):
    timestamp: datetime
