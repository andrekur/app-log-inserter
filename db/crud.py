from sqlalchemy.orm import Session

from .schemas import LogEntryDataIn
from .models import LogEntryModel


def create_log(db: Session, input_data: LogEntryDataIn, host: str):
    """
    Вставляет лог в базу для временного хранения
    """
    print(input_data.dict(), flush=True)
    db_log = LogEntryModel(**input_data.dict() | {'server_name': host})

    db.add(db_log)
    db.commit()

    return db_log

def get_count_logs(db: Session):
    return db.query(LogEntryModel).filter(LogEntryModel.is_sent == False).count()

def insert_logs(client, logs):
    """Вставка логов в ClickHouse"""
    if not logs:
        return False

    data = [
        log.get_dict_to_send()
        for log in logs
    ]

    query = '''INSERT INTO logs_data VALUES'''
    client.execute(query, data)
    client.disconnect()

    return True
