from datetime import datetime
from typing import List

from contextlib import contextmanager
import asyncio
from clickhouse_driver import Client
from dotenv import dotenv_values
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


CONFIG = dotenv_values('_CI/.env')

USER = CONFIG['DB_USER']
PASSWORD = CONFIG['DB_PASSWORD']
HOST = CONFIG['DB_HOST']
PORT = int(CONFIG['DB_PORT'])
DB = CONFIG['DB_NAME_DB']

engine = create_engine(
    f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')

Session = sessionmaker(autocommit=False, autoflush=True, bind=engine)

Base = declarative_base()


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


CLICKHOUSE_USER = CONFIG['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = CONFIG['CLICKHOUSE_PASSWORD']
CLICKHOUSE_HOST = CONFIG['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = int(CONFIG['CLICKHOUSE_PORT'])
CLICKHOUSE_DB = CONFIG['CLICKHOUSE_NAME_DB']


class ClickHouseClient:
    def __init__(self):
        self.client = None
    
    def get_client(self):
        """Асинхронное подключение к ClickHouse"""
        return Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
    
    async def insert_logs(self, logs):
        """Вставка логов в ClickHouse"""
        client = self.get_client()

        if not logs:
            return False

        data = [
            log.get_dict_to_send()
            for log in logs
        ]
        
        query = '''INSERT INTO logs_data VALUES'''
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, client.execute, query, data)
        client.disconnect()

        return True


clickhouse_client = ClickHouseClient()
