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


def get_ch_client():
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    try:
        yield client
    finally:
        client.disconnect()
