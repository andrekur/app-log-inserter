from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from contextlib import asynccontextmanager

from db.connector import get_db, get_ch_client
from db.crud import insert_logs
from db.models import LogEntryModel

SECONDS_INTERVAL = 30

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    scheduler.add_job(
        log_buffer_manager.send_buffered_logs,
        trigger=IntervalTrigger(seconds=SECONDS_INTERVAL),
        id='send_logs',
        replace_existing=True
    )
    scheduler.start()

    yield

    scheduler.shutdown()

    await log_buffer_manager.flush_on_shutdown()


class LogBufferManager:
    def __init__(self):
        self.batch_size = 100
        self.is_running = False

    async def start(self):
        """Запуск"""
        self.is_running = True

    async def stop(self):
        """Остановка"""
        self.is_running = False

    async def send_buffered_logs(self):
        """Отправка накопленных логов"""

        session = next(get_db())
        client = next(get_ch_client())
        count = 0

        try:
            unsent_logs = session.query(LogEntryModel).filter(LogEntryModel.is_sent == False).limit(self.batch_size).all()

            if not unsent_logs:
                return

            success = insert_logs(client, unsent_logs)

            if success:
                log_ids = [log.id for log in unsent_logs]
                count = len(log_ids)

                update_query = session.query(LogEntryModel).filter(
                    LogEntryModel.id.in_(log_ids)
                )

                update_query.update({
                    LogEntryModel.is_sent: True,
                    LogEntryModel.sent_at: datetime.now()
                })

                session.commit()
        finally:
            session.close()
            client.disconnect()

        return count

    async def flush_on_shutdown(self):
        """Принудительная отправка при завершении работы"""
        await self.send_buffered_logs()


log_buffer_manager = LogBufferManager()
