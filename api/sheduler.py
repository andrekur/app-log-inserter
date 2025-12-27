from datetime import datetime

import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from typing import List
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager

from db.connector import clickhouse_client, get_db
from db.models import LogEntryModel


scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    scheduler.add_job(
        log_buffer_manager.send_buffered_logs,
        trigger=IntervalTrigger(seconds=10),
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
        self.send_interval = 10000  # секунд
        self.is_running = False

    async def start(self):
        """Запуск периодической отправки"""
        self.is_running = True
        await clickhouse_client.connect()

        await self.send_buffered_logs()

        asyncio.create_task(self._periodic_sender())
    
    async def stop(self):
        """Остановка менеджера"""
        self.is_running = False

        await self.send_buffered_logs()
    
    async def _periodic_sender(self):
        """Периодическая отправка каждые 10 секунд"""
        while self.is_running:
            await asyncio.sleep(self.send_interval)
            await self.send_buffered_logs()
    
    async def send_buffered_logs(self):
        """Отправка накопленных логов"""

        session = next(get_db())
        count = 0

        try:
            unsent_logs = session.query(LogEntryModel).filter(LogEntryModel.is_sent == False).limit(self.batch_size).all()

            if not unsent_logs:
                return

            success = await clickhouse_client.insert_logs(unsent_logs)

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

        return count

    async def flush_on_shutdown(self):
        """Принудительная отправка при завершении работы"""
        await self.send_buffered_logs()


log_buffer_manager = LogBufferManager()
