from datetime import datetime

from fastapi import FastAPI, Depends, status, Request

from .sheduler import lifespan, log_buffer_manager

from db.connector import Session, get_db
from db.schemas import LogEntryDataIn, LogEntryDataOut
from db import crud


app = FastAPI(lifespan=lifespan)


@app.get("/")
def root(db: Session = Depends(get_db)):
    """Проверка работы API"""
    return {
        'status': 'ok',
        'service': 'ClickHouse Log Buffer',
        'buffered_logs': crud.get_count_logs(db),
        'timestamp': datetime.now().isoformat()
    }


@app.post(
    '/log/',
    status_code=status.HTTP_201_CREATED,
    response_model=LogEntryDataOut
)
def create_log(request: Request, log_data: LogEntryDataIn,
                db: Session = Depends(get_db)):
    return crud.create_log(db, input_data=log_data, host=request.client.host)


@app.post(
    '/log/force-send',
    status_code=status.HTTP_200_OK,
)
async def force_send():
    """Принудительная отправка буфера"""
    result = await log_buffer_manager.send_buffered_logs()
    return {'result': result}
