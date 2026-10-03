"""Два API-cases.

Кейс 1 (api/events.py): /publish → PUBLISH → listener каждой реплики →
WS-клиенты /ws/events. Тело события отправляется в канал как есть.

Кейс 2 (api/state.py): /orders → атомарное изменение счётчика и версии + сигнал в
канал → listener каждой реплики читает снапшот из Redis и рассылает его
WS-клиентам /ws/state. При подключении клиент сразу получает актуальное состояние.

Здесь только композиция: lifespan (Redis-клиент + слушатели каналов), подключение
роутеров кейсов и служебный /health.
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI

from redis_pubsub.api import events, state
from redis_pubsub.config import settings
from redis_pubsub.listener import run_listener
from redis_pubsub.redis_client import make_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Жизненный цикл приложения: Redis-клиент + две задачи-слушателя Pub/Sub.

    Слушатели стартуют до первого запроса и корректно отменяются при остановке —
    иначе uvicorn убьёт процесс вместе с «висящими» задачами.

    Args:
        app: Экземпляр FastAPI; в ``app.state`` кладём Redis-клиент.

    Yields:
        Управление работающему приложению.
    """
    redis = make_redis()
    await redis.ping()
    app.state.redis = redis

    tasks = [
        asyncio.create_task(
            run_listener(redis, settings.events_channel, events.on_message)
        ),
        asyncio.create_task(
            run_listener(redis, settings.state_channel, state.on_message)
        ),
    ]
    logger.info("replica %r up, redis connected", settings.replica_id)
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await redis.aclose()
        logger.info("replica %r stopped", settings.replica_id)


app = FastAPI(title="redis-pubsub", lifespan=lifespan)

app.include_router(events.router)
app.include_router(state.router)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Проверка жизни реплики: имя реплики и число WS-клиентов на ней."""
    return {
        "replica": settings.replica_id,
        "events_ws_clients": events.hub.client_count,
        "state_ws_clients": state.hub.client_count,
    }


def run() -> None:
    """Entry-point для запуска uvicorn из скрипта `server`."""
    uvicorn.run(
        "redis_pubsub.app:app",
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)
