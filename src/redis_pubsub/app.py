"""FastAPI-приложение: WS-выдача событий и эндпоинт публикации в Redis Pub/Sub."""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from redis_pubsub.config import settings
from redis_pubsub.hub import Hub
from redis_pubsub.listener import run_listener
from redis_pubsub.redis_client import make_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

hub = Hub()


class PublishBody(BaseModel):
    """Тело запроса на публикацию события.

    Attributes:
        type: Тип события в терминах домена, например "order_paid".
        payload: Произвольные данные события.
    """

    type: str = Field(examples=["order_paid"])
    payload: dict[str, Any] = Field(default_factory=dict, examples=[{"id": 42}])


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Жизненный цикл приложения: Redis-клиент + задача-слушатель Pub/Sub.

    Слушатель стартует до первого запроса и корректно отменяется при остановке —
    иначе uvicorn убьёт процесс вместе с «висящей» задачей.

    Args:
        app: Экземпляр FastAPI; в ``app.state`` кладём Redis-клиент.

    Yields:
        Управление работающему приложению.
    """
    redis = make_redis()
    await redis.ping()
    task = asyncio.create_task(run_listener(redis, hub))
    app.state.redis = redis
    logger.info("replica %r up, redis connected", settings.replica_id)
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await redis.aclose()
        logger.info("replica %r stopped", settings.replica_id)


app = FastAPI(title="redis-pubsub", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, Any]:
    """Проверка жизни реплики: показывает, к какой именно реплике вы попали."""
    return {"replica": settings.replica_id, "ws_clients": hub.client_count}


@app.post("/publish")
async def publish(body: PublishBody, request: Request) -> dict[str, Any]:
    """Публикует событие в Redis Pub/Sub.

    PUBLISH уходит в канал, и его получают ВСЕ подписчики канала — то есть
    listener'ы обеих реплик, а значит и все WS-клиенты на всех репликах.
    Ответ Redis — число подписчиков, забравших сообщение: 0 означает, что
    в этот момент слушателей не было (и событие потеряно — природа Pub/Sub).

    Args:
        body: Тип и payload события.
        request: Нужен, чтобы достать Redis-клиент из ``app.state``.

    Returns:
        Служебная информация о публикации: канал, число получателей-подписчиков,
        через какую реплику опубликовано.
    """
    redis = request.app.state.redis
    envelope = {
        "type": body.type,
        "payload": body.payload,
        "published_via": settings.replica_id,
    }
    receivers = await redis.publish(settings.events_channel, json.dumps(envelope))
    logger.info("published %r, subscribers: %s", body.type, receivers)
    return {
        "channel": settings.events_channel,
        "subscribers_received": receivers,
        "published_via": settings.replica_id,
    }


@app.websocket("/ws/events")
async def ws_events(ws: WebSocket) -> None:
    """Отдаёт клиенту поток событий, приходящих из Redis Pub/Sub.

    После подключения клиент получает ``hello`` с именем реплики — так видно,
    за какой именно репликой он висит (nginx балансирует между app1/app2).
    Дальше соединение просто живёт: входящие сообщения клиента игнорируются,
    важно лишь дождаться close/disconnect.

    Args:
        ws: WebSocket-соединение от FastAPI.
    """
    await hub.connect(ws)
    await ws.send_text(json.dumps({"type": "hello", "replica": settings.replica_id}))
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)


def run() -> None:
    """Entry-point для запуска uvicorn из скрипта `server`."""
    uvicorn.run(
        "redis_pubsub.app:app",
        host=settings.host,
        port=settings.port,
    )


if __name__ == "__main__":
    uvicorn.run(app, host=settings.host, port=settings.port)