"""Кейс 1 — broadcast-события поверх Redis Pub/Sub (fan-out).

Паттерн: событие публикуется в канал (`PUBLISH`), его получают ВСЕ подписчики
канала — то есть listener каждой реплики, — и рассылают своим WS-клиентам.
Тело события летит в канал как есть, без чтения Redis. Это fire-and-forget:
пропущенное при обрыве теряется навсегда.
"""

import json
import logging
from typing import Any

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from redis_pubsub.config import settings
from redis_pubsub.hub import Hub

logger = logging.getLogger(__name__)

# Хранилище всех WS-клиентов этой реплики (по одному Hub на реплику и на кейс).
hub = Hub()

router = APIRouter()


class PublishBody(BaseModel):
    """Тело запроса на публикацию broadcast-события.

    Attributes:
        type: Тип события в терминах домена, например "order_paid".
        payload: Произвольные данные события.
    """

    type: str = Field(examples=["order_paid"])
    payload: dict[str, Any] = Field(default_factory=dict, examples=[{"id": 42}])


async def on_message(_redis: Redis, data: str) -> None:
    """Обработчик канала events: тело сообщения — готовый JSON, просто fan-out.

    Args:
        _redis: Кейсу 1 не нужен — данные уже лежат в сообщении канала.
        data: Тело сообщения из Pub/Sub (строка JSON).
    """
    await hub.broadcast(data)


@router.post("/publish")
async def publish(body: PublishBody, request: Request) -> dict[str, Any]:
    """Публикует broadcast-событие в Redis Pub/Sub (кейс 1).

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


@router.websocket("/ws/events")
async def ws_events(ws: WebSocket) -> None:
    """Отдаёт клиенту поток broadcast-событий из Redis Pub/Sub (кейс 1).

    После подключения клиент получает ``hello`` с именем реплики — так видно,
    за какой именно репликой он висит. Дальше соединение просто живёт: входящие
    сообщения клиента игнорируются, важно лишь дождаться close/disconnect.

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
