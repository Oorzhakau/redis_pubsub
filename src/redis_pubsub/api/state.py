"""Кейс 2 — notify + fetch: сигнал в канал, состояние читается из Redis.

Паттерн: в канал уходит крошечный сигнал «state:changed» БЕЗ данных, а
актуальное состояние (данные + версия) каждый раз читается из Redis. Реплика,
получив сигнал, делает один fetch снапшота и рассылает его своим WS-клиентам.
Подключившийся клиент сразу получает актуальный снапшот — пропущенные дельты
восстанавливать не нужно, снапшот их перекрывает.
"""

import json
import logging
from typing import Any, Literal

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from redis_pubsub.config import settings
from redis_pubsub.hub import Hub

logger = logging.getLogger(__name__)

# Hash со счётчиками заказов по статусам: поле = статус, значение = количество.
STATE_HASH = "state:orders"
# Монотонная версия состояния: растёт на 1 при каждом изменении.
STATE_VERSION_KEY = "state:version"
# Сигнал в канал: намеренно без данных — только «состояние изменилось».
STATE_CHANGED_SIGNAL = json.dumps({"type": "state_changed"})

OrderStatus = Literal["new", "paid", "shipped", "cancelled"]

# WS-клиенты состояния этой реплики (по одному Hub на реплику и на кейс).
hub = Hub()

router = APIRouter()


class OrderBody(BaseModel):
    """Тело запроса на изменение счётчиков заказов (кейс 2).

    Attributes:
        status: Статус заказа, счётчик которого нужно увеличить.
    """

    status: OrderStatus = Field(examples=["paid"])


async def read_state(redis: Redis) -> dict[str, Any]:
    """Читает атомарный снапшот состояния: счётчики + версия.

    Операция MULTI/EXEC (pipeline с ``transaction=True``) нужна, чтобы версия
    гарантированно соответствовала данным. Иначе гонка «прочитал hash, потом
    version» может выдать пару (новая версия, старые данные) — и клиент навсегда
    «застрянет», решив, что уже видел всё до этой версии.

    Args:
        redis: Async-клиент Redis.

    Returns:
        Словарь ``{"version": int, "counts": {status: count}}``.
    """
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hgetall(STATE_HASH)
        pipe.get(STATE_VERSION_KEY)
        counts, version = await pipe.execute()
    return {
        "version": int(version or 0),
        "counts": {status: int(n) for status, n in (counts or {}).items()},
    }


async def bump_order(redis: Redis, status: str) -> dict[str, Any]:
    """Увеличивает счётчик статуса на 1, версию на 1 и шлёт сигнал изменения.

    Счётчик, версия и publish выполняются в одной транзакции (MULTI/EXEC) —
    атомарно: никто не увидит версию без данных. В канал уходит только сигнал;
    кто его получил, сам читает снапшот (это и есть fetch).

    Args:
        redis: Async-клиент Redis.
        status: Статус заказа (new/paid/shipped/cancelled).

    Returns:
        Актуальный снапшот после изменения (см. :func:`read_state`).
    """
    async with redis.pipeline(transaction=True) as pipe:
        pipe.hincrby(STATE_HASH, status, 1)
        pipe.incr(STATE_VERSION_KEY)
        pipe.publish(settings.state_channel, STATE_CHANGED_SIGNAL)
        await pipe.execute()
    return await read_state(redis)


async def on_message(redis: Redis, _data: str) -> None:
    """Обработчик канала state:changed: сигнал без данных → fetch → push клиентам.

    Тянет снапшот из Redis и рассылает его WS-клиентам (notify → fetch → push).

    Один fetch на реплику, а не на каждого клиента: 10 000 клиентов, получивших
    сигнал, создали бы 10 000 одинаковых запросов к Redis (stampede).

    Args:
        redis: Async-клиент Redis — по нему читаем снапшот (это и есть fetch).
        _data: Тело сигнала намеренно пустое (см. ``STATE_CHANGED_SIGNAL``).
    """
    snapshot = await read_state(redis)
    await hub.broadcast(json.dumps({"type": "state_snapshot", **snapshot}))

@router.get("/state")
async def state(request: Request) -> dict[str, Any]:
    """Актуальный снапшот состояния: счётчики заказов и версия.

    Args:
        request: Нужен, чтобы достать Redis-клиент из ``app.state``.

    Returns:
        Снапшот ``{"replica": ..., "version": N, "counts": {...}}``.
    """
    redis = request.app.state.redis
    return {"replica": settings.replica_id, **await read_state(redis)}


@router.post("/orders")
async def orders(body: OrderBody, request: Request) -> dict[str, Any]:
    """Увеличивает счётчик статуса и оповещает всех о новом состоянии.

    Изменение и сигнал — одна транзакция в Redis (см. :func:`bump_order`).
    Клиентам через WS уйдёт свежий снапшот (notify + fetch на стороне реплик).

    Args:
        body: Статус заказа.
        request: Нужен, чтобы достать Redis-клиент из ``app.state``.

    Returns:
        Снапшот после изменения + имя реплики, через которую его сделали.
    """
    redis = request.app.state.redis
    snapshot = await bump_order(redis, body.status)
    logger.info("order %r bumped, version=%s", body.status, snapshot["version"])
    return {"changed_via": settings.replica_id, **snapshot}


@router.websocket("/ws/state")
async def ws_state(ws: WebSocket) -> None:
    """Отдаёт клиенту актуальное состояние и его обновления (кейс 2).

    При подключении клиент получает СРАЗУ самое актуальное состояние — снапшот
    с версией. Пропущенные за время обрыва дельты не нужны: снапшот их перекрывает.
    Дальше каждое изменение приходит тем же снапшотом с новой версией — клиент
    просто перезаписывает локальное состояние.

    Args:
        ws: WebSocket-соединение от FastAPI.
    """
    await hub.connect(ws)
    redis = ws.app.state.redis
    await ws.send_text(json.dumps({"type": "hello", "replica": settings.replica_id}))
    await ws.send_text(
        json.dumps({"type": "state_snapshot", **await read_state(redis)})
    )
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)
