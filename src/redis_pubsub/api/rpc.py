"""Кейс 3 — синхронный request/reply (RPC) поверх Redis Lists.

Паттерн: view генерирует correlation_id, кладёт запрос в очередь `rpc:requests`
(LPUSH) и блокируется в ожидании ответа в личном списке `rpc:reply:{id}`
(BLPOP с таймаутом). Worker забирает запрос из очереди (BRPOP), обрабатывает
и отвечает в личный список view. Kafka не нужна: роль топиков играют два
списка Redis.

Это НЕ Pub/Sub: сообщение — не эфир, а запись в списке. Ответ, пришедший
раньше, чем view успел начать ждать, просто лежит в списке до BLPOP — поэтому
порядок «отправил → слушаю» здесь безопасен.
"""

import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from redis_pubsub.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Сколько секунд view ждёт ответ worker'а, прежде чем сдаться (HTTP 504).
REPLY_TIMEOUT = 5.0


class RpcBody(BaseModel):
    """Тело RPC-запроса к worker'у (кейс 3).

    Attributes:
        payload: Произвольные данные запроса — worker решает, что с ними делать.
    """

    payload: dict[str, Any] = Field(default_factory=dict, examples=[{"value": 21}])


@router.post("/rpc")
async def rpc_call(body: RpcBody, request: Request) -> dict[str, Any]:
    """Отправляет запрос worker'у и ждёт ответ по correlation_id (кейс 3).

    Три шага:
    1. Генерируем correlation_id — он связывает запрос с ответом между собой.
    2. LPUSH в очередь запросов: worker заберёт его с другого конца (BRPOP),
       поэтому очередь FIFO — запросы обрабатываются в порядке поступления.
    3. BLPOP личного списка ответов с таймаутом: блокируемся до ответа worker'а.

    Args:
        body: Данные запроса.
        request: Нужен, чтобы достать Redis-клиент из ``app.state``.

    Returns:
        Ответ worker'а с полем ``correlation_id`` для наглядности.

    Raises:
        HTTPException: 504, если worker не ответил за ``REPLY_TIMEOUT`` секунд.
    """
    redis = request.app.state.redis
    correlation_id = uuid.uuid4().hex
    reply_key = f"{settings.rpc_reply_prefix}{correlation_id}"

    message = json.dumps({"correlation_id": correlation_id, "payload": body.payload})
    await redis.lpush(settings.rpc_requests_queue, message)
    logger.info("rpc request %s queued", correlation_id)

    item = await redis.blpop(reply_key, timeout=REPLY_TIMEOUT)
    if item is None:
        # Таймаут — это нормальный исход: worker упал или просто слишком долгий.
        # Опоздавший ответ не пропадёт — worker повесит на него TTL (см. worker).
        logger.warning("rpc request %s timed out", correlation_id)
        raise HTTPException(status_code=504, detail="worker did not answer in time")

    _, raw = item
    logger.info("rpc request %s answered", correlation_id)
    return {"correlation_id": correlation_id, **json.loads(raw)}
