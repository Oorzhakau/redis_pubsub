"""Слушатель Redis Pub/Sub: пересылает тело каждого сообщения канала обработчику."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from redis.asyncio import Redis

from redis_pubsub.config import settings

logger = logging.getLogger(__name__)

# Задержка перед переподключением после обрыва соединения с Redis.
RECONNECT_DELAY = 1.0


async def run_listener(
    redis: Redis,
    channel: str,
    handler: Callable[[Redis, str], Awaitable[None]],
) -> None:
    """Бесконечно читает канал и отдаёт данные каждого сообщения обработчику.

    Задача запускается в lifespan приложения и живёт всё время работы реплики.
    Pub/Sub — fire-and-forget: пока подписчик отвалился, сообщения теряются,
    поэтому при обрыве сразу переподключаемся и поднимаем подписку заново.

    Args:
        redis: Async-клиент Redis (создаётся в lifespan).
        channel: Имя канала Pub/Sub.
        handler: Асинхронный обработчик ``(redis, data)`` — Redis передаётся,
            потому что кейсу 2 он нужен для fetch (чтения снапшота).

    Raises:
        asyncio.CancelledError: При остановке приложения (lifespan отменяет задачу).
    """
    while True:
        try:
            pubsub = redis.pubsub()
            await pubsub.subscribe(channel)
            logger.info("subscribed to channel %r", channel)
            async for message in pubsub.listen():
                # Первое сообщение после subscribe — служебное ("subscribe"), его
                # пропускаем; интересуют только "message" с данными канала.
                if message["type"] != "message":
                    continue
                logger.info(
                    "[%s] %s: %r", settings.replica_id, channel, message["data"]
                )
                await handler(redis, message["data"])
        except asyncio.CancelledError:
            raise
        except Exception:
            # Обрыв Redis, сетевые ошибки и т.п. — логируем и поднимаем подписку заново.
            logger.exception("pubsub listener for %r failed, reconnecting", channel)
            await asyncio.sleep(RECONNECT_DELAY)
