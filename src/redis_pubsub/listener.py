"""Слушатель Redis Pub/Sub: пересылает события из канала в Hub этой реплики."""

import asyncio
import logging

from redis.asyncio import Redis

from redis_pubsub.config import settings
from redis_pubsub.hub import Hub

logger = logging.getLogger(__name__)

# Задержка перед переподключением после обрыва соединения с Redis.
RECONNECT_DELAY = 1.0


async def run_listener(redis: Redis, hub: Hub) -> None:
    """Бесконечно читает канал событий и рассылает сообщения WS-клиентам.

    Задача запускается в lifespan приложения и живёт всё время работы реплики.
    Pub/Sub — fire-and-forget: пока подписчик отвалился, сообщения теряются,
    поэтому сразу переподключаемся и держим подписку висящей.

    Args:
        redis: Async-клиент Redis (создаётся в lifespan).
        hub: Локальный транспорт сообщений WS-клиентам этой реплики.

    Raises:
        asyncio.CancelledError: При остановке приложения (lifespan отменяет задачу).
    """
    while True:
        try:
            pubsub = redis.pubsub()
            await pubsub.subscribe(settings.events_channel)
            logger.info("subscribed to channel %r", settings.events_channel)
            async for message in pubsub.listen():
                # Первое сообщение после subscribe — служебное ("subscribe"), его
                # пропускаем; интересуют только "message" с данными канала.
                if message["type"] != "message":
                    continue
                logger.info(f"[{settings.replica_id}] received %r", message["data"])
                await hub.broadcast(message["data"])
        except asyncio.CancelledError:
            raise
        except Exception:
            # Обрыв Redis, сетевые ошибки и т.п. — логируем и поднимаем подписку заново.
            logger.exception("pubsub listener failed, reconnecting")
            await asyncio.sleep(RECONNECT_DELAY)
