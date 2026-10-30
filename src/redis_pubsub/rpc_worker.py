"""Worker кейса 3: обслуживает очередь RPC-запросов поверх Redis Lists.

Процесс живёт отдельно от API-реплик (как consumer в Kafka-схеме): в цикле
забирает запросы из очереди `rpc:requests` (BRPOP — блокируется, пока очередь
пуста), обрабатывает и отвечает в личный список запросившегося
(`rpc:reply:{correlation_id}`, LPUSH + EXPIRE одной транзакцией).

Запуск: `make worker` (или `REPLICA_ID=worker1 uv run rpc-worker`).
"""

import asyncio
import json
import logging
from typing import Any

from redis.asyncio import Redis

from redis_pubsub.config import settings
from redis_pubsub.redis_client import make_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# TTL ключа ответа: если view упал и не забрал ответ, ключ не должен висеть вечно.
REPLY_TTL_SECONDS = 60


async def handle(payload: dict[str, Any]) -> dict[str, Any]:
    """Демо-обработка запроса: удваивает поле ``value``.

    В реальной задаче здесь был бы вызов сервиса — нам важно, что worker
    вообще отдельный процесс со своей скоростью.

    Args:
        payload: Данные запроса из очереди.

    Returns:
        Результат обработки.
    """
    await asyncio.sleep(0.2)  # имитация работы, чтобы ожидание view было заметно
    return {"result": payload.get("value", 0) * 2}


async def run_worker(redis: Redis) -> None:
    """Бесконечный цикл: запрос из очереди → обработка → ответ в личный список.

    BRPOP с timeout=0 блокируется, пока в очереди нет запросов — отдельного
    sleep не нужно: Redis сам разбудит worker'а, когда view что-то положит.

    Args:
        redis: Async-клиент Redis.
    """
    logger.info("rpc worker up, queue=%r", settings.rpc_requests_queue)
    while True:
        item = await redis.brpop(settings.rpc_requests_queue, timeout=0)
        if item is None:  # timeout=0 → недостижимо; защита от сюрпризов версий
            continue
        _, raw = item
        req = json.loads(raw)
        correlation_id = req["correlation_id"]
        logger.info("got request %s, payload=%s", correlation_id, req["payload"])

        reply_key = f"{settings.rpc_reply_prefix}{correlation_id}"
        try:
            result = await handle(req["payload"])
            reply = {"ok": True, "worker": settings.replica_id, **result}
        except Exception as exc:
            # Ошибка обработки — тоже ответ: view не должен ждать таймаут
            # из-за упавшего handler'а.
            logger.exception("request %s failed", correlation_id)
            reply = {"ok": False, "worker": settings.replica_id, "error": str(exc)}

        # LPUSH + EXPIRE одной транзакцией: ответ и его TTL появятся атомарно.
        # Если view уже умер — ключ сам исчезнет через REPLY_TTL_SECONDS.
        async with redis.pipeline(transaction=True) as pipe:
            pipe.lpush(reply_key, json.dumps(reply))
            pipe.expire(reply_key, REPLY_TTL_SECONDS)
            await pipe.execute()
        logger.info("answered request %s", correlation_id)


async def main() -> None:
    """Подключается к Redis и запускает цикл обработки запросов."""
    redis = make_redis()
    await redis.ping()
    try:
        await run_worker(redis)
    finally:
        await redis.aclose()


def run() -> None:
    """Entry-point для запуска worker'а из скрипта `rpc-worker`."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("rpc worker stopped")


if __name__ == "__main__":
    run()
