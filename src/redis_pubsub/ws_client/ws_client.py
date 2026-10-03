"""Ручной WS-клиент: слушает события с указанной реплики (для экспериментов).

Использование:
    uv run python redis_pubsub.ws_client --port 8001
    uv run python redis_pubsub.ws_client --url ws://localhost:8002/ws/events
"""

import argparse
import asyncio

import websockets


async def listen(url: str) -> None:
    """Подключается к WS и печатает каждое входящее сообщение.

    Args:
        url: Полный адрес WS-эндпоинта, например ws://localhost:8001/ws/events.
    """
    async with websockets.connect(url) as ws:
        print(f"connected to {url}")
        async for raw in ws:
            print(raw)


def main() -> None:
    """Разбирает аргументы командной строки и запускает слушателя."""
    parser = argparse.ArgumentParser(description="Слушатель WS-событий демо-проекта")
    parser.add_argument(
        "--port", type=int, default=8001, help="порт реплики (по умолчанию 8001)"
    )
    parser.add_argument("--url", default=None, help="полный URL вместо --port")
    args = parser.parse_args()
    url = args.url or f"ws://localhost:{args.port}/ws/events"
    asyncio.run(listen(url))


if __name__ == "__main__":
    main()
