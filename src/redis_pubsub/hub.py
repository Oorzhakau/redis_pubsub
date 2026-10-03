"""Hub: учёт WS-клиентов, подключённых к ЭТОЙ реплике, и рассылка им сообщений."""

import asyncio
import logging

from fastapi import WebSocket

logger = logging.getLogger(__name__)

# Сколько секунд ждём отправки медленному клиенту, прежде чем считать его мёртвым.
SEND_TIMEOUT = 2.0


class Hub:
    """Держит множество открытых WS-соединений реплики и рассылает им broadcast.

    Один экземпляр на процесс (реплику) — про Redis тут ничего не знает,
    это чисто локальный транспорт «сервер → мои WS-клиенты».
    """

    def __init__(self) -> None:
        """Инициализирует пустой список клиентов."""
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        """Принимает WS-handshake и регистрирует клиента в рассылке.

        Args:
            ws: Уже известный FastAPI-овер WebSocket, рукопожатие ещё не принято.
        """
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        """Убирает клиента из рассылки (идемпотентно)."""
        self._clients.discard(ws)

    @property
    def client_count(self) -> int:
        """Количество подключённых клиентов этой реплики."""
        return len(self._clients)

    async def broadcast(self, text: str) -> None:
        """Отправляет текст всем клиентам, отцепляя мёртвых и медленных.

        Отправка идёт по одному клиенту за раз: asyncio переключается между
        send'ами, поэтому быстрые клиенты не ждут медленных дольше SEND_TIMEOUT.

        Args:
            text: Готовая строка (обычно JSON) для отправки каждому клиенту.
        """
        for ws in list(self._clients):
            try:
                await asyncio.wait_for(ws.send_text(text), timeout=SEND_TIMEOUT)
            except Exception:
                # Клиент закрыл соединение или не успел принять за SEND_TIMEOUT.
                logger.warning("dropping slow/dead WS client", exc_info=True)
                self.disconnect(ws)
