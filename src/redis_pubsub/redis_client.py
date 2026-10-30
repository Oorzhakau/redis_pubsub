"""Async-клиент Redis: mTLS-подключение с клиентским сертификатом и ACL-аутентификацией."""

from redis.asyncio import Redis

from redis_pubsub.config import settings


def make_redis(socket_timeout: float | None = None) -> Redis:
    """Создаёт async-клиент Redis поверх TLS с обязательной проверкой сервера.

    Два уровня защиты, потому что так настроена наша инфраструктура:
    1. TLS + проверка сертификата сервера по нашему CA (``ssl_ca_certs``)
       и его hostname (``ssl_check_hostname``) — защита от подмены сервера.
    2. mTLS: сервер запущен с ``--tls-auth-clients yes``, поэтому он требует
       наш клиентский сертификат (``ssl_certfile``/``ssl_keyfile``).

    Плюс ACL-аутентификация (username + password из users.acl.template).

    Args:
        socket_timeout: Клиентский таймаут чтения ответа, сек. По умолчанию
            ``None`` (ждать ответа вечно). Это принципиально для блокирующих
            команд (BLPOP/BRPOP): в redis-py по умолчанию ``socket_timeout=5``,
            и он обрывает чтение раньше серверного таймаута команды — команда
            падает ``TimeoutError`` вместо штатного ``None``, а retry повторяет
            её и растягивает ожидание в ~12 раз (10 попыток + backoff).

    Returns:
        Клиент с ленивым соединением: команда установит соединение при первом
        обращении. ``decode_responses=True`` — ответы как str, а не bytes.
    """
    return Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        username=settings.redis_user,
        password=settings.redis_password,
        ssl=True,
        ssl_ca_certs=str(settings.tls_ca),
        ssl_certfile=str(settings.tls_cert),
        ssl_keyfile=str(settings.tls_key),
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        socket_timeout=socket_timeout,
        decode_responses=True,
    )
