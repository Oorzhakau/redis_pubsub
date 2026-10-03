"""Async-клиент Redis: mTLS-подключение с клиентским сертификатом и ACL-аутентификацией."""

from redis.asyncio import Redis

from redis_pubsub.config import settings


def make_redis() -> Redis:
    """Создаёт async-клиент Redis поверх TLS с обязательной проверкой сервера.

    Два уровня защиты, потому что так настроена наша инфраструктура:
    1. TLS + проверка сертификата сервера по нашему CA (``ssl_ca_certs``)
       и его hostname (``ssl_check_hostname``) — защита от подмены сервера.
    2. mTLS: сервер запущен с ``--tls-auth-clients yes``, поэтому он требует
       наш клиентский сертификат (``ssl_certfile``/``ssl_keyfile``).

    Плюс ACL-аутентификация (username + password из users.acl.template).

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
        decode_responses=True,
    )
