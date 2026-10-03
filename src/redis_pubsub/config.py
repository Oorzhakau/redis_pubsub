"""Конфигурация приложения: подключение к Redis (mTLS + ACL) и идентичность реплики."""

import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from pydantic_settings import BaseSettings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CERTS_DIR = PROJECT_ROOT / "docker" / "certs"

load_dotenv(find_dotenv())


class Settings(BaseSettings):
    """Настройки реплики и подключения к Redis.

    Attributes:
        replica_id: Имя реплики (app1/app2) — показываем его клиентам,
            чтобы видеть, к какой именно реплике они подключены.
        redis_host: Хост Redis (из хоста это localhost, порт проброшен в compose).
        redis_port: Порт Redis (см. REDIS_PORT в .env).
        redis_user: ACL-пользователь Redis (см. users.acl.template).
        redis_password: Пароль ACL-пользователя.
        tls_ca: Корневой сертификат нашего CA — им проверяем сервер Redis.
        tls_cert: Клиентский сертификат (mTLS: сервер тоже требует сертификат).
        tls_key: Приватный ключ клиентского сертификата.
        events_channel: Имя канала Pub/Sub для broadcast-событий (кейс 1).
        state_channel: Имя канала сигналов «состояние изменилось» (кейс 2).
    """

    port: int = 8000
    host: str = "0.0.0.0"
    replica_id: str
    redis_host: str
    redis_port: int
    redis_user: str
    redis_password: str
    tls_ca: Path
    tls_cert: Path
    tls_key: Path
    events_channel: str = "events"
    state_channel: str = "state:changed"


settings = Settings(
    replica_id=os.environ.get("REPLICA_ID", "app1"),
    redis_host=os.environ.get("REDIS_HOST", "localhost"),
    redis_port=int(os.environ.get("REDIS_PORT", "16379")),
    redis_user=os.environ.get("REDIS_USER", "app"),
    redis_password=os.environ.get("REDIS_PASSWORD", ""),
    tls_ca=CERTS_DIR / "ca.crt",
    tls_cert=CERTS_DIR / "client.crt",
    tls_key=CERTS_DIR / "client.key",
)
