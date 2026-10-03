.RECIPEPREFIX := >

# Путь до compose-файла и имя проекта (видно в `docker ps`)
COMPOSE_FILE := docker/compose.yml
PROJECT      := redis-pubsub

COMPOSE := docker compose -p $(PROJECT) -f $(COMPOSE_FILE) --env-file .env

.DEFAULT_GOAL := help

TYPE ?= order_paid

.PHONY: help certs up down restart logs ps app1 app2 ws1 ws2 publish

help: ## Показать список доступных команд
>@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
>| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

certs:
>bash docker/generate_redis_certs.sh
>bash docker/generate_client_cert.sh

up: ## Поднять сервисы в фоне
>$(COMPOSE) up -d

down: ## Остановить и удалить контейнеры
>$(COMPOSE) down

restart: ## Перезапустить сервисы
>$(COMPOSE) restart

logs: ## Хвост логов (Ctrl+C для выхода)
>$(COMPOSE) logs -f --tail=100

ps: ## Статус сервисов
>$(COMPOSE) ps

app1: ## Запустить реплику app1 на :8001 (Ctrl+C — остановить)
>@REPLICA_ID=app1 PORT=8001 uv run server

app2: ## Запустить реплику app2 на :8002 (Ctrl+C — остановить)
>@REPLICA_ID=app2 PORT=8002 uv run server

ws1: ## WS-клиент к реплике app1
>@uv run ws --port 8001

ws2: ## WS-клиент к реплике app2
>@@uv run ws --port 8002

publish: ## Опубликовать событие в Redis (make publish TYPE=order_paid)
>@curl -s -X POST localhost:8001/publish -H 'content-type: application/json' -d '{"type":"$(TYPE)","payload":{"id":42}}' && echo
