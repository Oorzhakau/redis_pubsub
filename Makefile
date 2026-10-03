.RECIPEPREFIX := >

# Путь до compose-файла и имя проекта (видно в `docker ps`)
COMPOSE_FILE := docker/compose.yml
PROJECT      := redis-pubsub

COMPOSE := docker compose -p $(PROJECT) -f $(COMPOSE_FILE) --env-file .env

.DEFAULT_GOAL := help

TYPE ?= order_paid
STATUS ?= paid

.PHONY: help certs up down restart logs ps app1 app2 ws1 ws2 wsstate1 wsstate2 publish state bump

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
>@uv run ws --port 8002

wsstate1: ## WS-клиеgitнт состояния к реплике app1 (кейс 2)
>@uv run ws --url ws://localhost:8001/ws/state

wsstate2: ## WS-клиент состояния к реплике app2 (кейс 2)
>@uv run ws --url ws://localhost:8002/ws/state

publish: ## Опубликовать событие в Redis (make publish TYPE=order_paid)
>@curl -s -X POST localhost:8001/publish -H 'content-type: application/json' -d '{"type":"$(TYPE)","payload":{"id":42}}' && echo

state: ## Текущее состояние счётчиков заказов (GET /state)
>@curl -s localhost:8001/state && echo

order: ## Увеличить счётчик статуса (make bump STATUS=paid)
>@curl -s -X POST localhost:8001/orders -H 'content-type: application/json' -d '{"status":"$(STATUS)"}' && echo
