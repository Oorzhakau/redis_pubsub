.RECIPEPREFIX := >

# Путь до compose-файла и имя проекта (видно в `docker ps`)
COMPOSE_FILE := docker/compose.yml
PROJECT      := redis-pubsub

COMPOSE := docker compose -p $(PROJECT) -f $(COMPOSE_FILE) --env-file .env
certs: $(CERT_FILES)

.DEFAULT_GOAL := help

.PHONY: help up down restart logs ps test

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
