# GreenBankService

Go-сервис банковских счетов. Владелец: Go-команда.

## API

- `GET /health`
- `POST /accounts` — создать счёт;
- `POST /accounts/{accountId}/top-ups` — пополнить счёт и начислить cashback; тело запроса содержит `amount`, `correlationId` и категорию (`groceries`, `transport`, `entertainment`, `other`).

Денежные значения — целые числа в минимальных единицах валюты. `correlationId` обеспечивает идемпотентность команд.

Ставки, минимальные суммы и лимиты категорий предзаполняются в Redis. Персональная надбавка и месячный лимит находятся в `account_cashback_settings` и намеренно читаются из PostgreSQL на каждом пополнении — это оставленная точка для профилирования и последующей оптимизации.

События `AccountCreated`, `AccountToppedUp` и отдельное зачисление `CashbackGranted` сохраняются вместе с бизнес-изменениями в одной PostgreSQL-транзакции (`Transactional Outbox`). Фоновый publisher конкурентно выбирает записи через `FOR UPDATE SKIP LOCKED`, доставляет их в Kafka с гарантией at-least-once и повторяет ошибки с exponential backoff. Consumer обязан сохранять идемпотентность по `eventId`.

## Проверка

```powershell
go test ./...
```

## Локальный образ

```powershell
.\scripts\build-local.ps1
```

Скрипт собирает `highload/green-bank-service:dev` и импортирует образ в кластер `k3d-local-dev`.

Конфигурация окружения:

- `DATABASE_URL`;
- `REDIS_ADDR`;
- `KAFKA_BROKERS`;
- `KAFKA_TOPIC`.
