# GreenBankService

Go-сервис банковских счетов. Владелец: Go-команда.

## API

- `GET /health`
- `POST /accounts` — создать счёт;
- `POST /accounts/{accountId}/top-ups` — пополнить счёт.

Денежные значения — целые числа в минимальных единицах валюты. `correlationId` обеспечивает идемпотентность команд.

События `AccountCreated` и `AccountToppedUp` сохраняются вместе с бизнес-изменениями в одной PostgreSQL-транзакции (`Transactional Outbox`). Фоновый publisher конкурентно выбирает записи через `FOR UPDATE SKIP LOCKED`, доставляет их в Kafka с гарантией at-least-once и повторяет ошибки с exponential backoff. Consumer обязан сохранять идемпотентность по `eventId`.

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
- `KAFKA_BROKERS`;
- `KAFKA_TOPIC`.
