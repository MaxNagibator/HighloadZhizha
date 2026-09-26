# PaymentService

.NET 10 Worker, обрабатывающий события банковских счетов. Владелец: .NET-команда.

Consumer читает топик `bank.account.events`, сохраняет события в `processed_bank_events` и коммитит Kafka offset только после успешной записи. `event_id` является ключом идемпотентности.

## Проверка

```powershell
dotnet restore
dotnet build -c Release --no-restore
```

## Локальный образ

```powershell
.\scripts\build-local.ps1
```

Скрипт собирает `highload/payment-service:dev` и импортирует образ в кластер `k3d-local-dev`.

Конфигурация окружения:

- `DATABASE_URL`;
- `KAFKA_BROKERS`;
- `KAFKA_TOPIC`.

