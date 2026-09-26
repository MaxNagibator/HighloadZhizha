# HighloadZhizha workspace

Этот каталог объединяет три независимых репозитория, которыми владеют разные команды:

| Репозиторий | Команда | Ответственность |
|---|---|---|
| `GreenBankService` | Go | REST API счетов, PostgreSQL-транзакции, публикация событий в Kafka |
| `PaymentService` | .NET | Kafka consumer и идемпотентная обработка банковских событий |
| `MemoryZhorka` | Go | Демонстрация Kubernetes memory limit и восстановления после OOMKilled |
| `NotificationService` | .NET | Kafka consumer и эмуляция SMS через структурированные Loki-логи |
| `HighloadZhizha.Platform` | DevOps/SRE | Kubernetes, PostgreSQL, Kafka, Grafana, Loki, Alloy и локальный deploy |

Репозитории не используют общий исходный код и могут версионироваться и выпускаться независимо.

## Локальная сборка и развёртывание

```powershell
cd .\GreenBankService
.\scripts\build-local.ps1

cd ..\PaymentService
.\scripts\build-local.ps1

cd ..\MemoryZhorka
.\scripts\build-local.ps1

cd ..\NotificationService
.\scripts\build-local.ps1

cd ..\HighloadZhizha.Platform
.\scripts\deploy.ps1
```

API: `http://localhost:9080`  
Grafana: `http://grafana.localhost:9080` (`admin` / `admin` для локального стенда).
