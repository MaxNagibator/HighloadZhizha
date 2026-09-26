# NotificationService

.NET 10 Kafka consumer команды уведомлений.

Сервис использует отдельную consumer group `notification-service-v1`, читает `AccountCreated` и `AccountToppedUp`, формирует текст SMS и эмулирует отправку структурированным событием `SMS_SENT` в stdout. Grafana Alloy собирает stdout, отправляет его в Loki, а Grafana используется для поиска и анализа доставок.

Номера получателей искусственные и маскированные; реальные персональные данные сервис не хранит. `eventId` используется как стабильный `notificationId`, поэтому возможные повторы at-least-once доставки легко обнаруживаются в Loki.

Пример LogQL:

```logql
{namespace="highload-zhizha", app="notification-service"} |= "SMS_SENT"
```

## Сборка

```powershell
dotnet restore
dotnet build -c Release --no-restore
.\scripts\build-local.ps1
```

