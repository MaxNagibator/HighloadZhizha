# HighloadZhizha.Platform

Инфраструктурный репозиторий DevOps/SRE-команды.

Содержит Kubernetes-манифесты для:

- GreenBankService и PaymentService;
- MemoryZhorka с демонстрационным лимитом памяти 2 ГиБ;
- NotificationService, эмулирующий SMS структурированными логами в Loki;
- HighloadZhizha.ManagementService с веб-панелью управления нагрузкой;
- PostgreSQL и persistent volume;
- Apache Kafka в KRaft-режиме и Job создания топика;
- Grafana, Loki, Alloy и Prometheus со сбором CPU/памяти контейнеров через cAdvisor.

Сервисные образы выпускаются отдельными репозиториями. Для локального стенда сначала выполните `scripts/build-local.ps1` в каждом сервисном репозитории, затем:

```powershell
.\scripts\deploy.ps1
```

Проверка:

```powershell
kubectl get pods -n highload-zhizha
kubectl logs -n highload-zhizha deployment/payment-service
```

API: `http://localhost:9080`  
Grafana: `http://grafana.localhost:9080`.
Нагрузочная панель: `http://load.localhost:9080`.
Дашборд нагрузки и событий: `http://grafana.localhost:9080/d/highload-zhizha/highloadzhizha-e28094-nagruzka-i-sobytija`.
Дашборд CPU и памяти: `http://grafana.localhost:9080/d/highload-resources/highloadzhizha-e28094-cpu-i-pamjat`.
MemoryZhorka: `POST http://memory.localhost:9080/eat-memory`.

SMS-логи в Grafana Explore:

```logql
{namespace="highload-zhizha", app="notification-service"} |= "SMS_SENT"
```

Секреты из `k8s/00-foundation.yaml` предназначены только для локальной разработки.
