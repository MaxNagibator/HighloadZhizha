# HighloadZhizha.ManagementService

ASP.NET-панель управления синтетической нагрузкой на GreenBankService.

Настраиваются число параллельных воркеров, RPS каждого воркера, доля создания счетов, диапазон пополнения и HTTP-таймаут. Панель позволяет запускать и немедленно останавливать тест, показывает фактический RPS, успехи, ошибки и количество созданных счетов.

В Kubernetes сервис доступен по `http://load.localhost:9080`.

```powershell
dotnet build
docker build -t highload/highload-mock:dev .
```
