# MemoryZhorka

Намеренно прожорливый Go-сервис для демонстрации Kubernetes memory limits и `OOMKilled`.

## Endpoints

- `GET /health` — проверка готовности;
- `POST /eat-memory` — возвращает `202 Accepted`, затем начинает физически заполнять 3 ГиБ памяти.

В Kubernetes контейнер ограничен 2 ГиБ, поэтому kubelet завершит процесс и Deployment автоматически создаст новую рабочую реплику.

```powershell
Invoke-RestMethod -Method Post -Uri http://memory.localhost:9080/eat-memory
kubectl get pods -n highload-zhizha -w
kubectl describe pod -n highload-zhizha -l app=memory-zhorka
```

## Проверка и локальный образ

```powershell
go test ./...
.\scripts\build-local.ps1
```

