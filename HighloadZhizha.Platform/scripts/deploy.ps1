$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

$requiredImages = @(
    'highload/green-bank-service:dev',
    'highload/payment-service:dev',
    'highload/memory-zhorka:dev',
    'highload/notification-service:dev',
    'highload/management-service:dev'
)
foreach ($image in $requiredImages) {
    docker image inspect $image *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Required image '$image' is missing. Build it in its service repository first."
    }
}

kubectl apply -f (Join-Path $root 'k8s')
kubectl rollout status deployment/postgres -n highload-zhizha --timeout=240s
kubectl rollout status deployment/kafka -n highload-zhizha --timeout=300s
kubectl wait --for=condition=complete job/kafka-topic-init -n highload-zhizha --timeout=240s
kubectl rollout status deployment/green-bank-service -n highload-zhizha --timeout=240s
kubectl rollout status deployment/payment-service -n highload-zhizha --timeout=240s
kubectl rollout status deployment/memory-zhorka -n highload-zhizha --timeout=240s
kubectl rollout status deployment/notification-service -n highload-zhizha --timeout=240s
kubectl rollout status deployment/management-service -n highload-zhizha --timeout=240s
kubectl rollout status deployment/loki -n highload-zhizha --timeout=240s
kubectl rollout status deployment/grafana -n highload-zhizha --timeout=240s
kubectl rollout status deployment/alloy -n highload-zhizha --timeout=240s
kubectl get pods -n highload-zhizha
