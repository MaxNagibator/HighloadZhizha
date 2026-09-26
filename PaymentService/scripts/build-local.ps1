$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$k3d = (Get-Command k3d -ErrorAction SilentlyContinue).Source
if (-not $k3d) { $k3d = Join-Path $env:LOCALAPPDATA 'Programs\k3d\k3d.exe' }
if (-not (Test-Path -LiteralPath $k3d)) { throw 'k3d is not installed' }

docker build -t highload/payment-service:dev $root
& $k3d image import highload/payment-service:dev -c local-dev

