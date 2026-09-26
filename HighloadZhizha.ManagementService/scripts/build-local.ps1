$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
docker build -t highload/highload-mock:dev $repo
k3d image import highload/highload-mock:dev -c local-dev
