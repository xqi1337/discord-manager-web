$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    py -3 -m venv .venv
}

& $python -m pip install -r requirements.txt
New-Item -ItemType Directory -Force -Path logs | Out-Null

pm2 start ecosystem.config.cjs --update-env
pm2 save
pm2 status discord-manager-web
