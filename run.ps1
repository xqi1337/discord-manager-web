$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    py -3 -m venv .venv
}

& $python -m pip install -r requirements.txt

if (-not $env:HOST) {
    $env:HOST = "127.0.0.1"
}

if (-not $env:PORT) {
    $env:PORT = "5000"
}

if (-not $env:FLASK_DEBUG) {
    $env:FLASK_DEBUG = "1"
}

& $python app.py
