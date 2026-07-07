#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install -r requirements.txt
mkdir -p logs

pm2 start ecosystem.config.cjs --update-env
pm2 save
pm2 status discord-manager-web
