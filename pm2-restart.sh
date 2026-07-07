#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

pm2 restart ecosystem.config.cjs --update-env
pm2 save
pm2 status discord-manager-web
