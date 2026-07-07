#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

pm2 delete discord-manager-web
pm2 save
