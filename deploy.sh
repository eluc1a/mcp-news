#!/bin/sh
# Deploy news-mcp on fox: pull, sync deps, restart the service.
# The gatherer timer picks up new code on its next 2h firing.
set -eu
cd "$(dirname "$0")"

git pull --ff-only
uv sync
sudo systemctl restart news-mcp.service
systemctl is-active news-mcp.service
echo "deployed $(git rev-parse --short HEAD)"
