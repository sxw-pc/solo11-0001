#!/usr/bin/env bash
# 启动 mock 站点，跑一轮 API/HTML 采集并打印统计；退出时自动关闭 mock 站点。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PORT="${1:-8010}"

crawl-flow serve-mock --port "$PORT" &
MOCK_PID=$!
cleanup() {
  kill "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

sleep 2
crawl-flow run --source demo_api --limit 5
crawl-flow run --source demo_html --limit 5
crawl-flow db stats
