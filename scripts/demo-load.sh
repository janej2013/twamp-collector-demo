#!/usr/bin/env bash
# Dashboard companion: run the tiny-lane collector plus burst traffic for
# DURATION (default 60s), then clean up. Watch it at localhost:3300.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60s}

# metrics on a wildcard bind: the dockerized Prometheus scrapes it
# through the bridge gateway (host.docker.internal)
./bin/collector -listen 127.0.0.1:8620 -metrics-addr :9090 \
  -normal-cap 64 -high-cap 16 -workers 1 >/dev/null 2>&1 &
COL=$!
trap 'kill -TERM $COL 2>/dev/null || true; wait $COL 2>/dev/null || true' EXIT
sleep 0.5

echo "sending bursty traffic for $DURATION — watch http://localhost:3300"
./bin/sender -target 127.0.0.1:8620 -rate 30000 -duration "$DURATION" \
  -burst-size 200000 -burst-every 1s
