#!/usr/bin/env bash
# Backpressure demo: run the collector with deliberately tiny lanes, hit
# it with oversized bursts, and watch the drop counters and lane depths
# move on /metrics while steady-rate traffic keeps flowing.
set -euo pipefail
cd "$(dirname "$0")/.."

LISTEN=127.0.0.1:8620
METRICS=127.0.0.1:9090

./bin/collector -listen "$LISTEN" -metrics-addr "$METRICS" \
  -normal-cap 64 -high-cap 16 -workers 1 >/dev/null 2>collector-demo.log &
COL=$!
trap 'kill -TERM $COL 2>/dev/null || true; wait $COL 2>/dev/null || true' EXIT
sleep 0.5

./bin/sender -target "$LISTEN" -rate 30000 -duration 6s \
  -burst-size 200000 -burst-every 1s &
SND=$!

for i in 1 2 3 4 5 6; do
  sleep 1
  echo "--- t=${i}s"
  curl -s "http://$METRICS/metrics" | grep -E '^twamp_(dropped_packets_total|lane_depth)'
done
wait $SND
echo "--- final counters in collector log:"
kill -TERM $COL
wait $COL 2>/dev/null || true
tail -1 collector-demo.log
