#!/usr/bin/env bash
# Runs the backpressure demo and samples /metrics every 200ms into a CSV,
# for rendering the README chart with scripts/render_chart.py.
set -euo pipefail
cd "$(dirname "$0")/.."

LISTEN=127.0.0.1:8620
METRICS=127.0.0.1:9090
OUT=${1:-docs/backpressure.csv}
mkdir -p "$(dirname "$OUT")"

./bin/collector -listen "$LISTEN" -metrics-addr :9090 \
  -normal-cap 64 -high-cap 16 -workers 1 >/dev/null 2>/dev/null &
COL=$!
trap 'kill -TERM $COL 2>/dev/null || true; wait $COL 2>/dev/null || true' EXIT
sleep 0.5

./bin/sender -target "$LISTEN" -rate 30000 -duration 6s \
  -burst-size 200000 -burst-every 1s 2>/dev/null &
SND=$!

echo "t_s,dropped_high,dropped_normal,depth_high,depth_normal,received" >"$OUT"
start=$(date +%s.%N)
# 6s of traffic + 1s of tail: 35 samples at 200ms
for _ in $(seq 1 35); do
  m=$(curl -s "http://$METRICS/metrics")
  t=$(echo "$(date +%s.%N) - $start" | bc)
  # prefix match (no regex braces); %.0f folds Prometheus sci-notation to int
  pick() { echo "$m" | awk -v pre="$1" 'index($0,pre)==1 {v=$2; f=1} END {printf "%.0f", f?v:0}'; }
  dh=$(pick 'twamp_dropped_packets_total{priority="high"} ')
  dn=$(pick 'twamp_dropped_packets_total{priority="normal"} ')
  qh=$(pick 'twamp_lane_depth{priority="high"} ')
  qn=$(pick 'twamp_lane_depth{priority="normal"} ')
  rc=$(pick 'twamp_received_packets_total ')
  printf '%.2f,%s,%s,%s,%s,%s\n' "$t" "$dh" "$dn" "$qh" "$qn" "$rc" >>"$OUT"
  sleep 0.2
done
wait $SND 2>/dev/null || true
echo "wrote $OUT ($(wc -l <"$OUT") lines)"
