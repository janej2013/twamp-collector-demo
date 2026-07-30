#!/usr/bin/env bash
# Full-accounting verification run: the same hostile load profile as
# `make demo`, but every ledger RESULTS.md quotes is captured from ONE run:
#   - sender totals (sent, deliberately skipped seqs)
#   - collector batch JSON on stdout (per-batch seq_gaps summed)
#   - collector final counters (received, parse errors, drops per lane)
#   - kernel UDP RcvbufErrors delta from /proc/net/snmp (system-wide;
#     run on a quiet host)
set -euo pipefail
cd "$(dirname "$0")/.."

LISTEN=127.0.0.1:8620
OUT=${OUT:-/tmp/twamp-verify}
mkdir -p "$OUT"

snmp_rcvbuf() {
  python3 -c '
lines = [l.split() for l in open("/proc/net/snmp") if l.startswith("Udp:")]
print(lines[1][lines[0].index("RcvbufErrors")])'
}

BEFORE=$(snmp_rcvbuf)

./bin/collector -listen "$LISTEN" -metrics-addr "" \
  -normal-cap 64 -high-cap 16 -workers 1 \
  >"$OUT/batches.jsonl" 2>"$OUT/collector.log" &
COL=$!
trap 'kill -TERM $COL 2>/dev/null || true' EXIT
sleep 0.5

./bin/sender -target "$LISTEN" -rate 30000 -duration 6s \
  -burst-size 200000 -burst-every 1s 2>"$OUT/sender.log"
sleep 1 # let the tail interval-flush land
kill -TERM "$COL"
wait "$COL" 2>/dev/null || true
trap - EXIT

AFTER=$(snmp_rcvbuf)

python3 - "$OUT" "$BEFORE" "$AFTER" <<'EOF'
import json, re, sys

out, before, after = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
gaps = count = high = 0
for line in open(f"{out}/batches.jsonl"):
    b = json.loads(line)
    gaps += b["seq_gaps"]; count += b["count"]; high += b["high_count"]

sender = open(f"{out}/sender.log").read()
collector = open(f"{out}/collector.log").read()
g = lambda pat, s: int(re.search(pat, s).group(1))
sent, skipped = g(r"sent=(\d+)", sender), g(r"skipped_seqs=(\d+)", sender)
received = g(r"received=(\d+)", collector)
dh, dn = g(r"dropped_high=(\d+)", collector), g(r"dropped_normal=(\d+)", collector)
perr = g(r"parse_errors=(\d+)", collector)

kernel, shed = after - before, dh + dn
expected = skipped + kernel + shed
print(f"sent={sent}  skipped_seqs={skipped}  received={received}  parse_errors={perr}")
print(f"kernel RcvbufErrors delta={kernel}  (sent-received={sent-received}, should match)")
print(f"shed by collector: high={dh}  normal={dn}  total={shed}")
print(f"aggregated measurements={count} (received-shed={received-shed}), high={high}")
print(f"expected gaps = skipped {skipped} + kernel {kernel} + shed {shed} = {expected}")
print(f"reported gaps = {gaps}  (delta {gaps-expected:+d} = {100*(gaps-expected)/expected:+.2f}%)")
EOF
