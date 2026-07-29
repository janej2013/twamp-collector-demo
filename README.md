# twamp-collector-demo — 高吞吐 TWAMP 测量采集器 / High-Throughput TWAMP Measurement Collector

A compact Go project demonstrating idiomatic concurrency engineering for a
high-rate UDP telemetry pipeline: a simplified TWAMP-Test (RFC 5357)
collector with priority-aware backpressure, batched aggregation, and
Prometheus observability. ~1,200 lines of core code, built to be explained
line by line.

## Architecture

```
[cmd/sender]  --UDP-->  [internal/receiver]      N reader goroutines
 token-bucket            recvmmsg ReadBatch       early rx timestamps
 paced traffic           sync.Pool buffers        parse -> value copy
                              |
                              v  Enqueue (never blocks; full lane = counted drop)
                    [internal/pipeline]
                     high lane  ==== bounded chan ====\
                     normal lane ==== bounded chan ====>  worker pool
                       (nested-select priority)          |
                              |                          v
                              |             [internal/aggregator]
                              |              batch 1000 or 200ms
                              |              min/max/mean delay, seq gaps
                              |                          |
                              v                          v
                    [internal/metrics]            [Sink interface]
                     Prometheus /metrics           stdout JSON lines
                                                   (swap for Kafka)
```

| Module | Responsibility (one line) |
|---|---|
| `internal/packet` | TWAMP-Test wire codec: strict, panic-free, zero-alloc parse; NTP↔Unix epoch conversion |
| `internal/receiver` | UDP intake: batch syscalls, pooled buffers, earliest-possible timestamps |
| `internal/pipeline` | Bounded two-lane priority queue: drop-and-count instead of blocking the read path |
| `internal/aggregator` | Size-or-interval batching with correct timer reuse; per-batch summary stats |
| `internal/metrics` | Prometheus exporter sampling hot-path atomics at scrape time |
| `cmd/collector` | Wiring + ordered graceful shutdown |
| `cmd/sender` | Load generator: hand-rolled token bucket, priority mix, loss simulation, bursts |

## Design Decisions

**Non-blocking enqueue over blocking backpressure.** A full lane drops the
measurement and increments a Prometheus counter; the read path never waits.
If readers stall, packets die invisibly in the kernel socket buffer —
observed loss in the application beats unobserved loss below it. (The demo
below makes both kinds visible.)

**Nested-select priority, honestly scoped.** Workers poll the high lane
non-blockingly first, then block on both lanes together. High traffic wins
whenever present, and normal traffic is not starved as long as high traffic
is bursty rather than saturating — which alert-class measurements are by
definition. Sustained high-lane saturation *would* starve normal; weighted
round-robin is the fix if that assumption ever breaks.

**`sync.Pool` buffers, and no sub-slice retention.** Read buffers are
pooled (`*[]byte`, avoiding the SA6002 interface-allocation trap). The
parser copies scalar fields out; a `Measurement` holds no view of the 2KB
buffer, so a 15-byte packet can never pin its backing array in memory.

**Timer reset done right.** A size-triggered flush must rearm the interval
timer: `Stop()`, drain the channel if `Stop` reports the timer already
fired, then `Reset` — otherwise a stale tick causes a spurious immediate
flush. Go 1.23+ removed the trap; this module pins `go 1.22` semantics and
handles it explicitly.

**Timestamps taken as early as possible.** `time.Now()` runs immediately
after the read syscall returns, before parsing, because every instruction
in between inflates measured delay. One timestamp is shared per
`ReadBatch` call — an accepted, documented error bounded by batch size.

**Ordered shutdown with a force-stop lever.** Two contexts: the signal
context only stops intake; a separate hard context force-stops workers if
draining exceeds a timeout. Sequence: close the socket (readers exit, so
no enqueue is in flight) → close the lanes (workers drain) → aggregator
flushes the tail batch → metrics server exits last. Every step is numbered
in `cmd/collector/main.go`.

## How to Run

Requires Go 1.22+ on Linux (developed under WSL2/Ubuntu; `ReadBatch` uses
`recvmmsg`, and the portable `-force-readfrom` path covers other OSes).

```sh
make ci      # go vet + go test -race ./...
make build   # binaries into bin/
```

Terminal 1 — collector:

```sh
./bin/collector -listen 127.0.0.1:8620          # batches as JSON lines on stdout
```

Terminal 2 — sender (50k pps default, 5% high priority, 1% simulated loss):

```sh
./bin/sender -target 127.0.0.1:8620 -rate 50000 -duration 10s
curl -s 127.0.0.1:9090/metrics | grep twamp_    # while it runs
```

Backpressure demo — tiny lanes, oversized bursts, drop counters sampled
every second (`make demo`):

```
--- t=3s
twamp_dropped_packets_total{priority="high"} 22
twamp_dropped_packets_total{priority="normal"} 3591
twamp_lane_depth{priority="high"} 2
twamp_lane_depth{priority="normal"} 30
...
collector stopped received=1137703 parse_errors=0 dropped_high=22 dropped_normal=6547
```

Two things worth noticing: the priority lane loses 22 packets while the
normal lane loses 6,547 (priority protection working), and
`sent - received ≈ 950` packets vanished *before* the application — kernel
socket-buffer loss during bursts, the exact failure mode the non-blocking
design keeps out of the app layer.

## Benchmarks

`make bench` on WSL2, i7-13620H, Go 1.26:

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| `packet.Unmarshal` | 1.58 | 0 |
| `packet.Marshal` | 1.47 | 0 |
| Aggregator throughput (per measurement, incl. flush amortized) | 81–107 | 0 |

Zero allocations on the parse path and through the aggregator's steady
state (reused batch + scratch buffers); at 50k pps the collector's hot
path produces no GC pressure at all. The codec numbers are stable across
runs; the aggregator benchmark is channel-bound (scheduler-sensitive), so
its ns/op varies by ~25% run to run — hence the range.

## What I'd do differently in production

- **SO_REUSEPORT + one socket per reader.** Kernel-side flow steering
  removes the single-socket lock contention this demo tolerates. Skipped:
  needs raw socket options and muddies the reader logic the demo exists to
  show.
- **SO_TIMESTAMPING kernel timestamps.** Hardware/driver timestamps remove
  the userspace scheduling jitter from delay measurements. Skipped: cgo or
  raw control-message parsing for a demo whose accuracy story is "take it
  early, document the error".
- **Kafka (or similar) sink.** The `Sink` interface is the seam; a real
  deployment writes batches to a durable bus with retry/backoff instead of
  stdout. Skipped: adds a broker dependency and drowns the concurrency
  code in client configuration.
- **t-digest / DDSketch percentiles.** Min/max/mean per batch is
  deliberately simple; real SLO reporting needs mergeable quantile
  sketches. Skipped: an algorithm library, not a concurrency
  demonstration.
- **Per-session state.** Real TWAMP runs many sessions; gap detection
  would key sequence cursors by session ID and survive batch boundaries.
  The demo keeps one implicit session to stay small.
