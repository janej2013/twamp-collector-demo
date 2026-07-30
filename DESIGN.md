# DESIGN.md — twamp-collector-demo

> A one-page spec written **before** any code. Purpose: derive concrete, verifiable
> requirements for a TWAMP-style measurement collector from (a) the responsibilities
> of the target role and (b) RFC 5357, then scope a demo that can be built and
> benchmarked on a laptop. This document is the contract the implementation answers to.

---

## 1. Context

I don't have a production TWAMP system to build against, so this spec is
reverse-engineered from what a TWAMP collector role actually does: develop and
maintain high-performance Golang backend services for real-time collection and
processing of latency measurement data, optimize ingestion/storage/retrieval at
scale, and keep the collector highly available.

A latency measurement system has one property that ordinary ingestion systems
don't: **its output is only worth anything if it can be trusted.** Silently
dropped or delayed packets don't just lose data — they corrupt the statistics
the network team acts on. Every design decision below follows from that.

## 2. Requirements derived from the role

| Responsibility (paraphrased)                  | Design constraint for this demo                                        |
|-----------------------------------------------|------------------------------------------------------------------------|
| Real-time data collection and processing      | Streaming pipeline; no offline batch jobs; stats visible within ~200 ms |
| Optimize data ingestion at large scale        | The UDP receive path is the bottleneck to engineer around; measure it   |
| High availability of collector services       | Graceful shutdown loses no accepted data; self-metrics are first-class  |
| Troubleshoot production issues / perf tuning  | Every drop, queue depth, and flush latency is observable via /metrics   |

## 3. Input: what the collector receives (RFC 5357)

The unauthenticated TWAMP-Test sender packet (RFC 5357 §4.1.2, inheriting
OWAMP RFC 4656) begins:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Sequence Number                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Timestamp                            |
|                (NTP 64-bit: 32b seconds, 32b fraction)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         Error Estimate        |     Packet Padding ...        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

Consequences for the implementation:

- Packets are **small (tens of bytes), high-rate UDP** — syscall overhead and
  per-packet allocations dominate; batch receive and buffer reuse matter.
- **Sequence Number** enables loss detection via gap counting in the aggregator.
- **Timestamps are NTP format**: epoch 1900, so conversion to `time.Time`
  subtracts the 1900→1970 offset (2,208,988,800 s). Network byte order throughout.
- Malformed input is a certainty at these rates → the parser must never panic;
  every reject path returns an error and increments a counter. Fuzz-tested.

**Deliberate deviation:** the demo appends one custom `Priority` byte
(0 = routine telemetry, 1 = high-priority measurement) to exercise the
priority pipeline. Real TWAMP does not have this field; a production system
would derive class from the session or DSCP instead. Flagged here so the demo
never misrepresents the wire format.

## 4. System boundary

```
 upstream                       THIS DEMO                        downstream
 thousands of probes   ┌──────────────────────────────┐   monitoring platforms
 emitting small UDP ──▶│  receive → parse → prioritize │──▶ (stdout JSON here;
 test packets          │  → aggregate → export         │    Kafka/TSDB in prod)
                       └──────────────────────────────┘
```

The collector is a **bridge**. Everything outside the bridge is out of scope.

### Non-goals (explicitly scoped out)

- TWAMP-Control (TCP session negotiation), authenticated/encrypted modes
- Kernel/hardware timestamping (SO_TIMESTAMPING) — software timestamps only,
  taken as early as possible in the receive path; the gap is documented
- SO_REUSEPORT multi-socket sharding, kernel tuning
- Real storage backend — the exporter is an interface; stdout JSON stands in
  for a Kafka producer
- Clock synchronization (NTP/PTP) between probes and collector

Each non-goal is something a production collector needs; they are excluded to
keep the demo reviewable (~1,000–1,500 lines), not because they don't matter.

## 5. Verifiable targets

Numbers chosen to be honest for a laptop, not to imitate production:

- **T1 — Throughput:** sustain 50,000 packets/sec ingest on a single laptop
  process, verified by the included load generator and benchmark.
- **T2 — Aggregation latency:** a batch flushes at 1,000 packets or 200 ms,
  whichever comes first; flush latency exported as a histogram.
- **T3 — Parser efficiency:** packet Unmarshal at (or near) zero heap
  allocations per packet, verified by `go test -bench` with `allocs/op`.

## 6. Invariants (non-negotiable properties)

- **I1 — The receive path never blocks.** Enqueue is `select`+`default`;
  when a queue is full the packet is dropped, never the reader stalled.
- **I2 — No silent loss.** Every drop increments a Prometheus counter labeled
  by stage and priority. If a number can go down a drain, the drain is metered.
- **I3 — Shutdown loses nothing accepted.** SIGTERM → close intake → drain
  queues → flush the final partial batch → exit, bounded by a timeout.
- **I4 — Priority without starvation.** High-priority packets are served
  first via nested select, but routine traffic always retains a path.
- **I5 — Race-free.** `go test -race ./...` passes in CI; no exceptions.

## 7. Architecture (three stages)

1. **Readers that never block** — N receive goroutines; buffer reuse via
   `sync.Pool`; timestamp taken immediately after read; parse, then hand off.
2. **A priority queue that drops-and-counts** — two bounded channels
   (high / routine); non-blocking enqueue (I1, I2); worker pool with
   priority-aware nested select (I4).
3. **An aggregator that squeezes a thousand packets into one record** —
   per-batch count, min/max/mean RTT, and loss via sequence gaps; timer-based
   flush with correct `time.Timer` reset handling; exporter behind an interface.

## 8. Verification map

| Claim | Verified by |
|-------|-------------|
| T1    | `cmd/sender` load run + throughput benchmark      |
| T2    | flush-latency histogram + aggregator unit tests (fake clock) |
| T3    | `BenchmarkUnmarshal` allocs/op                    |
| I1,I4 | pipeline unit tests (full-queue, starvation)      |
| I2    | metrics assertions in tests; `/metrics` in demo   |
| I3    | shutdown test: kill under load, compare counters  |
| I5    | `-race` in CI target                              |

---

*Spec written prior to implementation; the README documents where the built
system met, missed, or revised these targets.*