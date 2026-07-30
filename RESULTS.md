# Results

All numbers below are from real runs on the development machine:
**WSL2 (Ubuntu) on an Intel i7-13620H, Go 1.26**, traffic over loopback via
the included `cmd/sender`. End-to-end numbers come from **one instrumented
run** of `make verify` ([scripts/demo-verify.sh](scripts/demo-verify.sh)),
which captures every ledger quoted here — sender totals, per-batch gap
counts, collector counters, and the kernel's UDP drop counter — so the
accounting below is internally consistent. This section reports results
against the targets and invariants declared in [DESIGN.md](./DESIGN.md);
where a result needs a caveat, the caveat is stated rather than the number
polished.

### Microbenchmarks (T3 — hot-path efficiency)

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| `packet.Unmarshal` | 2.15 | 0 |
| `packet.Marshal` | 1.89 | 0 |
| Aggregator, per measurement (flush amortized) | 81–107 | 0 (steady state) |

- **Zero heap allocations** on the parse path and through the aggregator's
  steady state (reused batch and scratch buffers). At the 50k pps design
  target, the hot path generates **no GC pressure at all** — which is the
  property that matters here, since GC pauses would pollute the very
  latencies being measured.
- The codec benchmarks use Go 1.24's `b.Loop()`, which keeps the loop body
  from being optimized away; its per-iteration guard costs ~0.5 ns and is
  included in the numbers above — the price of a figure that is provably
  not a dead-code artifact. At ~2 ns (≈10 cycles) the parse is fully
  inlined register-level work.
- The aggregator benchmark is **channel-bound and scheduler-sensitive**; its
  ns/op varies ~25% run to run, hence the range. Reporting the range rather
  than the best run is deliberate.
- **Headroom math:** at ~100 ns per measurement, a single worker's ceiling is
  roughly 10M measurements/sec — about **200× the 50k pps target**. The
  hot path costs under 1% of one core at design load.

### End-to-end backpressure under overload (T1, I1, I2, I4)

`make verify` drives the collector with a deliberately hostile load: a
30k pps base rate with a 200k-packet burst every second, 5% high-priority
traffic, 1% simulated loss (skipped sequence numbers), and deliberately
tiny lanes (normal 64, high 16, one worker).

**Load profile (from sender log):** 1,138,171 packets in 6.0 s —
an average of **~190k pps, nearly 4× the design target**.

| Counter (collector, final) | Value |
|---|---:|
| Packets received | 1,136,487 |
| Parse errors | **0** |
| Dropped — normal lane | 8,836 |
| Dropped — high-priority lane | 4 |
| Total shed | 8,840 (~0.78% of received) |
| Measurements aggregated | 1,127,647 (= received − shed, exactly) |

What the numbers show:

- **Priority protection worked (I4):** the normal lane absorbed the damage
  while the high-priority lane lost 4 packets — three orders of magnitude
  apart. The ratio varies run to run (from tens-to-one up to
  thousands-to-one, depending on where burst peaks land), but the shape
  never does: high-priority drops occur only inside the worst burst
  windows and the express lane runs lossless the rest of the time.
- **Nothing vanished silently at this layer (I2):** every shed packet
  appears on `twamp_dropped_packets_total`, labeled by lane, and the
  aggregate count reconciles exactly: received − shed = aggregated.
  Shedding under 1% under a 4× overload is the backpressure design behaving
  correctly, not a defect — the readers never stalled (I1), so intake kept
  pace with the flood.
- **Parser robustness:** zero parse errors across 1.1M packets at full rate
  (the malformed-input paths are exercised separately by fuzz tests).

### Where the missing 1,684 packets went (an honest accounting)

The sender emitted 1,138,171 packets; the collector received 1,136,487.
The difference — **1,684 packets — was lost before my code ever saw it**,
in the kernel's UDP socket buffer during burst peaks. This is not an
inference: the run measures the `Udp: RcvbufErrors` delta from
`/proc/net/snmp`, and it reads **exactly 1,684** — the kernel's own drop
counter matches `sent − received` to the packet.

This is the boundary of invariant I2: *this collector* guarantees no
silent loss **at its own layer** — but loss happens at every layer, so a
production deployment must also scrape the kernel's UDP drop counters,
raise `SO_RCVBUF`, and scale reader goroutines (see Non-goals in DESIGN.md).

### Gap detection audits the path — and catches its own bias

A measurement system should be able to account for its own losses. The
aggregator detects loss via batch-local sequence-number gaps, so in
principle the books should balance:

```
expected gaps = skipped by sender + kernel drops + collector shed
             =        11,534     +    1,684     +     8,840      = 22,058
reported gaps = 26,092   (+18.3%)
```

They don't — and the excess is itself a finding. Two controlled re-runs
isolate the cause:

| Variant | Expected | Reported | Delta |
|---|---:|---:|---:|
| Demo config (2 readers, 5% high-priority) | 22,058 | 26,092 | **+18.3%** |
| 1 reader, 5% high-priority | 24,604 | 28,034 | **+13.9%** |
| 1 reader, **0% high-priority** | 24,463 | 24,442 | **−0.09%** |

With priority traffic removed, the books balance to within a tenth of a
percent (the tiny undercount is the documented batch-boundary blindness).
The overcount, then, is **caused by the priority mechanism itself**: the
express lane lets high-priority packets overtake queued normal ones, the
aggregator sees a slightly reordered stream, and batch-local gap counting
charges every overtake as a hole plus a duplicate-range — reordering is
misread as loss. Batch-local detection is an approximation that is exact
on an ordered stream and biased on a prioritized one; the production fix
is a per-session (or per-priority) sequence cursor that tolerates bounded
reordering, already listed under "What I'd do differently in production".

### Caveats (read before quoting any number above)

1. **WSL2 loopback is not a NIC.** CPU microbenchmarks are trustworthy here,
   but end-to-end pps crosses no real network hardware — no NIC interrupts,
   no wire. Application-layer behavior (backpressure, priority, accounting)
   is representative; absolute pps is optimistic relative to a production
   host, and the Hyper-V virtual network path adds its own variance.
2. **`twamp_lane_depth` is an instantaneous gauge**; sampled at 1 s it reads
   0 most of the time (peaks of a few dozen) even while drops are occurring,
   because fill-and-drain cycles complete within milliseconds. A production
   collector should export queue depth as a histogram or max-since-scrape —
   kept as-is here to make exactly this observability lesson visible.
3. **Software timestamps only.** Timestamps are taken at the earliest point
   in userspace; kernel/hardware timestamping (`SO_TIMESTAMPING`) is a
   declared non-goal, so per-packet timing here is not measurement-grade.
   The demo's subject is the pipeline, not the clock.
4. **`RcvbufErrors` is a system-wide counter**, not per-socket; the runs
   were made on a quiet host. The overload scenario is **deliberately
   hostile** (4× design target in bursts) — quote the <1% shed rate
   together with that context.

### Verification map (DESIGN.md §8, filled)

| Claim | Verified by | Result |
|---|---|---|
| T1 — 50k pps sustained | `make verify` end-to-end run | ✅ exceeded: ~190k pps average under burst load |
| T2 — flush ≤ 1,000 pkts / 200 ms | fake-clock unit tests (`TestSizeTriggeredFlush`, `TestIntervalTriggeredFlush`, `TestTimerResetAfterSizeFlush`) + `twamp_batch_size` histogram | ✅ met |
| T3 — ~zero-alloc parse | `BenchmarkUnmarshal` / `BenchmarkMarshal`, allocs/op | ✅ 0 allocs, ~2 ns/op |
| I1 — receive path never blocks | `TestEnqueueDropsWhenLaneFull` (full lane returns immediately) + verify run: intake kept pace at 4× load | ✅ |
| I2 — no silent loss (at this layer) | drop counters reconcile exactly (received − shed = aggregated); kernel-layer boundary measured and documented above | ✅ with documented boundary |
| I3 — shutdown loses nothing accepted | `TestCloseDrains` (pipeline) + `TestShutdownFlushesTail` (aggregator tail batch) | ✅ |
| I4 — high priority first, normal not starved under bursts | `TestPriorityOrder` + verify run (drops three orders of magnitude apart, normal lane kept flowing) | ✅ for bursty high traffic; sustained high-lane saturation *would* starve normal — a documented trade-off in `pipeline.go`, with weighted round-robin as the alternative |
| I5 — race-free | `go test -race ./...` in `make ci` | ✅ |
