// Package aggregator batches parsed measurements and flushes summary
// statistics when a batch fills up or an interval elapses — whichever
// comes first. Batching amortizes per-measurement output cost (one JSON
// line / future Kafka message per ~1000 packets instead of per packet);
// the interval bounds how stale a quiet stream's data can get.
package aggregator

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
)

// FlushReason records what triggered a flush; it is exported in the
// batch JSON so backpressure and traffic patterns are visible downstream.
type FlushReason string

const (
	ReasonSize     FlushReason = "size"     // batch reached MaxBatch
	ReasonInterval FlushReason = "interval" // FlushInterval elapsed
	ReasonShutdown FlushReason = "shutdown" // input closed or ctx canceled
)

// BatchStats is the per-batch summary written to the Sink. Durations
// marshal as integer nanoseconds (encoding/json's default for
// time.Duration), hence the _ns field suffixes.
//
// "RTT" here is the sender→collector one-way delay (RxTime minus the
// sender's TWAMP timestamp) standing in for RTT: the demo has no
// reflector leg, and the math downstream is identical.
type BatchStats struct {
	FlushedAt time.Time     `json:"flushed_at"`
	Reason    FlushReason   `json:"reason"`
	Count     int           `json:"count"`
	HighCount int           `json:"high_count"`
	MinRTT    time.Duration `json:"min_rtt_ns"`
	MaxRTT    time.Duration `json:"max_rtt_ns"`
	MeanRTT   time.Duration `json:"mean_rtt_ns"`
	SeqGaps   int           `json:"seq_gaps"`
}

// Config for the aggregator. Zero values fall back to defaults.
type Config struct {
	MaxBatch      int           // flush when this many measurements are buffered (default 1000)
	FlushInterval time.Duration // flush at least this often (default 200ms)
	Clock         Clock         // injected for tests; defaults to RealClock
	Sink          Sink          // defaults to JSON lines on stdout

	// OnFlush, when set, observes every flush (batch size and duration
	// including the sink write). A plain func keeps this package free of
	// any metrics-library dependency.
	OnFlush func(batchSize int, took time.Duration)
}

type Aggregator struct {
	maxBatch int
	interval time.Duration
	clock    Clock
	sink     Sink
	onFlush  func(int, time.Duration)

	// batch and scratchSeqs are reused across flushes (reset to [:0]) so
	// the steady state allocates nothing per batch.
	batch       []pipeline.Measurement
	scratchSeqs []uint32
}

func New(cfg Config) *Aggregator {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock()
	}
	if cfg.Sink == nil {
		cfg.Sink = NewJSONLineSink(os.Stdout)
	}
	return &Aggregator{
		maxBatch: cfg.MaxBatch,
		interval: cfg.FlushInterval,
		clock:    cfg.Clock,
		sink:     cfg.Sink,
		onFlush:  cfg.OnFlush,
		batch:    make([]pipeline.Measurement, 0, cfg.MaxBatch),
	}
}

// Run consumes measurements until in closes (graceful: flush the tail and
// return nil) or ctx is canceled (fast stop: best-effort flush, return
// ctx.Err()). It runs in the caller's goroutine.
func (a *Aggregator) Run(ctx context.Context, in <-chan pipeline.Measurement) error {
	timer := a.clock.NewTimer(a.interval)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-in:
			if !ok {
				// Input closed: flush the residue so a graceful shutdown
				// never loses measurements that were already accepted.
				a.flush(ctx, ReasonShutdown)
				return nil
			}
			a.batch = append(a.batch, m)
			if len(a.batch) >= a.maxBatch {
				a.flush(ctx, ReasonSize)
				resetTimer(timer, a.interval)
			}
		case <-timer.C():
			a.flush(ctx, ReasonInterval)
			// The timer just fired and we drained its channel ourselves,
			// so a plain Reset is safe on this branch.
			timer.Reset(a.interval)
		case <-ctx.Done():
			a.flush(ctx, ReasonShutdown)
			return ctx.Err()
		}
	}
}

// resetTimer restarts a timer that may or may not have fired. The classic
// trap: if the timer already expired but its tick was not consumed, a
// bare Reset leaves the stale tick in C and the next select fires
// immediately — so Stop, drain non-blockingly, then Reset. (Go 1.23+
// removed the need for the drain by making timer channels unbuffered,
// but this module's go directive is 1.22, which keeps the old semantics.)
func resetTimer(t Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C():
		default:
		}
	}
	t.Reset(d)
}

// flush summarizes the current batch, hands it to the sink, and resets
// the batch buffer. Empty batches are skipped — an idle stream should
// not emit noise. A sink error is logged, not fatal: dropping one summary
// beats killing the whole collector.
func (a *Aggregator) flush(ctx context.Context, reason FlushReason) {
	if len(a.batch) == 0 {
		return
	}
	start := a.clock.Now()
	stats := a.summarize(reason)
	if err := a.sink.WriteBatch(ctx, stats); err != nil {
		slog.Error("sink write failed", "err", err, "count", stats.Count)
	}
	if a.onFlush != nil {
		a.onFlush(stats.Count, a.clock.Now().Sub(start))
	}
	a.batch = a.batch[:0]
}

func (a *Aggregator) summarize(reason FlushReason) BatchStats {
	b := BatchStats{
		FlushedAt: a.clock.Now(),
		Reason:    reason,
		Count:     len(a.batch),
	}
	var sum time.Duration
	a.scratchSeqs = a.scratchSeqs[:0]
	for i, m := range a.batch {
		rtt := m.RxTime.Sub(m.Pkt.Timestamp.Time())
		if i == 0 || rtt < b.MinRTT {
			b.MinRTT = rtt
		}
		if i == 0 || rtt > b.MaxRTT {
			b.MaxRTT = rtt
		}
		sum += rtt
		if m.Pkt.Priority == packet.PriorityHigh {
			b.HighCount++
		}
		a.scratchSeqs = append(a.scratchSeqs, m.Pkt.Seq)
	}
	b.MeanRTT = sum / time.Duration(b.Count)

	// Loss detection: sort the batch's sequence numbers and count the
	// holes between neighbours (d == 0 duplicates are ignored). This is
	// batch-local on purpose — gaps that straddle a flush boundary are
	// invisible; production would keep a per-session cursor instead.
	slices.Sort(a.scratchSeqs)
	for i := 1; i < len(a.scratchSeqs); i++ {
		if d := a.scratchSeqs[i] - a.scratchSeqs[i-1]; d > 1 {
			b.SeqGaps += int(d - 1)
		}
	}
	return b
}
