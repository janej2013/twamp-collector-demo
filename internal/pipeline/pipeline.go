// Package pipeline implements the collector's bounded two-lane priority
// queue sitting between the UDP receive path and the aggregator.
//
// Design: the receive path must never block, because a stalled reader
// means kernel socket-buffer overflow and *unobserved* loss. So enqueue
// is non-blocking (drop + count on a full lane), and backpressure shows
// up as an observable metric instead of as latency in the hot path.
package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
)

// Measurement is one parsed TWAMP-Test packet plus the receive timestamp
// taken at the socket, before parsing, to keep measurement error low.
type Measurement struct {
	Pkt    packet.Packet
	RxTime time.Time
}

// Config sizes the two lanes and the worker pool. Zero values fall back
// to the defaults below.
type Config struct {
	HighCap   int // high-priority lane capacity
	NormalCap int // normal lane capacity
	Workers   int // consuming goroutines
}

const (
	defaultHighCap   = 1024
	defaultNormalCap = 8192
	defaultWorkers   = 4
)

// Pipeline routes measurements through a high and a normal lane.
//
// Lifecycle contract: all Enqueue callers must have stopped before Close
// is called (send-on-closed-channel panics otherwise); this mirrors the
// collector's shutdown order — stop the UDP listener first, then Close.
type Pipeline struct {
	high   chan Measurement
	normal chan Measurement

	// Drop counts are plain atomics, not prometheus.Counter, so this
	// package stays metrics-agnostic; the metrics package exports them
	// via prometheus.NewCounterFunc.
	droppedHigh   atomic.Uint64
	droppedNormal atomic.Uint64

	workers   int
	closeOnce sync.Once
}

func New(cfg Config) *Pipeline {
	if cfg.HighCap <= 0 {
		cfg.HighCap = defaultHighCap
	}
	if cfg.NormalCap <= 0 {
		cfg.NormalCap = defaultNormalCap
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	return &Pipeline{
		high:    make(chan Measurement, cfg.HighCap),
		normal:  make(chan Measurement, cfg.NormalCap),
		workers: cfg.Workers,
	}
}

// Enqueue routes m to its lane without ever blocking. It reports whether
// the measurement was accepted; on a full lane it is dropped and counted.
// Tradeoff: measurement data tolerates *observed* loss, but the receive
// path cannot tolerate stalling — blocking here would push loss into the
// kernel socket buffer where it is invisible.
func (p *Pipeline) Enqueue(m Measurement) bool {
	lane, dropped := p.normal, &p.droppedNormal
	if m.Pkt.Priority == packet.PriorityHigh {
		lane, dropped = p.high, &p.droppedHigh
	}
	select {
	case lane <- m:
		return true
	default:
		dropped.Add(1)
		return false
	}
}

// Run starts the worker pool and returns the merged output channel. The
// channel is closed after every worker has exited: on ctx cancellation
// (fast stop, in-flight items abandoned) or after Close once both lanes
// are drained (graceful stop, nothing lost).
func (p *Pipeline) Run(ctx context.Context) <-chan Measurement {
	out := make(chan Measurement)
	var wg sync.WaitGroup
	wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go func() {
			defer wg.Done()
			p.worker(ctx, out)
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Close stops intake and lets workers drain both lanes. Callers must
// guarantee no Enqueue is in flight (see the Lifecycle contract above).
func (p *Pipeline) Close() {
	p.closeOnce.Do(func() {
		close(p.high)
		close(p.normal)
	})
}

// worker consumes with priority via a nested select: first a non-blocking
// poll of the high lane, then a blocking wait on both lanes.
//
// The tradeoff: while the high lane has items they are served strictly
// first, but the moment it is empty the second select gives both lanes an
// equal chance — so normal traffic is not starved as long as high traffic
// is bursty rather than saturating (alert-class measurements are rare by
// definition). Sustained high-lane saturation *would* starve normal;
// a weighted round-robin would fix that at the cost of latency for the
// traffic we prioritized on purpose.
func (p *Pipeline) worker(ctx context.Context, out chan<- Measurement) {
	high, normal := p.high, p.normal
	for high != nil || normal != nil {
		// Fast path: drain high first. A receive from a nil channel
		// blocks forever, so once a lane is closed and drained, setting
		// it to nil removes its case from both selects.
		select {
		case m, ok := <-high:
			if !ok {
				high = nil
				continue
			}
			if !deliver(ctx, out, m) {
				return
			}
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case m, ok := <-high:
			if !ok {
				high = nil
				continue
			}
			if !deliver(ctx, out, m) {
				return
			}
		case m, ok := <-normal:
			if !ok {
				normal = nil
				continue
			}
			if !deliver(ctx, out, m) {
				return
			}
		}
	}
}

// deliver forwards m downstream, giving up on ctx cancellation so a slow
// or stopped consumer can never wedge a worker.
func deliver(ctx context.Context, out chan<- Measurement, m Measurement) bool {
	select {
	case out <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

// DroppedHigh and DroppedNormal expose drop totals for metrics export.
func (p *Pipeline) DroppedHigh() uint64   { return p.droppedHigh.Load() }
func (p *Pipeline) DroppedNormal() uint64 { return p.droppedNormal.Load() }

// Depths reports current lane occupancy for gauge export. len on a
// channel is safe under concurrency and cheap; values are best-effort
// snapshots by nature.
func (p *Pipeline) Depths() (high, normal int) {
	return len(p.high), len(p.normal)
}
