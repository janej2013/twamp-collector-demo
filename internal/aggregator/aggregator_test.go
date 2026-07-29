package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
)

// --- fakes -----------------------------------------------------------------

// fakeClock hands out fakeTimers that fire only when the test says so.
// No test in this file sleeps to make time pass.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	timer *fakeTimer
	ready chan struct{} // closed once NewTimer has been called
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_000_000, 0).UTC(), ready: make(chan struct{})}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timer = &fakeTimer{ch: make(chan time.Time, 1), active: true, resets: make(chan struct{}, 16)}
	close(c.ready)
	return c.timer
}

// fire delivers a tick as if the flush interval elapsed. It first waits
// for the aggregator goroutine to have created the timer — firing races
// with Run's startup otherwise.
func (c *fakeClock) fire(t *testing.T) {
	t.Helper()
	select {
	case <-c.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("fire: aggregator never created a timer")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.timer.expire(c.now) {
		t.Fatal("fire: timer is not active")
	}
}

// awaitReset blocks until the aggregator rearms the timer — the only
// observable proof that an *empty* tick was consumed (it produces no
// flush the test could wait on).
func (c *fakeClock) awaitReset(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	timer := c.timer
	c.mu.Unlock()
	select {
	case <-timer.resets:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for timer reset")
	}
}

// fakeTimer mimics pre-Go-1.23 time.Timer semantics: the tick channel is
// buffered, Stop reports whether the timer was still pending, and an
// unconsumed tick survives until drained — exactly the behavior the
// resetTimer drain pattern exists to handle.
type fakeTimer struct {
	mu     sync.Mutex
	ch     chan time.Time
	active bool
	resets chan struct{}
}

func (ft *fakeTimer) C() <-chan time.Time { return ft.ch }

func (ft *fakeTimer) expire(now time.Time) bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if !ft.active {
		return false
	}
	ft.active = false
	ft.ch <- now // cap 1; aggregator always drains before Reset
	return true
}

func (ft *fakeTimer) Stop() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	was := ft.active
	ft.active = false
	return was
}

func (ft *fakeTimer) Reset(time.Duration) bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	was := ft.active
	ft.active = true
	select {
	case ft.resets <- struct{}{}:
	default:
	}
	return was
}

// fakeSink hands each flushed batch to the test through a channel so the
// test can block on "a flush happened" instead of polling.
type fakeSink struct {
	batches chan BatchStats
	err     error
}

func newFakeSink() *fakeSink {
	return &fakeSink{batches: make(chan BatchStats, 16)}
}

func (s *fakeSink) WriteBatch(_ context.Context, b BatchStats) error {
	s.batches <- b
	return s.err
}

func (s *fakeSink) next(t *testing.T) BatchStats {
	t.Helper()
	select {
	case b := <-s.batches:
		return b
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a flush")
		return BatchStats{}
	}
}

func (s *fakeSink) expectNone(t *testing.T) {
	t.Helper()
	select {
	case b := <-s.batches:
		t.Fatalf("unexpected flush: %+v", b)
	default:
	}
}

// --- helpers ---------------------------------------------------------------

const baseSec = 3_920_000_000 // arbitrary NTP-era seconds for test packets

// meas builds a measurement whose one-way delay is exactly rtt.
func meas(seq uint32, prio packet.Priority, rtt time.Duration) pipeline.Measurement {
	sent := packet.Timestamp{Seconds: baseSec, Fraction: 0}
	return pipeline.Measurement{
		Pkt:    packet.Packet{Seq: seq, Timestamp: sent, Priority: prio},
		RxTime: sent.Time().Add(rtt),
	}
}

type harness struct {
	clock *fakeClock
	sink  *fakeSink
	in    chan pipeline.Measurement
	done  chan error
}

// start runs an aggregator in a goroutine and returns the test harness.
func start(t *testing.T, ctx context.Context, maxBatch int) *harness {
	t.Helper()
	h := &harness{
		clock: newFakeClock(),
		sink:  newFakeSink(),
		in:    make(chan pipeline.Measurement), // unbuffered: send-return means "aggregator has it"
		done:  make(chan error, 1),
	}
	agg := New(Config{MaxBatch: maxBatch, Clock: h.clock, Sink: h.sink})
	go func() { h.done <- agg.Run(ctx, h.in) }()
	return h
}

func (h *harness) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("aggregator did not exit")
		return nil
	}
}

// --- tests -----------------------------------------------------------------

func TestSizeTriggeredFlush(t *testing.T) {
	h := start(t, context.Background(), 5)
	for i := 0; i < 5; i++ {
		h.in <- meas(uint32(i), packet.PriorityNormal, time.Millisecond)
	}
	b := h.sink.next(t)
	if b.Reason != ReasonSize || b.Count != 5 {
		t.Errorf("got reason=%q count=%d, want size/5", b.Reason, b.Count)
	}
	// The batch buffer must be reusable: a second full batch flushes too.
	for i := 5; i < 10; i++ {
		h.in <- meas(uint32(i), packet.PriorityNormal, time.Millisecond)
	}
	if b := h.sink.next(t); b.Reason != ReasonSize || b.Count != 5 {
		t.Errorf("second batch: got reason=%q count=%d, want size/5", b.Reason, b.Count)
	}
	close(h.in)
	if err := h.wait(t); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
}

func TestIntervalTriggeredFlush(t *testing.T) {
	h := start(t, context.Background(), 1000)
	for i := 0; i < 3; i++ {
		h.in <- meas(uint32(i), packet.PriorityNormal, time.Millisecond)
	}
	h.clock.fire(t) // interval elapses long before 1000 packets arrive
	b := h.sink.next(t)
	if b.Reason != ReasonInterval || b.Count != 3 {
		t.Errorf("got reason=%q count=%d, want interval/3", b.Reason, b.Count)
	}
	close(h.in)
	h.wait(t)
}

func TestEmptyIntervalEmitsNothing(t *testing.T) {
	h := start(t, context.Background(), 1000)
	h.clock.fire(t)
	h.clock.awaitReset(t) // tick consumed and timer rearmed — with no flush
	h.sink.expectNone(t)
	h.in <- meas(1, packet.PriorityNormal, time.Millisecond)
	close(h.in)
	if err := h.wait(t); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if b := h.sink.next(t); b.Reason != ReasonShutdown || b.Count != 1 {
		t.Errorf("got reason=%q count=%d, want shutdown/1", b.Reason, b.Count)
	}
	h.sink.expectNone(t)
}

func TestShutdownFlushesTail(t *testing.T) {
	h := start(t, context.Background(), 1000)
	for i := 0; i < 7; i++ {
		h.in <- meas(uint32(i), packet.PriorityNormal, time.Millisecond)
	}
	close(h.in)
	if err := h.wait(t); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if b := h.sink.next(t); b.Reason != ReasonShutdown || b.Count != 7 {
		t.Errorf("got reason=%q count=%d, want shutdown/7", b.Reason, b.Count)
	}
}

func TestContextCancelFlushesBestEffort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := start(t, ctx, 1000)
	h.in <- meas(1, packet.PriorityHigh, time.Millisecond)
	h.in <- meas(2, packet.PriorityNormal, time.Millisecond)
	cancel()
	if err := h.wait(t); !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if b := h.sink.next(t); b.Reason != ReasonShutdown || b.Count != 2 {
		t.Errorf("got reason=%q count=%d, want shutdown/2", b.Reason, b.Count)
	}
}

// TestTimerResetAfterSizeFlush covers the Stop+drain+Reset path: a size
// flush rearms the timer, and the rearmed timer still drives a correct
// interval flush afterwards.
func TestTimerResetAfterSizeFlush(t *testing.T) {
	h := start(t, context.Background(), 5)
	for i := 0; i < 5; i++ {
		h.in <- meas(uint32(i), packet.PriorityNormal, time.Millisecond)
	}
	if b := h.sink.next(t); b.Reason != ReasonSize {
		t.Fatalf("got reason=%q, want size", b.Reason)
	}
	h.in <- meas(100, packet.PriorityNormal, time.Millisecond)
	h.in <- meas(101, packet.PriorityNormal, time.Millisecond)
	h.clock.fire(t)
	if b := h.sink.next(t); b.Reason != ReasonInterval || b.Count != 2 {
		t.Errorf("got reason=%q count=%d, want interval/2", b.Reason, b.Count)
	}
	close(h.in)
	h.wait(t)
}

func TestSummaryStats(t *testing.T) {
	tests := []struct {
		name string
		ms   []pipeline.Measurement
		want BatchStats // Count/HighCount/MinRTT/MaxRTT/MeanRTT/SeqGaps only
	}{
		{
			name: "rtt spread and high count",
			ms: []pipeline.Measurement{
				meas(1, packet.PriorityNormal, 10*time.Millisecond),
				meas(2, packet.PriorityHigh, 30*time.Millisecond),
				meas(3, packet.PriorityNormal, 20*time.Millisecond),
			},
			want: BatchStats{Count: 3, HighCount: 1, MinRTT: 10 * time.Millisecond, MaxRTT: 30 * time.Millisecond, MeanRTT: 20 * time.Millisecond, SeqGaps: 0},
		},
		{
			name: "gaps counted across unordered input",
			ms: []pipeline.Measurement{
				meas(9, packet.PriorityNormal, time.Millisecond),
				meas(1, packet.PriorityNormal, time.Millisecond),
				meas(5, packet.PriorityNormal, time.Millisecond),
				meas(2, packet.PriorityNormal, time.Millisecond),
			},
			// sorted: 1,2,5,9 → holes 3,4 and 6,7,8 → 5 missing
			want: BatchStats{Count: 4, MinRTT: time.Millisecond, MaxRTT: time.Millisecond, MeanRTT: time.Millisecond, SeqGaps: 5},
		},
		{
			name: "duplicates are not gaps",
			ms: []pipeline.Measurement{
				meas(4, packet.PriorityNormal, time.Millisecond),
				meas(4, packet.PriorityNormal, time.Millisecond),
				meas(5, packet.PriorityNormal, time.Millisecond),
			},
			want: BatchStats{Count: 3, MinRTT: time.Millisecond, MaxRTT: time.Millisecond, MeanRTT: time.Millisecond, SeqGaps: 0},
		},
		{
			name: "single measurement",
			ms:   []pipeline.Measurement{meas(7, packet.PriorityHigh, 5*time.Millisecond)},
			want: BatchStats{Count: 1, HighCount: 1, MinRTT: 5 * time.Millisecond, MaxRTT: 5 * time.Millisecond, MeanRTT: 5 * time.Millisecond, SeqGaps: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := start(t, context.Background(), 1000)
			for _, m := range tt.ms {
				h.in <- m
			}
			close(h.in)
			h.wait(t)
			got := h.sink.next(t)
			got.FlushedAt, got.Reason = time.Time{}, "" // not under test here
			if got != tt.want {
				t.Errorf("stats mismatch:\n got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}
