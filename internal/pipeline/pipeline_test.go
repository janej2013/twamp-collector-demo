package pipeline

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
)

func meas(seq uint32, prio packet.Priority) Measurement {
	return Measurement{Pkt: packet.Packet{Seq: seq, Priority: prio}}
}

// collect reads from out until it closes or the deadline hits.
func collect(t *testing.T, out <-chan Measurement, deadline time.Duration) []Measurement {
	t.Helper()
	var got []Measurement
	timeout := time.After(deadline)
	for {
		select {
		case m, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, m)
		case <-timeout:
			t.Fatalf("output channel not closed after %v (got %d items)", deadline, len(got))
		}
	}
}

func TestEnqueueRouting(t *testing.T) {
	tests := []struct {
		name               string
		prio               packet.Priority
		wantHigh, wantNorm int
	}{
		{"high goes to high lane", packet.PriorityHigh, 1, 0},
		{"normal goes to normal lane", packet.PriorityNormal, 0, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(Config{HighCap: 4, NormalCap: 4, Workers: 1})
			if !p.Enqueue(meas(1, tt.prio)) {
				t.Fatal("Enqueue rejected with room to spare")
			}
			if h, n := p.Depths(); h != tt.wantHigh || n != tt.wantNorm {
				t.Errorf("Depths() = (%d, %d), want (%d, %d)", h, n, tt.wantHigh, tt.wantNorm)
			}
		})
	}
}

func TestEnqueueDropsWhenLaneFull(t *testing.T) {
	tests := []struct {
		name    string
		prio    packet.Priority
		dropped func(*Pipeline) uint64
	}{
		{"high lane", packet.PriorityHigh, (*Pipeline).DroppedHigh},
		{"normal lane", packet.PriorityNormal, (*Pipeline).DroppedNormal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(Config{HighCap: 1, NormalCap: 1, Workers: 1})
			if !p.Enqueue(meas(1, tt.prio)) {
				t.Fatal("first Enqueue should be accepted")
			}
			// No consumer is running: the lane is full, so this must
			// return false immediately rather than block.
			if p.Enqueue(meas(2, tt.prio)) {
				t.Fatal("second Enqueue should be dropped")
			}
			if got := tt.dropped(p); got != 1 {
				t.Errorf("dropped = %d, want 1", got)
			}
		})
	}
}

// TestPriorityOrder pre-fills both lanes, then starts a single worker:
// the non-blocking fast path must drain every high item before any
// normal item is delivered.
func TestPriorityOrder(t *testing.T) {
	const perLane = 8
	p := New(Config{HighCap: perLane, NormalCap: perLane, Workers: 1})
	for i := 0; i < perLane; i++ {
		p.Enqueue(meas(uint32(i), packet.PriorityNormal))
		p.Enqueue(meas(uint32(100+i), packet.PriorityHigh))
	}
	p.Close()
	got := collect(t, p.Run(context.Background()), 5*time.Second)
	if len(got) != 2*perLane {
		t.Fatalf("got %d measurements, want %d", len(got), 2*perLane)
	}
	for i, m := range got {
		wantHigh := i < perLane
		if isHigh := m.Pkt.Priority == packet.PriorityHigh; isHigh != wantHigh {
			t.Fatalf("position %d: priority %d breaks high-before-normal order", i, m.Pkt.Priority)
		}
	}
}

// TestCloseDrains verifies the graceful path: everything accepted before
// Close is delivered downstream, nothing is lost.
func TestCloseDrains(t *testing.T) {
	const n = 100
	p := New(Config{HighCap: n, NormalCap: n, Workers: 3})
	for i := 0; i < n; i++ {
		prio := packet.PriorityNormal
		if i%10 == 0 {
			prio = packet.PriorityHigh
		}
		if !p.Enqueue(meas(uint32(i), prio)) {
			t.Fatalf("Enqueue(%d) rejected", i)
		}
	}
	p.Close()
	got := collect(t, p.Run(context.Background()), 5*time.Second)
	if len(got) != n {
		t.Errorf("drained %d measurements, want %d", len(got), n)
	}
}

// TestContextCancel verifies the fast-stop path: cancellation closes the
// output channel even with items still queued and no reader draining them.
func TestContextCancel(t *testing.T) {
	p := New(Config{HighCap: 8, NormalCap: 8, Workers: 2})
	for i := 0; i < 8; i++ {
		p.Enqueue(meas(uint32(i), packet.PriorityNormal))
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := p.Run(ctx)
	cancel()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // workers exited, channel closed
			}
		case <-deadline:
			t.Fatal("workers did not exit after context cancellation")
		}
	}
}

// TestConcurrentEnqueueAccounting hammers Enqueue from several goroutines
// under -race and checks conservation: sent == delivered + dropped.
func TestConcurrentEnqueueAccounting(t *testing.T) {
	const producers, perProducer = 8, 500
	p := New(Config{HighCap: 64, NormalCap: 64, Workers: 4})
	out := p.Run(context.Background())

	delivered := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range out {
			delivered++
		}
	}()

	var wg sync.WaitGroup
	wg.Add(producers)
	for g := 0; g < producers; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				prio := packet.PriorityNormal
				if i%2 == 0 {
					prio = packet.PriorityHigh
				}
				p.Enqueue(meas(uint32(g*perProducer+i), prio))
			}
		}(g)
	}
	wg.Wait()
	p.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not finish")
	}

	dropped := int(p.DroppedHigh() + p.DroppedNormal())
	if total := delivered + dropped; total != producers*perProducer {
		t.Errorf("delivered %d + dropped %d = %d, want %d", delivered, dropped, total, producers*perProducer)
	}
}

// TestNoGoroutineLeak runs a full lifecycle and then requires the
// goroutine count to return to its baseline — the standard-library way
// to get goleak-style assurance.
func TestNoGoroutineLeak(t *testing.T) {
	baseline := runtime.NumGoroutine()

	p := New(Config{HighCap: 16, NormalCap: 16, Workers: 4})
	out := p.Run(context.Background())
	for i := 0; i < 16; i++ {
		p.Enqueue(meas(uint32(i), packet.PriorityHigh))
	}
	p.Close()
	for range out {
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines leaked: baseline %d, now %d", baseline, runtime.NumGoroutine())
		}
		runtime.Gosched()
	}
}
