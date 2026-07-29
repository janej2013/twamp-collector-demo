package receiver

import (
	"net"
	"testing"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
)

// chanEnqueuer forwards measurements to the test.
type chanEnqueuer struct {
	ch chan pipeline.Measurement
}

func (e *chanEnqueuer) Enqueue(m pipeline.Measurement) bool {
	e.ch <- m
	return true
}

// TestReceiveBothPaths exercises the recvmmsg batch path and the
// portable ReadFrom path over real loopback UDP.
func TestReceiveBothPaths(t *testing.T) {
	tests := []struct {
		name          string
		forceReadFrom bool
	}{
		{"batch path", false},
		{"readfrom path", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enq := &chanEnqueuer{ch: make(chan pipeline.Measurement, 64)}
			r, err := New(Config{Addr: "127.0.0.1:0", Readers: 2, Batch: 8, ForceReadFrom: tt.forceReadFrom}, enq)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			r.Start()

			conn, err := net.Dial("udp", r.LocalAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			const valid = 5
			buf := make([]byte, packet.Size)
			for i := 0; i < valid; i++ {
				p := packet.Packet{
					Seq:       uint32(100 + i),
					Timestamp: packet.TimestampFromTime(time.Now()),
					Priority:  packet.PriorityHigh,
				}
				if err := p.Marshal(buf); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Write(buf); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := conn.Write([]byte{1, 2, 3}); err != nil { // malformed: too short
				t.Fatal(err)
			}

			seqs := map[uint32]bool{}
			timeout := time.After(5 * time.Second)
			for len(seqs) < valid {
				select {
				case m := <-enq.ch:
					seqs[m.Pkt.Seq] = true
					if m.RxTime.IsZero() {
						t.Error("RxTime not set")
					}
				case <-timeout:
					t.Fatalf("got %d/%d measurements before timeout", len(seqs), valid)
				}
			}
			for i := 0; i < valid; i++ {
				if !seqs[uint32(100+i)] {
					t.Errorf("missing seq %d", 100+i)
				}
			}

			// The malformed datagram may still be in flight; poll the
			// counter with a deadline instead of sleeping a fixed time.
			deadline := time.Now().Add(5 * time.Second)
			for r.ParseErrors() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if got := r.ParseErrors(); got != 1 {
				t.Errorf("ParseErrors = %d, want 1", got)
			}
			if got := r.Received(); got != valid+1 {
				t.Errorf("Received = %d, want %d", got, valid+1)
			}
		})
	}
}

// TestCloseUnblocksReaders verifies shutdown: Close must return promptly
// even though every reader is blocked in a read syscall.
func TestCloseUnblocksReaders(t *testing.T) {
	enq := &chanEnqueuer{ch: make(chan pipeline.Measurement, 1)}
	r, err := New(Config{Addr: "127.0.0.1:0", Readers: 4}, enq)
	if err != nil {
		t.Fatal(err)
	}
	r.Start()

	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return; readers still blocked")
	}
}
