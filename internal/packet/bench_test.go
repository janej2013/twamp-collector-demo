package packet

import (
	"testing"
	"time"
)

// The parse path runs once per received packet — at 50k pps every
// allocation here multiplies into GC pressure, so the target is zero
// allocs/op for both directions.
//
// b.Loop (Go 1.24+) both scopes timing to the loop body and keeps the
// calls inside it from being optimized away, so the ~1.6ns figure is not
// a dead-code artifact; the error check doubles as a liveness guard.

func BenchmarkUnmarshal(b *testing.B) {
	src := Packet{Seq: 42, Timestamp: TimestampFromTime(time.Now()), ErrorEstimate: 0x8001, Priority: PriorityHigh}
	buf := make([]byte, Size)
	if err := src.Marshal(buf); err != nil {
		b.Fatal(err)
	}
	var p Packet
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Unmarshal(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal(b *testing.B) {
	p := Packet{Seq: 42, Timestamp: TimestampFromTime(time.Now()), ErrorEstimate: 0x8001, Priority: PriorityHigh}
	buf := make([]byte, Size)
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Marshal(buf); err != nil {
			b.Fatal(err)
		}
	}
}
