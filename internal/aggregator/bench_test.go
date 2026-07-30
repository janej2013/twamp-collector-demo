package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
)

type discardSink struct{}

func (discardSink) WriteBatch(context.Context, BatchStats) error { return nil }

// BenchmarkAggregatorThroughput measures end-to-end cost per measurement
// through the Run loop with 1000-sized batches — channel receive, append,
// and the amortized summarize (sort + min/max/mean) per flush.
func BenchmarkAggregatorThroughput(b *testing.B) {
	in := make(chan pipeline.Measurement, 4096)
	agg := New(Config{MaxBatch: 1000, Sink: discardSink{}})
	done := make(chan error, 1)
	go func() { done <- agg.Run(context.Background(), in) }()

	m := pipeline.Measurement{
		Pkt:    packet.Packet{Timestamp: packet.Timestamp{Seconds: 3_920_000_000}},
		RxTime: time.Now(),
	}
	b.ReportAllocs()
	seq := uint32(0)
	for b.Loop() {
		m.Pkt.Seq = seq
		seq++
		in <- m
	}
	close(in)
	<-done
}
