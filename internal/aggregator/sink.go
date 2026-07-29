package aggregator

import (
	"context"
	"encoding/json"
	"io"
)

// Sink consumes flushed batch statistics. This is the seam where a
// production deployment would plug in a Kafka producer or remote-write
// client; the demo ships JSON lines to stdout. Implementations are
// called from the single aggregator goroutine, so they need not be
// safe for concurrent use.
type Sink interface {
	WriteBatch(ctx context.Context, b BatchStats) error
}

// JSONLineSink writes one JSON object per line to w.
type JSONLineSink struct {
	enc *json.Encoder
}

func NewJSONLineSink(w io.Writer) *JSONLineSink {
	return &JSONLineSink{enc: json.NewEncoder(w)}
}

func (s *JSONLineSink) WriteBatch(_ context.Context, b BatchStats) error {
	return s.enc.Encode(b) // Encode appends the newline
}
