package packet

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		pkt  Packet
	}{
		{"zero value", Packet{}},
		{"normal telemetry", Packet{Seq: 42, Timestamp: Timestamp{Seconds: 3900000000, Fraction: 0x80000000}, ErrorEstimate: 0x8001, Priority: PriorityNormal}},
		{"high priority", Packet{Seq: 7, Timestamp: Timestamp{Seconds: 1, Fraction: 1}, ErrorEstimate: 0xFFFF, Priority: PriorityHigh}},
		{"max fields", Packet{Seq: 0xFFFFFFFF, Timestamp: Timestamp{Seconds: 0xFFFFFFFF, Fraction: 0xFFFFFFFF}, ErrorEstimate: 0xFFFF, Priority: PriorityHigh}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf [Size]byte
			if err := tt.pkt.Marshal(buf[:]); err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var got Packet
			if err := got.Unmarshal(buf[:]); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if got != tt.pkt {
				t.Errorf("round trip mismatch:\n got %+v\nwant %+v", got, tt.pkt)
			}
		})
	}
}

func TestUnmarshalErrors(t *testing.T) {
	valid := make([]byte, Size)
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{"nil input", nil, ErrShortPacket},
		{"empty input", []byte{}, ErrShortPacket},
		{"one byte short", make([]byte, Size-1), ErrShortPacket},
		{"invalid priority 2", append(append([]byte{}, valid[:Size-1]...), 2), ErrInvalidPriority},
		{"invalid priority 255", append(append([]byte{}, valid[:Size-1]...), 255), ErrInvalidPriority},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Packet
			err := p.Unmarshal(tt.input)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Unmarshal error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnmarshalIgnoresPadding(t *testing.T) {
	want := Packet{Seq: 9, Priority: PriorityHigh}
	buf := make([]byte, Size+41) // TWAMP-Test packets may carry padding
	if err := want.Marshal(buf); err != nil {
		t.Fatal(err)
	}
	var got Packet
	if err := got.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal with padding: %v", err)
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestMarshalShortBuffer(t *testing.T) {
	var p Packet
	if err := p.Marshal(make([]byte, Size-1)); !errors.Is(err, ErrShortBuffer) {
		t.Errorf("Marshal error = %v, want %v", err, ErrShortBuffer)
	}
}

func TestTimestampEpochConversion(t *testing.T) {
	tests := []struct {
		name string
		ts   Timestamp
		want time.Time
	}{
		// The NTP epoch (1900) leads Unix (1970) by exactly 2208988800s.
		{"unix epoch", Timestamp{Seconds: 2208988800, Fraction: 0}, time.Unix(0, 0).UTC()},
		{"half second", Timestamp{Seconds: 2208988800, Fraction: 0x80000000}, time.Unix(0, 500000000).UTC()},
		{"one second before unix epoch", Timestamp{Seconds: 2208988799, Fraction: 0}, time.Unix(-1, 0).UTC()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ts.Time(); !got.Equal(tt.want) {
				t.Errorf("Time() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimestampTimeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
	}{
		{"whole second", time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)},
		{"with nanos", time.Date(2026, 7, 29, 12, 0, 0, 123456789, time.UTC)},
		{"max nanos", time.Date(2026, 7, 29, 12, 0, 0, 999999999, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimestampFromTime(tt.t).Time()
			// The 32-bit fraction has ~233ps resolution; rounding on decode
			// keeps the round-trip error within 1ns.
			if d := got.Sub(tt.t); d < -time.Nanosecond || d > time.Nanosecond {
				t.Errorf("round trip drift %v: got %v, want %v", d, got, tt.t)
			}
		})
	}
}

func FuzzUnmarshal(f *testing.F) {
	seed := make([]byte, Size)
	f.Add(seed)
	f.Add([]byte{})
	f.Add(seed[:Size-1])
	f.Add(append(bytes.Repeat([]byte{0xFF}, Size-1), 1))
	f.Fuzz(func(t *testing.T, data []byte) {
		var p Packet
		if err := p.Unmarshal(data); err != nil {
			return // malformed input must error, never panic
		}
		// Accepted input must survive a canonical re-encode: Marshal of the
		// decoded packet reproduces the first Size bytes exactly.
		var out [Size]byte
		if err := p.Marshal(out[:]); err != nil {
			t.Fatalf("Marshal after successful Unmarshal: %v", err)
		}
		if !bytes.Equal(out[:], data[:Size]) {
			t.Errorf("re-encode mismatch:\n got %x\nwant %x", out, data[:Size])
		}
	})
}
