// Package packet implements the wire format for a simplified TWAMP-Test
// packet (RFC 5357, unauthenticated mode) extended with a 1-byte Priority
// field used by the collector's priority pipeline.
//
// Wire layout, network byte order (15 bytes fixed header):
//
//	offset  0..3   Sequence Number  (uint32)
//	offset  4..7   Timestamp        seconds since NTP epoch (uint32)
//	offset  8..11  Timestamp        fraction of a second, 2^-32 units (uint32)
//	offset 12..13  Error Estimate   (uint16)
//	offset 14      Priority         (uint8, custom extension)
package packet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Size is the fixed header size in bytes. Datagrams may carry trailing
// padding (RFC 5357 allows padding in TWAMP-Test packets); Unmarshal
// ignores it.
const Size = 15

var (
	ErrShortPacket     = errors.New("packet too short")
	ErrInvalidPriority = errors.New("invalid priority")
	ErrShortBuffer     = errors.New("destination buffer too short")
)

// Priority classifies a measurement for the collector's two-lane pipeline.
type Priority uint8

const (
	PriorityNormal Priority = 0 // routine telemetry
	PriorityHigh   Priority = 1 // alert-class measurement
)

// ntpEpochOffset is the number of seconds between the NTP epoch
// (1900-01-01) and the Unix epoch (1970-01-01). TWAMP timestamps use the
// NTP short format, so every conversion to/from time.Time must apply it.
const ntpEpochOffset = 2208988800

// Timestamp is an NTP 64-bit timestamp: 32-bit seconds + 32-bit fraction.
// It is kept in wire form (instead of time.Time) so Unmarshal stays a pure
// byte-copy with no time-zone or monotonic-clock baggage; callers convert
// only when they actually need wall-clock semantics.
type Timestamp struct {
	Seconds  uint32
	Fraction uint32
}

// TimestampFromTime converts a time.Time to NTP short format.
// The fraction is nanoseconds rescaled to 2^-32 units (ns * 2^32 / 1e9);
// the uint64 intermediate cannot overflow because nsec < 1e9 < 2^30.
func TimestampFromTime(t time.Time) Timestamp {
	return Timestamp{
		Seconds:  uint32(t.Unix() + ntpEpochOffset),
		Fraction: uint32(uint64(t.Nanosecond()) << 32 / 1e9),
	}
}

// Time converts back to time.Time (UTC). Rounding (+1<<31 before the
// shift) keeps the Marshal/Unmarshal round-trip error within 1ns; plain
// truncation would drift low by up to 1ns on every hop.
func (ts Timestamp) Time() time.Time {
	sec := int64(ts.Seconds) - ntpEpochOffset
	nsec := int64((uint64(ts.Fraction)*1e9 + 1<<31) >> 32)
	return time.Unix(sec, nsec).UTC()
}

// Packet is a decoded TWAMP-Test measurement packet.
type Packet struct {
	Seq           uint32
	Timestamp     Timestamp
	ErrorEstimate uint16
	Priority      Priority
}

// Marshal encodes p into dst, which must be at least Size bytes.
// It writes into the caller's buffer instead of allocating so the sender's
// hot path can reuse one buffer per goroutine.
func (p *Packet) Marshal(dst []byte) error {
	if len(dst) < Size {
		return fmt.Errorf("%w: need %d bytes, have %d", ErrShortBuffer, Size, len(dst))
	}
	binary.BigEndian.PutUint32(dst[0:4], p.Seq)
	binary.BigEndian.PutUint32(dst[4:8], p.Timestamp.Seconds)
	binary.BigEndian.PutUint32(dst[8:12], p.Timestamp.Fraction)
	binary.BigEndian.PutUint16(dst[12:14], p.ErrorEstimate)
	dst[14] = byte(p.Priority)
	return nil
}

// Unmarshal decodes b into p. It validates length and field ranges and
// returns an error for any malformed input — a UDP listener is fed
// attacker-controlled bytes, so this path must never panic.
// The decoded fields are plain values copied out of b; p holds no
// reference to b, so the caller may return b to its pool immediately.
func (p *Packet) Unmarshal(b []byte) error {
	if len(b) < Size {
		return fmt.Errorf("%w: need %d bytes, have %d", ErrShortPacket, Size, len(b))
	}
	if prio := Priority(b[14]); prio != PriorityNormal && prio != PriorityHigh {
		return fmt.Errorf("%w: %d", ErrInvalidPriority, b[14])
	}
	p.Seq = binary.BigEndian.Uint32(b[0:4])
	p.Timestamp.Seconds = binary.BigEndian.Uint32(b[4:8])
	p.Timestamp.Fraction = binary.BigEndian.Uint32(b[8:12])
	p.ErrorEstimate = binary.BigEndian.Uint16(b[12:14])
	p.Priority = Priority(b[14])
	return nil
}
