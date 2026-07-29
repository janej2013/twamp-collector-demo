package aggregator

import "time"

// Clock abstracts wall time and timers so tests can drive time-dependent
// flush logic deterministically instead of sleeping and hoping. Only the
// two methods the aggregator actually needs — no clockwork-style kitchen
// sink.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors the *time.Timer surface the aggregator uses. C is a
// method (not a field) so fakes can implement it.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// RealClock returns the production Clock backed by package time.
func RealClock() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time                 { return time.Now() }
func (realClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (rt realTimer) C() <-chan time.Time        { return rt.t.C }
func (rt realTimer) Stop() bool                 { return rt.t.Stop() }
func (rt realTimer) Reset(d time.Duration) bool { return rt.t.Reset(d) }
