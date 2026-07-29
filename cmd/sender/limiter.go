package main

import "time"

// tokenBucket is a hand-rolled token-bucket rate limiter.
//
// Algorithm: the bucket accrues `rate` tokens per second up to `burst`;
// sending one packet costs one token. Refill is computed lazily from the
// elapsed time on each call — no background goroutine, no ticker.
//
// The caller asks for a *chunk* of tokens at once. Pacing 50k pps one
// packet at a time would need ~20µs sleep precision, far below what
// timers deliver reliably (~1ms floor); granting up to `want` tokens per
// wakeup turns that into a few hundred wakeups per second while the
// long-run average still converges on `rate` exactly.
type tokenBucket struct {
	rate   float64 // tokens added per second
	burst  float64 // bucket capacity
	tokens float64 // current fill, 0..burst
	last   time.Time
}

func newTokenBucket(rate, burst float64, now time.Time) *tokenBucket {
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: now}
}

// grant refills the bucket for the time elapsed since the previous call,
// then hands out up to want whole tokens. When none are available it
// returns 0 and the wait until one token will exist. Time is an explicit
// parameter so tests drive the bucket deterministically.
func (tb *tokenBucket) grant(want int, now time.Time) (n int, wait time.Duration) {
	if elapsed := now.Sub(tb.last); elapsed > 0 { // guard against clock steps backwards
		tb.tokens = min(tb.burst, tb.tokens+elapsed.Seconds()*tb.rate)
	}
	tb.last = now
	if tb.tokens < 1 {
		return 0, time.Duration((1 - tb.tokens) / tb.rate * float64(time.Second))
	}
	n = min(want, int(tb.tokens))
	tb.tokens -= float64(n)
	return n, 0
}
