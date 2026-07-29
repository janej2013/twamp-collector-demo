package main

import (
	"testing"
	"time"
)

// All scenarios drive the bucket with explicit clock values — the
// limiter is pure state + arithmetic, so no test here ever sleeps.
func TestTokenBucketGrant(t *testing.T) {
	t0 := time.Unix(1000, 0)
	tests := []struct {
		name  string
		rate  float64
		burst float64
		steps []struct {
			at       time.Duration // offset from t0
			want     int
			wantN    int
			wantWait time.Duration
		}
	}{
		{
			name: "initial burst then empty",
			rate: 10, burst: 5,
			steps: []struct {
				at       time.Duration
				want     int
				wantN    int
				wantWait time.Duration
			}{
				{0, 3, 3, 0},                      // bucket starts full (5), grant 3
				{0, 10, 2, 0},                     // 2 left, partial grant
				{0, 1, 0, 100 * time.Millisecond}, // empty: 1 token at 10/s = 100ms away
			},
		},
		{
			name: "refill accrues with elapsed time",
			rate: 100, burst: 10,
			steps: []struct {
				at       time.Duration
				want     int
				wantN    int
				wantWait time.Duration
			}{
				{0, 10, 10, 0},                                        // drain the full bucket
				{50 * time.Millisecond, 10, 5, 0},                     // 50ms * 100/s = 5 tokens back
				{50 * time.Millisecond, 10, 0, 10 * time.Millisecond}, // same instant twice: no refill
			},
		},
		{
			name: "refill caps at burst",
			rate: 1000, burst: 4,
			steps: []struct {
				at       time.Duration
				want     int
				wantN    int
				wantWait time.Duration
			}{
				{time.Hour, 100, 4, 0}, // an hour of refill still yields only burst=4
			},
		},
		{
			name: "clock stepping backwards does not refill",
			rate: 10, burst: 1,
			steps: []struct {
				at       time.Duration
				want     int
				wantN    int
				wantWait time.Duration
			}{
				{time.Second, 1, 1, 0},
				{0, 1, 0, 100 * time.Millisecond}, // now < last: elapsed clamped to 0
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := newTokenBucket(tt.rate, tt.burst, t0)
			for i, s := range tt.steps {
				n, wait := tb.grant(s.want, t0.Add(s.at))
				if n != s.wantN {
					t.Errorf("step %d: granted %d, want %d", i, n, s.wantN)
				}
				// waits are float-derived; allow 1ms slack
				if d := wait - s.wantWait; d < -time.Millisecond || d > time.Millisecond {
					t.Errorf("step %d: wait %v, want %v", i, wait, s.wantWait)
				}
			}
		})
	}
}

// TestTokenBucketLongRunRate checks convergence: simulated 1s of steady
// polling must grant rate*1s tokens within rounding error.
func TestTokenBucketLongRunRate(t *testing.T) {
	t0 := time.Unix(1000, 0)
	const rate = 50000.0
	tb := newTokenBucket(rate, 64, t0)
	granted := 0
	for step := 0; step < 1000; step++ { // poll every 1ms of simulated time
		n, _ := tb.grant(64, t0.Add(time.Duration(step)*time.Millisecond))
		granted += n
	}
	// 999ms of refill + initial burst of 64
	want := int(rate*0.999) + 64
	if granted < want-100 || granted > want+100 {
		t.Errorf("granted %d tokens over 1s, want ~%d", granted, want)
	}
}
