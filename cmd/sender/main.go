// Command sender generates TWAMP-Test traffic against the collector:
// steady rate-limited load, a configurable high-priority share, random
// sequence skips that look like network loss, and optional flat-out
// bursts to make the collector's backpressure drops visible.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/janej2013/twamp-collector-demo/internal/packet"
)

// chunk is how many packets one limiter grant may cover; see the pacing
// note on tokenBucket.
const chunk = 64

func main() {
	if err := run(); err != nil {
		slog.Error("sender failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		target     = flag.String("target", "127.0.0.1:8620", "collector UDP address")
		rate       = flag.Int("rate", 50000, "steady packets per second")
		duration   = flag.Duration("duration", 0, "how long to send (0 = until Ctrl-C)")
		highRatio  = flag.Float64("high-ratio", 0.05, "share of packets sent with high priority")
		loss       = flag.Float64("loss", 0.01, "probability of skipping a sequence number (simulated loss)")
		burstSize  = flag.Int("burst-size", 0, "packets per burst, sent unpaced (0 = no bursts)")
		burstEvery = flag.Duration("burst-every", 5*time.Second, "interval between bursts")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := net.Dial("udp", *target)
	if err != nil {
		return fmt.Errorf("dial collector: %w", err)
	}
	defer conn.Close()

	s := &sender{
		conn:      conn,
		rng:       rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		highRatio: *highRatio,
		loss:      *loss,
		buf:       make([]byte, packet.Size), // one reused buffer: the send path allocates nothing
	}

	start := time.Now()
	end := time.Time{}
	if *duration > 0 {
		end = start.Add(*duration)
	}
	tb := newTokenBucket(float64(*rate), chunk, start)
	nextBurst := time.Time{}
	if *burstSize > 0 {
		nextBurst = start.Add(*burstEvery)
	}

	slog.Info("sending", "target", *target, "rate", *rate, "high_ratio", *highRatio,
		"loss", *loss, "burst_size", *burstSize)

	for ctx.Err() == nil {
		now := time.Now()
		if !end.IsZero() && !now.Before(end) {
			break
		}
		// Burst mode bypasses the limiter entirely: the point is to
		// overwhelm the collector's lanes and move the drop counters.
		if !nextBurst.IsZero() && !now.Before(nextBurst) {
			for i := 0; i < *burstSize; i++ {
				s.sendOne(now)
			}
			nextBurst = now.Add(*burstEvery)
			continue
		}
		n, wait := tb.grant(chunk, now)
		if n == 0 {
			if !sleepCtx(ctx, wait) {
				break
			}
			continue
		}
		for i := 0; i < n; i++ {
			s.sendOne(time.Now())
		}
	}

	elapsed := time.Since(start).Seconds()
	slog.Info("done", "sent", s.sent, "skipped_seqs", s.skipped,
		"elapsed_s", fmt.Sprintf("%.1f", elapsed),
		"actual_pps", fmt.Sprintf("%.0f", float64(s.sent)/elapsed))
	return nil
}

type sender struct {
	conn      net.Conn
	rng       *rand.Rand
	highRatio float64
	loss      float64
	buf       []byte
	seq       uint32
	sent      uint64
	skipped   uint64
}

func (s *sender) sendOne(now time.Time) {
	// Simulated loss: burn a sequence number without sending, so the
	// collector's gap detection has something to find.
	if s.loss > 0 && s.rng.Float64() < s.loss {
		s.seq++
		s.skipped++
	}
	prio := packet.PriorityNormal
	if s.rng.Float64() < s.highRatio {
		prio = packet.PriorityHigh
	}
	p := packet.Packet{
		Seq:       s.seq,
		Timestamp: packet.TimestampFromTime(now),
		Priority:  prio,
	}
	if err := p.Marshal(s.buf); err != nil {
		panic(err) // impossible: buf is exactly packet.Size
	}
	// Write errors (e.g. ICMP port unreachable surfacing on a connected
	// UDP socket) are ignored on purpose: a load generator should keep
	// offering load whether or not the collector is up yet.
	_, _ = s.conn.Write(s.buf)
	s.seq++
	s.sent++
}

// sleepCtx sleeps for d but wakes early on cancellation; reports whether
// the caller should keep running.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
