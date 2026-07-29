// Command collector receives TWAMP-Test packets over UDP, funnels them
// through the bounded priority pipeline, and emits per-batch summary
// statistics as JSON lines on stdout (logs go to stderr).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/janej2013/twamp-collector-demo/internal/aggregator"
	"github.com/janej2013/twamp-collector-demo/internal/metrics"
	"github.com/janej2013/twamp-collector-demo/internal/pipeline"
	"github.com/janej2013/twamp-collector-demo/internal/receiver"
)

func main() {
	if err := run(); err != nil {
		slog.Error("collector failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen        = flag.String("listen", ":8620", "UDP listen address")
		readers       = flag.Int("readers", 2, "UDP reader goroutines")
		readBatch     = flag.Int("read-batch", 32, "datagrams per ReadBatch syscall")
		forceReadFrom = flag.Bool("force-readfrom", false, "use portable per-datagram reads instead of ReadBatch")
		workers       = flag.Int("workers", 4, "pipeline worker goroutines")
		highCap       = flag.Int("high-cap", 1024, "high-priority lane capacity")
		normalCap     = flag.Int("normal-cap", 8192, "normal lane capacity")
		maxBatch      = flag.Int("max-batch", 1000, "aggregator flush size")
		flushInterval = flag.Duration("flush-interval", 200*time.Millisecond, "aggregator flush interval")
		drainTimeout  = flag.Duration("drain-timeout", 5*time.Second, "graceful shutdown budget before forcing exit")
		metricsAddr   = flag.String("metrics-addr", ":9090", "Prometheus /metrics listen address (empty = disabled)")
	)
	flag.Parse()

	// sigCtx ends on SIGINT/SIGTERM and only gates *intake*; hardCtx is
	// the separate force-stop lever for the drain phase. Tying workers to
	// sigCtx directly would kill them the moment the signal arrives —
	// exactly when we still need them to drain the lanes.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hardCtx, hardStop := context.WithCancel(context.Background())
	defer hardStop()

	pl := pipeline.New(pipeline.Config{HighCap: *highCap, NormalCap: *normalCap, Workers: *workers})

	rcv, err := receiver.New(receiver.Config{
		Addr: *listen, Readers: *readers, Batch: *readBatch, ForceReadFrom: *forceReadFrom,
	}, pl)
	if err != nil {
		return fmt.Errorf("bind %s: %w", *listen, err)
	}

	reg := prometheus.NewRegistry()
	inst := metrics.Register(reg, metrics.Sources{
		Received:      rcv.Received,
		ParseErrors:   rcv.ParseErrors,
		ReadErrors:    rcv.ReadErrors,
		DroppedHigh:   pl.DroppedHigh,
		DroppedNormal: pl.DroppedNormal,
		Depths:        pl.Depths,
	})
	var metricsSrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: mux}
		go func() {
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server failed", "err", err)
			}
		}()
	}

	out := pl.Run(hardCtx)
	agg := aggregator.New(aggregator.Config{
		MaxBatch: *maxBatch, FlushInterval: *flushInterval, OnFlush: inst.ObserveFlush,
	})
	aggDone := make(chan error, 1)
	go func() { aggDone <- agg.Run(hardCtx, out) }()

	rcv.Start()
	slog.Info("collector up", "listen", rcv.LocalAddr().String(),
		"readers", *readers, "workers", *workers, "metrics", *metricsAddr)

	<-sigCtx.Done() // 1. SIGINT/SIGTERM received
	slog.Info("shutting down")

	// 2. Stop the intake: close the UDP socket; Close returns only after
	//    every reader goroutine has exited, so no Enqueue is in flight.
	if err := rcv.Close(); err != nil {
		slog.Warn("receiver close", "err", err)
	}

	// 3. Close the lanes: safe now (step 2 guarantees no senders).
	//    Workers drain whatever is queued, then the merged channel closes.
	pl.Close()

	// 4. Wait for the aggregator: it sees the channel close, flushes the
	//    tail batch and returns nil — bounded by the drain timeout.
	select {
	case err := <-aggDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("aggregator: %w", err)
		}
	case <-time.After(*drainTimeout):
		// 5. Timeout fallback: force-cancel workers and aggregator so a
		//    wedged downstream can never turn shutdown into a hang.
		slog.Warn("drain timeout exceeded; forcing stop")
		hardStop()
		<-aggDone
	}

	// 6. Metrics server goes down last so the final counters stay
	//    scrapeable through the whole drain.
	if metricsSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
	}

	slog.Info("collector stopped",
		"received", rcv.Received(), "parse_errors", rcv.ParseErrors(),
		"dropped_high", pl.DroppedHigh(), "dropped_normal", pl.DroppedNormal())
	return nil
}
