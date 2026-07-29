// Package metrics exports the collector's observability surface as
// Prometheus metrics. The hot-path packages stay metrics-agnostic: they
// publish atomic counters and channel depths, and this package samples
// them lazily at scrape time through *Func collectors — a scrape costs a
// few atomic loads, the packet path costs nothing extra.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Sources are the read-only views the exporter samples at scrape time.
type Sources struct {
	Received      func() uint64
	ParseErrors   func() uint64
	ReadErrors    func() uint64
	DroppedHigh   func() uint64
	DroppedNormal func() uint64
	Depths        func() (high, normal int)
}

// Instruments are the push-style metrics the aggregator feeds directly
// via its OnFlush hook.
type Instruments struct {
	flushSeconds prometheus.Histogram
	batchSize    prometheus.Histogram
}

// ObserveFlush matches the aggregator's OnFlush signature.
func (i *Instruments) ObserveFlush(size int, took time.Duration) {
	i.batchSize.Observe(float64(size))
	i.flushSeconds.Observe(took.Seconds())
}

// Register wires all metrics into reg and returns the aggregator-facing
// instruments.
func Register(reg *prometheus.Registry, src Sources) *Instruments {
	counterFn := func(name, help string, labels prometheus.Labels, f func() uint64) {
		reg.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels},
			func() float64 { return float64(f()) },
		))
	}
	counterFn("twamp_received_packets_total", "Datagrams read off the UDP socket (rate() gives packets/s).", nil, src.Received)
	counterFn("twamp_parse_errors_total", "Datagrams rejected by the TWAMP parser.", nil, src.ParseErrors)
	counterFn("twamp_read_errors_total", "Transient UDP read errors.", nil, src.ReadErrors)

	// One time series per priority: the whole point of the demo is
	// watching these two diverge under burst load.
	counterFn("twamp_dropped_packets_total", "Measurements dropped at enqueue because a lane was full.",
		prometheus.Labels{"priority": "high"}, src.DroppedHigh)
	counterFn("twamp_dropped_packets_total", "Measurements dropped at enqueue because a lane was full.",
		prometheus.Labels{"priority": "normal"}, src.DroppedNormal)

	depthFn := func(prio string, f func() int) {
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Name: "twamp_lane_depth", Help: "Measurements currently queued in the lane.",
				ConstLabels: prometheus.Labels{"priority": prio}},
			func() float64 { return float64(f()) },
		))
	}
	depthFn("high", func() int { h, _ := src.Depths(); return h })
	depthFn("normal", func() int { _, n := src.Depths(); return n })

	inst := &Instruments{
		flushSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "twamp_flush_duration_seconds",
			Help:    "Time to summarize a batch and write it to the sink.",
			Buckets: prometheus.ExponentialBuckets(10e-6, 4, 8), // 10µs .. ~160ms
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "twamp_batch_size",
			Help:    "Measurements per flushed batch (max-batch full vs interval partials).",
			Buckets: prometheus.ExponentialBuckets(8, 2, 9), // 8 .. 2048
		}),
	}
	reg.MustRegister(inst.flushSeconds, inst.batchSize)

	// Runtime introspection (goroutines, GC, memory) for free — exactly
	// the numbers to watch while the backpressure demo runs.
	reg.MustRegister(collectors.NewGoCollector())
	return inst
}

// Handler serves reg as a /metrics endpoint.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
