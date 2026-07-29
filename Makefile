GO ?= go

.PHONY: build vet test race fuzz bench run-collector run-sender demo chart ci stack-up stack-down load clean

build:
	$(GO) build -o bin/ ./cmd/...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

# Fuzz the wire parser; extend -fuzztime locally for deeper runs.
fuzz:
	$(GO) test -run='^$$' -fuzz=FuzzUnmarshal -fuzztime=30s ./internal/packet/

bench:
	$(GO) test -run='^$$' -bench=. -benchmem ./internal/packet/ ./internal/aggregator/

run-collector: build
	./bin/collector

run-sender: build
	./bin/sender -duration 10s

# Backpressure demo: tiny lanes + oversized bursts, drop counters sampled
# from /metrics once a second.
demo: build
	./scripts/demo-backpressure.sh

# Re-run the demo while sampling /metrics at 200ms, then re-render the
# README chart (docs/backpressure.svg) from the capture.
chart: build
	./scripts/demo-capture.sh
	python3 scripts/render_chart.py

ci: vet race

# Prometheus (:9091) + Grafana (:3000, anonymous) scraping the collector.
stack-up:
	docker compose -f deploy/docker-compose.yml up -d

stack-down:
	docker compose -f deploy/docker-compose.yml down

# 60s of bursty demo traffic for the Grafana dashboard (make stack-up
# first). Override with: make load DURATION=5m
load: build
	DURATION=$(DURATION) ./scripts/demo-load.sh

DURATION ?= 60s

clean:
	rm -rf bin
