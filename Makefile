GO ?= go

.PHONY: build vet test race fuzz bench run-collector run-sender demo ci clean

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

ci: vet race

clean:
	rm -rf bin
