# cfgcanary developer tasks. `make help` lists them.
SHELL := /usr/bin/env bash
VERSION ?= 0.1.0
COMMIT  ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell git log -1 --format=%cI 2>/dev/null || echo unknown)
LDFLAGS := -s -w -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Version=$(VERSION) \
  -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Commit=$(COMMIT) \
  -X github.com/raunaksahahere/cfgcanary/internal/buildinfo.Date=$(DATE)
COMPOSE := docker compose -f deploy/compose/compose.yaml
TESTDEPS := docker compose -f deploy/compose/test-deps.yaml

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  %-20s %s\n",$$1,$$2}'

.PHONY: build
build: ## Build bin/cfgcanary
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/cfgcanary ./cmd/cfgcanary

.PHONY: wasm
wasm: ## Build the example consumer-parser validator (WASI)
	GOOS=wasip1 GOARCH=wasm go build -trimpath -o examples/ratelimits/validator.wasm ./examples/ratelimits/validator

.PHONY: gen
gen: ## Regenerate protobuf, gateway and OpenAPI artifacts
	scripts/gen.sh

.PHONY: gen-check
gen-check: gen ## Fail if generated files are out of date
	git diff --exit-code -- api/gen api/openapi

.PHONY: fmt
fmt: ## Format Go code
	gofmt -s -w $$(git ls-files '*.go' | grep -v '^api/gen/')

.PHONY: lint
lint: ## gofmt check, go vet, staticcheck
	@out=$$(gofmt -s -l $$(git ls-files '*.go' | grep -v '^api/gen/')); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...

.PHONY: vuln
vuln: ## Scan dependencies and code with govulncheck
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

.PHONY: test
test: wasm ## Unit, property and fuzz-seed tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-deps-up
test-deps-up: ## Start PostgreSQL, the S3 store and Prometheus for integration tests
	$(TESTDEPS) up -d --wait

.PHONY: test-deps-down
test-deps-down: ## Stop and remove the integration-test services
	$(TESTDEPS) down -v

.PHONY: test-integration
test-integration: wasm test-deps-up ## Integration tests against real services
	CFGCANARY_IT=1 go test -race -count=1 -timeout 20m ./test/integration/...

.PHONY: chaos
chaos: wasm test-deps-up ## Chaos suite (Toxiproxy, leader kills, outages)
	CFGCANARY_IT=1 go test -count=1 -timeout 30m ./test/chaos/...

.PHONY: bench
bench: build test-deps-up ## Scale and baseline measurements; writes bench/results
	scripts/bench.sh

.PHONY: demo
demo: ## Run the secure 50-agent Compose demo end to end
	scripts/demo.sh

.PHONY: up
up: ## Start the Compose stack
	$(COMPOSE) up -d --build --wait

.PHONY: down
down: ## Stop the Compose stack and remove its volumes
	$(COMPOSE) down -v

.PHONY: dist
dist: ## Cross-compile release archives and checksums into dist/
	scripts/dist.sh $(VERSION)

.PHONY: clean
clean: ## Remove build output
	rm -rf bin dist
