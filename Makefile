# keeper — Kubernetes backup operator.
# `make check` is the single gate: format, generated-code drift, vet, lint,
# unit + envtest integration tests, and the core coverage threshold.

SHELL := /bin/bash -o pipefail
GO    ?= go
export GOFLAGS ?= -p=3

ENVTEST_K8S_VERSION ?= 1.37.0
COVER_MIN ?= 80
IMG ?= keeper:dev

# Installed envtest binaries are reused offline; the first run downloads them
# (kube-apiserver + etcd, ~100 MB) into the per-user setup-envtest cache.
ENVTEST_ASSETS = $(shell $(GO) tool setup-envtest use -i $(ENVTEST_K8S_VERSION) -p path 2>/dev/null || $(GO) tool setup-envtest use $(ENVTEST_K8S_VERSION) -p path)

.PHONY: help
help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n",$$1,$$2}'

.PHONY: generate
generate: ## Regenerate deepcopy code, CRDs and RBAC from the API types and markers
	$(GO) tool controller-gen object paths=./api/...
	$(GO) tool controller-gen crd rbac:roleName=manager-role paths=./... \
		output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: verify-generated
verify-generated: ## Fail if generated files are stale
	@tmp=$$(mktemp -d); trap 'rm -rf $$tmp' EXIT; \
	cp api/v1alpha1/zz_generated.deepcopy.go $$tmp/deepcopy.go && \
	$(GO) tool controller-gen crd rbac:roleName=manager-role paths=./... \
		output:crd:artifacts:config=$$tmp/crd output:rbac:artifacts:config=$$tmp/rbac && \
	$(GO) tool controller-gen object paths=./api/... && \
	diff -r $$tmp/crd config/crd/bases && \
	diff $$tmp/rbac/role.yaml config/rbac/role.yaml && \
	diff $$tmp/deepcopy.go api/v1alpha1/zz_generated.deepcopy.go && \
	echo "generated files are up to date"

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-formatted
	@out=$$(gofmt -l $$(git ls-files '*.go')); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi

.PHONY: vet
vet: ## go vet (type-check + static checks)
	$(GO) vet ./...

.PHONY: lint
lint: ## golangci-lint (zero findings)
	golangci-lint run ./...

.PHONY: envtest
envtest: ## Install (or locate) the envtest kube-apiserver/etcd binaries
	@echo "$(ENVTEST_ASSETS)"

.PHONY: test
test: ## Unit + envtest integration tests with coverage of internal/...
	KUBEBUILDER_ASSETS="$(ENVTEST_ASSETS)" KEEPER_REQUIRE_ENVTEST=1 \
		$(GO) test -race -count=1 -timeout 15m -coverpkg=./internal/... -coverprofile=cover.out ./... \
		2>&1 | grep -v '^{"level"'

.PHONY: cover-check
cover-check: ## Enforce COVER_MIN% statement coverage on core packages
	hack/coverage.sh cover.out $(COVER_MIN)

.PHONY: check
check: fmt-check verify-generated vet lint test cover-check ## The full gate
	@echo "make check: OK"

.PHONY: demo
demo: ## Offline end-to-end demo against envtest + simulated cluster
	$(GO) build -o bin/keeper-demo ./cmd/keeper-demo
	KUBEBUILDER_ASSETS="$(ENVTEST_ASSETS)" ./bin/keeper-demo --days 60

.PHONY: fuzz
fuzz: ## Run the fuzzers for 30s each
	$(GO) test ./internal/mover -run '^$$' -fuzz FuzzRestore -fuzztime 30s
	$(GO) test ./internal/mover -run '^$$' -fuzz FuzzSafePath -fuzztime 30s

.PHONY: bench
bench: ## Retention benchmark
	$(GO) test ./internal/retention -run '^$$' -bench . -benchmem -count 3

.PHONY: integration
integration: ## Storage conformance against a real MinIO (Docker); skips without Docker
	hack/minio-integration.sh

.PHONY: build
build: ## Build the operator binary
	CGO_ENABLED=0 $(GO) build -trimpath -o bin/keeper ./cmd/keeper

.PHONY: docker-build
docker-build: ## Build the operator image
	docker build -t $(IMG) .

.PHONY: clean
clean: ## Remove build outputs
	rm -rf bin cover.out
