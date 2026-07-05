# kubehz-agent — developer convenience targets.
# Versions are pinned/verified in go.mod, Dockerfile, and .github/workflows.

VERSION ?= dev
IMAGE   ?= ghcr.io/kernpilot/kubehz-agent:$(VERSION)
LDFLAGS := -s -w -X github.com/kernpilot/kubehz-agent/internal/buildinfo.Version=$(VERSION)

.PHONY: all build test race vet fmt fmt-check lint vuln docker tidy clean

all: fmt-check vet test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/kubehz-agent ./cmd/kubehz-agent

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

lint:
	golangci-lint run ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.5.0 ./...

docker:
	docker build -t $(IMAGE) --build-arg VERSION=$(VERSION) .

tidy:
	go mod tidy

clean:
	rm -rf bin
