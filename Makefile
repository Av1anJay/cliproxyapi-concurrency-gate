.PHONY: all clean linux darwin windows test fmt vet

PLUGIN_NAME := concurrency-gate

all: linux

linux: bin/$(PLUGIN_NAME)-go.so

darwin: bin/$(PLUGIN_NAME)-go.dylib

windows: bin/$(PLUGIN_NAME)-go.dll

bin/$(PLUGIN_NAME)-go.so: $(wildcard *.go) go.mod
	@mkdir -p bin
	CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$(PLUGIN_NAME)-go.so .

bin/$(PLUGIN_NAME)-go.dylib: $(wildcard *.go) go.mod
	@mkdir -p bin
	CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$(PLUGIN_NAME)-go.dylib .

bin/$(PLUGIN_NAME)-go.dll: $(wildcard *.go) go.mod
	@mkdir -p bin
	CGO_ENABLED=1 go build -buildmode=c-shared -o bin/$(PLUGIN_NAME)-go.dll .

test:
	go test -count=1 ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -rf bin/ $(PLUGIN_NAME)-go.h
