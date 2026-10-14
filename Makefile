.PHONY: all test test-race bench run lint clean

all: test-race build

build:
	go build -o bin/workerpool-demo ./cmd/demo/main.go

run:
	go run ./cmd/demo/main.go

test:
	go test -v ./...

test-race:
	go test -v -race ./...

bench:
	go test -bench=. ./pkg/workerpool

clean:
	rm -rf bin/
