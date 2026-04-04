.PHONY: all build test lint clean migrate-up migrate-down docker-build docker-up docker-down

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
BINARY_DIR=bin
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -s -w"

# Services
SERVICES=api indexer liquidator worker

all: lint test build

## Build

build: $(SERVICES)

$(SERVICES):
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$@ ./cmd/$@/

clean:
	rm -rf $(BINARY_DIR)

## Test

test:
	$(GOTEST) ./... -count=1 -race -timeout 120s

test-cover:
	$(GOTEST) ./... -count=1 -race -coverprofile=coverage.out -timeout 120s
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

test-short:
	$(GOTEST) ./... -short -count=1 -timeout 60s

## Lint

lint:
	$(GOVET) ./...

lint-full:
	golangci-lint run ./...

## Database migrations (requires golang-migrate CLI)

migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

migrate-create:
	@read -p "Migration name: " name; \
	migrate create -ext sql -dir migrations -seq $$name

## Docker

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

## Helpers

deps:
	$(GOCMD) mod tidy
	$(GOCMD) mod verify

run-api:
	$(GOBUILD) -o $(BINARY_DIR)/api ./cmd/api/ && ./$(BINARY_DIR)/api

run-indexer:
	$(GOBUILD) -o $(BINARY_DIR)/indexer ./cmd/indexer/ && ./$(BINARY_DIR)/indexer

run-liquidator:
	$(GOBUILD) -o $(BINARY_DIR)/liquidator ./cmd/liquidator/ && ./$(BINARY_DIR)/liquidator

run-worker:
	$(GOBUILD) -o $(BINARY_DIR)/worker ./cmd/worker/ && ./$(BINARY_DIR)/worker
