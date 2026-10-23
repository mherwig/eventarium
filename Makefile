GO    ?= go
export PATH := $(HOME)/.local/go/bin:$(PATH)

.PHONY: build check up down logs infra admin token test tidy fmt clean

## build compiles every service into bin/
build:
	mkdir -p bin
	$(GO) build -o bin/ ./cmd/...

## check compiles everything and runs go vet
check:
	$(GO) build ./...
	$(GO) vet ./...

## up starts the local infrastructure
up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

## infra starts Kafka, PostgreSQL and ClickHouse and provisions topics and schemas
infra: up
	@echo "waiting for infrastructure..."
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T postgres pg_isready -U pubsub -d pubsub >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T clickhouse wget -qO- http://localhost:8123/ping >/dev/null 2>&1; do sleep 2; done
	$(GO) run ./cmd/admin all

## admin runs the provisioning helper directly against the environment
admin:
	$(GO) run ./cmd/admin all

## token OWNER=team-a [SCOPES=publish,subscribe] [TTL=24h] mints a dev JWT
token:
	@test -n "$(OWNER)" || (echo "usage: make token OWNER=team-a"; exit 1)
	$(GO) run ./cmd/minttoken -owner "$(OWNER)" $(if $(SCOPES),-scopes $(SCOPES)) $(if $(TTL),-ttl $(TTL))

test:
	$(GO) test ./...

tidy:
	$(GO) mod tidy

fmt:
	gofmt -l -w cmd internal migrations

clean:
	rm -rf bin
