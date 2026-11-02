GO    ?= go
export PATH := $(HOME)/.local/go/bin:$(PATH)

.PHONY: build check up down logs infra admin token test tidy fmt clean stack chaos chaos-test

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
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T kafka-2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-2:29092 --list >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T kafka-3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-3:29092 --list >/dev/null 2>&1; do sleep 2; done
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

## stack starts the whole platform locally: five services plus three replicas
## of each consumer. Always rebuilds first, so what runs is what was just
## written. Run `make infra` first; this replaces the single-instance commands
## of README §3 rather than adding to them.
stack:
	./scripts/stack.sh $(MODE)

## chaos SCEN=pods runs one failure scenario against a running stack and judges
## it with the harness oracle: SCEN is one of pods | pg | kafka | ch | rebalance
## | gwfail (rebalance also measures how long a killed replica's partitions stay
## unowned, against KAFKA_SESSION_TIMEOUT; gwfail kills a gateway and leaves it
## down, so its clients have to land on a survivor).
chaos:
	@test -n "$(SCEN)" || (echo "usage: make chaos SCEN=pods   (pods|pg|kafka|ch|rebalance|gwfail)"; exit 1)
	./scripts/chaos.sh $(SCEN)

## chaos-test runs the chaos driver's own tests (pure helpers, no Docker)
chaos-test:
	./scripts/chaos_test.sh

test:
	$(GO) test ./...

tidy:
	$(GO) mod tidy

fmt:
	gofmt -l -w cmd internal migrations

clean:
	rm -rf bin
