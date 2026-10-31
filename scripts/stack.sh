#!/bin/bash
# stack.sh starts the local platform in the topology the chaos scenarios run
# against: the five stateless services plus N replicas of each consumer.
#
#   ./scripts/stack.sh [replicas]     # default: 3 routers, 3 dispatchers, 3 gateways
#   ./scripts/stack.sh single         # one of each instead (README §3 by hand)
#
# Why replicas at all: killing "one of one" is not a failure mode. Three is the
# minimum in which a kill -9 hands the partitions to somebody, which is the
# only way to exercise a rebalance and the commit watermark's Keep() path.
#
# It always rebuilds first. The checked-in bin/ holds binaries that predate the
# readiness work - testing stale code silently is the one failure a chaos
# harness must not have, and a "does the binary exist" check cannot catch it.
#
# It refuses to start a service whose port already answers, so running this
# after the single-instance commands of README §3 tells you instead of starting
# a second router that dies on bind and then looks like a crash.
#
# Environment: GO, CHAOS_BIN (default <repo>/bin), CHAOS_LOG (default
# ${TMPDIR:-/tmp}/pubsub-chaos/logs), and PG_DSN / CH_DSN / JWT_SECRET for the
# services themselves.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${CHAOS_BIN:-$ROOT/bin}"
LOG="${CHAOS_LOG:-${TMPDIR:-/tmp}/pubsub-chaos}/logs"
MODE="${1:-replicas}"

export PG_DSN="${PG_DSN:-postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable}"
export CH_DSN="${CH_DSN:-clickhouse://pubsub:pubsub@localhost:9000/pubsub}"
export JWT_SECRET="${JWT_SECRET:-dev-secret}"

cd "$ROOT"
mkdir -p "$LOG"

# The Makefile exports this PATH for the same reason: a toolchain installed
# under ~/.local is not on a bare shell's PATH, and this script has to work
# when it is run directly rather than through make.
if ! command -v "${GO:-go}" >/dev/null 2>&1; then
  export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
fi

echo "building into $BIN ..."
"${GO:-go}" build -o "$BIN/" ./cmd/... || { echo "build failed" >&2; exit 1; }

# busy reports an endpoint that already serves, so a service is never started
# twice into a port it cannot bind: it would exit, and a chaos run would then
# find a replica missing at the worst possible moment.
busy() { [ "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$1/readyz" 2>/dev/null)" = "200" ]; }

refuse_if_serving() { # port, name
  if busy "$1"; then
    echo "port $1 ($2) already serves - stop that instance first," >&2
    echo "otherwise this would start a second one that dies on bind." >&2
    exit 1
  fi
}

# start <binary> <log-name> [ENV=...]
#
# Binary and log name are separate arguments on purpose: the replicas share one
# binary (router, router, router) but must not share a log or a pid file -
# scripts/chaos.sh reads $LOG/d1.pid to kill it and $LOG/d1.log to watch its
# heartbeat, and three replicas writing the same file would make both lie.
start() {
  local bin=$1 log=$2; shift 2
  nohup env "$@" "$BIN/$bin" > "$LOG/$log.log" 2>&1 &
  echo $! > "$LOG/$log.pid"
  echo "  $log pid $(cat "$LOG/$log.pid")"
}

start_services() {
  echo "== five stateless services =="
  start registry-api  registry-api  HTTP_ADDR=:8083
  start publish-api   publish-api   HTTP_ADDR=:8080
  start status-api    status-api    HTTP_ADDR=:8081
  start replay-api    replay-api    HTTP_ADDR=:8084
  start status-writer status-writer HTTP_ADDR=:8087
}

case "$MODE" in
replicas)
  for spec in "8080 publish-api" "8081 status-api" "8083 registry-api" "8084 replay-api" \
              "8087 status-writer" "8085 r1" "8086 d1" "8082 g1"; do
    port=${spec%% *}; name=${spec#* }
    refuse_if_serving "$port" "$name"
  done
  start_services

  echo "== three replicas of each consumer =="
  # Routers: one consumer group, three members.
  start router r1 HTTP_ADDR=:8085
  start router r2 HTTP_ADDR=:8185
  start router r3 HTTP_ADDR=:8285

  # Dispatchers: one group; the inbox duty elects a leader over a PostgreSQL
  # advisory lock, so nothing has to decide which one drains.
  start dispatcher d1 HTTP_ADDR=:8086
  start dispatcher d2 HTTP_ADDR=:8186
  start dispatcher d3 HTTP_ADDR=:8286

  # Gateways: one sse-live group per node, drainer on node-a only - the
  # arrangement docs/SPIKE.md specifies for a multi-node deployment.
  start sse-gateway g1 HTTP_ADDR=:8082 NODE_ID=node-a SSE_DRAINER=true
  start sse-gateway g2 HTTP_ADDR=:8182 NODE_ID=node-b SSE_DRAINER=false
  start sse-gateway g3 HTTP_ADDR=:8282 NODE_ID=node-c SSE_DRAINER=false
  PORTS="8080 8081 8083 8084 8087 8085 8185 8285 8086 8186 8286 8082 8182 8282"
  ;;
single)
  for spec in "8080 publish-api" "8081 status-api" "8083 registry-api" "8084 replay-api" \
              "8087 status-writer" "8085 router" "8086 dispatcher" "8082 sse-gateway"; do
    port=${spec%% *}; name=${spec#* }
    refuse_if_serving "$port" "$name"
  done
  echo "== five services, one instance of each consumer =="
  start registry-api  registry-api  HTTP_ADDR=:8083
  start publish-api   publish-api   HTTP_ADDR=:8080
  start status-api    status-api    HTTP_ADDR=:8081
  start replay-api    replay-api    HTTP_ADDR=:8084
  start status-writer status-writer HTTP_ADDR=:8087
  start router        router        HTTP_ADDR=:8085
  start dispatcher    dispatcher    HTTP_ADDR=:8086
  start sse-gateway   sse-gateway   HTTP_ADDR=:8082 SSE_DRAINER=true
  PORTS="8080 8081 8083 8084 8087 8085 8086 8082"
  ;;
*)
  echo "usage: $0 [replicas|single]" >&2
  exit 2
  ;;
esac

all_ready() {
  local port
  for port in $PORTS; do
    busy "$port" || return 1
  done
  return 0
}

echo "waiting for readiness..."
for _ in $(seq 1 45); do
  if all_ready; then
    echo "  all ready ($(echo $PORTS | wc -w) endpoints)"
    echo "logs: $LOG"
    exit 0
  fi
  sleep 1
done
echo "some services never became ready. Not-ready endpoints:" >&2
for port in $PORTS; do
  busy "$port" || echo "  :$port -> $(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/readyz" 2>/dev/null)" >&2
done
echo "see $LOG/*.log" >&2
exit 1
