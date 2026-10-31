#!/bin/bash
# chaos.sh - one failure scenario per invocation, each judged by the harness
# oracle. The shape of every scenario is the same: publish at a steady rate,
# inject one fault in the middle of it, let it drain, then ask whether anything
# was lost.
#
#   ./scripts/chaos.sh <pods|pg|kafka|ch>
#
#   pods   kill -9 one dispatcher, one router and one gateway mid-run
#   pg     receiver offline AND PostgreSQL down, overlapping by 20s
#   ch     ClickHouse down for 20s (observability only)
#   kafka  broker restart mid-run
#
# Prerequisites: 'make infra' for the databases, then './scripts/stack.sh' for
# the replica topology. The driver checks both and refuses to run otherwise -
# a scenario that passes because a service was missing proves nothing.
#
# Three properties are load-bearing and were each learned the hard way, so do
# not simplify them away:
#
#   - Faults are ASSERTED, not assumed. Every stop/restart is followed by
#     assert_down/assert_up; an unquoted $DOCKER word-split once made every
#     `docker compose stop` exit 127 into /dev/null and the first `pg` run
#     reported success with PostgreSQL never stopped.
#   - The judge waits for the queues AND the owner's inbox to reach 0.
#     Judging early reports work that is merely unfinished as lost - parked
#     deliveries are durable, just not delivered yet.
#   - Each run seeds its own owner. The status table is cumulative and the
#     oracle scopes its query to the run's publish window plus a minute of
#     skew, so an earlier run whose pub file is gone leaks into the next one
#     as `unexpected`. An aborted scenario once produced 18,432 phantom
#     unexpected that way; a trap now kills its publisher on exit.
#
# The SSE clients are deliberately NOT restarted when a gateway dies: they
# reconnect on their own (1s, with Last-Event-ID), which is the behaviour a
# real client depends on. Restarting them would test the harness, not the
# gateway.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${CHAOS_BIN:-$ROOT/bin}"
BASE="${CHAOS_DIR:-${TMPDIR:-/tmp}/pubsub-chaos}"
LOG="$BASE/logs"
DOCKER="${DOCKER:-docker}"
SCEN=${1:-}
cd "$ROOT"   # docker compose resolves compose.yaml from here

export JWT_SECRET="${JWT_SECRET:-dev-secret}"
# Where the API services answer; override if you run them elsewhere.
REGISTRY_API="${REGISTRY_API:-http://localhost:8083}"
PUBLISH_API="${PUBLISH_API:-http://localhost:8080}"
STATUS_API="${STATUS_API:-http://localhost:8081}"
RECEIVER="${RECEIVER:-http://localhost:9090}"

case "$SCEN" in
pods|pg|kafka|ch) ;;
*) echo "usage: $0 <pods|pg|kafka|ch>" >&2; exit 2 ;;
esac

D="$BASE/$SCEN"
rm -rf "$D"; mkdir -p "$D"

# ---------------------------------------------------------------------------
# preflight: refuse to run without the topology, rather than "pass" without it
# ---------------------------------------------------------------------------
[ -x "$BIN/harness" ] || [ -f "$BIN/harness" ] || {
  echo "no binaries in $BIN - run 'make build' first" >&2; exit 1
}
missing=""
for spec in "8080 publish-api" "8081 status-api" "8083 registry-api" "8087 status-writer" \
            "8085 r1" "8185 r2" "8285 r3" \
            "8086 d1" "8186 d2" "8286 d3" \
            "8082 g1" "8182 g2" "8282 g3"; do
  port=${spec%% *}; name=${spec#* }
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$port/readyz" 2>/dev/null)
  [ "$code" = "200" ] || missing="$missing $name(:$port)=$code"
done
if [ -n "$missing" ]; then
  echo "topology not ready - refusing to run a scenario that would prove nothing:" >&2
  echo "  missing:$missing" >&2
  echo "start it with ./scripts/stack.sh   (databases first: make infra)" >&2
  exit 1
fi
# The scenarios read and restart replicas by pid file, so the files have to be
# there too - a /readyz can answer from a process whose pid file was lost.
for n in d1 d2 d3 r1 r2 r3 g1 g2 g3; do
  [ -e "$LOG/$n.pid" ] || {
    echo "no pid file for $n in $LOG - start the topology with ./scripts/stack.sh" >&2
    exit 1
  }
done

# Each run gets its own owner - see the header.
OWNER="${SCEN}$(date +%H%M%S)"
RECV_PID="$BASE/recv.pid"
PUB_PID="$D/pub.pid"

# An aborted scenario must not leave a publisher running against this owner:
# it would keep writing events the next run's status window then reports as
# unexpected. Kill by pid file, never by pattern (see the header).
trap 'stop_publisher; cleanup_clients' EXIT

stop_publisher() {
  [ -e "$PUB_PID" ] && { kill "$(cat "$PUB_PID")" 2>/dev/null; rm -f "$PUB_PID"; }
  return 0
}
stop_receiver() {
  [ -e "$RECV_PID" ] && { kill "$(cat "$RECV_PID")" 2>/dev/null; rm -f "$RECV_PID"; }
  return 0
}
cleanup_clients() {
  local f
  for f in "$D"/sse-*.pid; do
    [ -e "$f" ] && kill "$(cat "$f")" 2>/dev/null
  done
  stop_receiver
  sleep 1
  return 0
}

# ---------------------------------------------------------------------------
# assertions: a scenario must not pass unless its fault actually landed
# ---------------------------------------------------------------------------
assert_down() { # name, command...
  local name=$1; shift
  if "$@" >/dev/null 2>&1; then
    echo "  FAULT NOT APPLIED: $name still reachable - aborting"; exit 3
  fi
  echo "  fault confirmed: $name is down"
}
# Polls, because a database takes seconds to accept connections again and a
# probe checked once at the wrong instant says nothing about whether the
# recovery worked.
assert_up() { # name, command...
  local name=$1; shift
  local i
  for i in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then
      echo "  recovery confirmed: $name is up"
      return 0
    fi
    sleep 1
  done
  echo "  RECOVERY FAILED: $name still not back after 60s - aborting"; exit 3
}
# probe must fail on a non-200: curl itself exits 0 on a 503, so a naive probe
# would make assert_down believe the fault was applied when it was not.
probe() { [ "$(curl -s -o /dev/null -w '%{http_code}' "$1")" = "200" ]; }
probe_code() { curl -s -o /dev/null -w '%{http_code}' "$1"; }
# Both assert_down and assert_up take a *reachability* predicate: it returns 0
# while the thing is still up. assert_down passes when it stops returning 0,
# assert_up when it starts. Passing an inverted "is it gone" helper is the easy
# mistake here and it reads as "FAULT NOT APPLIED" for a fault that landed.
# Through the container rather than an HTTP port: a probe that cannot reach an
# endpoint at all reads as "up or down" depending on nothing but connectivity.
clickhouse_up() {
  "$DOCKER" exec pubsub-clickhouse-1 clickhouse-client -q "SELECT 1" >/dev/null 2>&1
}
postgres_up() {
  "$DOCKER" exec pubsub-postgres-1 pg_isready -U pubsub -d pubsub >/dev/null 2>&1
}
kafka_up() {
  "$DOCKER" exec pubsub-kafka-1 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# counters and waits
# ---------------------------------------------------------------------------
# The dispatcher logs are shared by every scenario, so counters are cumulative
# across them; park_base snapshots them here to report this run's own delta.
park_base() { grep -h 'parking recovered' "$LOG"/d*.log 2>/dev/null | wc -l; }

# inflight is the worst currently-reported value across the dispatcher
# replicas. Taking "the last line of d1..d3" instead reports whichever file the
# glob ended on - often an idle replica, and always misleading.
inflight() {
  local m=0 v f
  for f in "$LOG"/d1.log "$LOG"/d2.log "$LOG"/d3.log; do
    [ -e "$f" ] || continue
    v=$(grep -o 'offsets_in_flight=[0-9]*' "$f" 2>/dev/null | tail -1 | cut -d= -f2)
    [ -n "${v:-}" ] && [ "$v" -gt "$m" ] && m=$v
  done
  echo "offsets_in_flight=$m"
}

# wait_drained waits until every dispatcher replica reports an empty queue.
wait_drained() {
  local deadline=$(( $(date +%s) + ${1:-180} )) q=999999 f v
  while [ "$(date +%s)" -lt "$deadline" ]; do
    q=0
    for f in "$LOG"/d1.log "$LOG"/d2.log "$LOG"/d3.log; do
      [ -e "$f" ] || continue
      v=$(grep "dispatcher heartbeat" "$f" 2>/dev/null | tail -1 | grep -o 'queue_depth=[0-9]*' | cut -d= -f2)
      [ -n "${v:-}" ] && [ "$v" -gt "$q" ] && q=$v
    done
    [ "$q" = "0" ] && break
    sleep 5
  done
  echo "  queue_depth=$q"
}

# wait_inbox waits until this run's owner has no parked rows left. queue_depth
# reaching zero only means the in-memory queues emptied: the inbox backlog is
# released by the circuit breaker's probe (probe_interval_s, 5 min by default)
# and then drained at drain_rate_per_s. Judging before that finishes reports
# parked deliveries as missing - they are durable, just not delivered yet.
wait_inbox() {
  local want=${1:-96} n=0 i
  for i in $(seq 1 "$want"); do
    n=$("$DOCKER" exec pubsub-postgres-1 psql -U pubsub -d pubsub -tAc \
      "SELECT count(*) FROM inbox i JOIN subscriptions s ON s.id = i.subscription_id WHERE s.owner = '$OWNER'" \
      2>/dev/null || echo -1)
    [ "$n" = "0" ] && { echo "  inbox drained for $OWNER (0 rows)"; return 0; }
    sleep 5
  done
  echo "  inbox still holds $n rows for $OWNER after $((want*5))s"
  return 1
}

# ---------------------------------------------------------------------------
# platform interaction
# ---------------------------------------------------------------------------
seed_owner() {
  JWT_SECRET="$JWT_SECRET" "$BIN/harness" seed -api "$REGISTRY_API" -owner "$OWNER" \
    -prefix "$OWNER" -types 4 -subs 40 -webhook-url "$RECEIVER/hook" \
    > "$D/seed.json" 2>"$D/seed.err" || { echo "  seeding $OWNER failed"; exit 4; }
  grep -o '"id": "[^"]*"' "$D/seed.json" | cut -d'"' -f4 > "$D/all_ids.txt"
  # The seed prints objects in creation order; take the SSE ones by channel.
  # (python3 is the one non-shell dependency of this script.)
  python3 - "$D/seed.json" "$D/sse_ids.txt" <<'PYEOF'
import json, sys
subs = json.load(open(sys.argv[1]))
open(sys.argv[2], "w").write("\n".join(s["id"] for s in subs if s.get("channel") == "sse") + "\n")
PYEOF
  echo "  owner=$OWNER subs=$(grep -c . "$D/all_ids.txt") sse=$(grep -c . "$D/sse_ids.txt")"
}

# start_receiver [extra harness recv flags]. One receiver per run, tracked by
# pid file across scenarios - never by pkill pattern.
start_receiver() {
  stop_receiver
  nohup "$BIN/harness" recv -addr :9090 -out "$D/recv.jsonl" "$@" \
    > "$D/recv.log" 2>&1 &
  echo $! > "$RECV_PID"
  echo $! > "$D/recv.pid"
}

# start_sse_clients spreads the run's SSE clients over the three gateways, so
# that a gateway kill takes out a third of them instead of all of them.
start_sse_clients() {
  local i=0 id gw
  for id in $(cat "$D/sse_ids.txt"); do
    i=$((i+1))
    case $((i % 3)) in
      0) gw=http://localhost:8082 ;;
      1) gw=http://localhost:8182 ;;
      2) gw=http://localhost:8282 ;;
    esac
    # 30 minutes, not the 7 the first draft used: the scenario's waits alone
    # (wait_drained 300 + wait_inbox 480) can run for 13 minutes, and a client
    # that exits mid-wait strands its parked rows - there would be nothing left
    # to drain to. cleanup_clients kills them at the end regardless.
    nohup "$BIN/harness" sse -sub "$id" -owner "$OWNER" -addr "$gw" \
      -duration 1800s -out "$D/sse-$i.jsonl" > "$D/sse-$i.log" 2>&1 &
    echo $! > "$D/sse-$i.pid"
  done
  sleep 5
  echo "  sse clients connected: $(grep -l consuming "$D"/sse-*.log 2>/dev/null | wc -l)/$(grep -c . "$D/sse_ids.txt")"
}

start_receivers() {
  start_receiver
  start_sse_clients
}

publish_bg() { # rate duration
  API="$PUBLISH_API" HARNESS_OWNER=$OWNER JWT_SECRET="$JWT_SECRET" \
    nohup "$BIN/harness" pub \
    -types "$OWNER.type.0,$OWNER.type.1,$OWNER.type.2,$OWNER.type.3" \
    -rate "${1:-200}" -duration "${2:-60s}" -payload 1024 \
    -out "$D/pub.jsonl" > "$D/pub.log" 2>&1 &
  echo $! > "$PUB_PID"
}

wait_publish() {
  wait "$(cat "$PUB_PID")" 2>/dev/null
  tail -1 "$D/pub.log"
}

# verify runs the oracle and then adds the check the oracle deliberately does
# not make. missing=0 means every expected delivery was either received or
# still *tracked* by a status record - a parked delivery reads as accounted
# even if it never reaches a receiver. For a loss test that is too weak: if
# received is short of expected, some deliveries are merely known about, and
# the run says so and fails.
verify() {
  echo "--- oracle ($SCEN, owner $OWNER) ---"
  local out line expected received
  out=$(JWT_SECRET="$JWT_SECRET" "$BIN/harness" verify -owner "$OWNER" -api "$REGISTRY_API" \
    -status-api "$STATUS_API" \
    -events "$D/pub.jsonl" -received "$D/recv.jsonl" \
    -sse "$(ls "$D"/sse-*.jsonl 2>/dev/null | tr '\n' ',' | sed 's/,$//')" 2>&1)
  echo "$out" | tail -6
  line=$(echo "$out" | grep -o 'subscriptions=[0-9]* expected=[0-9]* received=[0-9]*.*' | tail -1)
  # The leading space matters: bare 'expected=[0-9]*' also matches inside
  # 'unexpected=0', which made $expected two lines long and turned a perfect
  # run into a reported mismatch.
  expected=$(echo "$line" | grep -o ' expected=[0-9]*' | head -1 | cut -d= -f2)
  received=$(echo "$line" | grep -o ' received=[0-9]*' | head -1 | cut -d= -f2)
  if [ -n "$expected" ] && [ "$received" != "$expected" ]; then
    echo "DELIVERED MISMATCH: received $received of $expected expected deliveries." >&2
    echo "  missing=0 only says they are accounted for; this says whether they" >&2
    echo "  actually arrived. Check wait_inbox - a stranded inbox row never does." >&2
    return 3
  fi
  return 0
}

# start_bin <binary> <log-name> [ENV=...] - restarts a replica a scenario killed.
start_bin() {
  local bin=$1 name=$2; shift 2
  nohup env "$@" "$BIN/$bin" > "$LOG/$name.log" 2>&1 &
  echo $! > "$LOG/$name.pid"
  echo "  restarted $name (pid $(cat "$LOG/$name.pid"))"
}

# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------
case "$SCEN" in

pods)
  seed_owner
  echo "== pods: kill -9 one dispatcher, one router, one gateway mid-run =="
  start_receivers
  publish_bg 200 60s
  sleep 10
  echo "  t=10s  kill -9 d1 (dispatcher)";  kill -9 "$(cat "$LOG/d1.pid")" 2>/dev/null; sleep 15
  echo "  t=25s  kill -9 r1 (router)";      kill -9 "$(cat "$LOG/r1.pid")" 2>/dev/null; sleep 15
  echo "  t=40s  kill -9 g2 (gateway)";     kill -9 "$(cat "$LOG/g2.pid")" 2>/dev/null; sleep 10
  wait_publish
  echo "  publish done; bringing the killed replicas back"
  start_bin dispatcher d1  HTTP_ADDR=:8086
  start_bin router     r1  HTTP_ADDR=:8085
  start_bin sse-gateway g2 HTTP_ADDR=:8182 NODE_ID=node-b SSE_DRAINER=false
  sleep 20
  echo "  sse clients reconnected: $(grep -l reconnect "$D"/sse-*.log 2>/dev/null | wc -l) of 20 logs show a reconnect"
  wait_drained 300
  wait_inbox
  verify
  ;;

pg)
  seed_owner
  # The combination that used to lose data: deliveries cannot reach the
  # receiver, so they have to be parked - and the store that would hold them is
  # gone. Every park fails, so nothing is durable, and the offsets must stand
  # still instead of being committed past.
  echo "== pg: receiver offline AND PostgreSQL down, overlapping by 20s =="
  start_receiver -offline 45s -offline-after 1s
  start_sse_clients
  sleep 3
  publish_bg 200 60s
  sleep 10
  PARK0=$(park_base)
  echo "  t=10s  stop postgres (the receiver is offline, so parks will fail)"
  "$DOCKER" compose stop postgres >/dev/null 2>&1
  sleep 2
  assert_down "PostgreSQL" postgres_up
  assert_down "dispatcher /readyz" probe http://localhost:8086/readyz
  echo "  liveness still 200: $(probe_code http://localhost:8086/healthz)"
  sleep 10
  echo "  offsets in flight while both are down: $(inflight)"
  sleep 10
  echo "  t=30s  start postgres"
  "$DOCKER" compose start postgres >/dev/null 2>&1
  assert_up "dispatcher /readyz" probe http://localhost:8086/readyz
  wait_publish
  echo "  park failures that later recovered: $(( $(park_base) - PARK0 ))"
  wait_drained 300
  echo "  final offsets_in_flight: $(inflight)"
  wait_inbox
  verify
  ;;

kafka)
  seed_owner
  echo "== kafka: broker restart mid-run =="
  start_receivers
  publish_bg 200 60s
  sleep 10
  echo "  t=10s  restart kafka"
  "$DOCKER" compose restart kafka >/dev/null 2>&1
  assert_up "kafka" kafka_up
  sleep 20
  wait_publish
  wait_drained 300
  wait_inbox
  verify
  ;;

ch)
  seed_owner
  echo "== ch: ClickHouse down for 20s mid-run (observability only) =="
  start_receivers
  publish_bg 200 60s
  sleep 10
  echo "  t=10s  stop clickhouse"
  "$DOCKER" compose stop clickhouse >/dev/null 2>&1
  sleep 2
  assert_down "ClickHouse" clickhouse_up
  assert_down "status-api /readyz" probe http://localhost:8081/readyz
  echo "  status-api liveness still 200: $(probe_code http://localhost:8081/healthz)"
  echo "  dispatcher (no ClickHouse dependency) stays ready: $(probe_code http://localhost:8086/readyz)"
  sleep 18
  echo "  t=30s  start clickhouse"
  "$DOCKER" compose start clickhouse >/dev/null 2>&1
  assert_up "status-api /readyz" probe http://localhost:8081/readyz
  wait_publish
  wait_drained 300
  wait_inbox
  verify
  ;;
esac

cleanup_clients
