#!/bin/bash
# chaos.sh - one failure scenario per invocation, each judged by the harness
# oracle. The shape of every scenario is the same: publish at a steady rate,
# inject one fault in the middle of it, let it drain, then ask whether anything
# was lost.
#
#   ./scripts/chaos.sh <pods|pg|kafka|ch>
#
#   pods      kill -9 one dispatcher, one router and one gateway mid-run
#   pg        receiver offline AND PostgreSQL down, overlapping by 20s
#   ch        ClickHouse down for 20s (observability only)
#   kafka     broker restart mid-run
#   rebalance kill -9 one dispatcher mid-run and measure how long its
#             partitions stay unowned, against the configured session timeout
#   gwfail    kill -9 one gateway and leave it down: its clients must land on
#             a survivor and its subscriptions must become routable again
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
pods|pg|kafka|ch|rebalance|gwfail) ;;
*) echo "usage: $0 <pods|pg|kafka|ch|rebalance|gwfail>" >&2; exit 2 ;;
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
# The script's exit status. It has to be accumulated explicitly: the last
# statement after the case is cleanup_clients, which deliberately returns 0, so
# a bare `verify` at the end of a scenario would have made every run exit 0 -
# the driver would have been incapable of failing. scenarios call judged(),
# which ORs the oracle's verdict into this.
FAIL=0
# judged settles the status store, then asks the oracle. Membership comes
# first because it is the cause of the other two: a writer that has left its
# group cannot settle, and cannot be told apart from one that is merely busy -
# so asking it first keeps a wedged writer from costing a settle timeout before
# it is named. The settle comes before the oracle because the oracle reads
# ClickHouse through the Status API, and a verdict taken while status-writer is
# still catching up would report in-flight rows as lost - or, worse, pass a run
# whose rows are still sitting in Kafka.
judged() {
  status_writer_in_group 30 || FAIL=1
  status_settled 60 || FAIL=1
  verify || FAIL=1
}

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

# port_holder lists the pids listening on a port.
port_holder() {
  ss -ltnp 2>/dev/null | awk -v p=":$1" '$4 ~ p"$" {print}' | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u | tr '\n' ' '
}

# is_harness_recv reports whether a pid is a `harness recv` - the only process
# this driver will ever kill by port.
is_harness_recv() {
  tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null | grep -q 'harness recv'
}

stop_receiver() {
  local p
  [ -e "$RECV_PID" ] && { kill "$(cat "$RECV_PID")" 2>/dev/null; rm -f "$RECV_PID"; }
  # A receiver from an earlier run - or one started by hand - holding :9090
  # makes every receiver since fail to bind, and it fails *quietly*: the new
  # process exits, recv.jsonl stays empty, and the shortfall only surfaces at
  # the oracle minutes later as a mystery missing=39713. Clear the port here,
  # where the intent is unambiguous. Only a process that is itself a
  # `harness recv` is touched; anything else keeps the port and is reported
  # by wait_for_receiver.
  for p in $(port_holder 9090); do
    if is_harness_recv "$p"; then
      echo "  stopping a leftover delivery receiver on :9090 (pid $p)"
      kill "$p" 2>/dev/null
    fi
  done
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
# All three brokers, not one. With RF=3 the kafka scenario restarts broker 1,
# so checking broker 1 alone would report the cluster up while two of three
# were still down.
#
# Bootstrap on the INTERNAL listener, never CLIENT. These run inside the
# container, where localhost is that container: CLIENT metadata hands back
# localhost:9094 for broker 2, which is not listening inside broker 3, so a
# cluster-wide --list fails on every broker but its own. INTERNAL metadata
# returns kafka-N:29092, which resolves on the Compose network from anywhere.
kafka_up() {
  local n h
  for n in 1 2 3; do
    case $n in
      1) h=kafka ;;
      2) h=kafka-2 ;;
      3) h=kafka-3 ;;
    esac
    "$DOCKER" exec "pubsub-kafka-$n" \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$h:29092" --list >/dev/null 2>&1 || return 1
  done
}

# session_seconds turns KAFKA_SESSION_TIMEOUT into whole seconds for the
# rebalance bound. It understands plain seconds ("15s", "15") and whole minutes
# ("1m"); an unrecognised shape falls back to the Go default rather than
# producing arithmetic that fails inside the test. That fallback is pinned to
# kafkautil.DefaultSessionTimeout by TestSessionSecondsFallbackMatchesDefault,
# because a stale copy here once printed a 60s bound for a 15s session - the
# measurement was right and the number printed next to it was four times too
# generous, which is worse than no number at all.
session_seconds() {
  local v="${KAFKA_SESSION_TIMEOUT:-15s}" num unit
  num=${v%[a-z]}
  unit=${v#"$num"}
  case "$num" in ''|*[!0-9]*) echo 15; return ;; esac
  # 0 is numeric but not a usable session timeout (franz-go rejects it below
  # 100ms), and it would make the failover bound a nonsense 15s - fall back to
  # the tested default rather than asserting against a broken value.
  [ "$num" -gt 0 ] || { echo 15; return; }
  case "$unit" in
    ""|s) echo "$num" ;;
    m)    echo $(( num * 60 )) ;;
    h)    echo $(( num * 3600 )) ;;
    *)    echo 15 ;;
  esac
}

# assignment prints one line per group member: "<consumer-id> <partition> ...",
# sorted. A partition nobody owns shows as "-", which is how the rebalance
# scenario tells "taken over by a live member" apart from "still owned by the
# dead one" and "in transit, owned by nobody".
assignment() {
  "$DOCKER" exec pubsub-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group dispatcher 2>/dev/null \
    | awk 'NR>1 && $3 ~ /^[0-9]+$/ { p[$7] = p[$7] " " $3 }
           END { for (k in p) print k p[k] }' | sort
}

# unowned_count counts the partitions whose owner is "-".
unowned_count() {
  printf '%s\n' "$1" | tr ' ' '\n' | grep -c '^-$'
}

# handover_complete reports whether a post-kill assignment shows a *completed*
# handover: $1 is the assignment before the kill, $2 the one measured now.
#
# Every condition exists because leaving one out reads as success at the wrong
# moment: unchanged means the group has not noticed the death yet; a "-"
# anywhere means the group is mid-rebalance and those partitions are owned by
# nobody, which is the state that actually stops delivery. Accepting either
# would report a fast failover that had not happened.
# partition_count counts the partitions named across the assignment, so a
# shrunken answer cannot look complete.
partition_count() {
  printf '%s\n' "$1" | tr ' ' '\n' | grep -c '^[0-9][0-9]*$'
}

handover_complete() {
  [ -n "$2" ] || return 1                            # no answer at all
  [ "$1" != "$2" ] || return 1                       # still the pre-kill assignment
  [ "$(unowned_count "$2")" = "0" ] || return 1      # someone still in transit
  [ "$(partition_count "$2")" -ge "$(partition_count "$1")" ]  # nothing dropped
}

# ---------------------------------------------------------------------------
# counters and waits
# ---------------------------------------------------------------------------
# presence_rows <node> counts presence rows claiming a node, with no visibility
# join: how many rows a kill -9 leaves behind. The process dies without
# publishing a disconnect, so these stay - precisely the state the PostgreSQL
# design has to handle, and the one the sse-presence topic could not (a key is
# only ever superseded by a newer record for the same key, so a dead node's
# entries outlived it).
presence_rows() {
  docker_value "presence rows of $1" \
    "$DOCKER" exec pubsub-postgres-1 psql -U pubsub -d pubsub -tAc \
    "SELECT count(*) FROM sse_presence WHERE node_id = '$1'"
}

# visible_subs <port> reads one gateway's /healthz "presence": the number of
# subscriptions its in-memory view can route to, counting connections on *every*
# node. That view is what pick() answers from, so this number falling after a
# node dies is the observable proof that the dead node's rows stopped being
# routable - the whole point of the lease.
visible_subs() {
  curl -s --max-time 3 "http://localhost:$1/healthz" 2>/dev/null \
    | grep -o '"presence":[0-9]*' | cut -d: -f2
}

# docker_value <label> <command...> runs one docker command and returns its
# stdout, retrying.
#
# A docker exec that fails under load prints nothing and exits non-zero, and an
# empty answer is not a measurement. Reading it as one is the same class of bug
# as the unquoted $DOCKER that once made every `docker compose stop` exit 127
# into /dev/null while the first `pg` run reported success with PostgreSQL
# never stopped - this time failing the other way, on a run that had actually
# passed: presence_rows returned nothing and the assertion read "no answer" as
# "measured zero". Every assertion here is fed by one of these answers, so each
# is retried and a caller that still gets nothing has to say so rather than
# report a number it never obtained. DW_TRIES exists so the helper's own tests
# do not have to wait out the real retry budget.
docker_value() { # $1 = label, $2.. = command
  local label=$1 out try
  shift
  for try in $(seq 1 "${DW_TRIES:-5}"); do
    if out=$("$@" 2>/dev/null) && [ -n "$out" ]; then
      printf '%s' "$out"
      return 0
    fi
    sleep 1
  done
  echo "  could not read: $label (docker exec failed ${DW_TRIES:-5} times)" >&2
  return 1
}

# visible_nodes <port> prints the nodes a gateway currently routes to, from
# /healthz's routable_nodes. Unlike visible_subs this is per *node*, which is
# what a lease assertion needs: a dead node's subscriptions may have been
# re-registered elsewhere, hiding its disappearance from any count.
visible_nodes() {
  curl -s --max-time 3 "http://localhost:$1/healthz" 2>/dev/null \
    | grep -o '"routable_nodes":\[[^]]*\]' | sed 's/.*://'
}

# wait_presence_drop <before> <ports...> waits until every surviving gateway
# counts fewer routable subscriptions than before the kill, and prints the new
# count. The lease TTL bounds how long a dead node may stay visible, the reload
# poll and the NOTIFY trigger are both faster than that bound, so 30s is
# generous. Exits non-zero if the count never drops - which would mean the
# dead node's connections are still being routed to.
wait_presence_drop() {
  local before=$1 t0 now v port
  shift
  t0=$(date +%s)
  while :; do
    now=$(( $(date +%s) - t0 ))
    v=""
    for port in "$@"; do
      v=$(visible_subs "$port")
      [ -n "$v" ] && [ "$v" -lt "$before" ] || { v=""; break; }
    done
    [ -n "$v" ] && { echo "$v"; return 0; }
    [ "$now" -ge 30 ] && return 1
    sleep 1
  done
}

# lag_total sums the LAG column of a kafka-consumer-groups listing on stdin.
# A partition that has records but no committed offset reports "-" rather than
# a number; its whole log end is behind, so it counts as that many. A partition
# with nothing produced is settled at zero however it is displayed.
lag_total() {
  awk 'NR>1 && $3 ~ /^[0-9]+$/ {
         if ($6 ~ /^[0-9]+$/) s += $6
         else if ($5 ~ /^[0-9]+$/ && $5+0 > 0) s += $5
       }
       END { print s+0 }'
}

# status_writer_commit_trouble counts the offset-commit failures status-writer
# has logged. Any of them makes lag an unsound completeness signal, and this is
# the only place that fact is visible:
#
#   - flushOnce inserts before it commits, so a commit that fails does not mean
#     the rows are missing - they are in ClickHouse already.
#   - Once commits fail maxCommitFailures times the offsets are dropped on
#     purpose (cmd/status-writer: the partitions were revoked, the commit can
#     never succeed, the records may be re-consumed, the table is append-only),
#     and from then on the committed offset sits behind by exactly those
#     records. Lag then counts what may be *re-read*, not what has *not been
#     inserted*, and it grows by one scenario's production per run.
#   - A later successful commit would jump the offset forward again, but that
#     path only logs at debug level, so the writer cannot tell us when it
#     happened. After a broker restart it does not happen at all: the writer
#     keeps inserting from its stale assignment while the group reports no
#     active members, which is how lag reached 1.8M with unrecorded=0.
#
# Kafka lag itself cannot make this distinction - it is one number for two
# different conditions - so the row-level check (unrecorded) is what gates the
# verdict, and this only decides whether lag is allowed to fail a run.
status_writer_commit_trouble() {
  local n
  n=$(grep -c -E "committing offsets|dropping offsets" "$LOG/status-writer.log" 2>/dev/null)
  echo "${n:-0}"
}

# status_writer_in_group waits until status-writer is a member of its own
# consumer group, and fails if it never is.
#
# This is the regression guard for the bug these runs found, and lag cannot
# stand in for it: a writer that has lost its membership keeps inserting from
# its stale assignment, so the store fills in and every row-level check stays
# green while the committed offset stops moving forever. That is why
# status_settled treats lag as uninterpretable after commit trouble - and
# precisely why something has to assert membership separately, or the
# exemption would hide the very failure it was written to describe.
#
# The failure it guards against: BlockRebalanceOnPoll holds rebalance
# callbacks until the application calls AllowRebalance, and this loop never
# stopped polling. After a broker restart the coordinator evicted the member,
# kgo could not rejoin through the gate it was holding itself, and every
# commit failed with UNKNOWN_MEMBER_ID for the rest of the process's life -
# 28 dropped-offset batches and 22 minutes in the run that found it, with
# lag climbing to 1.8M and unrecorded=0 the whole way.
status_writer_in_group() {
  local want=${1:-30} t0 now
  t0=$(date +%s)
  while :; do
    local out
    if out=$(docker_value "status-writer group state" \
        "$DOCKER" exec pubsub-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:29092 --describe --group status-writer); then
      # stderr matters here: kafka-consumer-groups.sh writes the "no active
      # members" notice there, and reading stdout alone reports a wedged
      # writer as a healthy one.
      if ! printf '%s\n' "$out" | grep -q "has no active members"; then
        echo "  status-writer is a member of its consumer group"
        return 0
      fi
    fi
    now=$(( $(date +%s) - t0 ))
    if [ "$now" -ge "$want" ]; then
      echo "  STATUS-WRITER LEFT ITS GROUP: no member after ${now}s" >&2
      echo "    it keeps inserting, so every row-level check stays green, but it" >&2
      echo "    can never commit again: lag grows without bound and a restart" >&2
      echo "    re-consumes everything it has inserted since" >&2
      return 1
    fi
    sleep 2
  done
}

# status_settled waits until status-writer has caught up on delivery-status,
# then says so; exits non-zero only if it fails to catch up in <seconds> *and*
# the lag still means what lag usually means.
#
# Lag 0 is a sound sufficient claim, and it is the one the ch scenario exists
# to test: status-writer commits an offset only *after* the insert into
# ClickHouse succeeded (cmd/status-writer), so while lag reads 0 nothing can be
# left in Kafka - including the rows written while ClickHouse was down. Judging
# before that would read "still in flight" as "lost".
#
# The converse does not hold, and asserting it produced a false failure on
# every scenario that rebalances. flushOnce inserts and then commits; when the
# commit keeps failing with UNKNOWN_MEMBER_ID - which is what a broker restart
# leaves behind - status-writer deliberately gives up on those offsets after
# maxCommitFailures, on purpose and documented there: the partitions were
# revoked, the commit can never succeed, the records may be re-consumed and the
# table is append-only. From that moment on, lag counts what may be *re-read*,
# not what has *not been inserted*, and it grows by one scenario's production
# per run while the rows are in ClickHouse the whole time - which is exactly
# why the oracle prints unrecorded=0 in the same breath as a five-digit lag.
#
# So a non-zero lag is reported as incomplete only when the group still has a
# member, i.e. when it can only mean un-inserted rows. When the writer is not
# in its group, this says so and hands the verdict to the row-level check
# (unrecorded), which does not depend on offsets at all.
status_settled() {
  local want=${1:-60} t0 now lag
  t0=$(date +%s)
  while :; do
    if ! lag=$(status_lag); then
      echo "  cannot read status-writer's lag, which is not the same as lag 0:" >&2
      echo "    a run cannot be judged on a number it never obtained" >&2
      return 1
    fi
    if [ "$lag" = "0" ]; then
      echo "  status-writer caught up: delivery-status lag=0"
      return 0
    fi
    now=$(( $(date +%s) - t0 ))
    if [ "$now" -ge "$want" ]; then
      if [ "$(status_writer_commit_trouble)" -gt 0 ]; then
        echo "  lag=${lag} is not a statement about the store: status-writer has reported"
        echo "    offset-commit failures, it inserts before it commits, and dropped offsets"
        echo "    (cmd/status-writer, maxCommitFailures) leave the committed offset behind"
        echo "    by exactly those records. What may be re-read is not what is missing."
        echo "    Completeness is decided by unrecorded= below, which reads rows, not offsets."
        return 0
      fi
      echo "  STATUS INCOMPLETE: delivery-status still ${lag} record(s) behind after ${now}s" >&2
      echo "    status-writer is in its group and still committing, so these are rows" >&2
      echo "    ClickHouse has not received - the status store lost them" >&2
      return 1
    fi
    sleep 2
  done
}

# wait_presence_rise <target> <ports...> waits until every named gateway
# reports at least <target> routable subscriptions. The mirror of
# wait_presence_drop: after a gateway dies its subscriptions leave every view
# (the lease), and they only come back when their clients have reconnected to
# a *surviving* gateway - which is what makes this the failover assertion
# rather than a presence assertion.
wait_presence_rise() {
  local target=$1 t0 now v port
  shift
  t0=$(date +%s)
  while :; do
    now=$(( $(date +%s) - t0 ))
    v=""
    for port in "$@"; do
      v=$(visible_subs "$port")
      [ -n "$v" ] && [ "$v" -ge "$target" ] || { v=""; break; }
    done
    [ -n "$v" ] && { echo "$v"; return 0; }
    [ "$now" -ge 60 ] && return 1
    sleep 1
  done
}

# clients_moved <gateway-url> counts the run's SSE clients that were assigned
# to a gateway and whose own log shows they dialled a different one.
clients_moved() {
  local gw=$1 f log n=0
  for f in "$D"/sse-*.gw; do
    [ -e "$f" ] || continue
    [ "$(cat "$f")" = "$gw" ] || continue
    log="${f%.gw}.log"
    grep -q "trying http" "$log" 2>/dev/null && n=$((n+1))
  done
  echo "$n"
}

# assigned_to <gateway-url> counts the clients that started on a gateway.
assigned_to() {
  local gw=$1 f n=0
  for f in "$D"/sse-*.gw; do
    [ -e "$f" ] || continue
    [ "$(cat "$f")" = "$gw" ] && n=$((n+1))
  done
  echo "$n"
}

# wait_node_excluded <port> <node> <seconds> waits until the gateway on <port>
# no longer lists <node> among the nodes its view routes to.
#
# This is the lease assertion that does not depend on the routable
# *subscription* count falling - which it may not, when the dead node's
# clients have already reconnected elsewhere and re-registered there. What
# must stop is routing to a node that is not answering: the rows stay (a
# kill -9 writes no disconnect), the view must stop counting them.
wait_node_excluded() {
  local port=$1 node=$2 want=$3 t0 now
  t0=$(date +%s)
  while :; do
    if ! visible_nodes "$port" | grep -q "$node"; then
      echo "  $node no longer routed to from :$port (rows kept, lease expired)"
      return 0
    fi
    now=$(( $(date +%s) - t0 ))
    if [ "$now" -ge "$want" ]; then
      echo "  PRESENCE LEASE FAILED: :$port still routes to $node after ${now}s" >&2
      echo "    view: $(visible_nodes "$port")" >&2
      echo "    deliveries aimed there go to a consumer group nobody is in" >&2
      return 1
    fi
    sleep 1
  done
}

# status_lost_total sums the dispatchers' own counter of status records they
# could never hand to Kafka: the emitter's buffer filled, or shutdown gave up
# on them. This is the one link in the status path that neither lag nor the
# oracle can see - the record never reached the topic, so Kafka-to-ClickHouse
# completeness says nothing about it and no row is missing yet, only late to
# exist.
#
# The heartbeat counter is cumulative and the logs outlive a scenario, so a
# run snapshots it before publishing and asserts the delta after - the same
# reason park_base exists.
status_lost_total() {
  local sum=0 f v
  for f in "$LOG"/d1.log "$LOG"/d2.log "$LOG"/d3.log; do
    [ -e "$f" ] || continue
    v=$(grep -ohE 'status_lost=[0-9]+' "$f" 2>/dev/null | cut -d= -f2 | sort -n | tail -1)
    sum=$((sum + ${v:-0}))
  done
  echo "$sum"
}

# The dispatcher logs are shared by every scenario, so counters are cumulative
# across them; park_base snapshots them here to report this run's own delta.
park_base() { grep -h 'parking recovered' "$LOG"/d*.log 2>/dev/null | wc -l; }

# status_lag sums status-writer's lag on delivery-status: how many status
# records have been produced but not yet inserted into ClickHouse.
status_lag() {
  local out
  # Not piped straight into lag_total: a describe that failed would reach it
  # as no input, and lag_total answers 0 for no input - which would report a
  # wedged broker as a caught-up writer.
  out=$(docker_value "status-writer lag" \
    "$DOCKER" exec pubsub-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group status-writer) || return 1
  printf '%s\n' "$out" | lag_total
}

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
  wait_for_sse_visibility
}

# wait_for_sse_visibility holds setup until every gateway's in-memory index
# knows the subscriptions that were just created.
#
# The window is real and measured at 50-60 ms after the API returns 201 (the
# NOTIFY fast path, not the 1 s poll). It matters because the harness's SSE
# client treats HTTP 404 as fatal - and so does a browser: EventSource stops
# for good on a 4xx rather than retrying - so a client that connects inside
# that window dies at once, never reconnects, and its whole subscription's
# deliveries then park as no_connection with nobody left to drain them. Seeding
# and connecting back to back cost four of twenty clients and 3,187
# undelivered rows in the first rebalance run; the oracle reported
# received=55179 of 58366 and failed the run, which is the correct verdict on
# a race the *setup* caused rather than the fault under test.
#
# /sse/ack answers the existence question without opening a stream: 404 means
# the index has not caught up, anything else means it has. All three gateways
# must agree, since each keeps its own index.
wait_for_sse_visibility() {
  local last tok t0 code pending
  # The LAST SSE subscription created, not the first. A reload swaps the whole
  # index, so seeing the last one proves each gateway read it after every
  # insert and therefore holds all twenty. Probing the first one passes
  # instantly - it was visible long before the others existed - and proves
  # nothing, which is how this check first gave false confidence while two
  # clients still got 404.
  last=$(tail -1 "$D/sse_ids.txt")
  tok=$("$BIN/minttoken" -owner "$OWNER" -scopes subscribe 2>/dev/null) || true
  [ -n "$tok" ] || { echo "  cannot mint a probe token - skipping the visibility check"; return 0; }
  t0=$(date +%s)
  while :; do
    pending=""
    for port in 8082 8182 8282; do
      code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -X POST \
        -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' \
        -d "{\"subscription_id\":\"$last\",\"sequence\":\"0:0\"}" \
        "http://localhost:$port/sse/ack" 2>/dev/null)
      [ "$code" = "404" ] && pending="$pending :$port"
    done
    if [ -z "$pending" ]; then
      echo "  all three gateways know the newest subscription"
      return 0
    fi
    if [ $(( $(date +%s) - t0 )) -ge 5 ]; then
      echo "  SETUP FAILED: gateways$pending still answer 404 after 5s" >&2
      echo "    a client connecting now would be rejected for good, so the" >&2
      echo "    run would fail for a reason that is not the fault under test" >&2
      exit 4
    fi
    sleep 0.1
  done
}


# start_receiver [extra harness recv flags]. One receiver per run, tracked by
# pid file across scenarios - never by pkill pattern.
start_receiver() {
  stop_receiver
  nohup "$BIN/harness" recv -addr :9090 -out "$D/recv.jsonl" "$@" \
    > "$D/recv.log" 2>&1 &
  echo $! > "$RECV_PID"
  echo $! > "$D/recv.pid"
  wait_for_receiver
}

# wait_for_receiver refuses to continue until the receiver really is listening.
# A failed bind used to be invisible until the oracle: the process exits, its
# log gets one line nobody reads, and every webhook delivery of the run is
# silently never received - the run then fails minutes later as a shortfall
# that looks like the platform dropped them. Failing here, loudly, with the
# reason, is the whole improvement.
wait_for_receiver() {
  local pid t0 now holder
  pid=$(cat "$RECV_PID")
  t0=$(date +%s)
  while :; do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "SETUP FAILED: the delivery receiver exited at startup:" >&2
      sed 's/^/    /' "$D/recv.log" >&2
      exit 4
    fi
    holder=$(port_holder 9090)
    case " $holder " in
      *" $pid "*) return 0 ;;
    esac
    now=$(( $(date +%s) - t0 ))
    if [ "$now" -ge 5 ]; then
      echo "SETUP FAILED: :9090 is held by pid(s) ${holder:-nothing} and our receiver ($pid) never bound." >&2
      echo "  Stop whatever holds the port and re-run." >&2
      exit 4
    fi
    sleep 0.2
  done
}

# start_sse_clients spreads the run's SSE clients over the three gateways, so
# that a gateway kill takes out a third of them instead of all of them.
# start_sse_clients [fallback-gateway-url ...]
#
# Each client keeps its own gateway first, so the initial spread is exactly
# what it always was; the extra addresses are what a client behind a load
# balancer gets anyway - a way out when its gateway dies. Without them a client
# pins one address forever and a permanently dead gateway strands it, which is
# the state the gwfail scenario exists to move past.
#
# The assignment is by groups of three clients, not one. `harness seed` cycles
# delivery mode with j%3 and puts SSE on the odd j, so consecutive SSE clients
# are ordered, key_ordered, unordered, ordered, ... A gateway cycle of one
# therefore aliased onto the mode cycle exactly: one gateway ended up with
# every key_ordered client and nothing else. That made the first gwfail run
# pass vacuously - it killed the gateway holding seven clients whose filter
# rejected all 3000 events, so nothing was ever at risk of being lost. Three
# consecutive clients are one of each mode, so a group of three gives every
# gateway the same mix whatever the seed's cycles do.
start_sse_clients() {
  local i=0 id gw addrs fallback
  for id in $(cat "$D/sse_ids.txt"); do
    i=$((i+1))
    case $(( (i-1)/3 % 3 )) in
      0) gw=http://localhost:8082 ;;
      1) gw=http://localhost:8182 ;;
      2) gw=http://localhost:8282 ;;
    esac
    addrs="$gw"
    for fallback in "$@"; do
      [ "$fallback" = "$gw" ] && continue
      addrs="$addrs,$fallback"
    done
    # 30 minutes, not the 7 the first draft used: the scenario's waits alone
    # (wait_drained 300 + wait_inbox 480) can run for 13 minutes, and a client
    # that exits mid-wait strands its parked rows - there would be nothing left
    # to drain to. cleanup_clients kills them at the end regardless.
    nohup "$BIN/harness" sse -sub "$id" -owner "$OWNER" -addr "$addrs" \
      -duration 1800s -out "$D/sse-$i.jsonl" > "$D/sse-$i.log" 2>&1 &
    echo $! > "$D/sse-$i.pid"
    # Which gateway this client was assigned: without it the only trace of
    # "who had to move" is the client's own log, grepped after the fact.
    echo "$gw" > "$D/sse-$i.gw"
  done
  sleep 5
  echo "  sse clients connected: $(grep -l consuming "$D"/sse-*.log 2>/dev/null | wc -l)/$(grep -c . "$D/sse_ids.txt")"
}

# start_receivers [fallback-gateway-url ...] starts the webhook receiver and
# the SSE clients, passing any fallback gateways on to them (see
# start_sce_clients). Forwarded rather than repeated by a scenario, so no
# scenario can accidentally start two sets of clients.
start_receivers() {
  start_receiver
  start_sse_clients "$@"
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
  # Both summary lines, not only the last. The second is how many events
  # actually reached publish-api, and the kafka scenario deliberately shows
  # fewer than were sent: its broker is down mid-run and a failed batch writes
  # nothing - which is the whole point of batches being transactional. Printing
  # only the achieved rate hides those behind "achieved=181.5 ev/s", where they
  # read like a slow publisher instead of the injected fault doing its job, and
  # the run's smaller expected= never gets an explanation next to it.
  grep -E "^harness pub: (sent=|target=)" "$D/pub.log" | tail -2
}

# verify runs the oracle and then adds the check the oracle deliberately does
# not make. missing=0 means every expected delivery was either received or
# still *tracked* by a status record - a parked delivery reads as accounted
# even if it never reaches a receiver. For a loss test that is too weak: if
# received is short of expected, some deliveries are merely known about, and
# the run says so and fails.
verify() {
  echo "--- oracle ($SCEN, owner $OWNER) ---"
  local out rc line expected received
  # Declared separately from the assignment on purpose: `local out=$(...)`
  # would make $? report local's status, not the oracle's.
  out=$(JWT_SECRET="$JWT_SECRET" "$BIN/harness" verify -owner "$OWNER" -api "$REGISTRY_API" \
    -status-api "$STATUS_API" \
    -events "$D/pub.jsonl" -received "$D/recv.jsonl" \
    -sse "$(ls "$D"/sse-*.jsonl 2>/dev/null | tr '\n' ',' | sed 's/,$//')" 2>&1)
  rc=$?
  # Kept so a scenario can ask per-subscription questions of the verdict, not
  # just the totals: gwfail needs to know that the clients it moved were
  # carrying deliveries, which the summary line cannot express.
  echo "$out" > "$D/oracle.txt"
  echo "$out" | tail -6
  # The oracle's own verdict first: it fails on missing AND on unexpected, and
  # unexpected alone can leave received == expected, which the check below
  # would happily pass.
  if [ "$rc" != "0" ]; then
    echo "ORACLE FAILED (exit $rc) - see the table above." >&2
    return 3
  fi
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
  # Sample the routable-subscription count before the kill: this is the number
  # the presence lease has to bring down on its own, and it is read from the
  # two gateways that survive (node-a on :8082, node-c on :8282). g2 is node-b
  # on :8182.
  VIS_BEFORE=$(visible_subs 8082)
  echo "  t=40s  kill -9 g2 (gateway)";     kill -9 "$(cat "$LOG/g2.pid")" 2>/dev/null; sleep 10

  # Presence: the killed gateway wrote no disconnect (it could not - it was
  # killed), so its rows are still in PostgreSQL. What has to change is whether
  # anything routes to them. Both halves of that are asserted, because either
  # one alone would pass for the wrong reason: rows still present proves the
  # reaper has not silently made this test trivial, and the routable count
  # falling proves the lease - not the deletion - is what stops deliveries
  # being aimed at a connection nobody owns.
  NODEB_ROWS=$(presence_rows node-b)
  echo "  presence rows still claiming node-b: ${NODEB_ROWS:-?} (kill -9 wrote no disconnect)"
  if [ -z "$NODEB_ROWS" ]; then
    echo "  could not count node-b's presence rows - the query failed, which is not" >&2
    echo "    the same as zero: a run must not be judged on an answer it never got" >&2
    FAIL=1
  elif [ -z "$VIS_BEFORE" ]; then
    echo "  SETUP FAILED: could not read presence from :8082 before the kill" >&2
    FAIL=1
  elif [ "${NODEB_ROWS:-0}" -le 0 ]; then
    echo "  presence rows for node-b = ${NODEB_ROWS}, want > 0: nothing left to prove visibility from" >&2
    FAIL=1
  elif VIS_DROPPED=$(wait_presence_drop "$VIS_BEFORE" 8082 8282); then
    echo "  routable subscriptions fell ${VIS_BEFORE} -> ${VIS_DROPPED} on both surviving gateways (lease expired, rows kept)"
  else
    echo "  PRESENCE LEASE FAILED: ${VIS_BEFORE} routable subscriptions before the kill, still ${VIS_BEFORE} after 30s" >&2
    echo "    the dead node's connections are still being routed to - deliveries to them" >&2
    echo "    are written to sse-live for a group nobody consumes" >&2
    FAIL=1
  fi
  wait_publish
  echo "  publish done; bringing the killed replicas back"
  start_bin dispatcher d1  HTTP_ADDR=:8086
  start_bin router     r1  HTTP_ADDR=:8085
  start_bin sse-gateway g2 HTTP_ADDR=:8182 NODE_ID=node-b SSE_DRAINER=false
  sleep 20
  echo "  sse clients reconnected: $(grep -l reconnect "$D"/sse-*.log 2>/dev/null | wc -l) of 20 logs show a reconnect"
  wait_drained 300
  wait_inbox
  judged
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
  judged
  ;;

kafka)
  seed_owner
  echo "== kafka: broker restart mid-run =="
  start_receivers
  publish_bg 200 60s
  sleep 10
  echo "  t=10s  restart kafka (broker 1 of three; RF=3 means this is a leader"
  echo "         move and an ISR contraction, not an outage)"
  "$DOCKER" compose restart kafka >/dev/null 2>&1
  assert_up "kafka" kafka_up
  sleep 20
  wait_publish
  wait_drained 300
  wait_inbox
  judged
  ;;

ch)
  seed_owner
  echo "== ch: ClickHouse down for 20s mid-run (observability only) =="
  start_receivers
  publish_bg 200 60s
  sleep 10
  LOST0=$(status_lost_total)
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

  # Assert the writer's claim while the outage is still the subject rather
  # than history: status-writer commits an offset only after the insert
  # succeeded, so lag 0 means every status record produced during the outage -
  # including the ones buffered while ClickHouse was down - reached the store.
  # Checked here, before the drain adds thousands more rows that could mask it,
  # and again in judged() where the oracle reads.
  if status_settled 90; then :; else FAIL=1; fi

  wait_publish
  wait_drained 300
  wait_inbox

  # And the other half, from the producer side: records the dispatcher could
  # never hand to Kafka are invisible to lag (they are not in Kafka) and to the
  # oracle (no row exists to be missing), so only the emitter's own counter
  # sees them.
  LOST1=$(status_lost_total)
  if [ "$LOST1" -gt "$LOST0" ]; then
    echo "  STATUS ROWS LOST BY THE DISPATCHER: $((LOST1 - LOST0))" >&2
    echo "    these never reached Kafka, so no later check can see them:" >&2
    echo "    the emitter's buffer filled or shutdown gave up on the batch" >&2
    echo "    (was $LOST0, now $LOST1)" >&2
    FAIL=1
  else
    echo "  status records the dispatchers failed to write: 0 (of $LOST1 total)"
  fi

  judged
  ;;

rebalance)
  seed_owner
  echo "== rebalance: kill -9 a dispatcher mid-run, measure its handover =="
  start_receivers
  publish_bg 200 60s
  sleep 10

  BEFORE=$(assignment)
  MEMBERS=$(printf '%s\n' "$BEFORE" | grep -c .)
  DEAD_PID=$(cat "$LOG/d1.pid")
  T0=$(date +%s)
  echo "  t=10s  kill -9 d1 (pid $DEAD_PID); the group had $MEMBERS members"
  kill -9 "$DEAD_PID" 2>/dev/null || { echo "  could not kill d1" >&2; FAIL=1; }

  SESSION=$(session_seconds)
  # 15s of slack: polling runs every 2s and each kafka-consumer-groups call
  # costs a second or two of its own, so the measurement trails reality.
  BOUND=$(( SESSION + 15 ))
  failover=""; unowned=0; now=0
  while :; do
    now=$(( $(date +%s) - T0 ))
    cur=$(assignment)
    unowned=$(unowned_count "$cur")
    # See handover_complete: "the dead id is gone" on its own would also
    # accept a rebalance still in flight with partitions owned by nobody.
    if handover_complete "$BEFORE" "$cur"; then
      failover=$now
      break
    fi
    [ "$now" -ge "$BOUND" ] && break
    sleep 2
  done

  if [ -z "$failover" ]; then
    echo "  FAILOVER TOO SLOW: $unowned partition(s) still unowned after ${now}s" >&2
    echo "    bound ${BOUND}s = session ${SESSION}s + 15s of polling slack" >&2
    echo "    (nothing is lost - Kafka holds the records - but an unowned" >&2
    echo "    partition delivers nothing until the handover completes)" >&2
    FAIL=1
  else
    echo "  failover: all partitions owned again after ${failover}s (bound ${BOUND}s = session ${SESSION}s + 15s)"
  fi

  echo "  restarting d1"
  start_bin dispatcher d1 HTTP_ADDR=:8086
  wait_publish
  wait_drained 300
  wait_inbox
  judged
  ;;

gwfail)
  seed_owner
  echo "== gwfail: kill -9 a gateway and leave it down, clients must move =="
  # The counterpart of `pods`, which restarts the gateway it kills and so
  # proves only that the same node comes back. Here node-b never returns
  # during the run, which is the case an operator actually meets: a pod that
  # is replaced by one with a *new* identity, while the clients that were on
  # it have to land somewhere else.
  #
  # Every client is given all three gateways as its address list: it keeps
  # its own first, so the spread before the kill is unchanged, and the other
  # two are the way out when it dies - there is no load balancer in the stack
  # to redirect them for us.
  start_receivers http://localhost:8082 http://localhost:8182 http://localhost:8282
  publish_bg 200 60s
  sleep 10

  VIS_BEFORE=$(visible_subs 8082)
  ASSIGNED=$(assigned_to http://localhost:8182)
  echo "  t=10s  kill -9 g2 (node-b), left down; $ASSIGNED clients were on it"
  kill -9 "$(cat "$LOG/g2.pid")" 2>/dev/null || { echo "  could not kill g2" >&2; FAIL=1; }

  # Two halves of the same claim, and either alone would pass for the wrong
  # reason: the rows must still be there (a kill -9 writes no disconnect), and
  # they must have stopped being routed to (the lease is what makes a dead
  # node stop receiving deliveries). The count cannot show the second half
  # here - the clients re-register on a survivor within a second or two, so
  # the number of routable subscriptions never dips - which is exactly why
  # the node list, not the count, is what gets asserted.
  NODEB_ROWS=$(presence_rows node-b)
  echo "  presence rows still claiming node-b: ${NODEB_ROWS:-?} (kill -9 wrote no disconnect)"
  if [ -z "$NODEB_ROWS" ]; then
    echo "  could not count node-b's presence rows - the query failed, which is not" >&2
    echo "    the same as zero: a run must not be judged on an answer it never got" >&2
    FAIL=1
  elif [ "$NODEB_ROWS" -le 0 ]; then
    echo "  presence rows for node-b = ${NODEB_ROWS}, want > 0: nothing left to prove visibility from" >&2
    FAIL=1
  fi

  if wait_node_excluded 8082 node-b 30; then :; else FAIL=1; fi

  # And the other half: the clients that were on the dead gateway must have
  # actually reached a survivor, with its subscriptions routable again.
  # Bounded because a client reconnects about once a second.
  MOVED=""
  for _ in $(seq 1 30); do
    MOVED=$(clients_moved http://localhost:8182)
    [ "${MOVED:-0}" -ge "$ASSIGNED" ] 2>/dev/null && break
    sleep 1
  done
  echo "  clients that reconnected to another gateway: ${MOVED:-0}/${ASSIGNED}"
  if [ "${MOVED:-0}" -lt "$ASSIGNED" ] 2>/dev/null; then
    echo "  CLIENT FAILOVER FAILED: only ${MOVED:-0} of $ASSIGNED reached a surviving gateway" >&2
    FAIL=1
  fi

  if RISE=$(wait_presence_rise "$VIS_BEFORE" 8082 8282); then
    echo "  routable subscriptions back to ${VIS_BEFORE} on the survivors (:8082, :8282)"
  else
    echo "  FAILOVER FAILED: routable subscriptions never returned to $VIS_BEFORE" >&2
    echo "    after 60s: $(visible_subs 8082) on :8082, $(visible_subs 8282) on :8282" >&2
    FAIL=1
  fi

  wait_publish
  echo "  publish done; node-b is still down, judging against that"
  wait_drained 300
  wait_inbox
  judged

  # The assertion the first draft of this scenario was missing, and the reason
  # it passed while proving nothing: moving clients is not the claim. Moving
  # clients that were carrying deliveries is. The oracle's table says how many
  # events each subscription expected, so the deliveries the dead gateway was
  # responsible for can be summed here - and if that sum is zero, this
  # scenario just ran over idle subscriptions again and says so loudly rather
  # than reporting success.
  moved_deliveries() {
    local f sub sum=0 exp
    for f in "$D"/sse-*.gw; do
      [ -e "$f" ] || continue
      [ "$(cat "$f")" = "http://localhost:8182" ] || continue
      sub=$(grep -o 'consuming [0-9a-f-]*' "${f%.gw}.log" 2>/dev/null | head -1 | awk '{print $2}')
      [ -n "$sub" ] || continue
      exp=$(awk -v s="$sub" '$1 == s {print $4}' "$D/oracle.txt" 2>/dev/null | head -1)
      sum=$((sum + ${exp:-0}))
    done
    echo "$sum"
  }
  DELIVERED_MOVED=$(moved_deliveries)
  if [ "${DELIVERED_MOVED:-0}" -gt 0 ]; then
    echo "  deliveries the dead gateway was responsible for: ${DELIVERED_MOVED} (moved, not idle)"
  else
    echo "  VACUOUS RUN: the clients on the dead gateway expected 0 deliveries" >&2
    echo "    failover of client *connections* was exercised, failover of the" >&2
    echo "    deliveries behind them was not - the seed's mode cycle put only" >&2
    echo "    filter-rejecting subscriptions on that gateway" >&2
    FAIL=1
  fi

  # Restore the topology for the next scenario - after the verdict, so the run
  # above really was judged with the gateway gone. The preflight of every
  # scenario refuses to run against a half topology, so leaving it down would
  # only break the next run rather than prove anything more.
  echo "  restarting g2 (the verdict above was taken while it was down)"
  start_bin sse-gateway g2 HTTP_ADDR=:8182 NODE_ID=node-b SSE_DRAINER=false
  sleep 5
  echo "  g2 back: $(probe_code http://localhost:8182/readyz) on /readyz"
  ;;
esac

cleanup_clients
exit "$FAIL"
