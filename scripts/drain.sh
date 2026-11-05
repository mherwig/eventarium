#!/bin/bash
# drain.sh - TEMPORARY two-phase test: publish everything first, then start the
# consumers and measure the drain with no generator competing for the CPU.
#
#   scripts/drain.sh <rate> <duration> <owner>
#   e.g. scripts/drain.sh 1000 60s drain1
#
# WHY THIS EXISTS (and why it is not loadtest.sh):
# loadtest.sh runs publish and drain concurrently, which is the steady state the
# docs report - but it means the generator, the 20 SSE clients and the receiver
# all compete with the platform for the same 8 cores (load average 2.5 idle ->
# 22.6 mid-run). This splits the two so the drain window has no publisher in it,
# which is the only way to see how much of the router's Kafka cost is contention
# rather than replication and transactions.
#
# WHAT IT CAN MEASURE:
#   - drain throughput, uncontaminated
#   - router stage timings: produce_sync (record acks) vs produce_end (the
#     transaction), read from the router's own log line rather than from
#     histogram_quantile, because the histograms' bucket boundaries are wrong
#     (OTel's ms defaults on a _seconds instrument - see the fix in
#     otelmetric.DurationBuckets). sum/count means are still correct.
#   - decompose.py segments 2 and 3 (pending -> in_flight -> delivered), which
#     are stamped entirely inside phase 2
#
# WHAT IT CANNOT MEASURE: segment 1 (publish -> pending) and
# pubsub_e2e_duration_seconds. Both start at publish-api's stamp in phase 1, so
# they absorb the gap between the phases and report the gap, not the platform.
# Do not compare them to docs/performance.md.
#
# CONSTRAINT: the harness seeds 'short' (expires = ingested + 1h) and 'day'
# classes, and the dispatcher's attemptLoop drops an expired record before the
# first send attempt. Phase 2 therefore has to start within an hour of phase 1.
#
# Env: same as loadtest.sh - MAX_IN_FLIGHT, SUBS, TYPES, RATE_LIMITED, DOCKER,
# CH_CONTAINER, CH_KAFKA, DRAIN_TEST_DIR.
set -u
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${CHAOS_BIN:-$ROOT/bin}"
LOG="${CHAOS_LOG:-${TMPDIR:-/tmp}/pubsub-chaos}/logs"

if [ $# -ne 3 ]; then
  echo "usage: $0 <rate> <duration> <owner>" >&2
  exit 2
fi
RATE=$1; DUR=$2; OWNER=$3

# Same exports as stack.sh, for the same reason: the binaries disagree on their
# defaults and the stores are three-node.
export PG_DSN="${PG_DSN:-postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable}"
export CH_DSN="${CH_DSN:-clickhouse://pubsub:pubsub@localhost:9000/pubsub}"
export JWT_SECRET="${JWT_SECRET:-dev-secret}"
export CH_CLUSTER="${CH_CLUSTER:-pubsub}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092,localhost:9094,localhost:9095}"

OUT="${DRAIN_TEST_DIR:-${TMPDIR:-/tmp}/pubsub-drain}/$OWNER"
MIF=${MAX_IN_FLIGHT:-8}
SUBS=${SUBS:-40}; TYPES=${TYPES:-4}
DOCKER="${DOCKER:-docker}"
CH_CONTAINER="${CH_CONTAINER:-pubsub-clickhouse-1}"
CH_KAFKA="${CH_KAFKA:-pubsub-kafka-1}"
RATE_LIMITED="${RATE_LIMITED:--1}"
mkdir -p "$OUT"; cd "$ROOT"

[ -x "$BIN/harness" ] || { echo "no $BIN/harness - run 'make build' first" >&2; exit 1; }

# start <binary> <log-name> [ENV=...] - the same supervise.sh pattern as
# stack.sh, so a pid file exists for every later step to work from.
start() {
  local bin=$1 log=$2 i; shift 2
  rm -f "$LOG/$log.pid"
  nohup "$ROOT/scripts/supervise.sh" "$LOG/$log.pid" env "$@" "$BIN/$bin" \
    > "$LOG/$log.log" 2>&1 &
  for i in $(seq 1 50); do
    [ -s "$LOG/$log.pid" ] && break
    sleep 0.1
  done
  [ -s "$LOG/$log.pid" ] || { echo "  $log: supervisor never wrote a pid - see $LOG/$log.log" >&2; return 1; }
  echo "  $log pid $(cat "$LOG/$log.pid")"
}

ready() { [ "$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$1/readyz" 2>/dev/null)" = "200" ]; }

wait_ready() { # port, name, timeout-s
  local t0=$(date +%s)
  while :; do
    ready "$1" && return 0
    [ $(( $(date +%s) - t0 )) -ge "${3:-30}" ] && {
      echo "  $2 :$1 not ready after ${3:-30}s" >&2; return 1; }
    sleep 0.5
  done
}

echo "=== drain test: rate=$RATE/s dur=$DUR owner=$OWNER -> $OUT ==="

# ---- PHASE 0: bring the host services down, keep the stores ---------------
# stack.sh stop signals only the services it started; the Kafka / PostgreSQL /
# ClickHouse / Prometheus containers are docker-compose's and stay up.
echo "== phase 0: stopping host services =="
"$ROOT/scripts/stack.sh" stop | sed 's/^/  /'

# ---- PHASE 1: seed and publish with only publish-api serving --------------
# registry-api is needed to seed, publish-api to publish. The router, the
# dispatcher, the gateways and the status writer are all down, so nothing
# consumes: the events accumulate in Kafka instead of being processed while the
# generator is running.
echo "== phase 1: registry-api + publish-api only =="
start registry-api registry-api HTTP_ADDR=:8083
start publish-api  publish-api  HTTP_ADDR=:8080
wait_ready 8083 registry-api 30 || exit 1
wait_ready 8080 publish-api  30 || exit 1

echo "  seeding..."
./bin/harness seed -api http://localhost:8083 -owner "$OWNER" -prefix "$OWNER" \
  -types "$TYPES" -subs "$SUBS" -modes unordered -max-in-flight "$MIF" \
  -rate-limited "$RATE_LIMITED" \
  -webhook-url http://localhost:9090/hook > "$OUT/seed.json" 2>"$OUT/seed.err"
grep -q '"id"' "$OUT/seed.json" || {
  echo "seeding failed:" >&2; cat "$OUT/seed.err" >&2; exit 4; }
echo "  seeded $(grep -o '"id"' "$OUT/seed.json" | wc -l) ids"

echo "  publishing (no consumer is running)..."
T1=$(date +%s.%N)
TL=""
for t in $(seq 0 $((TYPES-1))); do TL="$TL${TL:+,}$OWNER.type.$t"; done
./bin/harness pub -owner "$OWNER" -rate "$RATE" -duration "$DUR" \
  -types "$TL" -payload 1024 -out "$OUT/pub.jsonl" >"$OUT/pub.log" 2>&1
PUB=$?
T2=$(date +%s.%N)
grep -E "sent=" "$OUT/pub.log" | tail -1 | sed 's/^/  /'
grep -q "sent=" "$OUT/pub.log" || {
  echo "FAILED: harness pub never reported sent= - see $OUT/pub.log" >&2; exit 3; }
PUBSECS=$(echo "$T2 $T1" | awk '{printf "%.1f", $1-$2}')
NSENT=$(grep -o 'sent=[0-9]*' "$OUT/pub.log" | tail -1 | grep -o '[0-9]*')
echo "  published $NSENT events in ${PUBSECS}s"

# How long the backlog sat before anything consumed it. This is the gap that
# segment 1 and the e2e metric would absorb, which is why they are excluded.
GAPSTART=$(date +%s)

# ---- PHASE 2: start everything, then measure the drain --------------------
# Order matters. The gateways come up before the receivers so the SSE clients
# have something to connect to, and the receivers come up before the dispatcher
# so no delivery is attempted against a port nobody is listening on - a webhook
# refused at this point is a retry, but an SSE subscription with no connection
# parks as no_connection and has to be drained afterwards.
echo "== phase 2: consumers =="
start sse-gateway   sse-gateway   HTTP_ADDR=:8082 SSE_DRAINER=true
start status-api    status-api    HTTP_ADDR=:8081
start status-writer status-writer HTTP_ADDR=:8087
wait_ready 8082 sse-gateway 30 || exit 1
wait_ready 8081 status-api  30 || exit 1
wait_ready 8087 status-writer 30 || exit 1

# webhook receiver
if p=$(ss -ltnp 2>/dev/null | grep ':9090' | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2); then
  [ -n "${p:-}" ] && kill "$p" 2>/dev/null && sleep 1
fi
nohup "$BIN/harness" recv -addr :9090 -out "$OUT/recv.jsonl" >"$OUT/recv.log" 2>&1 &
RECV=$!
sleep 1

# sse clients, one per seeded SSE subscription
SSE_IDS=$(python3 -c "
import json
[print(s['id']) for s in json.load(open('$OUT/seed.json')) if s.get('channel')=='sse']")
NSSE=$(printf '%s\n' "$SSE_IDS" | grep -c . || true)
i=0; PIDS=()
for id in $SSE_IDS; do
  i=$((i+1))
  nohup "$BIN/harness" sse -sub "$id" -owner "$OWNER" -addr http://localhost:8082 \
    -out "$OUT/sse-$i.jsonl" -duration 900s >"$OUT/sse-$i.log" 2>&1 &
  PIDS+=($!)
done
echo "  receivers: webhook=1 sse=$NSSE"
sleep 5

# The consumers themselves, last: by now every receiver is connected, so the
# dispatcher meets a listening port rather than a queue of refusals.
echo "== phase 2: router + dispatcher =="
start router     router     HTTP_ADDR=:8085
start dispatcher dispatcher HTTP_ADDR=:8086
wait_ready 8085 router     30 || exit 1
wait_ready 8086 dispatcher 30 || exit 1
T3=$(date +%s)
echo "  consumers up $(( T3 - GAPSTART ))s after publish finished"

# ---- DRAIN: both receipt streams stop growing -----------------------------
count_all() {
  local n=0 c
  c=$(wc -l < "$OUT/recv.jsonl" 2>/dev/null || echo 0); n=$((n+c))
  for f in "$OUT"/sse-*.jsonl; do
    [ -f "$f" ] || continue
    c=$(wc -l < "$f" 2>/dev/null || echo 0); n=$((n+c))
  done
  echo "$n"
}
echo "  waiting for full drain..."
prev=-1; stable=0; waited=0
# Every sample is kept, not just the printed ones: the drain is bimodal (the
# bulk lands in seconds, the three rate-limited subscriptions then take
# minutes by design), so a single events/second number over the whole window
# reports the rate limit as if it were the platform. The timeline below is
# what makes that visible.
: > "$OUT/receipts.timeline"
while [ $waited -lt 900 ]; do
  cur=$(count_all)
  echo "$waited $cur" >> "$OUT/receipts.timeline"
  if [ "$cur" = "$prev" ]; then
    stable=$((stable+1))
    [ $stable -ge 6 ] && break
  else
    stable=0
  fi
  prev=$cur
  sleep 3; waited=$((waited+3))
  [ $((waited % 30)) -eq 0 ] && echo "    t=${waited}s receipts=$cur"
done
T4=$(date +%s)
DRAINSECS=$(( T4 - T3 ))
FINAL=$(count_all)
echo "  drained: receipts=$FINAL in ${DRAINSECS}s"

# ---- settle status-writer before judging ----------------------------------
status_lag() {
  "$DOCKER" exec "$CH_KAFKA" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group status-writer 2>/dev/null |
    awk 'NR>1 && $6 ~ /^-?[0-9]+$/ {l+=$6} END {print l+0}'
}
echo "  settling status-writer..."
t0=$(date +%s)
while :; do
  lag=$(status_lag)
  [ "${lag:-}" = "0" ] && { echo "  status-writer caught up"; break; }
  [ $(( $(date +%s) - t0 )) -ge 120 ] && { echo "  (still ${lag:-?} behind)"; break; }
  sleep 2
done

# ---- oracle ---------------------------------------------------------------
SSEFILES=$(ls "$OUT"/sse-*.jsonl 2>/dev/null | paste -sd, -)
"$BIN/harness" verify -owner "$OWNER" -api http://localhost:8083 \
  -status-api http://localhost:8081 -events "$OUT/pub.jsonl" \
  -received "$OUT/recv.jsonl" -sse "$SSEFILES" 2>&1 | tail -3 | sed 's/^/  /'

# ---- where the time went --------------------------------------------------
# The router's own log line, not histogram_quantile: the histograms record
# seconds into millisecond-designed buckets, so every quantile is the bucket
# midpoint rather than the truth. The log line is arithmetic over real
# durations and needs no fix to be believed.
echo "== router stage timings, phase 2 (ms, per 50-batch average) =="
STAGELOG="$LOG/router.log"
if [ -f "$STAGELOG" ]; then
  # Only the lines written after the phase-2 start: the file survives a restart
  # and would otherwise average in a previous run.
  awk -v cut="$(date -d "@$T3" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null)" '
    /stage timings/ && (cut == "" || index($1, substr(cut,1,19)) > 0) {print}
  ' "$STAGELOG" | tail -20 > "$OUT/stages.log"
  N=$(wc -l < "$OUT/stages.log")
  if [ "$N" -gt 0 ]; then
    echo "  ($N samples, last 5)"
    tail -5 "$OUT/stages.log" | sed 's/.*kafka_wait/kafka_wait/;s/ level=INFO.*msg="router: stage timings"//' | sed 's/^/    /'
    echo "  --- mean over all $N samples ---"
    for k in kafka_wait process produce_sync produce_end poll cycle; do
      v=$(grep -o "${k}_ms=[0-9-]*" "$OUT/stages.log" | grep -o '[0-9-]*$' | \
          awk '{s+=$1;n++} END {if(n) printf "%.0f", s/n; else print "na"}')
      echo "    $(printf '%-14s' $k) $v ms"
    done
  else
    echo "  no stage lines after phase-2 start - see $STAGELOG"
  fi
else
  echo "  no router log at $STAGELOG"
fi

# sum/count means from Prometheus are correct even with bad buckets, so they
# cross-check the log rather than repeating it.
echo "== Prometheus means (sum/count - correct regardless of buckets) =="
python3 - "$T3" <<'PY'
import json, sys, urllib.parse, urllib.request
t0 = int(sys.argv[1])
def q(expr, ts=None):
    data = {'query': expr}
    if ts: data['time'] = str(ts)
    url = 'http://localhost:9091/api/v1/query?' + urllib.parse.urlencode(data)
    return json.load(urllib.request.urlopen(url, timeout=20))['data']['result']
now = None
for name, label in [('pubsub_router_stage_duration_seconds', 'router stage'),
                    ('pubsub_e2e_duration_seconds', 'e2e (phase1 stamp - includes the gap!)')]:
    r = q('%s_sum / %s_count' % (name, name))
    if r:
        print("  %-46s mean = %.1f ms" % (label, float(r[0]['value'][1]) * 1000))
    else:
        print("  %-46s no data" % label)
PY

# ---- segment decomposition ------------------------------------------------
# Segments 2 and 3 are stamped entirely inside phase 2 and are therefore
# meaningful here. Segment 1 (publish -> pending) absorbs the gap and is
# reported only so the number is not mistaken for a platform figure.
echo "== decompose (segment 1 INCLUDES the gap - not a platform number) =="
python3 scripts/decompose.py "$OWNER" "$OUT/seed.json" "$OUT/pub.jsonl" 2>&1 | tail -12 | sed 's/^/  /'

echo
echo "=== done: $OWNER ==="
echo "  published $NSENT in ${PUBSECS}s"
# The headline is time to 90% of receipts, not the whole window. The last 10%
# belongs to the three rate-limited subscriptions draining at their configured
# rate - that is a policy, not a platform limit, and dividing by the full
# drain time reports it as one. drain1 at 1,000 ev/s read "193 ev/s" this way
# while 83% of receipts had already landed in the first 30s.
python3 - "$OUT/receipts.timeline" "$FINAL" "$DRAINSECS" "$NSENT" <<'PY'
import sys
path, final, drain_secs, nsent = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
try:
    rows = [tuple(map(int, l.split())) for l in open(path) if l.strip()]
except FileNotFoundError:
    rows = []
if not rows or final == 0:
    print("  no receipt timeline")
else:
    for pct in (50, 90, 99):
        want = final * pct // 100
        hit = next((t for t, n in rows if n >= want), None)
        if hit is None:
            print("  t to %2d%%: never" % pct)
        else:
            got = max(n for t, n in rows if t <= hit)
            rate = got / hit if hit > 0 else 0
            print("  t to %2d%%: %4ds   (mean %5.0f deliveries/s over that window)" % (pct, hit, rate))
    print("  full drain (incl. rate-limited tail): %ds" % drain_secs)
    print("  note: the tail is the 3 rate-limited subscriptions at their configured")
    print("        rate - a policy, not the platform ceiling. Compare bulk phases only.")
PY
echo "  work dir: $OUT"
