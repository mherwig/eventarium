#!/usr/bin/env bash
# scripts/loadtest.sh - one end-to-end load run, judged only after a FULL drain.
#
#   usage: scripts/loadtest.sh <delivery-mode> <rate> <duration> <owner>
#   e.g.  scripts/loadtest.sh unordered 1000 40s load1
#
# delivery-mode is a `harness seed -modes` value (unordered, key_ordered,
# ordered) - NOT the publish mode. Publish one event per request with
# HARNESS_MODE=single, as docs/performance.md reports:
#
#   HARNESS_MODE=single scripts/loadtest.sh unordered 1000 40s load1
#
# Why this exists rather than `harness verify` on its own: verifying at the
# end of the publish window measures only what happened to have arrived. The
# first version of this tore the receiver down right after verify, and the
# platform's own queue-wait p90 was 63 s while the file-based p90 said 791 ms
# - both correct for what they measured, and mine measured the wrong subset,
# reporting a slow tail as if it were fast. Drain is therefore detected by
# watching BOTH receipt streams stop growing, never by a fixed sleep: a fixed
# sleep is exactly the guess that got those numbers wrong.
#
# Env: MAX_IN_FLIGHT (8), SUBS (40), TYPES (4), RATE_LIMITED (default: seed's
# own rule of every 7th webhook subscription; 0 disables rate limits for a
# throughput run), DOCKER, CH_CONTAINER, CH_KAFKA (pubsub-kafka-1),
# LOADTEST_DIR (work dir root).
set -uo pipefail

if [ $# -ne 4 ]; then
  echo "usage: $0 <delivery-mode> <rate> <duration> <owner>" >&2
  exit 2
fi
MODE=$1; RATE=$2; DUR=$3; OWNER=$4

# harness pub -duration is a Go time.ParseDuration flag. A bare `40` makes it
# print usage and exit 0-ish without publishing anything, which then looks
# exactly like a run in which the platform delivered nothing.
case "$DUR" in
  *[0-9]) echo "duration needs a unit, e.g. 40s (got '$DUR')" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${LOADTEST_DIR:-${TMPDIR:-/tmp}/pubsub-loadtest}/$OWNER"
MIF=${MAX_IN_FLIGHT:-8}
SUBS=${SUBS:-40}; TYPES=${TYPES:-4}
DOCKER="${DOCKER:-docker}"
CH_CONTAINER="${CH_CONTAINER:-pubsub-clickhouse-1}"
CH_KAFKA="${CH_KAFKA:-pubsub-kafka-1}"
# Passed explicitly rather than conditionally: ${X+"-a $X"} expands to ONE
# word because quotes inside ${...} suppress splitting, and the flag package
# then reports `flag provided but not defined: -rate-limited 3`. -1 keeps the
# seed's own rule (every 7th webhook subscription), 0 disables the limits.
RATE_LIMITED="${RATE_LIMITED:--1}"
mkdir -p "$OUT"; cd "$ROOT"

[ -x "$ROOT/bin/harness" ] || { echo "no bin/harness - run 'make build' first" >&2; exit 1; }

echo "=== mode=$MODE rate=$RATE/s dur=$DUR mif=$MIF owner=$OWNER -> $OUT ==="

./bin/harness seed -api http://localhost:8083 -owner "$OWNER" -prefix "$OWNER" \
  -types "$TYPES" -subs "$SUBS" -modes "$MODE" -max-in-flight "$MIF" \
  -rate-limited "$RATE_LIMITED" \
  -webhook-url http://localhost:9090/hook > "$OUT/seed.json" 2>"$OUT/seed.err"
grep -q '"id"' "$OUT/seed.json" || { echo "seeding failed:" >&2; cat "$OUT/seed.err" >&2; exit 4; }
echo "  seeded: $(grep -o '"max_in_flight": *[0-9]*' "$OUT/seed.json" | grep -o '[0-9]*' | sort -u | tr '\n' ' ')"

# receiver - anything already on :9090 would take deliveries this run never
# records, and the shortfall would surface as `missing` long after the cause
if p=$(ss -ltnp 2>/dev/null | grep ':9090' | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2); then
  [ -n "${p:-}" ] && kill "$p" 2>/dev/null && sleep 1
fi
nohup ./bin/harness recv -addr :9090 -out "$OUT/recv.jsonl" >"$OUT/recv.log" 2>&1 &
RECV=$!
sleep 1

# sse clients: one per seeded SSE subscription
SSE_IDS=$(python3 -c "
import json
[print(s['id']) for s in json.load(open('$OUT/seed.json')) if s.get('channel')=='sse']")
NSSE=$(printf '%s\n' "$SSE_IDS" | grep -c . || true)
i=0; PIDS=()
for id in $SSE_IDS; do
  i=$((i+1))
  nohup ./bin/harness sse -sub "$id" -owner "$OWNER" -addr http://localhost:8082 \
    -out "$OUT/sse-$i.jsonl" -duration 900s >"$OUT/sse-$i.log" 2>&1 &
  PIDS+=($!)
done
echo "  receivers: webhook=1 sse=$NSSE"
sleep 5

# publish
TL=""
for t in $(seq 0 $((TYPES-1))); do TL="$TL${TL:+,}$OWNER.type.$t"; done
./bin/harness pub -owner "$OWNER" -rate "$RATE" -duration "$DUR" \
  -types "$TL" -payload 1024 -out "$OUT/pub.jsonl" >"$OUT/pub.log" 2>&1
PUB=$?
sed -n '1p' "$OUT/pub.log" | sed 's/^/  /'
grep -E "sent=" "$OUT/pub.log" | tail -1 | sed 's/^/  /'
# A run that published nothing is a failed run, not a result: reporting it as
# one produced four identical plausible-looking "at/below ceiling" verdicts
# off an empty measurement.
grep -q "sent=" "$OUT/pub.log" || {
  echo "FAILED: harness pub never reported sent= - see $OUT/pub.log" >&2
  kill "$RECV" 2>/dev/null
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  exit 3
}
[ "$PUB" -eq 0 ] || echo "  (harness pub exited $PUB - see $OUT/pub.log)"

# ---- DRAIN: wait for both receipt streams to stop growing --------------
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
# 900s: the three rate-limited subscriptions drain at 150/s between them, so
# a 2,500 ev/s run needs ~340s of them alone. Hitting the cap would judge the
# run while deliveries were still arriving and report that backlog as
# `missing` - a false loss, indistinguishable from the real thing.
while [ $waited -lt 900 ]; do
  cur=$(count_all)
  if [ "$cur" = "$prev" ]; then
    stable=$((stable+1))
    [ $stable -ge 6 ] && break          # no new receipts for ~18s
  else
    stable=0
  fi
  prev=$cur
  sleep 3; waited=$((waited+3))
  [ $((waited % 30)) -eq 0 ] && echo "    t=${waited}s receipts=$cur"
done
echo "  drained: receipts=$(count_all) after ${waited}s"

# ---- settle the status store before judging -----------------------------
# The oracle reads ClickHouse through the Status API, so a verdict taken
# while status-writer is still catching up reports rows that are merely in
# flight as lost. Observed directly: a 2,500 ev/s run came back
# unrecorded=85157, and status-writer's lag read 0 again minutes later -
# every one of those rows had landed. Judging early made a passing run look
# like a five-digit loss.
#
# Lag 0 is a sound sufficient claim because status-writer commits an offset
# only AFTER the insert into ClickHouse succeeded (cmd/status-writer): while
# lag reads 0, nothing can be left in Kafka. The converse does not hold - if
# the writer does not catch up, `unrecorded=` below decides, because it reads
# rows rather than offsets.
status_lag() {
  "$DOCKER" exec "$CH_KAFKA" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group status-writer 2>/dev/null |
    awk 'NR>1 && $6 ~ /^-?[0-9]+$/ {l+=$6} END {print l+0}'
}
echo "  settling status-writer..."
t0=$(date +%s)
while :; do
  lag=$(status_lag)
  if [ "${lag:-}" = "0" ]; then
    echo "  status-writer caught up: delivery-status lag=0"
    break
  fi
  if [ $(( $(date +%s) - t0 )) -ge 120 ]; then
    echo "  (still ${lag:-?} behind after 120s - unrecorded= below decides)"
    break
  fi
  sleep 2
done

# ---- oracle ------------------------------------------------------------
SSEFILES=$(ls "$OUT"/sse-*.jsonl 2>/dev/null | paste -sd, -)
./bin/harness verify -owner "$OWNER" -api http://localhost:8083 \
  -status-api http://localhost:8081 -events "$OUT/pub.jsonl" \
  -received "$OUT/recv.jsonl" -sse "$SSEFILES" 2>&1 | tail -3 | sed 's/^/  /'
# pipefail is on, so this is verify's status, not sed's. Without this the
# script ended in `echo` and exited 0 no matter what the oracle said - which
# is how a run with unrecorded=85157 was reported as exit=0.
VERIFY=$?

# ---- latency, rate-limited separated -----------------------------------
# Three subscriptions are rate-limited to 50/s by the seed and take minutes
# by design. Folding them into the headline percentile is what made an
# end-to-end p50 of ~550 ms look like a regression when the platform's own
# attempt latency was p50=3 ms.
python3 "$ROOT/scripts/latency.py" "$OUT/seed.json" "$OUT/pub.jsonl" \
  "$OUT/recv.jsonl" "$OUT"/sse-*.jsonl 2>/dev/null | sed 's/^/  /'

# ---- platform-side decomposition ---------------------------------------
# Same three segments scripts/decompose.py reports per delivery, as platform
# queue-wait vs attempt. Useful because it comes straight from ClickHouse
# without joining the generator's file.
IDS_WH=$(python3 -c "
import json
print(','.join(\"'\"+s['id']+\"'\" for s in json.load(open('$OUT/seed.json')) if s['channel']=='webhook' and not s.get('rate_limit_per_s')))")
IDS_SSE=$(python3 -c "
import json
print(','.join(\"'\"+s['id']+\"'\" for s in json.load(open('$OUT/seed.json')) if s['channel']=='sse'))")

q() { "$DOCKER" exec "$CH_CONTAINER" clickhouse-client -u pubsub --password pubsub -d pubsub -q "$1" 2>&1; }

echo "  -- platform: webhook-unlimited queue wait vs attempt --"
q "WITH f AS (SELECT delivery_id, minIf(ts,status='pending') t_p,
       minIf(ts,status='in_flight') t_i, minIf(ts,status='delivered') t_d
  FROM delivery_status WHERE owner='$OWNER' AND subscription_id IN ($IDS_WH)
  GROUP BY delivery_id)
 SELECT '  n='||count()||'  queue p50/p90='||
   round(quantile(0.5)(toUnixTimestamp64Milli(t_i)-toUnixTimestamp64Milli(t_p)))||'/'||
   round(quantile(0.9)(toUnixTimestamp64Milli(t_i)-toUnixTimestamp64Milli(t_p)))||'ms'||
   '  attempt p50/p90='||
   round(quantile(0.5)(toUnixTimestamp64Milli(t_d)-toUnixTimestamp64Milli(t_i)))||'/'||
   round(quantile(0.9)(toUnixTimestamp64Milli(t_d)-toUnixTimestamp64Milli(t_i)))||'ms'
 FROM f WHERE t_i>t_p AND t_d>t_i"

echo "  -- platform: SSE pending -> sent --"
q "WITH f AS (SELECT delivery_id, minIf(ts,status='pending') t_p,
       minIf(ts,status='sent') t_s FROM delivery_status
  WHERE owner='$OWNER' AND subscription_id IN ($IDS_SSE) GROUP BY delivery_id)
 SELECT '  n='||count()||'  pending->sent p50/p90='||
   round(quantile(0.5)(toUnixTimestamp64Milli(t_s)-toUnixTimestamp64Milli(t_p)))||'/'||
   round(quantile(0.9)(toUnixTimestamp64Milli(t_s)-toUnixTimestamp64Milli(t_p)))||'ms'
 FROM f WHERE t_s>t_p"

kill "$RECV" 2>/dev/null
for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
echo "=== done: $OWNER  (work dir: $OUT, oracle exit: $VERIFY) ==="
exit "$VERIFY"
