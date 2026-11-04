#!/usr/bin/env bash
# scripts/loadtest.sh - throughput ceiling sweep.
#
#   usage: ceiling.sh <duration> <rate...>
#
# The oracle cannot find a ceiling: it judges correctness, and correctness
# held at every rate from 1,000 to 2,500 ev/s. The signal that decides it is
# the router's consumer lag sampled TWICE INSIDE the publish window - flat
# means the stack is absorbing the rate, growing means it is not. Drain time
# and the p90 tell the same story afterwards, but only the in-window sample
# says whether the backlog was still forming while you were publishing.
#
# Lag is written to the results file the moment it is sampled, not at the
# end: a guard that fired later once discarded four good runs' samples along
# with the failure it was reporting.
#
# Drives scripts/loadtest.sh (the committed full-drain runner).
set -uo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <duration> <rate...>" >&2
  exit 2
fi
DUR=$1; shift
case "$DUR" in
  *[0-9]) echo "duration needs a unit, e.g. 40s (got '$DUR')" >&2; exit 2 ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
OUTDIR="${CEILING_DIR:-${TMPDIR:-/tmp}/pubsub-ceiling}"
mkdir -p "$OUTDIR"
RESULTS="$OUTDIR/results.txt"
: > "$RESULTS"

lag() { # group -> total committed lag, or "-"
  docker exec "${CH_KAFKA:-pubsub-kafka-1}" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:29092 --describe --group "$1" 2>/dev/null |
    awk 'NR>1 && $6 ~ /^-?[0-9]+$/ {l+=$6} END {if (l=="") print "-"; else print l+0}'
}

for RATE in "$@"; do
  OWNER="c$(date +%s)$RATE"
  LOG="$OUTDIR/ceil-$RATE.log"
  echo "=== rate=$RATE ev/s, single, $DUR, owner=$OWNER ==="

  HARNESS_MODE=single ./scripts/loadtest.sh unordered "$RATE" "$DUR" "$OWNER" > "$LOG" 2>&1 &
  RUN=$!

  # Wait for the run to actually start, then sample inside its publish
  # window. Sampling relative to the run's first line rather than wall clock
  # means a slow seed cannot push both samples out of the window - and the
  # marker must NOT be `sent=`, which loadtest.sh prints only after the
  # publish finishes, by which time there is nothing left to sample.
  STARTED=0
  for _ in $(seq 1 180); do
    grep -q "=== mode=" "$LOG" 2>/dev/null && { STARTED=1; break; }
    kill -0 "$RUN" 2>/dev/null || break
    sleep 1
  done
  A=-; B=-; LA=-; LB=-
  if [ "$STARTED" = 1 ] && kill -0 "$RUN" 2>/dev/null; then
    sleep 15
    A=$(lag router); LA=$(cut -d' ' -f1 /proc/loadavg)
    echo "  rate=$RATE lag t+15s=$A load=$LA" | tee -a "$RESULTS"
    sleep 23
    B=$(lag router); LB=$(cut -d' ' -f1 /proc/loadavg)
    echo "  rate=$RATE lag t+38s=$B load=$LB" | tee -a "$RESULTS"
  else
    echo "  rate=$RATE: run did not stay up long enough to sample lag" | tee -a "$RESULTS"
  fi

  wait $RUN; RC=$?

  PUB=$(grep "sent=" "$LOG" | tail -1 | sed 's/^ *//')
  TGT=$(grep "target=" "$LOG" | tail -1 | sed 's/^ *//')
  DRAIN=$(grep "drained: receipts=" "$LOG" | tail -1 | sed 's/^ *//')
  ORACLE=$(grep "harness verify: subscriptions=" "$LOG" | tail -1 | sed 's/^ *//')
  WH=$(grep "webhook (unlimited)" "$LOG" | tail -1 | sed 's/^ *//')
  SSE=$(grep -E "sse +n=" "$LOG" | tail -1 | sed 's/^ *//')

  if [ -z "$PUB" ]; then
    echo "  rate=$RATE FAILED: no sent= line (exit $RC) - see $LOG" | tee -a "$RESULTS"
    continue
  fi

  VERDICT="n/a"
  if [ "$A" != "-" ] && [ "$B" != "-" ]; then
    # >30% growth between the two samples, and well above the ~23-record
    # baseline the group sits at when idle.
    if [ "$B" -gt $((A * 13 / 10)) ] && [ "$B" -gt 100 ]; then
      VERDICT="GROWING (above ceiling)"
    else
      VERDICT="flat (at/below ceiling)"
    fi
  fi
  {
    echo "  rate=$RATE exit=$RC verdict=$VERDICT"
    echo "    $PUB"
    echo "    $TGT"
    echo "    $DRAIN"
    echo "    $WH"
    echo "    $SSE"
    echo "    $ORACLE"
  } | tee -a "$RESULTS" | sed 's/^/  /'
  echo
done

echo "=== summary ($RESULTS) ==="
grep -E "lag|verdict" "$RESULTS" | sed 's/^/  /'
echo "SWEEP DONE"
