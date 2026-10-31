#!/bin/bash
# chaos_test.sh - tests for the pure helpers in scripts/chaos.sh.
#
# No Docker and no running stack: these are the parts of the driver that are
# decisions rather than I/O, and they are the parts that went wrong first. Two
# of them were caught by a one-off test while the driver was being written -
# session_seconds returning 0 for KAFKA_SESSION_TIMEOUT=0, which would have
# made the failover bound a nonsense 15s, and a handover check that would have
# accepted a rebalance still in flight. Worth keeping.
#
# Run with `make chaos-test`. Exits non-zero on the first failure.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$HERE/chaos_test.sh"

fail=0
pass() { echo "  ok    $1"; }
fail_case() { echo "  FAIL  $1" >&2; fail=1; }

# Pull the helpers out of chaos.sh verbatim - testing the real lines rather
# than a copy that can drift away from them.
extract() { # function name
  sed -n "/^$1()/,/^}/p" "$HERE/chaos.sh"
}
# shellcheck disable=SC1090
source <(extract session_seconds; extract unowned_count; extract partition_count; extract handover_complete)

echo "session_seconds"
t_session() { # env-value want
  local v=$1 want=$2 got
  if [ -n "$v" ]; then export KAFKA_SESSION_TIMEOUT="$v"; else unset KAFKA_SESSION_TIMEOUT; fi
  got=$(session_seconds)
  if [ "$got" = "$want" ]; then pass "${v:-unset} -> $got"
  else fail_case "${v:-unset} -> $got, want $want"; fi
}
t_session ""   45
t_session 45s  45
t_session 45   45
t_session 15s  15   # the value a tuned deployment would set
t_session 10s  10
t_session 1m   60
t_session 0    45   # not a usable session timeout: default, not bound=15
t_session bogus 45  # an unrecognised shape must not break the arithmetic
t_session 1m30s 45  # mixed units: fall back rather than guess
unset KAFKA_SESSION_TIMEOUT

echo "unowned_count"
t_unowned() { # assignment want
  local got
  got=$(unowned_count "$1")
  if [ "$got" = "$2" ]; then pass "$3 -> $got"
  else fail_case "$3 -> $got, want $2"; fi
}
t_unowned 'kgo-a 1 2 3
kgo-b 4 5 6' 0 "every partition owned"
t_unowned 'kgo-a 1 2 3
- 4 5 6' 1 "one partition in transit"
t_unowned '- 1
- 2
kgo-a 3' 2 "two partitions in transit"

echo "handover_complete"
BEFORE='kgo-a 1 2 3
kgo-b 4 5 6
kgo-c 7 8 9'
t_handover() { # after want(0=complete,1=incomplete) label
  local after=$1 want=$2 label=$3 rc=1
  if handover_complete "$BEFORE" "$after"; then rc=0; fi
  if [ "$rc" = "$want" ]; then pass "$label"
  else fail_case "$label: got rc=$rc, want rc=$want"; fi
}
t_handover "$BEFORE" 1 "unchanged assignment (the group has not noticed yet)"
t_handover 'kgo-a 1 2 3
kgo-b 4 5 6' 1 "the dead member vanished from the answer, its partitions with it"
t_handover 'kgo-a 1 2 3
kgo-b 4 5 6 10 11' 1 "a new member joined but nothing was rehomed yet"
t_handover 'kgo-a 1 2 3
kgo-b 4 5 6
- 7 8 9' 1 "changed but a partition belongs to nobody (mid-rebalance)"
t_handover 'kgo-a 1 2 3
kgo-b 4 5 6 7 8 9' 0 "took over every partition - the real handover"
t_handover '' 1 "no assignment reported at all"

[ "$fail" = "0" ] || { echo "chaos helper tests FAILED" >&2; exit 1; }
echo "all chaos helper tests passed"
