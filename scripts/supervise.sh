#!/bin/bash
# supervise.sh <pid-file> <command> [args...]
#
# Runs <command> as a child, writes the CHILD's pid to <pid-file>, and
# restarts it only when it crashed. Stdout and stderr are whatever the caller
# redirected them to - stack.sh points both at the service's log, so a
# supervisor notice lands in the same file as the service's own output.
#
# Why a supervisor exists: cmd/router exits by design on a non-retryable
# commit failure, and main.go says why - "exit so Kubernetes restarts the pod
# with a fresh session." That assumption holds in Kubernetes and did not hold
# in compose, so a broker restart during the `kafka` scenario left r2 dead:
# the scenario itself passed (two of three routers absorbed the loss), and
# the next three scenarios then refused to run against a topology they could
# no longer see. This makes compose behave like the environment the code was
# written for.
#
# Why the pid file holds the CHILD and not this script - do not "simplify"
# this: chaos.sh kills by pid file, and kill -9 is the fault in `pods`,
# `rebalance` and `gwfail`. If the file held this script, that kill would
# take out the supervisor and leave the service running, so the fault would
# silently never be applied and those scenarios would pass for the wrong
# reason. Holding the child means the kill lands on the service; this script
# then sees a signal death and does not restart it, which is the other half
# of the same requirement.
#
# The policy is the child's exit status. Three classes, and the middle one is
# the reason this is not simply "restart unless killed":
#
#   0          a clean exit - which is what SIGTERM produces here, because
#              every service installs signal.NotifyContext and returns from
#              main once its context is cancelled. Treating 0 as a crash
#              would make the stack impossible to stop: killing a service
#              would bring it straight back.
#   1..127     it exited by itself while failing - a crash, or the router's
#              deliberate commit-failure exit (os.Exit(1)). Restart, as
#              Kubernetes would.
#   >= 128     it was killed by a signal - SIGKILL from chaos, SIGTERM from
#              something that did not catch it, OOM. Do not restart: the
#              fault stays applied.
#
# The pid file is also a mandate, not just a label: before respawning, this
# script requires it to still exist and still name the child it just reaped.
# stack.sh's `stop` removes the file before signalling, so a supervisor that
# was asleep between restarts retires instead of bringing the service back.
#
# A SIGTERM sent to this script rather than to the child is forwarded, so
# nothing is left running behind a supervisor that has gone.
set -u

pidfile=$1; shift
if [ $# -eq 0 ]; then
  echo "usage: supervise.sh <pid-file> <command> [args...]" >&2
  exit 2
fi

child=""
stopping=0
on_term() {
  stopping=1
  [ -n "$child" ] && kill -TERM "$child" 2>/dev/null
  return 0
}
trap on_term TERM INT

# Ten exits in a row where the child lived under five seconds is not a flaky
# service, it is a broken one - a bad binary or a port already taken. Without
# this a restart loop would spin at full tilt forever and bury the actual
# error under a rolling log. Five seconds is longer than any start-up failure
# takes to show and shorter than a healthy service's first interval.
FAST_EXIT_S=5
FAST_EXIT_MAX=10
fast=0

while true; do
  started=$SECONDS
  "$@" &
  child=$!
  printf '%s\n' "$child" > "$pidfile"

  # wait in a loop: a trapped signal interrupts `wait`, which then returns
  # 128+N while the child is still alive - mistaking that for its death would
  # restart a service that never stopped.
  while :; do
    wait "$child"
    rc=$?
    kill -0 "$child" 2>/dev/null || break
  done

  if [ "$stopping" = 1 ]; then
    exit 0
  fi
  if [ "$rc" -ge 128 ]; then
    echo "supervise: killed by a signal (rc=$rc) - not restarting, the fault stays applied" >&2
    exit "$rc"
  fi
  if [ "$rc" -eq 0 ]; then
    echo "supervise: exited cleanly (rc=0) - not restarting" >&2
    exit 0
  fi

  if [ $((SECONDS - started)) -lt "$FAST_EXIT_S" ]; then
    fast=$((fast + 1))
  else
    fast=0
  fi
  if [ "$fast" -ge "$FAST_EXIT_MAX" ]; then
    echo "supervise: gave up - $fast consecutive exits within ${FAST_EXIT_S}s each, last rc=$rc" >&2
    exit 1
  fi

  echo "supervise: crashed rc=$rc, restarting in 1s (fast exits: $fast)" >&2
  sleep 1

  # A TERM that arrived during the sleep killed a child which was already
  # dead, so `stopping` was never observed by the loop below - without this
  # check the supervisor would spawn one more and wait on it forever.
  if [ "$stopping" = 1 ]; then
    exit 0
  fi

  # Retired while asleep: `stack.sh stop` removes the pid file before it
  # signals anything, so a file that has gone - or been taken over by a newer
  # supervisor - means this one must not spawn again.
  if [ ! -e "$pidfile" ] || [ "$(cat "$pidfile" 2>/dev/null)" != "$child" ]; then
    echo "supervise: pid file retired, not restarting" >&2
    exit 0
  fi
done
