#!/bin/sh
# Replica entrypoint: base-backup from the primary on first start, then hand
# over to the stock entrypoint.
#
# Must run as the postgres user (compose sets `user: postgres`). The data
# directory's ownership comes from the image on first use of the volume, and
# a pg_basebackup run as root would leave root-owned files that postgres
# cannot write to. Running as postgres also means the stock entrypoint skips
# its own privilege drop rather than fighting this one.
#
# -R is what writes standby.signal and primary_conninfo. The password has to
# travel in the connection string given to -d: -R copies that string into
# primary_conninfo verbatim and never looks at PGPASSWORD, so a backup taken
# with only PGPASSWORD set starts a replica that cannot authenticate - and
# fails after the fact, once the volume already exists.
set -eu

: "${PGDATA:=/var/lib/postgresql/data}"
: "${PGHOST:=postgres}"
: "${PGPORT:=5432}"
: "${PGUSER:=pubsub}"
: "${PGPASSWORD:=pubsub}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "pg-replica: waiting for primary $PGHOST:$PGPORT" >&2
  tries=0
  until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -t 2 >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "pg-replica: primary never became ready" >&2
      exit 1
    fi
    sleep 2
  done

  echo "pg-replica: taking base backup (this copies every table)" >&2
  pg_basebackup \
    -d "host=$PGHOST port=$PGPORT user=$PGUSER password=$PGPASSWORD dbname=postgres" \
    -D "$PGDATA" -R -X stream --checkpoint=fast
  echo "pg-replica: base backup complete" >&2
fi

exec docker-entrypoint.sh "$@"
