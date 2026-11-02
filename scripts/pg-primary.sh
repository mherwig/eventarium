#!/bin/sh
# Primary entrypoint: allow streaming replicas from the Compose network, then
# hand over to the stock entrypoint.
#
# The stock image only permits replication from 127.0.0.1, and `host all all
# all` does NOT cover it - PostgreSQL matches a replication request against a
# `replication` database entry, so a base backup from another container fails
# with:
#
#   no pg_hba.conf entry for replication connection from host "172.18.0.7"
#
# Measured against the running stack, not assumed.
#
# The scripts in /docker-entrypoint-initdb.d run only against an empty data
# directory, so an existing primary never sees them. This has to run on every
# start instead: it appends before postgres reads the file at startup, so no
# reload is needed, and it is idempotent. Scoped to an existing file so an
# uninitialised data directory is left for the stock entrypoint to create.
set -e

HBA="${PGDATA:-/var/lib/postgresql/data}/pg_hba.conf"
if [ -f "$HBA" ] && ! grep -qE '^host[[:space:]]+replication[[:space:]]+all[[:space:]]+all[[:space:]]' "$HBA"; then
  printf '\n# Added by scripts/pg-primary.sh: streaming replicas on the Compose\n# network. Dev topology - the same password gate as the existing host line.\nhost replication all all scram-sha-256\n' >> "$HBA"
  echo "pg-primary: allowed replication from the Compose network" >&2
fi

exec docker-entrypoint.sh "$@"
