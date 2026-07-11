#!/bin/sh
set -eu

: "${PGHOST:?PGHOST is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"

for file in /migrations/*.sql; do
    echo "Applying ${file}"
    psql -v ON_ERROR_STOP=1 -f "${file}"
done
