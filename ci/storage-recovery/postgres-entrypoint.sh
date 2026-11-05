#!/usr/bin/env bash
set -euo pipefail
[[ "$#" == 0 && "$(id -u)" == 0 ]]
[[ "${PGDATA:-}" == /opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker ]]
[[ "${POSTGRES_INITDB_ARGS:-}" == "--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8" ]]
# Host and container use the same absolute PGDATA; the root backup profile can
# verify the actual directory without a path alias or namespace exception.
pg_base=/opt/product-development/harbor-local-gateway/postgres-infra/db
for target in "$pg_base" "$pg_base/18" "$PGDATA"; do
 [[ ! -L "$target" ]]
 mkdir -p "$target"
 chown postgres:postgres "$target"
 chmod 0700 "$target"
done
# PG and its publisher run as the same actual postgres user in this namespace.
docker-entrypoint.sh postgres -c hba_file=/run/harbor-infra-control/pg_hba.conf -c ssl=on -c ssl_cert_file=/run/harbor-pg-tls/server.pem -c ssl_key_file=/run/harbor-pg-tls/server.key -c ssl_ca_file=/run/harbor-pg-tls/ca.pem -c password_encryption=scram-sha-256 -c wal_level=replica -c archive_mode=on -c archive_timeout=60 -c "archive_command=/usr/local/bin/node /opt/harbor-storage-report/storage-backup.mjs archive-wal %f" &
postgres_pid=$!
gosu postgres node /opt/harbor-storage-report/ci/storage-recovery/postgres-publisher.mjs &
publisher_pid=$!
cleanup(){ trap - TERM INT EXIT; kill -TERM "$publisher_pid" 2>/dev/null || true; kill -INT "$postgres_pid" 2>/dev/null || true; wait "$publisher_pid" 2>/dev/null || true; wait "$postgres_pid" 2>/dev/null || true; }
trap cleanup TERM INT EXIT
# Either child exiting ends the composition; never leave an unsupervised PG.
set +e
wait -n "$postgres_pid" "$publisher_pid"
result=$?
set -e
exit "$result"
