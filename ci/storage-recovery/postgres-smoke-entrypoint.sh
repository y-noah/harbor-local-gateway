#!/usr/bin/env bash
# Only the opt-in disposable GitLab service may manufacture these fixture files.
set -euo pipefail
[[ "$#" == 0 && "$(id -u)" == 0 && "${CI:-}" == true && "${HARBOR_INFRA_SMOKE:-}" == 1 ]]
[[ "${CI_JOB_NAME:-}" == smoke_postgres_infrastructure ]]
[[ "${CI_COMMIT_SHA:-}" =~ ^[a-f0-9]{40}$ && "${CI_PIPELINE_ID:-}" =~ ^[1-9][0-9]*$ && "${CI_JOB_ID:-}" =~ ^[1-9][0-9]*$ ]]
[[ "${POSTGRES_USER:-}" == postgres && "${POSTGRES_DB:-}" == harbor_infra_smoke && "${POSTGRES_PASSWORD:-}" == harbor-infra-smoke-only ]]
[[ "${POSTGRES_INITDB_ARGS:-}" == "--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8" ]]
[[ "${PGDATA:-}" == /opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker && ! -e "$PGDATA/PG_VERSION" ]]
[[ "${CI_PROJECT_DIR:-}" == /* && "$CI_PROJECT_DIR" != / && ! "$CI_PROJECT_DIR" == *'/../'* ]]
for target in /run/harbor-pg-tls /run/harbor-infra-control /run/harbor-report.key /run/harbor-postgres-password; do [[ ! -e "$target" ]]; done
umask 077
mkdir /run/harbor-pg-tls /run/harbor-infra-control
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=Harbor isolated infra smoke CA' -addext basicConstraints=critical,CA:TRUE -keyout /tmp/infra-smoke-ca.key -out /run/harbor-pg-tls/ca.pem >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=harbor-infra-pg' -keyout /run/harbor-pg-tls/server.key -out /tmp/infra-smoke.csr >/dev/null 2>&1
printf '%s\n' 'basicConstraints=critical,CA:FALSE' 'extendedKeyUsage=serverAuth' 'subjectAltName=DNS:harbor-infra-pg,DNS:localhost,IP:127.0.0.1' > /tmp/infra-smoke.ext
openssl x509 -req -in /tmp/infra-smoke.csr -CA /run/harbor-pg-tls/ca.pem -CAkey /tmp/infra-smoke-ca.key -set_serial 1 -days 1 -extfile /tmp/infra-smoke.ext -out /run/harbor-pg-tls/server.pem >/dev/null 2>&1
node --input-type=module -e "import{writeFileSync}from'node:fs';import{randomBytes}from'node:crypto';writeFileSync('/run/harbor-report.key',randomBytes(32),{flag:'wx',mode:0o600});writeFileSync('/run/harbor-infra-control/publisher.json',JSON.stringify({version:1,database:'harbor_infra_smoke',pgDataDir:'/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker',socketDirectory:'/var/run/postgresql',port:5432,keyFile:'/run/harbor-report.key'}),{flag:'wx',mode:0o644});writeFileSync('/run/harbor-infra-control/smoke.json',JSON.stringify({schema:'harbor-infra-smoke-v1',commit:process.env.CI_COMMIT_SHA,pipeline:process.env.CI_PIPELINE_ID,job:process.env.CI_JOB_ID}),{flag:'wx',mode:0o644});"
printf '%s\n' 'local all postgres peer' 'local all all scram-sha-256' 'hostnossl all all all reject' 'hostssl all all 0.0.0.0/0 scram-sha-256' 'hostssl all all ::/0 scram-sha-256' > /run/harbor-infra-control/pg_hba.conf
printf '%s\n' "$POSTGRES_PASSWORD" > /run/harbor-postgres-password
chown -R postgres:postgres /run/harbor-pg-tls
chown postgres:postgres /run/harbor-report.key /run/harbor-postgres-password
chmod 0700 /run/harbor-pg-tls
chmod 0755 /run/harbor-infra-control
chmod 0600 /run/harbor-pg-tls/server.key /run/harbor-report.key /run/harbor-postgres-password
chmod 0644 /run/harbor-pg-tls/ca.pem /run/harbor-pg-tls/server.pem /run/harbor-infra-control/*.json /run/harbor-infra-control/pg_hba.conf
unset POSTGRES_PASSWORD
export POSTGRES_PASSWORD_FILE=/run/harbor-postgres-password
# The job requests the public CA after checkout, avoiding Runner git-clean races.
share="$CI_PROJECT_DIR/.ci-postgres-infra-smoke/$CI_JOB_ID"
(
 for _ in $(seq 1 1200); do
  if [[ -f "$share/request" && ! -L "$share/request" ]]; then
   [[ ! -e "$share/ca.pem" && ! -e "$share/ca.pem.pending" ]]
   cp --no-clobber /run/harbor-pg-tls/ca.pem "$share/ca.pem.pending"
   chmod 0644 "$share/ca.pem.pending"
   mv -T "$share/ca.pem.pending" "$share/ca.pem"
   exit 0
  fi
  sleep 0.1
 done
 exit 1
) &
# Exercise the exact ordinary infrastructure/official PG entry, not a CI PG clone.
exec /usr/local/bin/harbor-postgres-infra
