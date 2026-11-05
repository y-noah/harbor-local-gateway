#!/usr/bin/env bash
set -euo pipefail
[[ "${CI:-}" == true && "${HARBOR_PRIVATE_CANDIDATE_SERVICE:-}" == 1 ]]
[[ "${POSTGRES_USER:-}" == harbor_ci && "${POSTGRES_DB:-}" == harbor_ci && "${POSTGRES_PASSWORD:-}" == harbor-ci-test-only ]]
gosu postgres node --input-type=module -e "import{writeFileSync}from'node:fs';import{randomBytes}from'node:crypto';import{imageManifest}from'/app/ci/postgres-image-manifest.mjs';const manifest=await imageManifest();if(manifest.revision!==process.env.CI_COMMIT_SHA)throw Error('CI service revision mismatch');writeFileSync('/tmp/harbor-ci-report.key',randomBytes(32),{flag:'wx',mode:0o600});writeFileSync('/tmp/harbor-private-ci-service',JSON.stringify({version:1,source:'harbor-ci-storage-v1',revision:manifest.revision}),{flag:'wx',mode:0o600});"
docker-entrypoint.sh postgres &
postgres_pid=$!
publisher_pid=''
cleanup(){ trap - TERM INT EXIT; [[ -z "$publisher_pid" ]] || kill -TERM "$publisher_pid" 2>/dev/null || true; kill -TERM "$postgres_pid" 2>/dev/null || true; [[ -z "$publisher_pid" ]] || wait "$publisher_pid" 2>/dev/null || true; wait "$postgres_pid" 2>/dev/null || true; }
trap cleanup TERM INT EXIT
# This loop is confined to the disposable CI service, not installed on a production host.
node --input-type=module -e "
import{readFile}from'node:fs/promises';import{spawn}from'node:child_process';
let child,timer,stopped=false;process.on('SIGTERM',()=>{stopped=true;clearTimeout(timer);child?.kill('SIGTERM');});
async function tick(){if(stopped)return;try{const name=(await readFile('/tmp/harbor-ci-database','utf8')).trim();if(!/^harbor_candidate_[a-f0-9]{32}$/.test(name))throw Error();const url=new URL('postgresql://harbor_ci@localhost/'+name);url.searchParams.set('host','/var/run/postgresql');child=spawn('gosu',['postgres','node','/app/postgres-storage-report.mjs'],{env:{...process.env,HARBOR_STORAGE_REPORT_DATABASE_URL:url.href,HARBOR_RECORDING_PG_DATA_DIR:process.env.PGDATA,HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE:'/tmp/harbor-ci-report.key',HARBOR_STORAGE_REPORT_TTL_MS:'15000'},stdio:['ignore','inherit','inherit']});await new Promise(resolve=>child.once('close',resolve));child=null;}catch{}if(!stopped)timer=setTimeout(tick,5000);}await tick();
" &
publisher_pid=$!
wait "$postgres_pid"
