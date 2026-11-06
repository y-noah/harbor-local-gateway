import {readFile,lstat} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomBytes} from 'node:crypto';
import {createServer,isIP} from 'node:net';
import {checkServerIdentity} from 'node:tls';
import https from 'node:https';
import {setTimeout as delay} from 'node:timers/promises';
import {PostgresStore} from './postgres-store.mjs';
import {revealStoredKey} from './store.mjs';
import {createOwnedRuntime} from './tools/capacity/owned-runtime.mjs';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {inspectPostgresBackupRestoreSession,beginPostgresRestoreRuntimeCleanupScope,endPostgresRestoreRuntimeCleanupScope,markPostgresRestoreApplicationCleanupUnknown} from './postgres-backup-restore.mjs';
import {inspectPeriodicApplicationRestore} from './postgres-periodic-application-restore.mjs';
import {publishRecordingStorageReport} from './postgres-storage-report.mjs';
import {collectRestoredRuntimeCloses,restoredRuntimeClosesSucceeded} from './storage-restored-runtime-shutdown.mjs';
import {transferError} from './migration-artifacts.mjs';
import {handoffReceiverRestoredTree} from './storage-receiver-candidate-files.mjs';

const fail=code=>{throw transferError(code);},ident=value=>'"'+value.replaceAll('"','""')+'"';
async function freePort(){const server=createServer();await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});const port=server.address().port;await new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()));return port;}
async function request(origin,path,{ca,key,signal}={}){
  return new Promise((resolve,reject)=>{const target=new URL(path,origin),identity=target.hostname.replace(/^\[|\]$/g,'');
    const req=https.request({hostname:'127.0.0.1',port:Number(target.port),path:target.pathname,servername:isIP(identity)?'':identity,checkServerIdentity:(_host,certificate)=>checkServerIdentity(identity,certificate),ca,headers:{host:target.host,...(key?{authorization:'Bearer '+key}:{})},agent:false,timeout:5000,signal},res=>{let bytes=0;res.on('data',chunk=>{if((bytes+=chunk.length)>1048576)res.destroy(transferError('periodic_application_http_capacity'));});res.once('error',reject);res.once('end',()=>resolve(res.statusCode));});
    req.once('timeout',()=>req.destroy(transferError('periodic_application_http_timeout')));req.once('error',reject);req.end();
  });
}
function client(runtime,role,{signal,timeoutMs,providedChild}){
  const child=providedChild??runtime.start(process.execPath,[fileURLToPath(new URL('./postgres-periodic-application-runtime-child.mjs',import.meta.url)),'--periodic-restored-runtime'],{role,stdio:['ignore','ignore','ignore','ipc']});
  let sequence=0;const pending=new Map();
  const rejectAll=()=>{for(const row of pending.values()){clearTimeout(row.timer);row.reject(transferError('periodic_application_process_exited'));}pending.clear();};
  child.once('exit',rejectAll);child.once('error',rejectAll);
  child.on('message',message=>{const row=pending.get(message?.id);if(!row)return;pending.delete(message.id);clearTimeout(row.timer);message.ok?row.resolve(message.result):row.reject(transferError(message.error??'periodic_application_process_failed'));});
  const command=(operation,configuration)=>new Promise((resolve,reject)=>{
    if(!child.connected||child.exitCode!==null||child.signalCode!==null)return reject(transferError('periodic_application_process_exited'));
    const id=++sequence,timer=setTimeout(()=>{pending.delete(id);reject(transferError('periodic_application_command_timeout'));},timeoutMs);pending.set(id,{resolve,reject,timer});
    child.send({id,operation,...(configuration?{configuration}:{})},error=>{if(error){clearTimeout(timer);pending.delete(id);reject(transferError('periodic_application_send_failed'));}});
  });
  const aborted=()=>rejectAll();signal?.addEventListener('abort',aborted,{once:true});if(signal?.aborted)aborted();
  return {child,command,close:async()=>{signal?.removeEventListener('abort',aborted);runtime.proof(child);if(runtime.resume)await runtime.resume(child);else try{process.kill(child.pid,'SIGCONT');}catch(error){if(error.code!=='ESRCH')throw error;}const result=await command('close');if(result?.closed!==true||result.drained!==true)fail('periodic_application_drain_failed');if(child.exitCode===null&&child.signalCode===null)await runtime.stop(child);return result;}};
}

// All mutations below target the already isolated, Unix-only restored cluster.
// The original source cluster and its role/password generations are untouched.
export async function verifyPeriodicApplicationRuntime(session,{token,destination,signal}={}){
  const live=inspectPostgresBackupRestoreSession(session),{source,installedPolicy,archivedBackup}=inspectPeriodicApplicationRestore(token,{...live}),record=postgresBackupContext(live.context),installed=record.scope==='existing-cluster';
  if(installed?process.getuid()!==0||(!installedPolicy&&!archivedBackup)||record.restoreUid<1:record.scope!=='task-owned'||record.restoreUid!==process.getuid())fail('periodic_application_runtime_scope');
  if(destination!==join(dirname(live.connection.host),'application')||(await lstat(destination)).uid!==process.getuid())fail('periodic_application_runtime_task_scope');
  signal?.throwIfAborted();const policy=source.configuration,store=new PostgresStore({connection:{...live.connection,database:policy.database.name},poolSize:2}),publisherStop=new AbortController(),timeoutMs=Math.min(record.profile.processTimeoutMs,120000);
  const role='harbor_periodic_'+randomBytes(12).toString('hex'),password=randomBytes(32).toString('base64url'),application='harbor-periodic-'+randomBytes(12).toString('hex');
  let master,runtime,gateway,worker,publisher,publisherClient,publisherError,publisherRole,publisherPassword,publisherApplication,publisherRoleCreated=false,roleCreated=false,cleanupScope=false,summary;
  try{
    beginPostgresRestoreRuntimeCleanupScope(session);cleanupScope=true;
    if(installed){
      const {createInstalledPeriodicApplicationProcesses}=await import('./postgres-periodic-application-installed-runtime.mjs');
      runtime=await createInstalledPeriodicApplicationProcesses(live.context,installedPolicy,{protectedRoot:dirname(destination),signal,...(archivedBackup?{archivedRestore:{token,session}}:{})});
      await handoffReceiverRestoredTree(destination,{uid:record.restoreUid,gid:record.restoreGid,signal});
    }else runtime=createOwnedRuntime();
    master=await readFile(join(destination,'data','key-encryption.key'));
    const old=await store.get('SELECT k.id,k.digest,k.secret_cipher,m.device_mode,k.device_ids FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id WHERE k.secret_cipher IS NOT NULL AND k.enabled=1 AND m.enabled=1 AND k.removed_at IS NULL AND m.removed_at IS NULL AND (k.expires IS NULL OR k.expires>floor(extract(epoch FROM clock_timestamp())*1000)::bigint) ORDER BY (m.device_mode=\'compatible\' AND k.device_ids IS NULL) DESC,k.id LIMIT 1');
    if(!old)fail('periodic_application_original_key_missing');const oldKey=revealStoredKey(master,old);if(!oldKey)fail('periodic_application_original_key_invalid');await store.authenticate(oldKey);
    await store.transaction(async()=>{
      await store.query("SET LOCAL password_encryption='scram-sha-256'");
      await store.query('CREATE ROLE '+ident(role)+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 32 PASSWORD '"+password+"' VALID UNTIL '"+new Date(Date.now()+timeoutMs).toISOString()+"'");
      await store.query('GRANT CONNECT ON DATABASE '+ident(policy.database.name)+' TO '+ident(role));await store.query('GRANT USAGE ON SCHEMA harbor TO '+ident(role));
      for(const row of await store.all("SELECT table_name FROM information_schema.tables WHERE table_schema='harbor' AND table_type='BASE TABLE' ORDER BY table_name"))await store.query('GRANT '+(['schema_migrations','data_contract','recording_storage_reports'].includes(row.table_name)?'SELECT':'SELECT,INSERT,UPDATE,DELETE')+' ON TABLE harbor.'+ident(row.table_name)+' TO '+ident(role));
      await store.query('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA harbor TO '+ident(role));
      for(const setting of ['statement_timeout','idle_session_timeout','idle_in_transaction_session_timeout','transaction_timeout'])await store.query('ALTER ROLE '+ident(role)+' SET '+setting+"='"+timeoutMs+"ms'");
      signal?.throwIfAborted();inspectPostgresBackupRestoreSession(session);
    });roleCreated=true;
    const reportKey=join(destination,'secrets','postgres-report.key');let first;
    if(installed){
      publisherRole='harbor_periodic_pub_'+randomBytes(12).toString('hex');publisherPassword=randomBytes(32).toString('base64url');publisherApplication=application+'-publisher';
      await store.transaction(async()=>{
        await store.query("SET LOCAL password_encryption='scram-sha-256'");await store.query('CREATE ROLE '+ident(publisherRole)+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4 PASSWORD '"+publisherPassword+"' VALID UNTIL '"+new Date(Date.now()+timeoutMs).toISOString()+"'");
        await store.query('GRANT CONNECT ON DATABASE '+ident(policy.database.name)+' TO '+ident(publisherRole));await store.query('GRANT USAGE ON SCHEMA harbor TO '+ident(publisherRole));await store.query('GRANT SELECT ON ALL TABLES IN SCHEMA harbor TO '+ident(publisherRole));await store.query('GRANT INSERT,UPDATE ON harbor.recording_storage_reports TO '+ident(publisherRole));await store.query('GRANT pg_read_all_settings TO '+ident(publisherRole)+' WITH INHERIT TRUE');
        for(const setting of ['statement_timeout','idle_session_timeout','idle_in_transaction_session_timeout','transaction_timeout'])await store.query('ALTER ROLE '+ident(publisherRole)+' SET '+setting+"='"+timeoutMs+"ms'");signal?.throwIfAborted();
      });publisherRoleCreated=true;
      publisherClient=client(runtime,'publisher',{signal,timeoutMs,providedChild:await runtime.start('publisher')});
      const ready=await publisherClient.command('initialize',{database:{connection:{...live.connection,database:policy.database.name,user:publisherRole,password:publisherPassword,application_name:publisherApplication},poolSize:2},pgDataDir:join(dirname(destination),'pgdata'),keyFile:reportKey,timeoutMs});
      if(!ready.publisherStarted||ready.uid!==record.restoreUid)fail('periodic_application_publisher_unverified');first={sampled_at:ready.sampledAt};
    }else{
      const publish=()=>publishRecordingStorageReport(store,{pgDataDir:join(dirname(destination),'pgdata'),keyFile:reportKey});first=await publish();
      publisher=(async()=>{try{for(;;){await delay(5000,undefined,{signal:publisherStop.signal});await publish();}}catch(error){if(!publisherStop.signal.aborted)publisherError=error;}})();
    }
    const tls=join(destination,'secrets','device'),port=await freePort(),host=new URL(policy.deviceOrigin).hostname,origin='https://'+host+':'+port,env={HARBOR_DATA_DIR:join(destination,'data'),HARBOR_RUNTIME_DIR:join(destination,'runtime'),HARBOR_RECORDING_ENABLED:'1',HARBOR_RECORDING_WORKER_ENABLED:'1',HARBOR_RECORDING_STORAGE_MODE:'report',HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE:reportKey,HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:String(port),HARBOR_DEVICE_PUBLIC_ORIGIN:origin,HARBOR_PUBLIC_ORIGINS:origin,HARBOR_DEVICE_TLS_KEY_FILE:join(tls,'server.key'),HARBOR_DEVICE_TLS_CERT_FILE:join(tls,'server.pem'),HARBOR_DEVICE_CA_CERT_FILE:join(tls,'ca.pem'),HARBOR_DEVICE_CA_KEY_FILE:join(tls,'ca.key')},database={connection:{...live.connection,database:policy.database.name,user:role,password,application_name:application}};
    gateway=client(runtime,'gateway',{signal,timeoutMs,...(installed?{providedChild:await runtime.start('gateway')}:{})});const gatewayReady=await gateway.command('initialize',{role:'gateway',database,env});
    if(gatewayReady.role!=='gateway'||gatewayReady.tlsPort!==port||!gatewayReady.storage||!gatewayReady.materials.storage_verified||!gatewayReady.materials.device_tls_verified)fail('periodic_application_gateway_unverified');
    const started=(await store.get('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint at')).at;
    worker=client(runtime,'worker',{signal,timeoutMs,...(installed?{providedChild:await runtime.start('worker')}:{})});const workerReady=await worker.command('initialize',{role:'worker',database,env});
    if(workerReady.role!=='worker'||!workerReady.storage||worker.child.pid===gateway.child.pid)fail('periodic_application_worker_unverified');
    const ca=await readFile(join(tls,'ca.pem'));if(await request(origin,'/health',{ca,signal})!==200)fail('periodic_application_tls_unverified');
    let oldKeyHttpVerified=false;if(old.device_mode==='compatible'&&old.device_ids===null){if(await request(origin,'/api/me',{ca,key:oldKey,signal})!==200)fail('periodic_application_key_http_unverified');oldKeyHttpVerified=true;}
    const deadline=Date.now()+timeoutMs;
    for(;;){signal?.throwIfAborted();inspectPostgresBackupRestoreSession(session);if(publisherError)throw publisherError;
      if(publisherClient){const status=await publisherClient.command('status');if(!status.publisherReady||status.uid!==record.restoreUid)fail('periodic_application_publisher_unverified');}
      if((await store.get("SELECT EXISTS(SELECT 1 FROM harbor.background_jobs WHERE status='completed' AND updated>=$1 AND kind IN ('recording.purge','routing.expiry','recording.index','recording.export')) progress",started)).progress)break;
      if(Date.now()>deadline)fail('periodic_application_worker_timeout');await delay(20,undefined,{signal});
    }
    for(const candidate of [gateway,worker]){const status=await candidate.command('status');if(!status.storage||!status.captureAllowed)fail('periodic_application_runtime_storage');}
    if(installed)await runtime.revalidate();
    summary={scope:installed?'installed-restored-periodic-source-runtime':'task-restored-periodic-source-runtime',...(installed?{sourceDigest:runtime.sourceDigest,nodeDigest:runtime.nodeDigest,crossUidExecutionVerified:true,publisherUidVerified:true,restoreUid:record.restoreUid}:{}),gatewayStartedVerified:true,independentWorkerStartedVerified:true,workerProgressVerified:true,originalKeyDatabaseVerified:true,oldKeyHttpVerified,restoredTlsServerVerified:true,freshRestoredDatabaseReportVerified:true,reportSampledAt:first.sampled_at,upstreamCallsDisabled:true,originalDeviceClientChallengeVerified:false,candidateVerified:false,offhostVerified:false,deploymentAllowed:false};return summary;
  }finally{
    const clients=[gateway,worker,...(publisherClient?[publisherClient]:[])],closed=await collectRestoredRuntimeCloses(clients,{timeoutMs});publisherStop.abort();await publisher;
    let processesExited=false,roleRevoked=false;
    try{
      try{await runtime?.close();processesExited=true;}catch{markPostgresRestoreApplicationCleanupUnknown(session);}
      if(roleCreated){await store.query('ALTER ROLE '+ident(role)+" NOLOGIN PASSWORD NULL VALID UNTIL '1970-01-01T00:00:00Z'");await store.all('SELECT pg_terminate_backend(pid,1000) FROM pg_stat_activity WHERE usename=$1 AND application_name=$2',role,application);if((await store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1) present',role)).present)fail('periodic_application_runtime_sessions_remaining');roleRevoked=(await store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',role)).rolcanlogin===false;}
      if(publisherRoleCreated){await store.query('ALTER ROLE '+ident(publisherRole)+" NOLOGIN PASSWORD NULL VALID UNTIL '1970-01-01T00:00:00Z'");await store.all('SELECT pg_terminate_backend(pid,1000) FROM pg_stat_activity WHERE usename=$1 AND application_name=$2',publisherRole,publisherApplication);if((await store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1) present',publisherRole)).present||(await store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',publisherRole)).rolcanlogin!==false)fail('periodic_application_publisher_revoke_failed');}
      const drained=restoredRuntimeClosesSucceeded(closed,clients.filter(Boolean).map(value=>value.child));
      if(summary)summary.cleanup={applicationProcessesExitedVerified:processesExited,applicationDrainVerified:drained,publisherStoppedVerified:true,temporaryLoginRevokedVerified:roleRevoked};
      if(!processesExited)fail('periodic_application_cleanup_unknown');if(!drained)fail('periodic_application_drain_failed');if(roleCreated&&!roleRevoked)fail('periodic_application_role_revoke_failed');
    }finally{master?.fill(0);try{await store.close();}finally{if(cleanupScope)await endPostgresRestoreRuntimeCleanupScope(session);}}
  }
}
