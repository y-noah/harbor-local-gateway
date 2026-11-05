import {randomBytes} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);},ident=value=>{if(!/^harbor_(?:runtime|rehearsal)_[a-f0-9]{24}$/.test(value))fail('storage_operator_lease_identity');return '"'+value+'"';};
async function lockRole(store,expected){const row=await store.get('SELECT oid::text oid FROM pg_catalog.pg_authid WHERE rolname=$1 FOR UPDATE',expected.runtime),oid=expected.runtimeOid??expected.roleGenerations?.runtimeOid;if(!row||oid!==undefined&&row.oid!==oid)fail('storage_operator_lease_binding');}
async function binding(store,expected,{lock=false}={}){
  const identity=await store.get('SELECT current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid'),actual=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton=true'+(lock?' FOR UPDATE':''));
  const requiredState=expected.activationState??'pending';if(!['pending','failed'].includes(requiredState))fail('storage_operator_lease_binding');
  if(!actual||actual.state!==requiredState||actual.run_id!==expected.runId||actual.binding!==expected.bindingDigest||actual.database_oid!==expected.databaseOid||actual.runtime!==expected.runtime||identity.database_oid!==expected.databaseOid)fail('storage_operator_lease_binding');if(requiredState==='failed'){const journal=await store.get('SELECT * FROM harbor_operator.deployment_activation WHERE singleton=true'),role=await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',expected.runtime);if(!journal||journal.state!=='failed'||journal.run_id!==expected.runId||journal.binding!==expected.bindingDigest||journal.database_oid!==expected.databaseOid||journal.runtime_oid!==expected.runtimeOid||role?.oid!==expected.runtimeOid||journal.deployment_id!==expected.deploymentId||journal.receipt_digest!==expected.receiptDigest||journal.epoch!==expected.activationEpoch)fail('storage_operator_lease_binding');}return identity;
}
export async function verifyPrivateRuntimeExpiryAuthentication(store,runtime){
  ident(runtime);const rules=await store.all('SELECT file_name,user_name,auth_method,error FROM pg_hba_file_rules ORDER BY rule_number');let matches=0;
  for(const row of rules){
    if(row.error)fail('storage_operator_password_expiry_auth_unverified');
    const users=row.user_name??[];if(users.some(value=>value.startsWith('@')||value.startsWith('/')))fail('storage_operator_password_expiry_auth_unverified');
    let matching=users.some(value=>value==='all'||value===runtime);for(const user of users.filter(value=>value.startsWith('+'))){const group=await store.get('SELECT oid FROM pg_roles WHERE rolname=$1',user.slice(1));if(group&&(await store.get("SELECT pg_has_role($1,$2,'MEMBER') member",runtime,user.slice(1))).member)matching=true;}if(!matching)continue;
    // Check every database/address path this new role could use. PostgreSQL has
    // no DENY to cancel another DB's PUBLIC CONNECT. Non-password HBA methods
    // would ignore VALID UNTIL, so an unknown path cannot establish this lease.
    if(!['scram-sha-256','reject'].includes(row.auth_method))fail('storage_operator_password_expiry_auth_unverified');matches++;
  }
  if(!matches)fail('storage_operator_password_expiry_auth_unverified');
  for(const file of new Set(rules.map(row=>row.file_name))){const value=await store.get('SELECT (pg_stat_file($1)).modification<=pg_conf_load_time() loaded',file);if(value?.loaded!==true)fail('storage_operator_password_expiry_auth_unverified');}
  return {passwordExpiryEnforced:true,authentication:'scram-sha-256',existingSessionsRevokedByExpiry:false};
}
async function leaseTable(store){await store.query('CREATE TABLE IF NOT EXISTS harbor_operator.preflight_lease(singleton boolean PRIMARY KEY CHECK(singleton),run_id text NOT NULL,binding text NOT NULL,database_oid text NOT NULL,runtime text NOT NULL,application text NOT NULL,issued_ms bigint NOT NULL,expires_ms bigint NOT NULL,revoked boolean NOT NULL)');}
export async function enablePrivateRuntimeLease(store,expected,{password,timeoutMs,deadline,signal,beforeCommit=async()=>{}}={}){
  ident(expected.runtime);if(!/^[A-Za-z0-9_-]{43}$/.test(password??'')||!Number.isSafeInteger(timeoutMs)||timeoutMs<1000||timeoutMs>120000)fail('storage_operator_lease_configuration');
  if(deadline!==undefined&&(!Number.isFinite(deadline)||deadline-performance.now()>120000))fail('storage_operator_lease_configuration');
  const remaining=()=>{signal?.throwIfAborted();const budget=deadline===undefined?timeoutMs:Math.min(timeoutMs,Math.floor(deadline-performance.now()));if(budget<1000)fail('storage_operator_preflight_deadline');return budget;};remaining();
  const identity=await binding(store,expected);await verifyPrivateRuntimeExpiryAuthentication(store,expected.runtime);let lease;
  await store.transaction(async()=>{
    await lockRole(store,expected);await store.query('SELECT pg_advisory_xact_lock(19028731,668191)');await binding(store,expected,{lock:true});await leaseTable(store);const prior=await store.get('SELECT * FROM harbor_operator.preflight_lease WHERE singleton=true FOR UPDATE');if(prior&&!prior.revoked)fail('storage_operator_lease_not_reclaimed');
    const at=(await store.get('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint at')).at,budget=remaining(),application='harbor-op-'+randomBytes(16).toString('hex');lease={...expected,application,issuedMs:at,expiresMs:at+budget,timeoutMs:budget};
    await store.run('INSERT INTO harbor_operator.preflight_lease VALUES(true,$1,$2,$3,$4,$5,$6,$7,false) ON CONFLICT(singleton) DO UPDATE SET run_id=EXCLUDED.run_id,binding=EXCLUDED.binding,database_oid=EXCLUDED.database_oid,runtime=EXCLUDED.runtime,application=EXCLUDED.application,issued_ms=EXCLUDED.issued_ms,expires_ms=EXCLUDED.expires_ms,revoked=false',expected.runId,expected.bindingDigest,expected.databaseOid,expected.runtime,application,at,lease.expiresMs);
    await store.query('ALTER ROLE '+ident(expected.runtime)+" LOGIN PASSWORD '"+password+"' VALID UNTIL '"+new Date(lease.expiresMs).toISOString()+"'");
    for(const setting of ['statement_timeout','idle_session_timeout','idle_in_transaction_session_timeout','transaction_timeout'])await store.query('ALTER ROLE '+ident(expected.runtime)+' IN DATABASE '+ident(identity.database)+' SET '+setting+"='"+budget+"ms'");
    await beforeCommit();remaining();
  });return lease;
}
export async function revokePrivateRuntimeLogin(store,expected){
  return store.transaction(async()=>{await lockRole(store,expected);await store.query('SELECT pg_advisory_xact_lock(19028731,668191)');const identity=await binding(store,expected,{lock:true});await leaseTable(store);const lease=await store.get('SELECT * FROM harbor_operator.preflight_lease WHERE singleton=true');
  if(lease&&(lease.run_id!==expected.runId||lease.binding!==expected.bindingDigest||lease.database_oid!==expected.databaseOid||lease.runtime!==expected.runtime))fail('storage_operator_lease_binding');
  await store.query('ALTER ROLE '+ident(expected.runtime)+" NOLOGIN PASSWORD NULL VALID UNTIL '1970-01-01T00:00:00Z'");
  return {identity,lease};});
}
export async function revokePrivateRuntimeLease(store,expected){
  const {identity,lease}=await revokePrivateRuntimeLogin(store,expected);
  if(lease){
    // Re-read predicates in the terminating statement, rather than keeping a
    // PID list from a prior epoch. Only this pending DB/role/application/time
    // interval belongs to the private preflight; other sessions are untouched.
    await store.get('SELECT pg_stat_clear_snapshot()');
    await store.all("SELECT pg_terminate_backend(pid,1000) terminated FROM pg_stat_activity WHERE datid=$1::oid AND usename=$2 AND application_name=$3 AND floor(extract(epoch FROM backend_start)*1000)>=$4 AND floor(extract(epoch FROM backend_start)*1000)<=$5",expected.databaseOid,expected.runtime,lease.application,lease.issued_ms,lease.expires_ms);
    await store.get('SELECT pg_stat_clear_snapshot()');
    const remaining=await store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datid=$1::oid AND usename=$2 AND application_name=$3 AND floor(extract(epoch FROM backend_start)*1000)>=$4 AND floor(extract(epoch FROM backend_start)*1000)<=$5) present',expected.databaseOid,expected.runtime,lease.application,lease.issued_ms,lease.expires_ms);if(remaining.present)fail('storage_operator_preflight_sessions_pending');
  }
  return store.transaction(async()=>{
    await lockRole(store,expected);await store.query('SELECT pg_advisory_xact_lock(19028731,668191)');await binding(store,expected,{lock:true});const current=await store.get('SELECT * FROM harbor_operator.preflight_lease WHERE singleton=true FOR UPDATE');
    const fields=['run_id','binding','database_oid','runtime','application','issued_ms','expires_ms'];if(lease?(!current||fields.some(key=>current[key]!==lease[key])):!!current)fail('storage_operator_lease_generation_changed');
    if((await store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',expected.runtime)).rolcanlogin!==false)fail('storage_operator_preflight_login_changed');
    await store.query('ALTER ROLE '+ident(expected.runtime)+' IN DATABASE '+ident(identity.database)+' RESET ALL');
    const foreign=await store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1) present',expected.runtime);if(foreign.present)fail('storage_operator_foreign_runtime_sessions');
    if(lease)await store.run('UPDATE harbor_operator.preflight_lease SET revoked=true WHERE singleton=true AND application=$1 AND issued_ms=$2 AND expires_ms=$3',lease.application,lease.issued_ms,lease.expires_ms);
    return {loginRevoked:true,ownPreflightSessionsEnded:true,existingSessionsRevokedByExpiry:false};
  });
}
