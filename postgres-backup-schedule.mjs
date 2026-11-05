import {randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {realpath} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {acquireInstance} from './instance.mjs';
import {postgresBackupContext,verifyBackupCluster} from './postgres-backup-context.mjs';
import {createPostgresBaseBackup,verifyPostgresBackup,checkpointPostgresBackup,postgresBackupRestoreSource,inspectPostgresBackupCheckpointProtection} from './postgres-backup.mjs';
import {readManifest,writeManifest,transferError} from './migration-artifacts.mjs';

const fail=code=>{throw transferError(code);},proofCache=new WeakMap();
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const object=value=>value&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,keys)=>object(value)&&Object.keys(value).every(key=>keys.includes(key));
const time=value=>Number.isSafeInteger(value)&&value>=0;
const scope='harbor-periodic-database-backup-v1';
export function postgresBackupSchedulePolicy(value){
  if(!exact(value,['version','enabled','fullIntervalMs','checkpointIntervalMs','pollIntervalMs','pinnedBackupIds'])||value.version!==1||typeof value.enabled!=='boolean')fail('backup_schedule_policy_invalid');
  const policy={fullIntervalMs:86400000,checkpointIntervalMs:300000,pollIntervalMs:60000,pinnedBackupIds:[],...value};
  if(![policy.fullIntervalMs,policy.checkpointIntervalMs,policy.pollIntervalMs].every(n=>Number.isSafeInteger(n)&&n>=1000&&n<=30*86400000)||policy.pollIntervalMs>policy.checkpointIntervalMs||policy.checkpointIntervalMs>policy.fullIntervalMs||!Array.isArray(policy.pinnedBackupIds)||policy.pinnedBackupIds.length>1024||policy.pinnedBackupIds.some(id=>!uuid(id))||new Set(policy.pinnedBackupIds).size!==policy.pinnedBackupIds.length)fail('backup_schedule_policy_invalid');
  return Object.freeze({...policy,pinnedBackupIds:Object.freeze([...policy.pinnedBackupIds])});
}
function generation(value){return object(value)&&typeof value.postmasterStart==='string'&&/^\d+(?:\.\d+)?$/.test(value.postmasterStart)&&Number.isSafeInteger(value.timeline)&&value.timeline>=1&&value.timeline<=0xffffffff;}
function validateState(value,record){
  if(!exact(value,['version','scope','configurationDigest','systemId','keyVersion','active','pending','updatedAt'])||value.version!==1||value.scope!==scope||value.configurationDigest!==record.configurationDigest||value.systemId!==record.expected.system_id||value.keyVersion!==record.keyVersion||!time(value.updatedAt))fail('backup_schedule_state_binding');
  for(const field of ['active','pending']){
    const row=value[field];if(row===null)continue;
    if(!exact(row,field==='active'?['backupId','postmasterStart','timeline','createdAt','checkpointAt']:['backupId','postmasterStart','timeline','createdAt'])||!uuid(row?.backupId)||!generation(row)||!time(row.createdAt)||row.createdAt>value.updatedAt||field==='active'&&(!time(row.checkpointAt)||row.checkpointAt<row.createdAt||row.checkpointAt>value.updatedAt))fail('backup_schedule_state_invalid');
  }
  if(value.active?.backupId===value.pending?.backupId&&value.active)fail('backup_schedule_state_invalid');
  return value;
}
const output=(state,extra={})=>({state,scope:'database-only',applicationMaterialsVerified:false,restoreVerified:false,offhostVerified:false,deploymentAllowed:false,...extra});

// One cycle is useful to an operator; the service retains the verified proof in
// memory between cycles so each WAL checkpoint does not reread an entire base.
// A restarted service fully verifies the saved base before reusing its proof.
export async function runPostgresBackupScheduleCycle(context,configuration,{signal,clock=Date.now}={}){
  const policy=postgresBackupSchedulePolicy(configuration);if(!policy.enabled)return output('schedule_disabled');
  signal?.throwIfAborted();const record=postgresBackupContext(context);
  if(record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('backup_schedule_context_invalid');
  let lock;try{lock=await acquireInstance(join(await realpath(record.artifactDirectory),'.periodic-backup'));}catch(error){if(error.code==='EADDRINUSE')fail('backup_schedule_busy');throw error;}
  const file=join(record.artifactDirectory,'periodic-state.enc');
  try{
    const current=await verifyBackupCluster(context),now=()=>{const value=clock();if(!time(value))fail('backup_schedule_clock_invalid');return value;};
    let state;try{state=validateState(await readManifest(file,record.key,{signal}),record);}catch(error){if(error.code!=='ENOENT')throw error;state={version:1,scope,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,active:null,pending:null,updatedAt:now()};}
    if(now()<state.updatedAt)fail('backup_schedule_clock_reversed');
    const same=row=>row&&row.postmasterStart===current.postmaster_start&&row.timeline===current.timeline;
    const save=async()=>{signal?.throwIfAborted();const at=now();if(at<state.updatedAt)fail('backup_schedule_clock_reversed');state.updatedAt=at;validateState(state,record);await writeManifest(file,state,record.key,{replace:true});};
    const active=state.active,sealed=active?(await inspectPostgresBackupCheckpointProtection(context,active.backupId,{signal})).sealed:false,rotate=!same(active)||sealed||policy.pinnedBackupIds.includes(active?.backupId)||now()-active.createdAt>=policy.fullIntervalMs;
    let proof,created=false,checkpointCreated=false;
    if(rotate||state.pending){
      if(!same(state.pending)){
        state.pending={backupId:randomUUID(),postmasterStart:current.postmaster_start,timeline:current.timeline,createdAt:now()};await save();
      }
      if(policy.pinnedBackupIds.includes(state.pending.backupId))fail('backup_schedule_pending_pinned');
      // The saved UUID makes interruption/lost acknowledgement resumable. Never
      // adopt an arbitrary pre-existing backup or change the pinned baseline.
      proof=await createPostgresBaseBackup(context,{backupId:state.pending.backupId,signal});
      const metadata=postgresBackupRestoreSource(proof).metadata;
      if(metadata.postmasterStart!==state.pending.postmasterStart||metadata.timeline!==state.pending.timeline)fail('backup_schedule_proof_generation');
      await checkpointPostgresBackup(proof,{signal});
      state.active={...state.pending,checkpointAt:now()};state.pending=null;await save();created=true;checkpointCreated=true;
    }else{
      const cached=proofCache.get(context);proof=cached?.backupId===active.backupId?cached:await verifyPostgresBackup(context,active.backupId,{signal});
      const metadata=postgresBackupRestoreSource(proof).metadata;
      if(metadata.postmasterStart!==active.postmasterStart||metadata.timeline!==active.timeline)fail('backup_schedule_proof_generation');
      if(now()-active.checkpointAt>=policy.checkpointIntervalMs){await checkpointPostgresBackup(proof,{signal});active.checkpointAt=now();await save();checkpointCreated=true;}
    }
    proofCache.set(context,proof);
    return output(created?'base_and_checkpoint_created':checkpointCreated?'checkpoint_created':'checkpoint_not_due',{backupId:state.active.backupId,baseCycleStartedAt:state.active.createdAt,checkpointAt:state.active.checkpointAt,retentionApplied:false});
  }finally{await lock.release();}
}

export async function watchPostgresBackupSchedule(context,configuration,{signal,onCycle=()=>{}}={}){
  const policy=postgresBackupSchedulePolicy(configuration);if(!policy.enabled)return output('schedule_disabled');
  if(typeof onCycle!=='function')fail('backup_schedule_callback_invalid');
  for(;;){
    if(signal?.aborted)return output('schedule_stopped');
    try{await onCycle(await runPostgresBackupScheduleCycle(context,policy,{signal}));await delay(policy.pollIntervalMs,undefined,{signal});}
    catch(error){if(signal?.aborted&&(error===signal.reason||error?.name==='AbortError'))return output('schedule_stopped');throw error;}
  }
}
