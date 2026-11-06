import {withPostgresBackupOperation,withPostgresBackupScheduleRegistration} from './postgres-backup-lock.mjs';
import {randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {realpath} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {acquireInstance} from './instance.mjs';
import {postgresBackupContext,verifyBackupCluster,createPeriodicBasePreparationContext,closePostgresBackupContext} from './postgres-backup-context.mjs';
import {createPostgresBaseBackup,createPeriodicSourceBase,verifyPeriodicSourceBase,loadAuthenticatedPeriodicBase,revalidatePeriodicSourceBase,verifyPostgresBackup,checkpointPostgresBackup,postgresBackupRestoreSource,inspectPostgresBackupCheckpointProtection,checkpointPeriodicPostgresBackup,isPostgresBackupRecoveryCompatible} from './postgres-backup.mjs';
import {readManifest,writeManifest,transferError} from './migration-artifacts.mjs';
import {requirePeriodicBaseSource,publishPeriodicBaseVerification} from './postgres-periodic-base-verification.mjs';
import {periodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';
import {capturePeriodicApplicationBackup,readPeriodicApplicationBackupState} from './postgres-periodic-application-backup.mjs';
import {verifyInstalledPeriodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';

const fail=code=>{throw transferError(code);},proofCache=new WeakMap();
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const object=value=>value&&typeof value==='object'&&!Array.isArray(value);
const exact=(value,keys)=>object(value)&&Object.keys(value).every(key=>keys.includes(key));
const time=value=>Number.isSafeInteger(value)&&value>=0;
const scope='harbor-periodic-database-backup-v1';
export function postgresBackupSchedulePolicy(value){
  if(!exact(value,['version','enabled','fullIntervalMs','checkpointIntervalMs','pollIntervalMs','pinnedBackupIds','reuseAuthenticatedBase'])||value.version!==1||typeof value.enabled!=='boolean')fail('backup_schedule_policy_invalid');
  const policy={reuseAuthenticatedBase:false,fullIntervalMs:86400000,checkpointIntervalMs:300000,pollIntervalMs:60000,pinnedBackupIds:[],...value};
  if(typeof policy.reuseAuthenticatedBase!=='boolean'||![policy.fullIntervalMs,policy.checkpointIntervalMs,policy.pollIntervalMs].every(n=>Number.isSafeInteger(n)&&n>=1000&&n<=30*86400000)||policy.pollIntervalMs>policy.checkpointIntervalMs||policy.checkpointIntervalMs>policy.fullIntervalMs||!Array.isArray(policy.pinnedBackupIds)||policy.pinnedBackupIds.length>1024||policy.pinnedBackupIds.some(id=>!uuid(id))||new Set(policy.pinnedBackupIds).size!==policy.pinnedBackupIds.length)fail('backup_schedule_policy_invalid');
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

// The control lease protects only the small active/pending state. Physical
// base creation and a complete restore run under independent lifetime leases.
async function withScheduleState(context,callback,{signal,clock=Date.now}={}){
  const record=postgresBackupContext(context),deadline=Date.now()+Math.min(record.profile.processTimeoutMs,30000);let lock;
  while(!lock){signal?.throwIfAborted();try{lock=await acquireInstance(join(record.artifactDirectory,'.periodic-backup'));}catch(error){if(error.code!=='EADDRINUSE')throw error;if(Date.now()>=deadline)fail('backup_schedule_busy');await delay(10,undefined,{signal});}}
  try{return await withPostgresBackupOperation(record,async()=>{
    const path=join(record.artifactDirectory,'periodic-state.enc');let state;
    try{state=validateState(await readManifest(path,record.key,{signal}),record);}catch(error){if(error.code!=='ENOENT')throw error;state={version:1,scope,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,active:null,pending:null,updatedAt:clock()};}
    if(!time(clock())||clock()<state.updatedAt)fail('backup_schedule_clock_reversed');
    return callback(state,async()=>{signal?.throwIfAborted();state.updatedAt=clock();validateState(state,record);await writeManifest(path,state,record.key,{replace:true});});
  },{signal});}finally{await lock.release();}
}

async function baseMaintenanceLifecycle(record,signal,callback){
  const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));record.periodicApplicationCancellation=cancellation;
  const pending=withPostgresBackupOperation(record,()=>callback(combined),{signal:combined});record.periodicApplicationPending=pending;
  try{return await pending;}finally{record.periodicApplicationPending=null;record.periodicApplicationCancellation=null;}
}

export async function maintainPostgresBackupBase(context,configuration,{signal,clock=Date.now,applicationConfiguration={version:1,enabled:false}}={}){
  const policy=postgresBackupSchedulePolicy(configuration),application=periodicApplicationPolicy(applicationConfiguration);
  if(!policy.enabled||!application.enabled)return output('base_maintenance_disabled');
  const record=postgresBackupContext(context);if(record.closed||record.busy||record.periodicApplicationPending||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('backup_schedule_context_invalid');
  return baseMaintenanceLifecycle(record,signal,async signal=>{
  if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(application,{signal});
  let lease;try{lease=await acquireInstance(join(record.artifactDirectory,'.periodic-base-maintenance'));}catch(error){if(error.code==='EADDRINUSE')fail('backup_base_maintenance_busy');throw error;}
  try{
    const current=await verifyBackupCluster(context),same=row=>row?.postmasterStart===current.postmaster_start&&row.timeline===current.timeline;
    const pending=await withScheduleState(context,async(state,save)=>{
      const active=state.active,sealed=active?(await inspectPostgresBackupCheckpointProtection(context,active.backupId,{signal})).sealed:false;
      if(!state.pending&&active&&active.timeline===current.timeline&&!sealed&&!policy.pinnedBackupIds.includes(active.backupId)&&clock()-active.createdAt<policy.fullIntervalMs){
        const metadata=await readManifest(join(record.artifactDirectory,active.backupId,'backup.enc'),record.key,{signal});if(metadata.backupId!==active.backupId||metadata.postmasterStart!==active.postmasterStart||metadata.timeline!==active.timeline)fail('backup_schedule_proof_generation');
        if(await isPostgresBackupRecoveryCompatible(context,metadata,{signal,current}))return null;
      }
      if(!same(state.pending)){state.pending={backupId:randomUUID(),postmasterStart:current.postmaster_start,timeline:current.timeline,createdAt:clock()};await save();}
      if(policy.pinnedBackupIds.includes(state.pending.backupId))fail('backup_schedule_pending_pinned');return structuredClone(state.pending);
    },{signal,clock});
    if(!pending)return output('base_maintenance_not_due');
    // No schedule/control lease is held during these potentially long steps.
    // The old active base remains usable by an independent producer context.
    let authenticationLease;if(policy.reuseAuthenticatedBase){requirePeriodicBaseSource(context);try{authenticationLease=await acquireInstance(join(record.artifactDirectory,'.base-auth-'+pending.backupId));}catch(error){if(error.code==='EADDRINUSE')return output('base_verification_pending',{backupId:pending.backupId});throw error;}}
    try{
    const proof=await (policy.reuseAuthenticatedBase?createPeriodicSourceBase:createPostgresBaseBackup)(context,{backupId:pending.backupId,signal}),metadata=postgresBackupRestoreSource(proof).metadata;
    if(metadata.postmasterStart!==pending.postmasterStart||metadata.timeline!==pending.timeline)fail('backup_schedule_proof_generation');
    await checkpointPeriodicPostgresBackup(proof,{signal});
    if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(application,{signal});
    if(policy.reuseAuthenticatedBase)await publishPeriodicBaseVerification(proof,{signal});
    await withScheduleState(context,async(state,save)=>{
      if(!state.pending||JSON.stringify(state.pending)!==JSON.stringify(pending))fail('backup_schedule_pending_changed');
      if(!policy.reuseAuthenticatedBase){state.active={...pending,checkpointAt:clock()};state.pending=null;await save();}
    },{signal,clock});if(!policy.reuseAuthenticatedBase)proofCache.set(context,proof);
    return output(policy.reuseAuthenticatedBase?'base_candidate_prepared':'base_maintenance_completed',{backupId:proof.backupId,baseCycleStartedAt:pending.createdAt});
    }finally{await authenticationLease?.release();}
  }finally{await lease.release();}
  });
}

// Full authentication runs on an independently owned context. The producer
// never awaits its large file reads and its old, still-authenticated base stays
// active until a completed candidate receipt passes the small adoption check.
function prepareBaseInBackground(context,id){
 const record=postgresBackupContext(context);if(record.periodicBasePreparationPending)return;
 const child=createPeriodicBasePreparationContext(context),cancellation=new AbortController();record.periodicBasePreparationCancellation=cancellation;
 const operation=(async()=>{let lease;try{
  try{lease=await acquireInstance(join(record.artifactDirectory,'.base-auth-'+id));}catch(error){if(error.code==='EADDRINUSE')return;throw error;}
  await withPostgresBackupOperation(postgresBackupContext(child),async()=>{const proof=await verifyPeriodicSourceBase(child,id,{signal:cancellation.signal});try{await readManifest(join(record.artifactDirectory,id,'checkpoint.enc'),record.key,{signal:cancellation.signal});}catch(error){if(error.code!=='ENOENT')throw error;await checkpointPeriodicPostgresBackup(proof,{signal:cancellation.signal});}await publishPeriodicBaseVerification(proof,{signal:cancellation.signal});preparationErrors.delete(context);},{signal:cancellation.signal});
 }catch(error){if(!cancellation.signal.aborted&&error.code!=='ENOENT')preparationErrors.set(context,error.code??'periodic_base_preparation_failed');}
 finally{await lease?.release();await closePostgresBackupContext(child);}})();
 record.periodicBasePreparationPending=operation;operation.finally(()=>{if(record.periodicBasePreparationPending===operation){record.periodicBasePreparationPending=null;record.periodicBasePreparationCancellation=null;}}).catch(()=>{});
}
const preparationErrors=new WeakMap();
async function readyBase(context,row,{signal,allowCached=false}={}){
 try{const proof=await loadAuthenticatedPeriodicBase(context,row.backupId,{signal});const metadata=postgresBackupRestoreSource(proof).metadata;if(metadata.postmasterStart!==row.postmasterStart||metadata.timeline!==row.timeline)fail('backup_schedule_proof_generation');return proof;}
 catch(error){signal?.throwIfAborted();const cached=proofCache.get(context);prepareBaseInBackground(context,row.backupId);
  if(allowCached&&cached?.backupId===row.backupId){try{return await revalidatePeriodicSourceBase(cached,{signal});}catch{signal?.throwIfAborted();}}
  if(cached?.backupId===row.backupId)proofCache.delete(context);return null;}
}

async function runApplicationBackupScheduleCycle(context,policy,applicationPolicy,{signal,clock}){
  const record=postgresBackupContext(context);if(record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('backup_schedule_context_invalid');
  if(policy.reuseAuthenticatedBase)requirePeriodicBaseSource(context);
  const current=await verifyBackupCluster(context),stateSnapshot=await withScheduleState(context,state=>({active:structuredClone(state.active),pending:structuredClone(state.pending)}),{signal,clock});let active=stateSnapshot.active,prepared,candidateUnavailableReason;
  const candidate=stateSnapshot.pending,result=(state,extra)=>output(state,{...extra,...(candidateUnavailableReason?{candidate:{backupId:candidate.backupId,state:'candidate_unavailable',reason:candidateUnavailableReason}}:{})});
  if(policy.reuseAuthenticatedBase&&candidate&&!policy.pinnedBackupIds.includes(candidate.backupId)&&candidate.postmasterStart===current.postmaster_start&&candidate.timeline===current.timeline){
   const candidateProof=await readyBase(context,candidate,{signal});let checkpointReady=false;
   if(candidateProof){
    try{const checkpoint=await readManifest(join(record.artifactDirectory,candidate.backupId,'checkpoint.enc'),record.key,{signal});if(checkpoint.version!==2||checkpoint.backupId!==candidate.backupId||checkpoint.configurationDigest!==record.configurationDigest||checkpoint.systemId!==record.expected.system_id||checkpoint.keyVersion!==record.keyVersion||checkpoint.timeline!==candidate.timeline)fail('backup_schedule_candidate_checkpoint');checkpointReady=true;}
    catch(error){signal?.throwIfAborted();candidateUnavailableReason=error.code==='ENOENT'?'checkpoint_missing':'checkpoint_invalid';}
    // A candidate-local checkpoint failure cannot stop a still-trusted active.
    // State/CAS conflicts remain outside this catch and are never swallowed.
    if(checkpointReady){await withScheduleState(context,async(state,save)=>{if(JSON.stringify(state.pending)!==JSON.stringify(candidate)||JSON.stringify(state.active)!==JSON.stringify(active))fail('backup_schedule_pending_changed');state.active={...candidate,checkpointAt:clock()};state.pending=null;await save();active=structuredClone(state.active);},{signal,clock});prepared=candidateProof;}
   }else candidateUnavailableReason='base_verification_pending';
  }
  if(!active||active.timeline!==current.timeline)return result('bootstrap_not_ready',{scope:'database-and-application',baseMaintenanceRequired:true});
  if((await inspectPostgresBackupCheckpointProtection(context,active.backupId,{signal})).sealed||policy.pinnedBackupIds.includes(active.backupId))return result('base_maintenance_required',{scope:'database-and-application',baseMaintenanceRequired:true});
  const cached=proofCache.get(context),proof=policy.reuseAuthenticatedBase?(prepared??await readyBase(context,active,{signal,allowCached:true})):cached?.backupId===active.backupId?cached:await verifyPostgresBackup(context,active.backupId,{signal});
  if(!proof)return result('base_verification_pending',{scope:'database-and-application',backupId:active.backupId,baseMaintenanceRequired:true,reason:preparationErrors.get(context)??'authenticated_base_not_ready'});proofCache.set(context,proof);
  const metadata=postgresBackupRestoreSource(proof).metadata;if(metadata.postmasterStart!==active.postmasterStart||metadata.timeline!==active.timeline)fail('backup_schedule_proof_generation');
  if(!await isPostgresBackupRecoveryCompatible(context,metadata,{signal,current}))return result('base_maintenance_required',{scope:'database-and-application',baseMaintenanceRequired:true,reason:metadata.postmasterStart!==current.postmaster_start?'source_generation_changed':'recovery_parameters_incompatible'});
  const state=await readPeriodicApplicationBackupState(context,{signal}),latest=state.latestBackup;
  if(!state.pending&&latest?.backupId===active.backupId&&clock()-latest.checkpointAt<policy.checkpointIntervalMs)return result('checkpoint_not_due',{scope:'database-and-application',backupId:active.backupId,checkpointAt:latest.checkpointAt,application:{state:'application_backup_not_due',backupComplete:true,restoreVerified:false,applicationMaterialsVerified:false,deploymentAllowed:false}});
  const application=await capturePeriodicApplicationBackup(context,proof,applicationPolicy,{signal});
  let databaseCheckpointAt;
  if(!application.backupComplete&&clock()-active.checkpointAt>=policy.checkpointIntervalMs){await checkpointPeriodicPostgresBackup(proof,{signal});databaseCheckpointAt=Date.now();}
  if(databaseCheckpointAt)await withScheduleState(context,async(state,save)=>{if(state.active?.backupId===active.backupId&&databaseCheckpointAt>state.active.checkpointAt){state.active.checkpointAt=databaseCheckpointAt;await save();}},{signal,clock});
  if(application.backupComplete)await withScheduleState(context,async(state,save)=>{if(state.active?.backupId===active.backupId&&application.checkpointAt>state.active.checkpointAt){state.active.checkpointAt=application.checkpointAt;await save();}},{signal,clock});
  return result(application.backupComplete?'checkpoint_created':databaseCheckpointAt?'database_only_checkpoint_created':'application_capture_failed',{scope:'database-and-application',backupId:active.backupId,checkpointAt:application.checkpointAt??databaseCheckpointAt??active.checkpointAt,baseMaintenanceRequired:clock()-active.createdAt>=policy.fullIntervalMs,application});
}

// One cycle is useful to an operator; the service retains the verified proof in
// memory between cycles so each WAL checkpoint does not reread an entire base.
// With explicit source-only receipt reuse a restart checks the authenticated
// immutable identity set; all other callers retain complete authentication.
export async function runPostgresBackupScheduleCycle(context,configuration,{signal,clock=Date.now,applicationConfiguration={version:1,enabled:false}}={}){
  const policy=postgresBackupSchedulePolicy(configuration);if(!policy.enabled)return output('schedule_disabled');
  const applicationPolicy=periodicApplicationPolicy(applicationConfiguration);
  if(policy.reuseAuthenticatedBase&&!applicationPolicy.enabled)fail('periodic_base_application_required');
  if(applicationPolicy.enabled)return withPostgresBackupOperation(postgresBackupContext(context),()=>runApplicationBackupScheduleCycle(context,policy,applicationPolicy,{signal,clock}),{signal});
  signal?.throwIfAborted();const record=postgresBackupContext(context);
  if(record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('backup_schedule_context_invalid');
  let lock;try{lock=await acquireInstance(join(await realpath(record.artifactDirectory),'.periodic-backup'));}catch(error){if(error.code==='EADDRINUSE')fail('backup_schedule_busy');throw error;}
  const file=join(record.artifactDirectory,'periodic-state.enc');
  try{
    const current=await verifyBackupCluster(context),now=()=>{const value=clock();if(!time(value))fail('backup_schedule_clock_invalid');return value;};
    let state;try{state=validateState(await readManifest(file,record.key,{signal}),record);}catch(error){if(error.code!=='ENOENT')throw error;state={version:1,scope,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,active:null,pending:null,updatedAt:now()};}
    if(now()<state.updatedAt)fail('backup_schedule_clock_reversed');
    const same=row=>row&&row.postmasterStart===current.postmaster_start&&row.timeline===current.timeline;
    const save=async()=>{signal?.throwIfAborted();const at=now();if(at<state.updatedAt)fail('backup_schedule_clock_reversed');state.updatedAt=at;validateState(state,record);await withPostgresBackupOperation(record,()=>writeManifest(file,state,record.key,{replace:true}),{signal});};
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
      if(!applicationPolicy.enabled&&now()-active.checkpointAt>=policy.checkpointIntervalMs){await checkpointPostgresBackup(proof,{signal});active.checkpointAt=now();await save();checkpointCreated=true;}
    }
    proofCache.set(context,proof);
    return output(created?'base_and_checkpoint_created':checkpointCreated?'checkpoint_created':'checkpoint_not_due',{backupId:state.active.backupId,baseCycleStartedAt:state.active.createdAt,checkpointAt:state.active.checkpointAt,retentionApplied:false});
  }finally{await lock.release();}
}

export async function watchPostgresBackupSchedule(context,configuration,{signal,onCycle=()=>{},applicationConfiguration={version:1,enabled:false}}={}){
  const policy=postgresBackupSchedulePolicy(configuration);if(!policy.enabled)return output('schedule_disabled');
  if(typeof onCycle!=='function')fail('backup_schedule_callback_invalid');
  return withPostgresBackupScheduleRegistration(postgresBackupContext(context),async()=>{
  for(;;){
    if(signal?.aborted)return output('schedule_stopped');
    try{await onCycle(await runPostgresBackupScheduleCycle(context,policy,{signal,applicationConfiguration}));await delay(policy.pollIntervalMs,undefined,{signal});}
    catch(error){if(signal?.aborted&&(error===signal.reason||error?.name==='AbortError'))return output('schedule_stopped');throw error;}
  }
  },{signal});
}
