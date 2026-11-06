import {periodicApplicationKey,periodicApplicationKeyBinding,verifyPeriodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {mkdir,lstat,realpath,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import {postgresBackupContext,verifyBackupCluster} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,authenticatePostgresBackupFileSet} from './postgres-backup.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {periodicApplicationPolicy,installedPeriodicApplicationPolicy,verifyInstalledPeriodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';
import {captureFileSet,verifyFileSetSources,readManifest,writeManifest,privateDirectory,syncDirectory,canonical,transferError} from './migration-artifacts.mjs';

const captures=new WeakMap(),scope='harbor-periodic-application-capture-v3';
const fail=code=>{throw transferError(code);};
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const overlaps=(a,b)=>a===b||a.startsWith(b+'/')||b.startsWith(a+'/');
const validFence=value=>value&&Object.keys(value).sort().join(',')==='postmasterStart,timeline,walInsertLsn'&&typeof value.postmasterStart==='string'&&/^\d+(?:\.\d+)?$/.test(value.postmasterStart)&&Number.isSafeInteger(value.timeline)&&value.timeline>0&&value.timeline<=0xffffffff&&typeof value.walInsertLsn==='string'&&/^[A-F0-9]+\/[A-F0-9]+$/.test(value.walInsertLsn);
function sourceFor(context,proof,configuration){
  const policy=periodicApplicationPolicy(configuration),record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);periodicApplicationKey(record);
  if(!policy.enabled||record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan||source.context!==context)fail('periodic_application_context');
  if(record.scope==='existing-cluster')installedPeriodicApplicationPolicy(policy);
  if(!record.expected.databases.some(row=>row.name===policy.database.name&&row.oid===policy.database.oid))fail('periodic_application_database_binding');
  if(record.applicationEncryption?.keyFile&&policy.materialRoots.some(root=>record.applicationEncryption.keyFile===root.directory||record.applicationEncryption.keyFile.startsWith(root.directory+'/')))fail('periodic_application_key_coverage');
  for(const root of policy.materialRoots)for(const protectedPath of [record.artifactDirectory,record.archiveDirectory,record.restoreDirectory,record.expected.data_directory,record.archiveKeyFile])if(overlaps(root.directory,protectedPath))fail('periodic_application_source_overlap');
  const maxBytes=Math.min(policy.maxBytes,record.profile.maxBackupBytes-source.metadata.files.bytes);
  if(!Number.isSafeInteger(maxBytes)||maxBytes<1)fail('periodic_application_capacity');
  return {policy,record,source,maxBytes,base:join(record.artifactDirectory,proof.backupId)};
}
async function operate(record,operation,{signal}={}){
  if(record.busy)fail('backup_operation_busy');record.busy=true;
  const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal,AbortSignal.timeout(record.profile.processTimeoutMs)].filter(Boolean));record.cancellation=cancellation;
  const pending=withPostgresBackupOperation(record,()=>operation(combined),{signal:combined});record.pending=pending;
  try{return await pending;}finally{record.busy=false;record.pending=null;record.cancellation=null;}
}
async function sourceRoots(policy){
  for(const root of policy.materialRoots)if(await realpath(root.directory)!==resolve(root.directory))fail('periodic_application_source_alias');
  return policy.materialRoots.map(root=>({id:root.id,source:root.directory}));
}
function mint(context,proof,policy,path,manifest,{sourceVerified=true}={}){
  const token=Object.freeze({captureId:manifest.captureId,backupId:proof.backupId,scope});captures.set(token,{context,proof,policy,path,manifest,sourceVerified});return token;
}
async function verifyCapture(context,proof,configuration,captureId,{signal}={}){
  if(!uuid(captureId))fail('periodic_application_capture_id');
  const {policy,record,maxBytes,base}=sourceFor(context,proof,configuration),path=join(base,'application-staging',captureId);
  if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(policy,{signal});
  await privateDirectory(join(base,'application-staging'));await privateDirectory(path);
  const manifest=await readManifest(join(path,'capture.enc'),periodicApplicationKey(record),{signal});
  verifyPeriodicApplicationKeyBinding(manifest,record);
  if(manifest.version!==3||manifest.scope!==scope||manifest.captureId!==captureId||manifest.backupId!==proof.backupId||manifest.configurationDigest!==record.configurationDigest||manifest.systemId!==record.expected.system_id||manifest.keyVersion!==record.keyVersion||manifest.applicationPolicyDigest!==hash(policy)||manifest.version===3&&hash(periodicApplicationPolicy(manifest.applicationPolicy))!==manifest.applicationPolicyDigest||!validFence(manifest.walFence)||manifest.walFence.timeline!==postgresBackupRestoreSource(proof).metadata.timeline||!Number.isSafeInteger(manifest.capturedAt)||manifest.capturedAt<0||manifest.files?.files>policy.maxFiles||manifest.files?.bytes>maxBytes)fail('periodic_application_capture_binding');
  await authenticatePostgresBackupFileSet({directory:path,key:periodicApplicationKey(record),files:manifest.files,signal});
  const files=await readManifest(join(path,'files.enc'),periodicApplicationKey(record),{signal});
  if(canonical(files.roots.map(({id,source})=>({id,source})))!==canonical(await sourceRoots(policy)))fail('periodic_application_source_binding');
  await verifyFileSetSources(files,{signal,maxFiles:policy.maxFiles,maxFileBytes:maxBytes,maxTotalBytes:maxBytes});
  signal?.throwIfAborted();return mint(context,proof,policy,path,manifest);
}

// This is only a pre-checkpoint capture. Neither a stable double scan nor its
// encrypted manifest proves an atomic application/database snapshot. A complete
// recovery group still needs a later checkpoint, source recheck and an actual
// isolated restore of credentials, recordings and device materials.
export async function capturePeriodicApplicationMaterials(context,proof,configuration,{captureId=randomUUID(),signal}={}){
  if(!periodicApplicationPolicy(configuration).enabled)return {state:'periodic_application_disabled',applicationMaterialsVerified:false,deploymentAllowed:false};
  if(!uuid(captureId))fail('periodic_application_capture_id');
  const {policy,record,maxBytes,base}=sourceFor(context,proof,configuration);
  return operate(record,async signal=>{
    if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(policy,{signal});
    const generation=await verifyBackupCluster(context);signal.throwIfAborted();const roots=await sourceRoots(policy),parent=join(base,'application-staging'),path=join(parent,captureId);
    await syncDirectory(record.artifactDirectory);await mkdir(parent,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await privateDirectory(parent);await syncDirectory(base);
    await mkdir(path,{mode:0o700});const identity=await lstat(path);let complete=false;
    try{
      await syncDirectory(parent);
      const files=await captureFileSet({roots,directory:path,key:periodicApplicationKey(record),maxFiles:policy.maxFiles,maxFileBytes:maxBytes,maxTotalBytes:maxBytes,signal});
      await verifyFileSetSources(await readManifest(join(path,'files.enc'),periodicApplicationKey(record),{signal}),{signal,maxFiles:policy.maxFiles,maxFileBytes:maxBytes,maxTotalBytes:maxBytes});
      const store=new PostgresStore({...record.adminOptions,poolSize:1});let walFence;
      try{
        const fence=await store.get("SELECT pg_current_wal_insert_lsn()::text lsn,extract(epoch FROM pg_postmaster_start_time())::text postmaster_start,(SELECT timeline_id::integer FROM pg_control_checkpoint()) timeline");
        walFence={postmasterStart:fence.postmaster_start,timeline:fence.timeline,walInsertLsn:fence.lsn};
        const metadata=postgresBackupRestoreSource(proof).metadata;
        if(!validFence(walFence)||walFence.postmasterStart!==generation.postmaster_start||walFence.timeline!==generation.timeline||walFence.timeline!==metadata.timeline)fail('periodic_application_capture_source_generation');
      }finally{await store.close();}
      signal.throwIfAborted();const manifest={version:3,...periodicApplicationKeyBinding(record),scope,captureId,backupId:proof.backupId,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,applicationPolicy:structuredClone(policy),applicationPolicyDigest:hash(policy),files,walFence,capturedAt:Date.now()};
      await writeManifest(join(path,'capture.enc'),manifest,periodicApplicationKey(record));
      const token=await verifyCapture(context,proof,policy,captureId,{signal});complete=true;return token;
    }finally{
      if(!complete){const actual=await lstat(path);if(actual.isDirectory()&&!actual.isSymbolicLink()&&actual.dev===identity.dev&&actual.ino===identity.ino&&actual.uid===process.getuid()){await rm(path,{recursive:true});await syncDirectory(parent);}else fail('periodic_application_cleanup_identity');}
    }
  },{signal});
}

export async function verifyPeriodicApplicationCapture(context,proof,configuration,captureId,{signal}={}){
  const {record}=sourceFor(context,proof,configuration);
  return operate(record,signal=>verifyCapture(context,proof,configuration,captureId,{signal}),{signal});
}

// Archive authentication does not grant a fresh source/checkpoint pairing.
// An actual backup-complete point is required separately before restoration.
export async function authenticatePeriodicApplicationCapture(context,proof,captureId,{signal}={}){
  if(!uuid(captureId))fail('periodic_application_capture_id');const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);
  if(record.closed||source.context!==context)fail('periodic_application_context');
  return operate(record,async signal=>{
    const path=join(record.artifactDirectory,proof.backupId,'application-staging',captureId),manifest=await readManifest(join(path,'capture.enc'),periodicApplicationKey(record),{signal}),policy=periodicApplicationPolicy(manifest.applicationPolicy);
    verifyPeriodicApplicationKeyBinding(manifest,record);
    if(manifest.version!==3||manifest.scope!==scope||manifest.captureId!==captureId||manifest.backupId!==proof.backupId||manifest.configurationDigest!==record.configurationDigest||manifest.systemId!==record.expected.system_id||manifest.keyVersion!==record.keyVersion||!policy.enabled||hash(policy)!==manifest.applicationPolicyDigest||!validFence(manifest.walFence)||manifest.walFence.timeline!==source.metadata.timeline||manifest.files?.files>policy.maxFiles||manifest.files?.bytes>Math.min(policy.maxBytes,record.profile.maxBackupBytes-source.metadata.files.bytes))fail('periodic_application_capture_binding');
    await authenticatePostgresBackupFileSet({directory:path,key:periodicApplicationKey(record),files:manifest.files,signal});
    const files=await readManifest(join(path,'files.enc'),periodicApplicationKey(record),{signal});
    if(canonical(files.roots.map(({id,source})=>({id,source})))!==canonical(policy.materialRoots.map(root=>({id:root.id,source:root.directory}))))fail('periodic_application_source_binding');
    return mint(context,proof,policy,path,manifest,{sourceVerified:false});
  },{signal});
}

export function periodicApplicationCaptureSource(token,{context,proof}={}){
  const value=captures.get(token);
  if(!value||context!==undefined&&value.context!==context||proof!==undefined&&(postgresBackupRestoreSource(proof).context!==value.context||proof.backupId!==value.proof.backupId))fail('periodic_application_capture_required');
  if(postgresBackupContext(value.context).closed)fail('backup_context_closed');
  return {context:value.context,backupId:value.proof.backupId,captureId:token.captureId,directory:value.path,configuration:structuredClone(value.policy),manifest:structuredClone(value.manifest),sourceVerified:value.sourceVerified,applicationMaterialsVerified:false,deploymentAllowed:false};
}
