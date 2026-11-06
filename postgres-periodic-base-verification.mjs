import {lstat,readdir,realpath,readFile,mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupFullSource} from './postgres-backup.mjs';
import {backupCoordinationCodeDigest} from './postgres-backup-lock.mjs';
import {canonical,privateDirectory,readManifest,writeManifest,digestFile,transferError} from './migration-artifacts.mjs';

const scope='harbor-periodic-source-base-verification-v1',capabilities=new WeakMap(),fail=code=>{throw transferError(code);};
const same=(a,b)=>canonical(a)===canonical(b),hash=value=>createHash('sha256').update(value).digest('hex');
const immutable=name=>/^(?:backup|files|file-catalog-\d+|file-\d+)\.enc$/.test(name);
const loadedCode=backupCoordinationCodeDigest();
let executionPromise;const executionIdentity=()=>executionPromise??=(async()=>({nodeVersion:process.version,nodeDigest:await digestFile(await realpath('/proc/self/exe')),machine:hash((await readFile('/etc/machine-id','utf8')).trim()),codeDigest:await loadedCode}))();
export function requirePeriodicBaseSource(context){
 const record=postgresBackupContext(context);
 if(record.closed||record.receiverBinding||record.pendingBinding||record.resumeBinding||record.applicationPlan||!record.applicationKey||!record.applicationKeyVersion||!record.applicationKeyDigest||record.scope==='existing-cluster'&&process.getuid()!==0)fail('periodic_base_source_required');
 return record;
}
async function identity(path,{directory=false}={}){
 const value=await lstat(path,{bigint:true});
 if(value.isSymbolicLink()||(directory?!value.isDirectory():!value.isFile()))fail('periodic_base_identity_invalid');
 if(value.uid!==BigInt(process.getuid())||(value.mode&0o777n)!==(directory?0o700n:0o600n)||!directory&&value.nlink!==1n)fail('periodic_base_identity_untrusted');
 return Object.fromEntries((directory?['dev','ino','uid','mode']:['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs']).map(name=>[name,String(value[name])]));
}
export async function capturePeriodicBaseIdentity(context,id,{signal,allowPlain=false}={}){
 const record=requirePeriodicBaseSource(context);if(typeof id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id))fail('backup_id_invalid');
 const directory=join(record.artifactDirectory,id);await privateDirectory(directory);const root=await identity(directory,{directory:true});if(await realpath(directory)!==directory)fail('periodic_base_identity_invalid');
 const files=[];for(const name of (await readdir(directory)).sort()){
  signal?.throwIfAborted();if(name==='plain'&&!allowPlain)fail('periodic_base_plain_present');
  if(immutable(name))files.push({name,identity:await identity(join(directory,name)),...(!/^file-\d+\.enc$/.test(name)?{digest:await digestFile(join(directory,name),{signal})}:{})});
  else if(!/^(?:state\.enc|checkpoint\.enc|restore\.enc|operator-final\.enc|application-final\.enc|application|handoff|points|application-cycles|application-staging|base-verification|wal-catalog-[a-f0-9]{32}|\.manifest-[a-f0-9]{24}|plain)$/.test(name))fail('periodic_base_unknown_entry');
 }
 if(files.length<2||files.length>1000002||!files.some(row=>row.name==='backup.enc')||!files.some(row=>row.name==='files.enc'))fail('periodic_base_file_set_invalid');
 const execution=await executionIdentity();if(execution.codeDigest!==await backupCoordinationCodeDigest())fail('periodic_base_execution_changed');
 signal?.throwIfAborted();if(!same(root,await identity(directory,{directory:true})))fail('periodic_base_source_changed');
 return {root,files,execution};
}
const binding=(record,id)=>({backupId:id,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,contextScope:record.scope,applicationKeyVersion:record.applicationKeyVersion,applicationKeyDigest:record.applicationKeyDigest,artifactDirectory:record.artifactDirectory});
// This accepts only the private capability produced by a completed full AEAD
// authentication, never a JSON claim or a capability loaded from a receipt.
export async function publishPeriodicBaseVerification(proof,{signal}={}){
 const source=postgresBackupFullSource(proof),record=requirePeriodicBaseSource(source.context),id=proof.backupId,current=await capturePeriodicBaseIdentity(source.context,id,{signal});
 if(!same(source.identity,current))fail('periodic_base_source_changed');
 const directory=join(record.artifactDirectory,id,'base-verification');await mkdir(directory,{mode:0o700,recursive:true});await privateDirectory(directory);
 const generation=randomBytes(16).toString('hex'),pages=[];
 for(let offset=0;offset<current.files.length;offset+=512){
  signal?.throwIfAborted();const name=generation+'-'+pages.length+'.enc';await writeManifest(join(directory,name),{version:1,scope,generation,ordinal:pages.length,files:current.files.slice(offset,offset+512)},record.applicationKey);pages.push({name,digest:await digestFile(join(directory,name),{signal})});
 }
 const value={version:1,scope,...binding(record,id),generation,verifiedAt:source.verifiedAt,metadata:source.metadata,root:current.root,execution:current.execution,pages,fileCount:current.files.length};
 if(!same(current,await capturePeriodicBaseIdentity(source.context,id,{signal})))fail('periodic_base_source_changed');
 await writeManifest(join(directory,'current.enc'),value,record.applicationKey,{replace:true});return {backupId:id,verifiedAt:source.verifiedAt};
}
async function receiptFiles(record,directory,value,{signal}={}){
 if(value?.version!==1||value.scope!==scope||!/^([a-f0-9]{32})$/.test(value.generation??'')||!Number.isSafeInteger(value.verifiedAt)||value.verifiedAt<0||value.verifiedAt>Date.now()||!Array.isArray(value.pages)||value.pages.length<1||value.pages.length>1954||!Number.isSafeInteger(value.fileCount)||value.fileCount<2||value.fileCount>1000002)fail('periodic_base_receipt_invalid');
 const files=[];for(let i=0;i<value.pages.length;i++){
  const row=value.pages[i];if(row.name!==value.generation+'-'+i+'.enc'||!same(row.digest,await digestFile(join(directory,row.name),{signal})))fail('periodic_base_receipt_changed');
  await identity(join(directory,row.name));const page=await readManifest(join(directory,row.name),record.applicationKey,{signal});if(page.version!==1||page.scope!==scope||page.generation!==value.generation||page.ordinal!==i||!Array.isArray(page.files)||page.files.length<1||page.files.length>512)fail('periodic_base_receipt_invalid');files.push(...page.files);
 }
 if(files.length!==value.fileCount||files.some((row,i)=>!immutable(row.name)||i&&files[i-1].name>=row.name))fail('periodic_base_receipt_invalid');return files;
}
export async function loadPeriodicBaseVerification(context,id,{signal}={}){
 const record=requirePeriodicBaseSource(context),directory=join(record.artifactDirectory,id,'base-verification');await privateDirectory(directory);await identity(join(directory,'current.enc'));
 const value=await readManifest(join(directory,'current.enc'),record.applicationKey,{signal});
 if(!Object.entries(binding(record,id)).every(([key,expected])=>same(value[key],expected)))fail('periodic_base_receipt_binding');
 const files=await receiptFiles(record,directory,value,{signal}),current=await capturePeriodicBaseIdentity(context,id,{signal});
 if(!same({root:value.root,files,execution:value.execution},current))fail('periodic_base_source_changed');
 const metadata=await readManifest(join(record.artifactDirectory,id,'backup.enc'),record.key,{signal});if(!same(metadata,value.metadata))fail('periodic_base_receipt_binding');
 // The small control is checked twice so an atomic receipt replacement cannot
 // mix authenticated pages from different completed verification generations.
 if(!same(value,await readManifest(join(directory,'current.enc'),record.applicationKey,{signal})))fail('periodic_base_receipt_changed');
 const capability=Object.freeze({backupId:id});capabilities.set(capability,{context,metadata,verifiedAt:value.verifiedAt,identity:current});return capability;
}
export function periodicBaseVerificationSource(capability){const value=capabilities.get(capability);if(!value)fail('periodic_base_receipt_capability_required');return value;}
// Retention authenticates every historical page, including interrupted
// publication generations. Such pages grant no source or restore capability.
export async function authenticatePeriodicBaseRetention(record,base,{signal}={}){
 let names;const directory=join(base,'base-verification');try{names=await readdir(directory);}catch(error){if(error.code==='ENOENT')return [];throw error;}
 if(!record.applicationKey)fail('periodic_base_source_required');await privateDirectory(directory);const allowed=[];
 for(const name of names){
  if(name!=='current.enc'&&!/^[a-f0-9]{32}-\d+\.enc$/.test(name))fail('backup_retention_unknown_file_requires_pin');await identity(join(directory,name));
  const value=await readManifest(join(directory,name),record.applicationKey,{signal});if(value.version!==1||value.scope!==scope)fail('periodic_base_receipt_invalid');
  if(name==='current.enc'){if(value.backupId!==base.split('/').at(-1)||value.configurationDigest!==record.configurationDigest||value.systemId!==record.expected.system_id||value.applicationKeyVersion!==record.applicationKeyVersion||value.applicationKeyDigest!==record.applicationKeyDigest)fail('periodic_base_receipt_binding');await receiptFiles(record,directory,value,{signal});}
  else if(name!==value.generation+'-'+value.ordinal+'.enc'||!Array.isArray(value.files)||value.files.length<1||value.files.length>512)fail('periodic_base_receipt_invalid');
  allowed.push('base-verification/'+name);
 }
 return allowed;
}

export async function periodicBaseRetentionReferences(record,base,{signal}={}){const files=await authenticatePeriodicBaseRetention(record,base,{signal}),result=[];for(const name of files.sort())result.push({name,digest:await digestFile(join(base,name),{signal})});return result;}
