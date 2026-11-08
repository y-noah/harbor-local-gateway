import {constants} from 'node:fs';
import {open} from 'node:fs/promises';
import {resolve} from 'node:path';
import {canonical,transferError,fileSetExclusions} from './migration-artifacts.mjs';

const fail=()=>{throw transferError('periodic_application_policy_invalid');};
const exact=(value,keys)=>value!==null&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));
const path=value=>typeof value==='string'&&value!=='/'&&!value.includes('\0')&&value===resolve(value);
const positive=value=>Number.isSafeInteger(value)&&value>0;
const overlap=(a,b)=>a===b||a.startsWith(b+'/')||b.startsWith(a+'/');
const installedPolicies=new WeakSet();
function origin(value,{tls=false}={}){
  if(typeof value!=='string')fail();
  let parsed;try{parsed=new URL(value);}catch{fail();}
  if(!['http:','https:'].includes(parsed.protocol)||tls&&parsed.protocol!=='https:'||parsed.origin!==value||parsed.username||parsed.password)fail();
  return value;
}

// This parser does not confer root configuration authority. The coordinator
// must load its own fixed profile, never accept roots from an HTTP caller.
export function periodicApplicationPolicy(value){
  if(installedPolicies.has(value))return value;
  if(!exact(value,['version','enabled','database','materialRoots','publicOrigin','deviceOrigin','maxBytes','maxFiles','runtimeSqliteCaches'])||value.version!==1||typeof value.enabled!=='boolean')fail();
  if(!value.enabled&&Object.keys(value).length===2)return Object.freeze({version:1,enabled:false});
  const database=value.database;
  if(!exact(database,['name','oid'])||typeof database.name!=='string'||!database.name||Buffer.byteLength(database.name)>63||/[\0\r\n]/.test(database.name)||typeof database.oid!=='string'||!/^[1-9]\d{0,9}$/.test(database.oid)||BigInt(database.oid)>4294967295n)fail();
  if(!Array.isArray(value.materialRoots)||value.materialRoots.length!==3||!positive(value.maxBytes)||!positive(value.maxFiles)||value.maxFiles>1000000)fail();
  const ids=new Set(),roots=[];
  for(const root of value.materialRoots){
    if(!exact(root,['id','directory','databasePrefix'])||!['data','runtime','secrets'].includes(root.id)||ids.has(root.id)||!path(root.directory)||root.databasePrefix!==undefined&&!path(root.databasePrefix)||roots.some(other=>overlap(other.directory,root.directory)||overlap(other.databasePrefix??other.directory,root.databasePrefix??root.directory)))fail();
    // The independent activation signer must not enter a periodic backup.
    if(root.directory==='/etc'||'/etc/harbor-storage-activation.key'.startsWith(root.directory+'/'))fail();
    ids.add(root.id);roots.push(Object.freeze({id:root.id,directory:root.directory,...(root.databasePrefix!==undefined?{databasePrefix:root.databasePrefix}:{})}));
  }
  const caches=value.runtimeSqliteCaches??[];
  if(value.runtimeSqliteCaches===null||!Array.isArray(caches)||caches.some(pattern=>typeof pattern!=='string'||!pattern.endsWith('.sqlite')))fail();
  try{periodicApplicationFileExclusions({runtimeSqliteCaches:caches});}catch{fail();}
  const publicOrigin=origin(value.publicOrigin),deviceOrigin=origin(value.deviceOrigin,{tls:true});
  if(publicOrigin===deviceOrigin)fail();
  return Object.freeze({version:1,enabled:value.enabled,database:Object.freeze({name:database.name,oid:database.oid}),materialRoots:Object.freeze(roots),publicOrigin,deviceOrigin,maxBytes:value.maxBytes,maxFiles:value.maxFiles,...(caches.length?{runtimeSqliteCaches:Object.freeze([...caches])}:{})});
}

// Only operator-declared rebuildable SQLite caches in the runtime root. Each
// cache and its two SQLite sidecars are selected together; JSON and keys stay.
export function periodicApplicationFileExclusions(policy){
  return fileSetExclusions((policy.runtimeSqliteCaches??[]).flatMap(pattern=>['','-wal','-shm'].map(suffix=>({root:'runtime',pattern:pattern+suffix}))));
}

export async function loadPeriodicApplicationPolicy(){
  if(process.platform!=='linux'||process.getuid()!==0)throw transferError('periodic_application_root_profile_required');
  let file;try{file=await open('/etc/harbor-storage-application.json',constants.O_RDONLY|constants.O_NOFOLLOW);}catch(error){if(error.code==='ENOENT')return periodicApplicationPolicy({version:1,enabled:false});throw error;}
  try{
    const info=await file.stat();
    if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.nlink!==1||info.size>65536)throw transferError('periodic_application_profile_not_trusted');
    let value;try{value=JSON.parse(await file.readFile('utf8'));}catch{fail();}
    const policy=periodicApplicationPolicy(value);installedPolicies.add(policy);return policy;
  }finally{await file.close();}
}

export async function verifyInstalledPeriodicApplicationPolicy(policy,{signal}={}){
  signal?.throwIfAborted();installedPeriodicApplicationPolicy(policy);
  const current=await loadPeriodicApplicationPolicy();signal?.throwIfAborted();
  if(!current.enabled||canonical(current)!==canonical(policy))throw transferError('periodic_application_installed_policy_changed');
  return policy;
}

export function installedPeriodicApplicationPolicy(policy){
  if(!installedPolicies.has(policy)||!policy.enabled||process.platform!=='linux'||process.getuid()!==0)throw transferError('periodic_application_installed_policy_required');
  return policy;
}
