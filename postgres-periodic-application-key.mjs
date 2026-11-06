import {open,lstat,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {resolve,dirname} from 'node:path';
import {createHash,timingSafeEqual} from 'node:crypto';
import {transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);};
const exact=(value,names)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...names].sort().join(',');
export const periodicApplicationKeyDigest=key=>createHash('sha256').update('harbor-periodic-application-key-v1\0').update(key).digest('hex');
export function periodicApplicationEncryption(value){
  if(value===undefined||value===null)return null;
  if(!exact(value,['version','keyFile','keyVersion','keyDigest'])||value.version!==1||typeof value.keyFile!=='string'||value.keyFile==='/'||value.keyFile.includes('\0')||value.keyFile!==resolve(value.keyFile)||!/^[-a-zA-Z0-9_.]{1,64}$/.test(value.keyVersion??'')||!/^[a-f0-9]{64}$/.test(value.keyDigest??''))fail('periodic_application_encryption_invalid');
  return Object.freeze({...value});
}
export function taskPeriodicApplicationKey(key,keyVersion,archiveKey){
  if(key===undefined)return {};
  if(!Buffer.isBuffer(key)||key.length!==32||!/^[-a-zA-Z0-9_.]{1,64}$/.test(keyVersion??''))fail('periodic_application_key_invalid');
  if(timingSafeEqual(key,archiveKey))fail('periodic_application_independent_key_required');
  return {applicationKey:Buffer.from(key),applicationKeyVersion:keyVersion,applicationKeyDigest:periodicApplicationKeyDigest(key)};
}
export async function loadPeriodicApplicationKey(configuration,archiveKey){
  const policy=periodicApplicationEncryption(configuration);if(!policy)return {};
  if(process.platform!=='linux'||process.getuid()!==0)fail('periodic_application_root_key_required');
  for(let at=dirname(policy.keyFile);;at=dirname(at)){const info=await lstat(at);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022)fail('periodic_application_key_parent_untrusted');if(at==='/')break;}
  if(await realpath(policy.keyFile)!==policy.keyFile)fail('periodic_application_key_untrusted');
  const file=await open(policy.keyFile,constants.O_RDONLY|constants.O_NOFOLLOW);let key;
  try{
    const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.nlink!==1||info.size!==32)fail('periodic_application_key_untrusted');
    key=await file.readFile();const loaded=taskPeriodicApplicationKey(key,policy.keyVersion,archiveKey);
    if(loaded.applicationKeyDigest!==policy.keyDigest){loaded.applicationKey.fill(0);fail('periodic_application_key_digest_mismatch');}
    return {...loaded,applicationEncryption:policy};
  }finally{key?.fill(0);await file.close();}
}
export function periodicApplicationKey(record){
  if(record.closed)fail('backup_context_closed');
  if(!Buffer.isBuffer(record.applicationKey)||record.applicationKey.length!==32||!record.applicationKeyVersion||!record.applicationKeyDigest)fail('periodic_application_key_required');
  if(periodicApplicationKeyDigest(record.applicationKey)!==record.applicationKeyDigest)fail('periodic_application_key_changed');
  return record.applicationKey;
}
export function periodicApplicationKeyBinding(record){periodicApplicationKey(record);return {applicationKeyVersion:record.applicationKeyVersion,applicationKeyDigest:record.applicationKeyDigest};}
export function verifyPeriodicApplicationKeyBinding(value,record){
  periodicApplicationKey(record);if(value?.applicationKeyVersion!==record.applicationKeyVersion||value.applicationKeyDigest!==record.applicationKeyDigest)fail('periodic_application_key_binding');
}
