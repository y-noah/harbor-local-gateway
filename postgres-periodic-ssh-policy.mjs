import {open,lstat} from 'node:fs/promises';
import {constants} from 'node:fs';
import {dirname,resolve} from 'node:path';
import {isIP} from 'node:net';
import {transferError} from './migration-artifacts.mjs';

export const periodicSshPolicyFile='/etc/harbor-storage-periodic-ssh.json';
export const periodicSshExecutable='/usr/bin/ssh';
export const periodicSshRemoteCommand='harbor-periodic-receive';
const disabled=Object.freeze({version:1,enabled:false}),maxProfileBytes=16384;
const fields=['version','enabled','host','port','user','identityFile','knownHostsFile','indexDirectory','timeoutMs','connectTimeoutSeconds'];
const fail=code=>{throw transferError('periodic_ssh_'+code);};
const path=value=>typeof value==='string'&&value!=='/'&&resolve(value)===value&&!value.includes('\0');
function validHost(value){
 if(typeof value!=='string'||!value.length||value.length>253||/[\s\0]/.test(value))return false;
 if(isIP(value))return true;
 const name=value.endsWith('.')?value.slice(0,-1):value;
 return name.split('.').every(label=>/^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$/.test(label));
}

// Syntax validation and argument construction grant no filesystem, root or
// network authority. The fixed CLI must load its policy through the root gate.
export function periodicSshPolicy(value){
 if(!value||typeof value!=='object'||Array.isArray(value)||value.version!==1||typeof value.enabled!=='boolean'||Object.keys(value).some(key=>!fields.includes(key)))fail('policy_invalid');
 if(!value.enabled)return disabled;
 const result={port:22,timeoutMs:60000,connectTimeoutSeconds:10,...value};
 if(!validHost(result.host)||typeof result.user!=='string'||!/^[a-z_][a-z0-9_-]{0,63}$/.test(result.user)||!Number.isSafeInteger(result.port)||result.port<1||result.port>65535||!Number.isSafeInteger(result.timeoutMs)||result.timeoutMs<1000||result.timeoutMs>3600000||!Number.isSafeInteger(result.connectTimeoutSeconds)||result.connectTimeoutSeconds<1||result.connectTimeoutSeconds*1000>result.timeoutMs)fail('policy_invalid');
 for(const name of ['identityFile','knownHostsFile','indexDirectory'])if(!path(result[name]))fail('policy_invalid');
 // OpenSSH expands tokens in identity/host-file names and parses -o values.
 // These operational files use literal paths, not SSH expressions or lists.
 if([result.identityFile,result.knownHostsFile].some(file=>/[\s%$"'\\]/.test(file)))fail('policy_invalid');
 if(result.identityFile===result.knownHostsFile||[result.identityFile,result.knownHostsFile].some(file=>file===result.indexDirectory||file.startsWith(result.indexDirectory+'/')))fail('policy_invalid');
 return Object.freeze(result);
}
export function buildPeriodicSshArguments(configuration){
 const value=periodicSshPolicy(configuration);if(!value.enabled)fail('disabled');
 return Object.freeze(['-F','/dev/null','-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','CheckHostIP=no','-o','UpdateHostKeys=no','-o','GlobalKnownHostsFile=/dev/null','-o','UserKnownHostsFile='+value.knownHostsFile,'-o','IdentitiesOnly=yes','-o','IdentityAgent=none','-o','PasswordAuthentication=no','-o','KbdInteractiveAuthentication=no','-o','ClearAllForwardings=yes','-o','PermitLocalCommand=no','-o','ProxyCommand=none','-o','ProxyJump=none','-o','LogLevel=ERROR','-o','ConnectTimeout='+value.connectTimeoutSeconds,'-p',String(value.port),'-l',value.user,'-i',value.identityFile,'--',value.host,periodicSshRemoteCommand]);
}
async function trustedAncestors(file){
 for(let at=dirname(file);;at=dirname(at)){
  const info=await lstat(at);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022)fail('policy_path_untrusted');
  if(at==='/')return;
 }
}
async function trustedFile(file,kind,{secret=false,executable=false}={}){
 let handle;try{
  await trustedAncestors(file);handle=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);const info=await handle.stat();
  if(!info.isFile()||info.uid!==0||info.nlink!==1||info.mode&0o022||secret&&(info.mode&0o777)!==0o600||executable&&!(info.mode&0o111)||!info.size)fail(kind+'_untrusted');
 }catch(error){if(error.code==='ENOENT')fail(kind+'_missing');if(error.code?.startsWith('periodic_ssh_'))throw error;fail(kind+'_untrusted');}finally{await handle?.close();}
}
async function policyBytes(){
 await trustedAncestors(periodicSshPolicyFile);let handle;
 try{handle=await open(periodicSshPolicyFile,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);}catch(error){if(error.code==='ENOENT')return null;fail('policy_untrusted');}
 try{
  const before=await handle.stat({bigint:true});if(!before.isFile()||before.uid!==0n||(before.mode&0o777n)!==0o600n||before.nlink!==1n||before.size<1n||before.size>BigInt(maxProfileBytes))fail('policy_untrusted');
  const buffer=Buffer.alloc(maxProfileBytes+1);let used=0;
  while(used<buffer.length){const {bytesRead}=await handle.read(buffer,used,buffer.length-used,used);if(!bytesRead)break;used+=bytesRead;}
  const after=await handle.stat({bigint:true});if(used!==Number(before.size)||['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].some(key=>before[key]!==after[key]))fail('policy_changed');
  return buffer.subarray(0,used);
 }finally{await handle.close();}
}
export async function loadPeriodicSshPolicy(...args){
 if(args.length)fail('fixed_profile_required');
 if(process.platform!=='linux'||process.getuid?.()!==0)fail('root_required');
 const bytes=await policyBytes();if(!bytes)return disabled;let configuration;
 try{configuration=periodicSshPolicy(JSON.parse(new TextDecoder('utf8',{fatal:true}).decode(bytes)));}catch(error){if(error.code?.startsWith('periodic_ssh_'))throw error;fail('policy_invalid');}
 if(!configuration.enabled)return configuration;
 await trustedFile(configuration.identityFile,'identity',{secret:true});await trustedFile(configuration.knownHostsFile,'known_hosts');await trustedFile(periodicSshExecutable,'executable',{executable:true});
 try{await trustedAncestors(configuration.indexDirectory);const info=await lstat(configuration.indexDirectory);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||(info.mode&0o777)!==0o700)fail('index_untrusted');}catch(error){if(error.code==='ENOENT')fail('index_missing');if(error.code?.startsWith('periodic_ssh_'))throw error;fail('index_untrusted');}
 return configuration;
}
