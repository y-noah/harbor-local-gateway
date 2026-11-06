import {lstat} from 'node:fs/promises';
import {join} from 'node:path';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {inspectArchivedWal,readWalManifest} from './postgres-recovery.mjs';
import {canonical,transferError} from './migration-artifacts.mjs';

const caches=new WeakMap(),fail=code=>{throw transferError(code);};
const same=(a,b)=>canonical(a)===canonical(b);
async function identity(path,uid){
  const value=await lstat(path,{bigint:true});
  if(!value.isFile()||value.isSymbolicLink()||value.uid!==BigInt(uid)||(value.mode&0o777n)!==0o600n)fail('periodic_wal_cache_file_untrusted');
  // Immutable archive publication briefly links its private temporary inode.
  // Never cache that identity; the checkpoint may await single-link closure
  // within its existing archive deadline. A persistent link never succeeds.
  if(value.nlink!==1n)fail('periodic_wal_cache_publication_pending');
  return Object.fromEntries(['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].map(name=>[name,String(value[name])]));
}

// A hit proves reuse of the same already authenticated immutable bytes. It
// deliberately retains verifiedAt: a metadata check is not a new full scrub.
export async function inspectPeriodicArchivedWal(context,name,{sourceGeneration,signal,scrub=false}={}){
  const record=postgresBackupContext(context);signal?.throwIfAborted();
  if(record.closed||record.receiverBinding||record.resumeBinding||!/^[0-9A-F]{24}$/.test(name)||typeof sourceGeneration!=='string'||!/^\d+(?:\.\d+)?$/.test(sourceGeneration)||typeof scrub!=='boolean')fail('periodic_wal_cache_context');
  let cache=caches.get(context);if(!cache){cache=new Map();caches.set(context,cache);}
  const path=join(record.archiveDirectory,name+'.enc'),metadataPath=join(record.archiveDirectory,name+'.meta.enc'),metadata=await readWalManifest(metadataPath,record.key,{ownerUid:record.restoreUid}),before=await identity(path,record.restoreUid),metadataBefore=await identity(metadataPath,record.restoreUid),cluster=await readWalManifest(join(record.archiveDirectory,'cluster.enc'),record.key,{ownerUid:record.restoreUid});
  if(metadata.version!==2||metadata.systemIdentifier!==record.expected.system_id||cluster.systemIdentifier!==record.expected.system_id||cluster.primaryData!==record.expected.data_directory||record.archiveProfileDigest!==undefined&&cluster.archiveProfileDigest!==record.archiveProfileDigest||metadata.identity?.name!==name||metadata.encrypted?.bytes!==Number(before.size)||metadata.content?.bytes!==Number(record.expected.segment_bytes))fail('periodic_wal_cache_binding');
  const binding={configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,sourceGeneration,cluster,identity:before,metadataIdentity:metadataBefore,metadata},prior=cache.get(name);
  if(!scrub&&prior&&same(prior.binding,binding)){
    signal?.throwIfAborted();if(!same(before,await identity(path,record.restoreUid))||!same(metadataBefore,await identity(metadataPath,record.restoreUid)))fail('periodic_wal_cache_source_changed');
    return {metadata:structuredClone(metadata),cacheHit:true,verifiedAt:prior.verifiedAt};
  }
  const verified=await inspectArchivedWal({name,directory:record.archiveDirectory,key:record.key,ownerUid:record.restoreUid,requireBoundName:true});signal?.throwIfAborted();
  if(!same(before,await identity(path,record.restoreUid))||!same(metadataBefore,await identity(metadataPath,record.restoreUid))||!same(metadata,verified))fail('periodic_wal_cache_source_changed');
  const verifiedAt=Date.now();cache.delete(name);cache.set(name,{binding,verifiedAt});
  while(cache.size>record.profile.maxWalSegments)cache.delete(cache.keys().next().value);
  return {metadata:structuredClone(verified),cacheHit:false,verifiedAt};
}
