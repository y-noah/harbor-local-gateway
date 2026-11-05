import {open,lstat} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);},same=(a,b)=>a.dev===b.dev&&a.ino===b.ino&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
// Content verification is data only, never receiver/root authority. Formal
// callers pin ownerUid=0 and separately validate the fixed root-owned layout.
export async function verifyReceiverCandidateOciLayers(image,{maxBytes,timeoutMs=120000,signal,ownerUid=0}={}){
 if(!image||typeof image.layout!=='string'||!Array.isArray(image.layers)||image.layers.length<1||image.layers.length>256||!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>120000||!Number.isSafeInteger(ownerUid)||ownerUid<0)fail('backup_receiver_candidate_oci_budget');signal=AbortSignal.any([signal,AbortSignal.timeout(timeoutMs)].filter(Boolean));let expectedBytes=0,actualBytes=0;
 for(const layer of image.layers){if(!/^sha256:[a-f0-9]{64}$/.test(layer?.digest??'')||!Number.isSafeInteger(layer.size)||layer.size<1||(expectedBytes+=layer.size)>maxBytes)fail('backup_receiver_candidate_oci_budget');}
 for(const layer of image.layers){signal.throwIfAborted();const path=join(image.layout,'blobs','sha256',layer.digest.slice(7)),before=await lstat(path,{bigint:true});if(!before.isFile()||before.isSymbolicLink()||before.uid!==BigInt(ownerUid)||before.mode&0o022n||before.size!==BigInt(layer.size))fail('backup_receiver_candidate_oci_file');const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{if(!same(before,await file.stat({bigint:true})))fail('backup_receiver_candidate_oci_changed');const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let bytes=0;for(;;){signal.throwIfAborted();const {bytesRead}=await file.read(buffer,0,Math.min(buffer.length,layer.size+1-bytes),null);signal.throwIfAborted();if(!bytesRead)break;bytes+=bytesRead;actualBytes+=bytesRead;if(bytes>layer.size||actualBytes>maxBytes)fail('backup_receiver_candidate_oci_budget');hash.update(buffer.subarray(0,bytesRead));}if(bytes!==layer.size||'sha256:'+hash.digest('hex')!==layer.digest)fail('backup_receiver_candidate_oci_digest');if(!same(before,await file.stat({bigint:true}))||!same(before,await lstat(path,{bigint:true})))fail('backup_receiver_candidate_oci_changed');}finally{await file.close();}}
 signal.throwIfAborted();return {scope:'oci-layer-content-only',layerCount:image.layers.length,bytes:actualBytes,rootAuthority:false};
}
