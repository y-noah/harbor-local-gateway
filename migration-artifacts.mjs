import {createCipheriv,createDecipheriv,randomBytes,createHash} from 'node:crypto';
import {createReadStream,createWriteStream} from 'node:fs';
import {mkdir,lstat,open,readFile,writeFile,rename,rm,link,opendir} from 'node:fs/promises';
import {pipeline} from 'node:stream/promises';
import {Readable,Transform,Writable} from 'node:stream';
import {resolve,join,dirname} from 'node:path';
import {AppError} from './store.mjs';

const magic=Buffer.from('HBRART01');
export const transferError=(code,message='存储演练未完成，请核对脱敏错误代码')=>new AppError(409,message,code);
export function artifactKey(key){if(!Buffer.isBuffer(key)||key.length!==32)throw transferError('transfer_key_required');return key;}
export function canonical(value){
  if(typeof value==='bigint')return JSON.stringify({$integer:value.toString()});
  if(Buffer.isBuffer(value)||value instanceof Uint8Array)return JSON.stringify({$bytes:Buffer.from(value).toString('base64')});
  if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
  if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}';
  if(value===undefined||typeof value==='number'&&!Number.isFinite(value))throw transferError('transfer_value_invalid');return JSON.stringify(value);
}
export async function privateDirectory(path,{create=false}={}){
  const absolute=resolve(path);if(create)await mkdir(absolute,{mode:0o700});const info=await lstat(absolute);
  if(!info.isDirectory()||info.isSymbolicLink()||process.platform!=='win32'&&((info.mode&0o077)!==0||info.uid!==process.getuid()))throw transferError('transfer_directory_not_private');return absolute;
}
export async function regularFile(path){const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink())throw transferError('transfer_file_not_regular');return info;}
export async function digestFile(path,{signal,maxBytes=Number.MAX_SAFE_INTEGER}={}){signal?.throwIfAborted();if(!Number.isSafeInteger(maxBytes)||maxBytes<0)throw transferError('transfer_artifact_limit_invalid');const info=await regularFile(path);if(info.size>maxBytes)throw transferError('transfer_file_capacity');const hash=createHash('sha256');let bytes=0;for await(const chunk of createReadStream(path,{signal})){bytes+=chunk.length;if(bytes>maxBytes)throw transferError('transfer_file_capacity');hash.update(chunk);}signal?.throwIfAborted();return {sha256:hash.digest('hex'),bytes};}
export async function sealStream(input,destination,key,{maxBytes=Number.MAX_SAFE_INTEGER,signal}={}){
  signal?.throwIfAborted();artifactKey(key);if(!Number.isSafeInteger(maxBytes)||maxBytes<0)throw transferError('transfer_artifact_limit_invalid');const plaintextHash=createHash('sha256');let plaintextBytes=0;const tap=new Transform({transform(chunk,encoding,callback){plaintextBytes+=chunk.length;if(plaintextBytes>maxBytes){callback(transferError('transfer_file_capacity'));return;}plaintextHash.update(chunk);callback(null,chunk);}}),iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
  async function* encrypted(){yield Buffer.concat([magic,iv]);const writing=pipeline(input,tap,cipher,{signal});writing.catch(()=>{});for await(const chunk of cipher)yield chunk;await writing;yield cipher.getAuthTag();}
  let owned=false;const output=createWriteStream(destination,{flags:'wx',mode:0o600});output.once('open',()=>{owned=true;});try{await pipeline(Readable.from(encrypted()),output,{signal});signal?.throwIfAborted();await syncFile(destination);return {...await digestFile(destination,{signal}),plaintext:{sha256:plaintextHash.digest('hex'),bytes:plaintextBytes}};}catch{input.destroy();cipher.destroy();if(owned)await rm(destination,{force:true});if(signal?.aborted)throw signal.reason;throw transferError('transfer_artifact_seal_failed');}
}
export async function sealFile(source,destination,key,options){options?.signal?.throwIfAborted();await regularFile(source);options?.signal?.throwIfAborted();return sealStream(createReadStream(source,{signal:options?.signal}),destination,key,options);}
export async function openFile(source,destination,key,{signal}={}){
  signal?.throwIfAborted();artifactKey(key);const info=await regularFile(source);if(info.size<36)throw transferError('transfer_artifact_invalid');const handle=await open(source,'r');let header,tag;
  try{header=Buffer.alloc(20);tag=Buffer.alloc(16);await handle.read(header,0,20,0);await handle.read(tag,0,16,info.size-16);}finally{await handle.close();}
  if(!header.subarray(0,8).equals(magic))throw transferError('transfer_artifact_invalid');const decipher=createDecipheriv('aes-256-gcm',key,header.subarray(8));decipher.setAAD(magic);decipher.setAuthTag(tag);
  let owned=false;const output=createWriteStream(destination,{flags:'wx',mode:0o600});output.once('open',()=>{owned=true;});try{await pipeline(info.size===36?Readable.from([]):createReadStream(source,{start:20,end:info.size-17}),decipher,output,{signal});}catch{if(owned)await rm(destination,{force:true});if(signal?.aborted)throw signal.reason;throw transferError('transfer_artifact_authentication_failed');}
}
export async function writeManifest(path,value,key,{replace=false}={}){
  artifactKey(key);const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);const bytes=Buffer.concat([magic,iv,cipher.update(JSON.stringify(value)),cipher.final(),cipher.getAuthTag()]);
  if(bytes.length>1048576)throw transferError('transfer_manifest_too_large');const temp=join(dirname(path),'.manifest-'+randomBytes(12).toString('hex'));try{await writeFile(temp,bytes,{flag:'wx',mode:0o600});await syncFile(temp);if(!replace){await link(temp,path);await rm(temp);}else await rename(temp,path);await syncDirectory(dirname(path));}catch(error){await rm(temp,{force:true});throw transferError('transfer_manifest_write_failed');}
}
export async function readManifest(path,key,{signal}={}){
  signal?.throwIfAborted();artifactKey(key);const info=await regularFile(path);if(info.size>1048576||process.platform!=='win32'&&((info.mode&0o077)!==0||info.uid!==process.getuid()))throw transferError('transfer_manifest_not_private');
  try{const bytes=await readFile(path,{signal});signal?.throwIfAborted();if(bytes.length<36||!bytes.subarray(0,8).equals(magic))throw Error();const decipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(8,20));decipher.setAAD(magic);decipher.setAuthTag(bytes.subarray(-16));return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(20,-16)),decipher.final()]).toString('utf8'));}catch{if(signal?.aborted)throw signal.reason;throw transferError('transfer_manifest_authentication_failed');}
}

export async function syncFile(path){const handle=await open(path,'r');try{await handle.sync();}finally{await handle.close();}}
export async function syncDirectory(path){const handle=await open(path,'r');try{await handle.sync();}finally{await handle.close();}}
export async function digestOpenedFile(source,key,{signal}={}){signal?.throwIfAborted();artifactKey(key);const info=await regularFile(source);if(info.size<36)throw transferError('transfer_artifact_invalid');const file=await open(source,'r'),header=Buffer.alloc(20),tag=Buffer.alloc(16);try{await file.read(header,0,20,0);await file.read(tag,0,16,info.size-16);}finally{await file.close();}signal?.throwIfAborted();if(!header.subarray(0,8).equals(magic))throw transferError('transfer_artifact_invalid');const decipher=createDecipheriv('aes-256-gcm',key,header.subarray(8));decipher.setAAD(magic);decipher.setAuthTag(tag);const hash=createHash('sha256');let bytes=0;try{await pipeline(info.size===36?Readable.from([]):createReadStream(source,{start:20,end:info.size-17,signal}),decipher,new Writable({write(chunk,encoding,callback){hash.update(chunk);bytes+=chunk.length;callback();}}),{signal});}catch{if(signal?.aborted)throw signal.reason;throw transferError('transfer_artifact_authentication_failed');}signal?.throwIfAborted();return {sha256:hash.digest('hex'),bytes};}

const safeRelative=value=>typeof value==='string'&&value.length<=4096&&!value.includes('\\')&&!value.includes('\0')&&!value.startsWith('/')&&value.split('/').every(part=>part!=='.'&&part!=='..'&&part!=='');
async function* walkFiles(root,relative='',{signal,maxEntries=1000000}={}){
  signal?.throwIfAborted();const path=relative?join(root,...relative.split('/')):root,info=await lstat(path);
  if(info.isSymbolicLink()||!info.isFile()&&!info.isDirectory())throw transferError('transfer_unsupported_file_type');
  if(relative)yield {relative,kind:info.isDirectory()?'directory':'file',mode:info.mode&0o777,bytes:info.size};
  if(info.isDirectory()){
    const names=[],directory=await opendir(path);
    for await(const entry of directory){signal?.throwIfAborted();if(names.length>=maxEntries)throw transferError('transfer_file_capacity');names.push(entry.name);}
    for(const name of names.sort())yield* walkFiles(root,relative?relative+'/'+name:name,{signal,maxEntries});
  }
}
export async function captureFileSet({roots,directory,key,maxFiles=100000,maxFileBytes=1073741824,maxTotalBytes=274877906944,exclude=[],beforeFile,afterFile,signal}={}){
  signal?.throwIfAborted();await privateDirectory(directory);artifactKey(key);if(!Array.isArray(roots)||roots.length<1||roots.length>32||!Number.isSafeInteger(maxFiles)||maxFiles<1||maxFiles>1000000||!Number.isSafeInteger(maxFileBytes)||maxFileBytes<1||!Number.isSafeInteger(maxTotalBytes)||maxTotalBytes<maxFileBytes||!Array.isArray(exclude))throw transferError('transfer_file_configuration_invalid');
  const configured=[],seen=new Set();for(const root of roots){if(!root||!/^[a-z][a-z0-9-]{0,79}$/.test(root.id)||seen.has(root.id)||typeof root.source!=='string')throw transferError('transfer_file_root_invalid');seen.add(root.id);const source=resolve(root.source);await regularDirectory(source);if(resolve(directory)===source||resolve(directory).startsWith(source+'/'))throw transferError('transfer_artifact_inside_source');if(configured.some(value=>source===value.source||source.startsWith(value.source+'/')||value.source.startsWith(source+'/')))throw transferError('transfer_overlapping_roots');configured.push({id:root.id,source});}
  const catalog=[],fingerprints=new Map();let chunk=[],chunkBytes=0,count=0,bytes=0;
  const saveChunk=async()=>{if(!chunk.length)return;const name='file-catalog-'+catalog.length+'.enc';await writeManifest(join(directory,name),{version:1,entries:chunk},key);catalog.push({name,entries:chunk.length,digest:await digestFile(join(directory,name))});chunk=[];chunkBytes=0;};
  for(const root of configured){signal?.throwIfAborted();const hash=createHash('sha256');for await(const entry of walkFiles(root.source,'',{signal,maxEntries:maxFiles+exclude.length})){
      if(exclude.some(item=>item.root===root.id&&item.relative===entry.relative))continue;
      if(!safeRelative(entry.relative)||++count>maxFiles||entry.kind==='file'&&(entry.bytes>maxFileBytes||(bytes+=entry.bytes)>maxTotalBytes))throw transferError('transfer_file_capacity');
      const record={...entry,root:root.id};if(entry.kind==='file'){await beforeFile?.({root:root.id,relative:entry.relative});record.artifact='file-'+count+'.enc';const sealed=await sealFile(join(root.source,...entry.relative.split('/')),join(directory,record.artifact),key,{maxBytes:Math.min(maxFileBytes,maxTotalBytes-bytes+entry.bytes),signal});record.content=sealed.plaintext;record.encrypted={sha256:sealed.sha256,bytes:sealed.bytes};if(record.content.bytes!==entry.bytes)throw transferError('transfer_files_changed');await afterFile?.({root:root.id,relative:entry.relative});}hash.update(canonical({...entry,...(record.content?{content:record.content}:{})})+'\n');
      const size=Buffer.byteLength(JSON.stringify(record));if(chunkBytes+size>524288)await saveChunk();chunk.push(record);chunkBytes+=size;
    }fingerprints.set(root.id,hash.digest('hex'));}
  signal?.throwIfAborted();await saveChunk();await verifyFileSetSources({roots:configured.map(root=>({...root,sha256:fingerprints.get(root.id)})),files:count},{exclude,signal,maxFiles,maxFileBytes,maxTotalBytes});
  signal?.throwIfAborted();const manifest={version:1,roots:configured.map(root=>({...root,sha256:fingerprints.get(root.id)})),catalog,files:count,bytes,created:Date.now()};await writeManifest(join(directory,'files.enc'),manifest,key);signal?.throwIfAborted();return {files:count,bytes,roots:configured.length,manifestDigest:await digestFile(join(directory,'files.enc'),{signal})};
}
export async function verifyFileSetSources(manifest,{exclude=[],signal,maxFiles=1000000,maxFileBytes=Number.MAX_SAFE_INTEGER,maxTotalBytes=Number.MAX_SAFE_INTEGER}={}){
  signal?.throwIfAborted();if(!Number.isSafeInteger(maxFiles)||maxFiles<0||maxFiles>1000000||![maxFileBytes,maxTotalBytes].every(value=>Number.isSafeInteger(value)&&value>=0)||!Array.isArray(exclude))throw transferError('transfer_file_configuration_invalid');
  if(!Array.isArray(manifest?.roots)||!Number.isSafeInteger(manifest.files)||manifest.files<0||manifest.files>maxFiles)throw transferError('transfer_file_manifest_invalid');let count=0,bytes=0;
  for(const root of manifest.roots){signal?.throwIfAborted();await regularDirectory(root.source);const hash=createHash('sha256');for await(const entry of walkFiles(root.source,'',{signal,maxEntries:maxFiles+exclude.length})){if(exclude.some(item=>item.root===root.id&&item.relative===entry.relative))continue;if(++count>manifest.files)throw transferError('transfer_files_changed');let content;if(entry.kind==='file'){if(entry.bytes>maxFileBytes||entry.bytes>maxTotalBytes-bytes)throw transferError('transfer_file_capacity');content=await digestFile(join(root.source,...entry.relative.split('/')),{signal,maxBytes:Math.min(maxFileBytes,maxTotalBytes-bytes)});bytes+=content.bytes;}hash.update(canonical({...entry,...(content?{content}:{})})+'\n');}if(hash.digest('hex')!==root.sha256)throw transferError('transfer_files_changed');}
  signal?.throwIfAborted();if(count!==manifest.files)throw transferError('transfer_files_changed');return true;
}
async function regularDirectory(path){const info=await lstat(path);if(!info.isDirectory()||info.isSymbolicLink())throw transferError('transfer_file_root_invalid');}
export async function restoreFileSet({directory,destination,key,maxTotalBytes=274877906944,expectedManifestDigest,signal}={}){
  signal?.throwIfAborted();await privateDirectory(directory);await privateDirectory(destination);if((await (await import('node:fs/promises')).readdir(destination)).length)throw transferError('transfer_restore_directory_not_empty');if(expectedManifestDigest&&canonical(await digestFile(join(directory,'files.enc'),{signal}))!==canonical(expectedManifestDigest))throw transferError('transfer_file_manifest_changed');const manifest=await readManifest(join(directory,'files.enc'),key,{signal});
  if(manifest.version!==1||!Array.isArray(manifest.roots)||!Array.isArray(manifest.catalog)||manifest.roots.some(root=>!/^[a-z][a-z0-9-]{0,79}$/.test(root.id)))throw transferError('transfer_file_manifest_invalid');
  const roots=new Set(manifest.roots.map(root=>root.id));if(roots.size!==manifest.roots.length)throw transferError('transfer_file_manifest_invalid');const staging=join(destination,'.staging-'+randomBytes(12).toString('hex'));await mkdir(staging,{mode:0o700});let count=0,bytes=0;
  try{for(const root of roots)await mkdir(join(staging,root),{mode:0o700});for(const catalog of manifest.catalog){signal?.throwIfAborted();if(!/^file-catalog-[0-9]+\.enc$/.test(catalog.name))throw transferError('transfer_file_manifest_invalid');if(!catalog.digest||canonical(await digestFile(join(directory,catalog.name),{signal}))!==canonical(catalog.digest))throw transferError('transfer_file_catalog_changed');const part=await readManifest(join(directory,catalog.name),key,{signal});if(part.version!==1||part.entries?.length!==catalog.entries)throw transferError('transfer_file_manifest_invalid');for(const entry of part.entries){signal?.throwIfAborted();if(!roots.has(entry.root)||!safeRelative(entry.relative)||!['file','directory'].includes(entry.kind)||!Number.isInteger(entry.mode)||entry.mode<0||entry.mode>511)throw transferError('transfer_file_manifest_invalid');const path=join(staging,entry.root,...entry.relative.split('/'));count++;
        if(entry.kind==='directory'){await mkdir(path,{mode:0o700});continue;}if(!/^file-[0-9]+\.enc$/.test(entry.artifact)||!Number.isSafeInteger(entry.content?.bytes)||(bytes+=entry.content.bytes)>maxTotalBytes)throw transferError('transfer_file_capacity');if(canonical(await digestFile(join(directory,entry.artifact),{signal}))!==canonical(entry.encrypted))throw transferError('transfer_artifact_changed');await openFile(join(directory,entry.artifact),path,key,{signal});if(canonical(await digestFile(path,{signal}))!==canonical(entry.content))throw transferError('transfer_file_content_mismatch');await (await import('node:fs/promises')).chmod(path,entry.mode&0o700);
      }}if(count!==manifest.files||bytes!==manifest.bytes)throw transferError('transfer_file_count_mismatch');signal?.throwIfAborted();for(const root of roots)await rename(join(staging,root),join(destination,root));await rm(staging,{recursive:true});return {files:count,bytes,roots:manifest.roots.map(root=>({id:root.id,source:root.source,destination:join(destination,root.id)}))};
  }catch(error){await rm(staging,{recursive:true,force:true});throw error;}
}
