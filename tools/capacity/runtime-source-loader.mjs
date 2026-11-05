import {registerHooks} from 'node:module';
import {openSync,readSync,closeSync,fstatSync,realpathSync,constants} from 'node:fs';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {createHash,randomUUID} from 'node:crypto';

// This preload constrains filesystem module origins. It does not claim to
// prohibit eval, generated machine code, or arbitrary execution in memory.
const loader='/app/tools/capacity/runtime-source-loader.mjs';
const entries=new Map([['/app/tools/capacity/process-adapter.mjs','--owned-capacity-child'],['/app/tools/capacity/restored-runtime-entry.mjs','--owned-restored-report'],['/app/tools/capacity/remote-broker-runtime.mjs','--owned-capacity-remote-broker'],['/app/tools/capacity/remote-endpoint.mjs','--owned-capacity-remote-endpoint'],['/app/tools/capacity/remote-launcher.mjs','--owned-capacity-remote-executor']]);
const fail=code=>{throw Error(code);};
if(fileURLToPath(import.meta.url)!==loader||!entries.has(process.argv[1])||process.argv[2]!==entries.get(process.argv[1])||typeof registerHooks!=='function')fail('capacity_runtime_source_entry');
const fsOpen=openSync,fsRead=readSync,fsClose=closeSync,fsStat=fstatSync,fsReal=realpathSync;
const sourceRoots=['/app','/usr','/lib','/lib64'],maxModules=4096,maxCodeBytes=64*1024**2,maxPathBytes=1024;
const digest=value=>createHash('sha256').update(value).digest('hex'),epoch=randomUUID(),rows=[],byPath=new Map();
let codeBytes=0,fault=null,closed=false,pendingMessages=0,droppedMessages=0,ipcBackpressured=false,coalescedMessages=0;const queuedRows=new Map();
const maxPendingMessages=128;
const send=typeof process.send==='function'?process.send.bind(process):null;
const beneath=(path,root)=>root==='/'?path.startsWith('/'):path===root||path.startsWith(root+'/');
const decode=value=>value.replace(/\\([0-7]{3})/g,(_,octal)=>String.fromCharCode(parseInt(octal,8)));
function bounded(path,limit){
 const fd=fsOpen(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK),chunks=[];let total=0;try{if(!fsStat(fd).isFile())fail('capacity_runtime_source_type');for(;;){const buffer=Buffer.alloc(Math.min(65536,limit+1-total)),n=fsRead(fd,buffer,0,buffer.length,null);if(!n)break;total+=n;if(total>limit)fail('capacity_runtime_source_budget');chunks.push(buffer.subarray(0,n));}return Buffer.concat(chunks,total);}finally{fsClose(fd);}
}
function mountFor(path){
 const text=bounded('/proc/self/mountinfo',1048576).toString('utf8');let selected=null;
 for(const line of text.trim().split('\n')){const fields=line.split(' '),separator=fields.indexOf('-');if(separator<6)continue;const target=decode(fields[4]);if(beneath(path,target)&&(!selected||target.length>selected.target.length))selected={target,readonly:fields[5].split(',').includes('ro')};}
 return selected;
}
function filePath(url){
 if(typeof url!=='string'||!url.startsWith('file:'))fail('capacity_runtime_source_protocol');let path;try{path=fileURLToPath(url);}catch{fail('capacity_runtime_source_protocol');}
 if(Buffer.byteLength(path)>maxPathBytes||beneath(path,'/proc')||beneath(path,'/dev/fd'))fail('capacity_runtime_source_path');return path;
}
function readonly(path){
 const real=fsReal(path);if(Buffer.byteLength(real)>maxPathBytes)fail('capacity_runtime_source_path');if(!sourceRoots.some(root=>beneath(real,root))||beneath(real,'/app/data')||beneath(real,'/app/runtime'))fail('capacity_runtime_source_not_readonly');
 const mount=mountFor(real);if(!mount?.readonly)fail('capacity_runtime_source_not_readonly');return real;
}
function pump(){if(!send||closed||ipcBackpressured)return;while(pendingMessages<maxPendingMessages&&queuedRows.size&&!ipcBackpressured){const [sequence,row]=queuedRows.entries().next().value;queuedRows.delete(sequence);pendingMessages++;try{const accepted=send({kind:'capacity-runtime-source-v1',epoch,row},error=>{pendingMessages--;if(pendingMessages===0)ipcBackpressured=false;if(error){fault='capacity_runtime_source_ipc_unavailable';droppedMessages++;queuedRows.clear();}else pump();});if(!accepted)ipcBackpressured=true;}catch{pendingMessages--;fault='capacity_runtime_source_ipc_unavailable';droppedMessages++;queuedRows.clear();return;}}}
function emit(row){if(!send||closed)return;if(queuedRows.has(row.sequence))coalescedMessages++;else if(queuedRows.size>=maxModules){droppedMessages++;fault='capacity_runtime_source_ipc_budget';return;}queuedRows.set(row.sequence,row);pump();}
function observe(url,format,loadedSource,outcome='observed'){
 const path=readonly(filePath(url));if(byPath.has(path))return byPath.get(path);
 if(rows.length>=maxModules)fail('capacity_runtime_source_budget');const bytes=bounded(path,maxCodeBytes-codeBytes);codeBytes+=bytes.length;
 const loaded=typeof loadedSource==='string'?Buffer.from(loadedSource):ArrayBuffer.isView(loadedSource)?Buffer.from(loadedSource.buffer,loadedSource.byteOffset,loadedSource.byteLength):loadedSource instanceof ArrayBuffer?Buffer.from(loadedSource):null;
 const row=Object.freeze({sequence:rows.length+1,path,format:String(format??'unknown').slice(0,40),outcome,bytes:bytes.length,fileSha256:digest(bytes),loadedSha256:loaded?digest(loaded):null});rows.push(row);byPath.set(path,row);
 emit(row);return row;
}
function nativeOutcome(row,outcome){const updated=Object.freeze({...row,outcome});rows[row.sequence-1]=updated;byPath.set(row.path,updated);emit(updated);}
function guard(operation){try{return operation();}catch(error){if(/^capacity_runtime_source_[a-z_]+$/.test(error?.message??''))fault=error.message;throw error;}}
// Kernel mount state is read after chroot, never a caller's readonly boolean.
if(!mountFor('/')?.readonly)fail('capacity_runtime_source_root_writable');readonly(loader);readonly(process.execPath);observe(import.meta.url,'preload',null);
registerHooks({
 resolve(specifier,context,next){return guard(()=>{if(specifier.startsWith('file:'))filePath(specifier);else if(specifier.startsWith('/'))filePath(pathToFileURL(specifier).href);else if((specifier.startsWith('./')||specifier.startsWith('../'))&&context.parentURL?.startsWith('file:'))filePath(new URL(specifier,context.parentURL).href);const result=next(specifier,context);if(result.url.startsWith('node:'))return result;const path=filePath(result.url);if(result.format!=='json'&&!path.endsWith('.json'))readonly(path);return result;});},
 load(url,context,next){return guard(()=>{if(url.startsWith('node:'))return next(url,context);const path=filePath(url);if(context.format!=='json'&&!path.endsWith('.json'))readonly(path);const result=next(url,context);if(result.format!=='json')observe(url,result.format,result.source);return result;});}
});
// Direct native addon loading is a separate public Node entry from require().
const originalDlopen=process.dlopen.bind(process);
Object.defineProperty(process,'dlopen',{configurable:false,writable:false,value:(module,filename,...args)=>guard(()=>{const path=readonly(filePath(pathToFileURL(resolve(filename)).href));const row=observe(pathToFileURL(path).href,'addon',null,'attempted');try{const result=originalDlopen(module,path,...args);nativeOutcome(row,'loaded');return result;}catch(error){nativeOutcome(row,'failed');fault='capacity_runtime_source_native_load_failed';throw error;}})});
process.once('disconnect',()=>{closed=true;queuedRows.clear();});

export function capacityRuntimeSourceObservation({after=0,through=rows.length,limit=128}={}){
 if(!Number.isSafeInteger(after)||!Number.isSafeInteger(through)||!Number.isSafeInteger(limit)||after<0||through<after||through>rows.length||limit<1||limit>256)fail('capacity_runtime_source_cursor');
 const selected=rows.filter(row=>row.sequence>after&&row.sequence<=through).slice(0,limit);
 return Object.freeze({schema:'capacity-runtime-source-observation-v1',scope:'primary-node-module-origin-only',epoch,closed,fault,ipc:Object.freeze({pendingMessages,droppedMessages,queuedMessages:queuedRows.size,coalescedMessages,backpressured:ipcBackpressured,maxPendingMessages,maxQueuedMessages:maxModules}),through,total:rows.length,codeBytes,limits:Object.freeze({maxModules,maxCodeBytes,maxPathBytes}),rows:Object.freeze(selected),next:selected.length&&selected.at(-1).sequence<through?selected.at(-1).sequence:null,formalQualified:false});
}
