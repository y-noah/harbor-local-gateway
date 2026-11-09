import {mkdir,lstat,readdir,readFile,writeFile,rename,unlink} from 'node:fs/promises';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {sanitizeRecord} from './record-redaction.mjs';
import {AppError} from './store.mjs';
import {consumeRecordingAtomicCommit} from './recording-store.mjs';

const filePattern=/^\d{20}-[a-f0-9-]{36}\.record$/;
// A single writer keeps command order across transient database failures. Files
// contain only encrypted sanitized copies and are removed only after DB commit.
export class RecordingBuffer{
  #pendingAppends=new Map();
  static async create(repository,{directory,maxMemoryBytes=4*1024*1024,maxDiskBytes=64*1024*1024,maxFiles=1024,maxCommands=256,flushMs=250,atomicBatch,atomicCommands,clock=()=>performance.now()}={}){
    if(!repository||!Number.isSafeInteger(repository.maxBytes)||repository.maxBytes<256)throw new AppError(503,'正文仓储容量未配置','recording_configuration');
    if(typeof directory!=='string'||!directory||![maxMemoryBytes,maxDiskBytes,maxFiles,maxCommands,flushMs].every(Number.isSafeInteger)||maxMemoryBytes<1024||maxDiskBytes<1024||maxFiles<1||maxCommands<1||flushMs<10||flushMs>10000||atomicBatch!==undefined&&typeof atomicBatch!=='function'||atomicCommands!==undefined&&typeof atomicCommands!=='function'||typeof clock!=='function')throw new AppError(503,'正文缓冲配置无效','recording_configuration');
    await mkdir(directory,{recursive:true,mode:0o700});const info=await lstat(directory);
    if(!info.isDirectory()||info.isSymbolicLink()||process.platform!=='win32'&&(info.mode&0o077||info.uid!==process.getuid()))throw new AppError(503,'正文缓冲目录权限不安全','recording_configuration');
    const buffer=new RecordingBuffer(repository,{directory,maxMemoryBytes,maxDiskBytes,maxFiles,maxCommands,flushMs,atomicBatch,atomicCommands,clock});
    for(let name of (await readdir(directory)).filter(name=>filePattern.test(name)||name.endsWith('.pending')&&filePattern.test(name.slice(0,-8))).sort()){
      const info=await lstat(join(directory,name));
      if(!info.isFile()||info.isSymbolicLink()||process.platform!=='win32'&&(info.mode&0o077||info.uid!==process.getuid()))throw new AppError(503,'正文缓冲文件权限不安全','recording_configuration');
      if(name.endsWith('.pending')){
        const committed=name.slice(0,-8);
        try{
          if(info.size>maxMemoryBytes+64)throw Error('Oversized pending recording');
          repository.open(await readFile(join(directory,name)),committed);
          // A matching committed name is never overwritten on recovery.
          try{await lstat(join(directory,committed));throw Error('Conflicting recording');}catch(e){if(e.code!=='ENOENT')throw e;}
          await rename(join(directory,name),join(directory,committed));name=committed;
        }catch{
          buffer.orphanFiles++;buffer.orphanBytes+=info.size;buffer.diskBytes+=info.size;buffer.files.push({name,bytes:info.size,orphan:true});buffer.failures++;buffer.counter=BigInt(committed.slice(0,20));continue;
        }
      }
      buffer.files.push({name,bytes:info.size});buffer.diskBytes+=info.size;buffer.counter=BigInt(name.slice(0,20));
    }
    buffer.schedule();return buffer;
  }
  constructor(repository,options){Object.assign(this,options);this.repository=repository;this.queue=[];this.files=[];this.memoryBytes=0;this.diskBytes=0;this.orphanFiles=0;this.orphanBytes=0;this.counter=0n;this.failures=0;this.dropped=0;this.closed=false;this.timer=null;this.running=null;
    this.dropReasons={sanitize:0,command_limit:0,memory_limit:0,both_limits:0};this.peakQueued=0;this.peakMemoryBytes=0;this.maxRejectedSanitizedBytes=0;this.peakPendingCommitResidenceMs=0;this.lastClock=null;this.clockFailures=0;this.writerPhase='idle';this.writerStarted=null;this.phaseStats=Object.fromEntries(['atomic','replay','spool'].map(name=>[name,{attempts:0,succeeded:0,failed:0,totalMs:0,maxMs:0}]));
  }
  // Diagnostic clock failure must never reject a command or interrupt a write.
  readClock(){try{const value=this.clock();if(value&&typeof value.then==='function')void Promise.resolve(value).catch(()=>{});if(typeof value!=='number'||!Number.isFinite(value)||value<0||this.lastClock!==null&&value<this.lastClock)throw Error('clock');this.lastClock=value;return value;}catch{this.clockFailures++;return null;}}
  reject(reason,bytes){this.dropped++;this.dropReasons[reason]++;this.maxRejectedSanitizedBytes=Math.max(this.maxRejectedSanitizedBytes,bytes);return false;}
  beginPhase(phase){const at=this.readClock();this.writerPhase=phase;this.writerStarted=at;this.phaseStats[phase].attempts++;return at;}
  endPhase(phase,started,succeeded){const at=this.readClock(),stats=this.phaseStats[phase];stats[succeeded?'succeeded':'failed']++;if(started!==null&&at!==null){const elapsed=at-started;stats.totalMs+=elapsed;stats.maxMs=Math.max(stats.maxMs,elapsed);}this.writerPhase='idle';this.writerStarted=null;}
  freezeEntry(entry){if(this.#pendingAppends.get(entry.value.requestId)===entry)this.#pendingAppends.delete(entry.value.requestId);}
  released(entries){const at=this.readClock();for(const entry of entries){this.freezeEntry(entry);this.memoryBytes-=entry.bytes;if(at!==null&&entry.enqueuedAt!==null)this.peakPendingCommitResidenceMs=Math.max(this.peakPendingCommitResidenceMs,at-entry.enqueuedAt);}}
  enqueue(command){
    if(this.closed)return false;
    let clipped=false;const value=sanitizeRecord(command,{maxBytes:Math.min(this.maxMemoryBytes,this.repository.maxBytes),onTruncate:()=>{clipped=true;}}),bytes=Buffer.byteLength(JSON.stringify(value));
    // Only never-attempted appends may change. Other requests have independent
    // event sequences; a lifecycle command for this request is always a fence.
    const append=value.type==='append'&&typeof value.requestId==='string'&&Array.isArray(value.events)&&value.events.length<=128;
    const previous=this.#pendingAppends.get(value.requestId);
    if(!append)this.#pendingAppends.delete(value.requestId);
    if(clipped)return this.reject('sanitize',bytes);
    if(append&&previous&&previous.value.events.length+value.events.length<=128){
      let mergedClipped=false;const merged=sanitizeRecord({...previous.value,events:[...previous.value.events,...value.events]},{maxBytes:Math.min(this.maxMemoryBytes,this.repository.maxBytes),onTruncate:()=>{mergedClipped=true;}}),mergedBytes=Buffer.byteLength(JSON.stringify(merged)),delta=mergedBytes-previous.bytes;
      if(!mergedClipped&&mergedBytes<=Math.min(this.maxMemoryBytes,this.repository.maxBytes)&&this.memoryBytes+delta<=this.maxMemoryBytes){previous.value=merged;previous.bytes=mergedBytes;this.memoryBytes+=delta;this.peakMemoryBytes=Math.max(this.peakMemoryBytes,this.memoryBytes);this.schedule();return true;}
    }
    const commandLimit=this.queue.length>=this.maxCommands,memoryLimit=this.memoryBytes+bytes>this.maxMemoryBytes;
    if(commandLimit||memoryLimit)return this.reject(commandLimit&&memoryLimit?'both_limits':commandLimit?'command_limit':'memory_limit',bytes);
    const entry={value,bytes,enqueuedAt:this.readClock()};this.queue.push(entry);if(append)this.#pendingAppends.set(value.requestId,entry);this.memoryBytes+=bytes;this.peakQueued=Math.max(this.peakQueued,this.queue.length);this.peakMemoryBytes=Math.max(this.peakMemoryBytes,this.memoryBytes);this.schedule();return true;
  }
  start(requestId,turnState){return this.enqueue({type:'start',requestId,turnState});}
  append(requestId,events){return this.enqueue({type:'append',requestId,events});}
  contextSnapshot(requestId,components,coverage){if(!this.repository.contextRepository)return false;return this.enqueue({type:'context',requestId,components,coverage});}
  finish(requestId,result){return this.enqueue({type:'finish',requestId,result});}
  settle(requestId,result,at=Date.now()){
    // Only billing facts enter recovery; never store a model response or replay it.
    const facts=Object.fromEntries(['status','input','output','cached','usageKnown','actualModel'].filter(name=>result[name]!==undefined).map(name=>[name,result[name]]));
    return this.enqueue({type:'settle',requestId,result:facts,at});
  }
  schedule(){if(this.closed||this.timer||this.running)return;this.timer=setTimeout(()=>{this.timer=null;void this.drain();},this.flushMs);this.timer.unref();}
  async apply(value){
    if(value.type==='start')await this.repository.start(value.requestId,{turnState:value.turnState});
    else if(value.type==='append')await this.repository.append(value.requestId,value.events);
    else if(value.type==='context')await this.repository.contextSnapshot(value.requestId,value.components,value.coverage);
    else if(value.type==='finish')await this.repository.finish(value.requestId,value.result);
    else if(value.type==='settle')await this.repository.store.finish(value.requestId,value.result,value.at);
    else throw new AppError(503,'正文缓冲命令无效','recording_buffer_invalid');
  }
  drain(){
    if(this.running)return this.running;
    this.running=this.work().catch(()=>{this.failures++;}).finally(()=>{this.running=null;if(this.queue.length||this.files.length)this.schedule();});return this.running;
  }
  async work(){
    // Recover committed files before creating more. An unavailable DB does not
    // prevent bounded persistence of newly accepted commands below.
    let databaseAvailable=true;
    const replay=async()=>{
      while(databaseAvailable&&this.files.length){
        const file=this.files[0],path=join(this.directory,file.name),started=this.beginPhase('replay');let succeeded=false;
        try{if(file.orphan){this.failures++;databaseAvailable=false;break;}const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>this.maxMemoryBytes+64)throw Error('Invalid recording file');
          const value=this.repository.open(await readFile(path),file.name);await this.apply(value);
          await unlink(path);this.files.shift();this.diskBytes-=file.bytes;succeeded=true;
        }catch{this.failures++;databaseAvailable=false;}finally{this.endPhase('replay',started,succeeded);}
      }
    };
    await replay();
    while(this.queue.length){
      // A healthy transactional repository is already durable storage. Commit a
      // bounded prefix before removing it, without a second file/fsync per event.
      // Existing encrypted files always replay first; uncertain commits retain
      // the prefix and fall back to the unchanged idempotent recovery protocol.
      if(databaseAvailable&&!this.files.length&&(this.atomicCommands||this.atomicBatch)){
        const entries=[];let bytes=2;
        for(const entry of this.queue){const next=entry.bytes+(entries.length?1:0);if(bytes+next>Math.min(this.maxMemoryBytes,this.repository.maxBytes))break;entries.push(entry);bytes+=next;}
        if(entries.length){
          // Freeze the whole attempted prefix before the transaction's first
          // await. A failed or unknown commit must replay these exact commands.
          for(const entry of entries)this.freezeEntry(entry);
          const started=this.beginPhase('atomic');let succeeded=false;
          try{
            if(this.atomicCommands){const commands=Object.freeze(entries.map(entry=>entry.value)),receipt=await this.atomicCommands(commands);if(!consumeRecordingAtomicCommit(receipt,commands,this.repository))throw new AppError(503,'正文批量提交未确认','recording_buffer_invalid');}
            else{let applied=false;await this.atomicBatch(async()=>{for(const entry of entries)await this.apply(entry.value);applied=true;});if(!applied)throw new AppError(503,'正文批量事务未执行','recording_buffer_invalid');}
            this.queue.splice(0,entries.length);this.released(entries);succeeded=true;
            continue;
          }catch{this.failures++;databaseAvailable=false;}finally{this.endPhase('atomic',started,succeeded);}
        }
      }
      const entry=this.queue[0];this.freezeEntry(entry);
      const started=this.beginPhase('spool');let succeeded=false;
      try{
        const name=String(++this.counter).padStart(20,'0')+'-'+randomUUID()+'.record',cipher=this.repository.seal(entry.value,name);
        if(this.files.length>=this.maxFiles||this.diskBytes+cipher.length>this.maxDiskBytes){this.failures++;return;}
        const path=join(this.directory,name),temp=path+'.pending';
        try{await writeFile(temp,cipher,{mode:0o600,flag:'wx',flush:true});await rename(temp,path);}
        catch{await unlink(temp).catch(()=>{});this.failures++;return;}
        this.files.push({name,bytes:cipher.length});this.diskBytes+=cipher.length;this.queue.shift();this.released([entry]);succeeded=true;
      }finally{this.endPhase('spool',started,succeeded);}
      await replay();
    }
  }
  status(){const at=this.readClock(),available=this.clockFailures===0;return {queued:this.queue.length,memoryBytes:this.memoryBytes,files:this.files.length,diskBytes:this.diskBytes,orphanFiles:this.orphanFiles,orphanBytes:this.orphanBytes,failures:this.failures,dropped:this.dropped,
    diagnostics:{dropReasons:{...this.dropReasons},peakQueued:this.peakQueued,peakMemoryBytes:this.peakMemoryBytes,maxRejectedSanitizedBytes:this.maxRejectedSanitizedBytes,timingAvailable:available,clockFailures:this.clockFailures,
      // Residence ends at DB commit or encrypted spool publication, and includes
      // a prefix already executing but awaiting COMMIT. It is
      // deliberately not described as pure queue wait or database execution.
      oldestPendingCommitMs:available&&at!==null?(this.queue.length?this.queue[0].enqueuedAt===null?null:at-this.queue[0].enqueuedAt:0):null,peakPendingCommitResidenceMs:available?this.peakPendingCommitResidenceMs:null,
      writer:{phase:this.writerPhase,elapsedMs:!available?null:this.writerPhase==='idle'?0:at!==null&&this.writerStarted!==null?at-this.writerStarted:null},phases:Object.fromEntries(Object.entries(this.phaseStats).map(([name,stats])=>[name,{...stats,totalMs:available?stats.totalMs:null,maxMs:available?stats.maxMs:null}]))}};
  }
  async flush(){if(this.timer){clearTimeout(this.timer);this.timer=null;}await this.drain();return this.status();}
  async close(){this.closed=true;if(this.timer)clearTimeout(this.timer);this.timer=null;await this.drain();return this.status();}
}
