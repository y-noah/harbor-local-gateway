import {transferError} from './migration-artifacts.mjs';
import {performance} from 'node:perf_hooks';

// Version 1 carries small closed control records; ciphertext travels separately
// in bounded chunks and is authenticated by the existing handoff/object store.
export const periodicSshWireLimits=Object.freeze({jsonBytes:4096,dataBytes:16777216});
const fail=code=>{throw transferError('periodic_ssh_'+code);};
export class PeriodicSshWire{
 constructor(input,output,{signal,timeoutMs=60000}={}){
  if(!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>3600000||!input?.read||!output?.write)fail('configuration_invalid');
  this.input=input;this.output=output;this.cancellation=new AbortController();this.signal=AbortSignal.any([signal,this.cancellation.signal].filter(Boolean));this.reading=false;this.writing=false;this.closed=false;this.deadline=performance.now()+timeoutMs;this.onError=()=>this.cancellation.abort(transferError('periodic_ssh_stream_failed'));
  for(const stream of new Set([input,output])){stream.on('error',this.onError);if(stream.closed)stream.off('error',this.onError);else stream.once('close',()=>stream.off('error',this.onError));}input.pause();
  this.onAbort=()=>{clearTimeout(this.timer);input.destroy();if(output!==input)output.destroy();};this.signal.addEventListener('abort',this.onAbort,{once:true});this.timer=setTimeout(()=>this.cancellation.abort(transferError('periodic_ssh_deadline')),timeoutMs);if(this.signal.aborted)this.onAbort();
 }
 assert(){if(performance.now()>=this.deadline)this.cancellation.abort(transferError('periodic_ssh_deadline'));this.signal.throwIfAborted();}
 async waitReadable(){
  this.assert();if(this.input.readableLength)return;if(this.input.readableEnded||this.input.destroyed)fail('unexpected_eof');
  await new Promise((resolve,reject)=>{
   const cleanup=()=>{this.input.off('readable',ready);this.input.off('end',ended);this.input.off('close',ended);this.input.off('error',failed);this.signal?.removeEventListener('abort',aborted);};
   const ready=()=>{cleanup();resolve();},ended=()=>{cleanup();reject(transferError('periodic_ssh_unexpected_eof'));},failed=()=>{cleanup();reject(transferError('periodic_ssh_stream_failed'));},aborted=()=>{cleanup();reject(this.signal.reason);};
   this.input.once('readable',ready);this.input.once('end',ended);this.input.once('close',ended);this.input.once('error',failed);this.signal?.addEventListener('abort',aborted,{once:true});
   if(this.signal?.aborted)aborted();else if(this.input.readableLength)ready();else if(this.input.readableEnded||this.input.destroyed)ended();
  });this.assert();
 }
 async exact(bytes){
  const output=Buffer.alloc(bytes);let used=0;
  while(used<bytes){this.assert();if(!this.input.readableLength){await this.waitReadable();continue;}const part=this.input.read(Math.min(bytes-used,65536,this.input.readableLength));if(part===null){await this.waitReadable();continue;}if(!Buffer.isBuffer(part))fail('stream_invalid');part.copy(output,used);used+=part.length;}
  this.assert();return output;
 }
 async readJson(){
  if(this.reading)fail('read_busy');this.reading=true;try{const size=(await this.exact(4)).readUInt32BE();if(size<2||size>periodicSshWireLimits.jsonBytes)fail('frame_capacity');const body=await this.exact(size);let value;try{value=JSON.parse(new TextDecoder('utf8',{fatal:true}).decode(body));}catch{fail('frame_invalid');}if(!value||typeof value!=='object'||Array.isArray(value))fail('frame_invalid');return value;}finally{this.reading=false;}
 }
 async readBytes(bytes){if(this.reading)fail('read_busy');if(!Number.isSafeInteger(bytes)||bytes<0||bytes>periodicSshWireLimits.dataBytes)fail('data_capacity');this.reading=true;try{return await this.exact(bytes);}finally{this.reading=false;}}
 async write(bytes){
  this.assert();await new Promise((resolve,reject)=>{let settled=false;const finish=error=>{if(settled)return;settled=true;this.signal.removeEventListener('abort',aborted);this.output.off('close',closed);this.output.off('error',failed);if(error)reject(error);else resolve();},aborted=()=>finish(this.signal.reason),closed=()=>finish(transferError('periodic_ssh_stream_closed')),failed=()=>finish(transferError('periodic_ssh_stream_failed'));this.signal.addEventListener('abort',aborted,{once:true});this.output.once('close',closed);this.output.once('error',failed);if(this.signal.aborted){aborted();return;}if(this.output.destroyed||this.output.writableEnded){closed();return;}this.output.write(bytes,error=>finish(error?transferError('periodic_ssh_stream_failed'):null));});this.assert();
 }
 async writeJson(value){if(this.writing)fail('write_busy');if(!value||typeof value!=='object'||Array.isArray(value))fail('frame_invalid');const body=Buffer.from(JSON.stringify(value));if(body.length<2||body.length>periodicSshWireLimits.jsonBytes)fail('frame_capacity');const prefix=Buffer.alloc(4);prefix.writeUInt32BE(body.length);this.writing=true;try{await this.write(Buffer.concat([prefix,body]));}finally{this.writing=false;}}
 async writeBytes(bytes){if(this.writing)fail('write_busy');if(!Buffer.isBuffer(bytes)||bytes.length>periodicSshWireLimits.dataBytes)fail('data_capacity');this.writing=true;try{await this.write(bytes);}finally{this.writing=false;}}
 close(){if(this.closed)return;this.closed=true;clearTimeout(this.timer);this.cancellation.abort(transferError('periodic_ssh_closed'));this.signal.removeEventListener('abort',this.onAbort);}
}
