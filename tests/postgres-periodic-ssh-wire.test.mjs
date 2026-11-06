import test from 'node:test';
import assert from 'node:assert/strict';
import {PassThrough,Writable} from 'node:stream';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {performance} from 'node:perf_hooks';
import {PeriodicSshWire} from '../postgres-periodic-ssh-wire.mjs';

function pair(options){const input=new PassThrough(),output=new PassThrough(),wire=new PeriodicSshWire(input,output,options);return {input,output,wire};}
test('fragments remain bounded and yield while a partial header or body is buffered',async()=>{
 const {input,wire}=pair(),body=Buffer.from('{"kind":"cipher"}'),prefix=Buffer.alloc(4);prefix.writeUInt32BE(body.length);const frame=Buffer.concat([prefix,body]),reading=wire.readJson();
 try{for(const byte of frame){input.write(Buffer.from([byte]));await new Promise(resolve=>setImmediate(resolve));}assert.deepEqual(await reading,{kind:'cipher'});}finally{wire.close();}
});
test('capacity, invalid UTF8, malformed JSON and EOF fail without accepting controls',async()=>{
 for(const body of [Buffer.from([0xff,0xff]),Buffer.from('[]'),Buffer.from('{x')]){const {input,wire}=pair(),prefix=Buffer.alloc(4);prefix.writeUInt32BE(body.length);input.end(Buffer.concat([prefix,body]));try{await assert.rejects(wire.readJson(),{code:'periodic_ssh_frame_invalid'});}finally{wire.close();}}
 for(const size of [0,4097,0xffffffff]){const {input,wire}=pair(),prefix=Buffer.alloc(4);prefix.writeUInt32BE(size);input.end(prefix);try{await assert.rejects(wire.readJson(),{code:'periodic_ssh_frame_capacity'});}finally{wire.close();}}
 const {input,wire}=pair();input.end(Buffer.from([0,0]));try{await assert.rejects(wire.readJson(),{code:'periodic_ssh_unexpected_eof'});await assert.rejects(wire.readBytes(16777217),{code:'periodic_ssh_data_capacity'});}finally{wire.close();}
});
test('deadline, cancellation and close settle pending reads and blocked writes',async()=>{
 for(const mode of ['deadline','abort','close'])for(const operation of ['read','write']){
  const cancellation=new AbortController(),input=new PassThrough(),output=new Writable({write(_bytes,_encoding,_callback){}}),wire=new PeriodicSshWire(input,output,{signal:cancellation.signal,timeoutMs:mode==='deadline'?100:5000});
  const pending=operation==='read'?wire.readJson():wire.writeBytes(Buffer.from('cipher'));const rejected=assert.rejects(pending,mode==='deadline'?{code:'periodic_ssh_deadline'}:mode==='close'?{code:'periodic_ssh_closed'}:{name:'AbortError'});
  if(mode==='abort')cancellation.abort();if(mode==='close')wire.close();try{await rejected;assert.equal(input.destroyed,true);assert.equal(output.destroyed,true);}finally{wire.close();}
 }
});
test('monotonic deadline rejects buffered frames even before the timer can run',async()=>{
 const {input,wire}=pair({timeoutMs:100}),body=Buffer.from('{}'),prefix=Buffer.alloc(4);prefix.writeUInt32BE(body.length);input.write(Buffer.concat([prefix,body]));while(performance.now()<wire.deadline+1){}assert.equal(wire.signal.aborted,false);
 try{await assert.rejects(wire.readJson(),{code:'periodic_ssh_deadline'});}finally{wire.close();}
});
test('an error on the opposite stream cancels pending reads and blocked writes immediately',async()=>{
 for(const operation of ['read','write']){const input=new PassThrough(),output=new Writable({write(_bytes,_encoding,_callback){}}),wire=new PeriodicSshWire(input,output,{timeoutMs:5000}),pending=operation==='read'?wire.readJson():wire.writeBytes(Buffer.alloc(1)),rejected=assert.rejects(pending,{code:'periodic_ssh_stream_failed'});(operation==='read'?output:input).emit('error',Error('synthetic cross-direction error'));try{await rejected;}finally{wire.close();}}
});
test('close retains error handling through asynchronous destroy and then removes listeners',async()=>{
 const input=new PassThrough(),output=new Writable({write(_bytes,_encoding,callback){callback();},destroy(_error,callback){setImmediate(()=>callback(Error('synthetic delayed destroy error')));}}),wire=new PeriodicSshWire(input,output),closed=Promise.all([input,output].map(stream=>new Promise(resolve=>stream.once('close',resolve))));
 wire.close();assert.equal(output.listenerCount('error'),1);await closed;assert.equal(input.listenerCount('error'),0);assert.equal(output.listenerCount('error'),0);wire.close();
});
test('standard subprocess pipes exchange bounded controls and ciphertext with clean exit',async()=>{
 const moduleUrl=new URL('../postgres-periodic-ssh-wire.mjs',import.meta.url).href;
 const program=`import {PeriodicSshWire} from ${JSON.stringify(moduleUrl)};const wire=new PeriodicSshWire(process.stdin,process.stdout,{timeoutMs:5000});try{const request=await wire.readJson();if(request.kind!=='probe'||request.bytes!==32||Object.keys(request).length!==2)throw Error('invalid');const bytes=await wire.readBytes(32);await wire.writeJson({kind:'accepted',bytes:bytes.length});await wire.writeBytes(bytes);}finally{wire.close();}`;
 const child=spawn(process.execPath,['--input-type=module','-e',program],{stdio:['pipe','pipe','ignore']}),exited=once(child,'exit'),wire=new PeriodicSshWire(child.stdout,child.stdin,{timeoutMs:5000}),bytes=Buffer.alloc(32,7);
 try{await wire.writeJson({kind:'probe',bytes:32});await wire.writeBytes(bytes);assert.deepEqual(await wire.readJson(),{kind:'accepted',bytes:32});assert.deepEqual(await wire.readBytes(32),bytes);assert.deepEqual(await exited,[0,null]);}finally{wire.close();if(child.exitCode===null)child.kill('SIGKILL');await exited;}
});
