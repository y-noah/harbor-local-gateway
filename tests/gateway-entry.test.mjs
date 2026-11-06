import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp,mkdir,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {gzipSync,gunzipSync} from 'node:zlib';
import {createGatewayEntry} from '../gateway-entry.mjs';
import {gatewayEntryRequestBytes} from '../gateway-entry-forward.mjs';
import {gatewayClientAddress,gatewayClientAddressHeader,createRootUnixGatewayReceiver} from '../gateway-transport.mjs';

const limits={maxInflightRequests:2,maxQueuedRequests:3,maxQueuedBytes:32768,maxRequestBytes:4096,maxWaitMs:3000};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function until(check){const deadline=Date.now()+3000;while(!check()){assert.ok(Date.now()<deadline,'bounded HTTP state condition');await new Promise(resolve=>setImmediate(resolve));}}
async function listen(server,address){await new Promise((resolve,reject)=>{server.once('error',reject);if(typeof address==='string')server.listen(address,resolve);else server.listen(0,'127.0.0.1',resolve);});}
function request(entry,{method='POST',path='/v1/responses',body='',headers={},framing='length'}={}){
 let req;const done=new Promise((resolve,reject)=>{
  req=http.request({host:'127.0.0.1',port:entry.server.address().port,path,method,headers:{...(framing==='chunked'?{'Transfer-Encoding':'chunked'}:{'Content-Length':Buffer.byteLength(body)}),...headers},agent:false},res=>{
   const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.once('end',()=>{const bytes=Buffer.concat(chunks);resolve({status:res.statusCode,headers:res.headers,body:bytes.toString(),bytes});});res.once('error',reject);
  });req.once('error',reject);if(framing==='chunked'){const bytes=Buffer.from(body),split=Math.floor(bytes.length/2);req.write(bytes.subarray(0,split));req.end(bytes.subarray(split));}else req.end(body);
 });return {done,get req(){return req;}};
}
async function fixture(t){
 const directory=await mkdtemp(join(tmpdir(),'harbor-entry-')),servers=[],entries=[],sockets=new Set();
 const track=server=>{server.on('connection',socket=>{sockets.add(socket);socket.on('error',()=>{});socket.once('close',()=>sockets.delete(socket));});};
 t.after(async()=>{const closed=entries.map(entry=>entry.close());for(const socket of sockets)socket.destroy();await Promise.allSettled(closed);await Promise.all(servers.map(server=>new Promise(resolve=>server.close(()=>resolve()))));await rm(directory,{recursive:true,force:true});});
 return {directory,async backend(handler,name='backend.sock'){const server=http.createServer(handler),path=join(directory,name);track(server);servers.push(server);await listen(server,path);return path;},async entry(path,config={}){const entry=createGatewayEntry({route:{socketPath:path},limits:{...limits,...config}});track(entry.server);entries.push(entry);await listen(entry.server);return entry;}};
}

test('real Unix forwarding preserves Host Origin Cookie and session headers, while stripping caller transport identities',async t=>{
 const f=await fixture(t);let calls=0;
 const path=await f.backend(async(req,res)=>{calls++;const chunks=[];for await(const chunk of req)chunks.push(chunk);res.writeHead(200,{'Content-Type':'application/json','Set-Cookie':['one=synthetic; HttpOnly; SameSite=Strict','two=synthetic; Path=/']});res.end(JSON.stringify({headers:req.headers,peer:gatewayClientAddress(req),body:Buffer.concat(chunks).toString()}));}),entry=await f.entry(path);
 const reply=await request(entry,{body:'synthetic Ω',headers:{Host:'original.example:8092',Origin:'http://original.example:8092',Cookie:'session=synthetic',Authorization:'Bearer synthetic-credential','X-Harbor-Session':'existing-synthetic-session',Forwarded:'for=203.0.113.9','X-Forwarded-For':'203.0.113.9','X-Real-IP':'203.0.113.9',[gatewayClientAddressHeader]:'203.0.113.9',Connection:gatewayClientAddressHeader}}).done;
 assert.equal(reply.status,200);const observed=JSON.parse(reply.body);assert.equal(observed.body,'synthetic Ω');assert.equal(observed.headers.host,'original.example:8092');assert.equal(observed.headers.origin,'http://original.example:8092');assert.equal(observed.headers.cookie,'session=synthetic');assert.equal(observed.headers.authorization,'Bearer synthetic-credential');assert.equal(observed.headers['x-harbor-session'],'existing-synthetic-session');assert.equal(observed.headers[gatewayClientAddressHeader],'127.0.0.1');
 for(const name of ['forwarded','x-forwarded-for','x-real-ip'])assert.equal(observed.headers[name],undefined);
 assert.equal(observed.peer,null,'an ordinary Unix server does not independently establish root transport trust');assert.deepEqual(reply.headers['set-cookie'],['one=synthetic; HttpOnly; SameSite=Strict','two=synthetic; Path=/']);assert.equal(calls,1);
});

for(const method of ['GET','DELETE'])test('real Unix forwards '+method+' chunked request bytes without inventing Content-Length',async t=>{
 const f=await fixture(t);let calls=0;const path=await f.backend(async(req,res)=>{calls++;const chunks=[];for await(const chunk of req)chunks.push(chunk);res.end(JSON.stringify({method:req.method,body:Buffer.concat(chunks).toString(),length:req.headers['content-length']??null,encoding:req.headers['transfer-encoding']}));}),entry=await f.entry(path),body=JSON.stringify({synthetic:true,method,text:'two chunks Ω'});
 const reply=await request(entry,{method,body,framing:'chunked'}).done;assert.equal(reply.status,200);assert.deepEqual(JSON.parse(reply.body),{method,body,length:null,encoding:'chunked'});assert.equal(calls,1);
});

for(const encoding of ['gzip, chunked','gzip'])test('real Unix preserves binary response transfer coding '+encoding,async t=>{
 const f=await fixture(t),body=Buffer.from('data: synthetic Ω\n\n'),compressed=gzipSync(body);let calls=0;
 const path=await f.backend((req,res)=>{calls++;req.resume();res.writeHead(200,{'Transfer-Encoding':encoding,'Content-Type':'text/event-stream'});res.write(compressed.subarray(0,7));res.end(compressed.subarray(7));}),entry=await f.entry(path);
 const reply=await request(entry).done;assert.equal(reply.status,200);assert.equal(reply.headers['transfer-encoding'],'gzip, chunked');assert.equal(reply.headers['content-length'],undefined);assert.equal(reply.headers['content-encoding'],undefined);assert.deepEqual(reply.bytes,compressed);assert.deepEqual(gunzipSync(reply.bytes),body);assert.equal(calls,1);
});

test('real Unix preserves compressed request transfer coding and bytes',async t=>{
 const f=await fixture(t),compressed=gzipSync('synthetic request Ω');let calls=0;
 const path=await f.backend(async(req,res)=>{calls++;const chunks=[];for await(const chunk of req)chunks.push(chunk);assert.equal(req.headers['transfer-encoding'],'gzip, chunked');assert.equal(req.headers['content-length'],undefined);res.end(Buffer.concat(chunks));}),entry=await f.entry(path);
 const reply=await request(entry,{method:'DELETE',body:compressed,framing:'chunked',headers:{'Transfer-Encoding':'gzip, chunked'}}).done;assert.equal(reply.status,200);assert.deepEqual(reply.bytes,compressed);assert.equal(calls,1);
});

test('compressed response bytes stream before completion and cancellation closes the single upstream',{timeout:5000},async t=>{
 const f=await fixture(t),prefix=gzipSync('data: synthetic Ω\n\n');let calls=0,closed=false;
 const path=await f.backend((req,res)=>{calls++;req.resume();res.writeHead(200,{'Transfer-Encoding':'gzip, chunked'});res.write(prefix);res.once('close',()=>{closed=true;});}),entry=await f.entry(path);
 await new Promise((resolve,reject)=>{const req=http.get({host:'127.0.0.1',port:entry.server.address().port,path:'/',agent:false},res=>{assert.equal(res.headers['transfer-encoding'],'gzip, chunked');const chunks=[];res.on('data',chunk=>{chunks.push(chunk);const bytes=Buffer.concat(chunks);if(bytes.length>=prefix.length){assert.deepEqual(bytes,prefix);res.destroy();resolve();}});res.on('error',()=>{});});req.on('error',reject);});
 await until(()=>entry.status().inflight===0&&closed);assert.equal(calls,1);
});

test('SSE bytes arrive before upstream completion and a client disconnect closes the single upstream',async t=>{
 const f=await fixture(t),ended=deferred();let calls=0,completed=false;
 const path=await f.backend((_req,res)=>{calls++;res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache'});res.flushHeaders();res.write('data: first\n\n');res.once('close',()=>ended.resolve());}),entry=await f.entry(path);
 await new Promise((resolve,reject)=>{const req=http.get({host:'127.0.0.1',port:entry.server.address().port,path:'/v1/events',agent:false},res=>{let text='';res.on('data',chunk=>{text+=chunk;if(text.includes('\n\n')){assert.equal(completed,false);assert.equal(text,'data: first\n\n');res.destroy();resolve();}});res.on('error',()=>{});});req.on('error',reject);});
 await Promise.race([ended.promise,new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error('upstream cancellation deadline')),3000);timer.unref();ended.promise.finally(()=>clearTimeout(timer));})]);completed=true;assert.equal(calls,1);await until(()=>entry.status().inflight===0);
});

test('holding stops new forwarding, waits for old streams, and switches the queued request exactly once',async t=>{
 const f=await fixture(t),started=deferred(),release=deferred();let oldCalls=0,newCalls=0;
 const old=await f.backend(async(_req,res)=>{oldCalls++;started.resolve();await release.promise;if(!res.destroyed)res.end('old');},'old.sock'),next=await f.backend((_req,res)=>{newCalls++;res.end('new');},'new.sock'),entry=await f.entry(old,{maxInflightRequests:1});
 t.after(()=>release.resolve());const first=request(entry);await started.promise;entry.hold();const waiting=request(entry);await until(()=>entry.status().queued===1);assert.equal(oldCalls,1);assert.equal(newCalls,0);assert.throws(()=>entry.activate({socketPath:next}),{code:'gateway_entry_not_drained'});
 release.resolve();await entry.waitForDrain({timeoutMs:3000});entry.activate({socketPath:next});assert.equal((await first.done).body,'old');assert.equal((await waiting.done).body,'new');await until(()=>entry.status().inflight===0);assert.equal(oldCalls,1);assert.equal(newCalls,1);assert.equal(entry.status().queued,0);
});

test('capacity queues active traffic, while a drain timeout preserves the original request for a safe resume',async t=>{
 const f=await fixture(t),started=deferred(),release=deferred();let calls=0;
 const path=await f.backend(async(_req,res)=>{calls++;if(calls===1){started.resolve();await release.promise;}if(!res.destroyed)res.end('original');}),entry=await f.entry(path,{maxInflightRequests:1});t.after(()=>release.resolve());
 const first=request(entry);await started.promise;const second=request(entry);await until(()=>entry.status().queued===1);assert.equal(calls,1);entry.hold();
 await assert.rejects(entry.waitForDrain({timeoutMs:10}),{code:'gateway_entry_drain_timeout'});assert.equal(entry.status().inflight,1);assert.equal(calls,1);entry.resume();release.resolve();assert.equal((await first.done).body,'original');assert.equal((await second.done).body,'original');assert.equal(calls,2);
});

test('queue limits, cancellation, and controlled expiry never dispatch an unadmitted request',async t=>{
 const f=await fixture(t);let calls=0;const path=await f.backend((_req,res)=>{calls++;res.end('unexpected');}),entry=await f.entry(path,{maxQueuedRequests:1,maxWaitMs:100});entry.hold();
 const cancelled=request(entry,{path:'/?synthetic='+ 'x'.repeat(200)});void cancelled.done.catch(()=>{});await until(()=>entry.status().queued===1);assert.ok(entry.status().reservedQueuedBytes>200);assert.equal((await request(entry).done).status,503);cancelled.req.destroy();await until(()=>entry.status().queued===0);assert.equal(entry.status().reservedQueuedBytes,0);
 t.mock.timers.enable({apis:['setTimeout']});const expiring=request(entry,{path:'/?synthetic='+ 'x'.repeat(200)});await until(()=>entry.status().queued===1);assert.ok(entry.status().reservedQueuedBytes>200);t.mock.timers.tick(99);assert.equal(entry.status().queued,1);t.mock.timers.tick(1);assert.equal((await expiring.done).status,503);t.mock.timers.reset();assert.equal(calls,0);entry.resume();assert.equal(entry.status().queued,0);assert.equal(entry.status().reservedQueuedBytes,0);
});

test('reserved request line, header and body bytes bound a held queue before forwarding',async t=>{
 const f=await fixture(t);let calls=0;const path=await f.backend((_req,res)=>{calls++;res.end('unexpected');}),entry=await f.entry(path,{maxQueuedBytes:500,maxRequestBytes:64});entry.hold();
 const long=request(entry,{path:'/'+ 'a'.repeat(12000)});let rejected=false;void long.done.then(()=>{rejected=true;});await until(()=>rejected||entry.status().queued>0);assert.equal(entry.status().queued,0,'long target must be rejected before it can queue or expire');assert.equal((await long.done).status,503);assert.equal(entry.status().reservedQueuedBytes,0);assert.equal((await request(entry,{headers:{'X-Synthetic-Padding':'x'.repeat(600)}}).done).status,503);assert.equal((await request(entry,{body:'x'.repeat(65)}).done).status,413);assert.equal(entry.status().queued,0);assert.equal(calls,0);
});

test('request byte reservation includes exact HTTP syntax and unknown-length body allowance',()=>{
 const req={method:'DELETE',url:'/?synthetic=%CE%A9',httpVersion:'1.1',headers:{'content-length':'3'},rawHeaders:['Host','example.test','Content-Length','3']};
 const head=Buffer.byteLength('DELETE /?synthetic=%CE%A9 HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n\r\n');assert.equal(gatewayEntryRequestBytes(req,64),head+3);
 assert.equal(gatewayEntryRequestBytes({...req,headers:{},rawHeaders:['Host','example.test']},64),Buffer.byteLength('DELETE /?synthetic=%CE%A9 HTTP/1.1\r\nHost: example.test\r\n\r\n')+64);
});

test('a real held request fits the exact byte budget and one extra target byte is refused',async t=>{
 const f=await fixture(t);let calls=0;const path=await f.backend((_req,res)=>{calls++;res.end('unexpected');}),entry=await f.entry(path,{maxQueuedBytes:500,maxRequestBytes:64});entry.hold();
 const overhead=Buffer.byteLength('POST  HTTP/1.1\r\nContent-Length: 0\r\nHost: example.test\r\nConnection: close\r\n\r\n'),target='/'+ 'a'.repeat(500-overhead-1);
 const waiting=request(entry,{path:target,headers:{Host:'example.test'}});void waiting.done.catch(()=>{});await until(()=>entry.status().queued===1);assert.equal(entry.status().reservedQueuedBytes,500);
 assert.equal((await request(entry,{headers:{Host:'example.test'}}).done).status,503,'aggregate reservation cannot exceed the budget');waiting.req.destroy();await until(()=>entry.status().queued===0);assert.equal(entry.status().reservedQueuedBytes,0);
 assert.equal((await request(entry,{path:target+'a',headers:{Host:'example.test'}}).done).status,503);assert.equal(entry.status().reservedQueuedBytes,0);assert.equal(calls,0);
});

test('an early upstream response closes an unfinished upload before transport drain completes',async t=>{
 const f=await fixture(t),upstreamClosed=deferred();let calls=0;const path=await f.backend((req,res)=>{calls++;req.socket.once('close',()=>upstreamClosed.resolve());res.writeHead(403);res.end('early');}),entry=await f.entry(path),agent=new http.Agent({keepAlive:true});t.after(()=>agent.destroy());
 const result=await new Promise((resolve,reject)=>{const req=http.request({host:'127.0.0.1',port:entry.server.address().port,path:'/v1/responses',method:'POST',headers:{'Transfer-Encoding':'chunked'},agent},res=>{let body='';res.on('data',b=>{body+=b;});res.once('end',()=>resolve({status:res.statusCode,connection:res.headers.connection,body}));res.once('error',reject);});req.on('error',()=>{});req.write('synthetic prefix');});
 assert.equal(result.status,403);assert.equal(result.body,'early');assert.equal(result.connection,'close');await upstreamClosed.promise;await until(()=>entry.status().inflight===0);assert.equal(calls,1);
});

test('an upstream failure after receiving the body is never retried or replayed',async t=>{
 const f=await fixture(t);let calls=0,body='';const path=await f.backend(async(req)=>{calls++;for await(const chunk of req)body+=chunk.toString();req.socket.destroy();}),entry=await f.entry(path);
 const result=await request(entry,{body:'single synthetic mutation'}).done;assert.equal(result.status,502);assert.equal(calls,1);assert.equal(body,'single synthetic mutation');assert.equal(entry.status().queued,0);
});

test('a direct TCP request cannot make a forwarded identity trusted or forge a root-listener descriptor',async t=>{
 const f=await fixture(t),server=http.createServer((req,res)=>res.end(String(gatewayClientAddress(req))));await listen(server);t.after(()=>new Promise(resolve=>server.close(resolve)));
 const value=await new Promise((resolve,reject)=>http.get({host:'127.0.0.1',port:server.address().port,path:'/',headers:{[gatewayClientAddressHeader]:'203.0.113.88','X-Forwarded-For':'203.0.113.88'},agent:false},res=>{let b='';res.on('data',c=>{b+=c;});res.on('end',()=>resolve(b));}).on('error',reject));assert.equal(value,'127.0.0.1');
 const file=join(f.directory,'not-a-listener');await writeFile(file,'synthetic',{mode:0o600});await assert.rejects(createRootUnixGatewayReceiver({fd:0,path:file,verified:true},()=>{}),{code:'gateway_transport_listener_invalid'});
});

test('root-created inherited Unix listener is independently checked by a non-root backend',{
 skip:process.getuid?.()!==0?'需真实root监听FD/权限；非root不伪造正向证明':false,timeout:20000
},async t=>{
 const base=process.env.HARBOR_ENTRY_ROOT_DIRECTORY||'/run',directory=await mkdtemp(join(base,'harbor-entry-test-'));await mkdir(directory,{recursive:true,mode:0o755});const {chmod}=await import('node:fs/promises');await chmod(directory,0o755);const path=join(directory,'backend.sock'),module=new URL('../gateway-transport.mjs',import.meta.url).href;
 const backend=`import {createRootUnixGatewayReceiver,gatewayClientAddress} from ${JSON.stringify(module)};const app=await createRootUnixGatewayReceiver({fd:Number(process.argv[1]),path:process.argv[2]},(req,res)=>res.end(JSON.stringify({peer:gatewayClientAddress(req),uid:process.getuid(),host:req.headers.host})));process.stdout.write('ready\\n');for await(const input of process.stdin){if(input.toString().includes('stop'))break;}await app.close();`;
 const python=`import os,sys,socket,subprocess,signal\ns=socket.socket(socket.AF_UNIX);s.bind(sys.argv[1]);os.chmod(sys.argv[1],0o600);s.listen();fd=s.fileno()\ndef drop():\n os.setgroups([]);os.setgid(1000);os.setuid(1000)\np=subprocess.Popen([sys.argv[2],'--input-type=module','-e',sys.argv[3],str(fd),sys.argv[1]],pass_fds=(fd,),preexec_fn=drop,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)\ns.close()\ndef stop(*args):\n try:\n  if p.poll() is None:p.stdin.write(b'stop\\n');p.stdin.flush()\n  p.wait(timeout=5)\n except BaseException:\n  p.kill();p.wait()\n finally:\n  if args:sys.exit(1)\nsignal.signal(signal.SIGTERM,stop)\ntry:\n line=p.stdout.readline();sys.stdout.buffer.write(line);sys.stdout.flush()\n if line!=b'ready\\n':raise RuntimeError('backend readiness failed')\n sys.stdin.readline()\nfinally:stop()\nif p.returncode:raise RuntimeError('backend exited unsuccessfully')\n`;
 const child=spawn('/usr/bin/python3',['-I','-B','-c',python,path,process.execPath,backend],{stdio:['pipe','pipe','pipe']});const exited=once(child,'close');let stderr='';child.stderr.on('data',b=>{stderr+=b;});
 t.after(async()=>{child.stdin.end('stop\n');await Promise.race([exited,new Promise(resolve=>{const timer=setTimeout(()=>{child.kill('SIGTERM');resolve();},6000);timer.unref();exited.finally(()=>clearTimeout(timer));})]);await rm(directory,{recursive:true,force:true});});
 const ready=await Promise.race([once(child.stdout,'data').then(([b])=>b.toString()),exited.then(()=>{throw Error('root fixture exited: '+stderr);})]);assert.equal(ready,'ready\n');
 const f=await fixture(t),entry=await f.entry(path),reply=await request(entry,{headers:{Host:'original.example:8092',[gatewayClientAddressHeader]:'203.0.113.88'}}).done;assert.equal(reply.status,200);assert.deepEqual(JSON.parse(reply.body),{peer:'127.0.0.1',uid:1000,host:'original.example:8092'});
 const forbidden=`import http from 'node:http';http.get({socketPath:process.argv[1],path:'/',headers:{${JSON.stringify(gatewayClientAddressHeader)}:'203.0.113.88'}},()=>process.exit(2)).on('error',e=>process.exit(e.code==='EACCES'?0:3));`;
 const other=spawn(process.execPath,['--input-type=module','-e',forbidden,path],{uid:1000,gid:1000,stdio:'ignore'});assert.equal((await once(other,'close'))[0],0,'unprivileged code cannot reach the trusted root socket');
});
