import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import {once} from 'node:events';
import {mkdtemp,rm,lstat,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {gatewayClientAddress} from '../gateway-transport.mjs';
import {createGatewayEntryProcess,validateGatewayEntryProcessConfig} from '../gateway-entry-process.mjs';
const limits={maxInflightRequests:2,maxQueuedRequests:3,maxQueuedBytes:32768,maxRequestBytes:4096,maxWaitMs:1000};
const listen=(server,options)=>new Promise((resolve,reject)=>{server.once('error',reject);server.listen(options,()=>resolve(server.address()));});
async function call(options,body){return new Promise((resolve,reject)=>{const req=http.request({...options,agent:false},res=>{let text='';res.on('data',chunk=>text+=chunk);res.once('end',()=>resolve({status:res.statusCode,text,value:()=>JSON.parse(text)}));res.on('error',reject);});req.on('error',reject);req.end(body);});}
async function fixture(t,{timeout=80,handler}={}){
 const dir=await mkdtemp(join(tmpdir(),'entry-process-')),servers=[],runtime=[];t.after(async()=>{await Promise.all(runtime.map(item=>item.shutdown()));for(const server of servers){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await rm(dir,{recursive:true,force:true});});
 const probe=http.createServer(),address=await listen(probe,{host:'::',port:0});await new Promise(resolve=>probe.close(resolve));
 const config={listenPort:address.port,slots:{legacy:join(dir,'old.sock'),candidate:join(dir,'new.sock')},initialSlot:'legacy',limits,shutdownTimeoutMs:timeout},controlPath=join(dir,'control.sock');
 for(const [slot,path] of Object.entries(config.slots)){const server=http.createServer((req,res)=>{if(handler)return handler(req,res,slot);req.resume();if(req.url==='/hang'){res.write('stream');return;}res.end(slot);});servers.push(server);await listen(server,path);}
 return {config,controlPath,async start(){const item=await createGatewayEntryProcess({config,controlPath});runtime.push(item);return item;},command(command,expectedEpoch,slot){const body=JSON.stringify({command,expectedEpoch,...(slot?{slot}:{})});return call({socketPath:controlPath,path:'/control',method:'POST',headers:{'Content-Type':'application/json'}},body);},state:()=>call({socketPath:controlPath,path:'/state',method:'GET'}),data:()=>call({host:'127.0.0.1',port:config.listenPort,path:'/',method:'GET'})};
}
test('real private control CAS and held queue activate only preconfigured slots',{timeout:5000},async t=>{
 const f=await fixture(t),runtime=await f.start();assert.equal((await lstat(f.controlPath)).mode&0o777,0o600);assert.equal((await f.data()).text,'legacy');assert.equal((await f.state()).value().slot,'legacy');
 assert.equal((await f.command('activate',0,'candidate')).status,409);assert.equal((await f.command('hold',0)).status,200);assert.equal((await f.command('hold',0)).status,409);
 const waiting=f.data();assert.equal((await f.command('drain',1)).value().phase,'drained');assert.equal((await f.command('activate',2,'candidate')).status,200);assert.equal((await waiting).text,'candidate');assert.equal((await f.data()).text,'candidate');
 const value=(await f.state()).value();assert.deepEqual(Object.keys(value).sort(),['epoch','slot','phase','state','inflight','queued','reservedQueuedBytes'].sort());assert.equal(value.epoch,3);assert.equal((await call({host:'127.0.0.1',port:f.config.listenPort,path:'/state'})).text,'candidate','public listener forwards paths, never exposes control');await runtime.shutdown();await assert.rejects(access(f.controlPath));
});
test('invalid control schema paths and oversize bodies cannot change routing',{timeout:5000},async t=>{
 const f=await fixture(t);await f.start();for(const body of [{command:'activate',expectedEpoch:0,slot:'/tmp/evil.sock'},{command:'hold',expectedEpoch:0,path:'/tmp/evil'},{command:'hold',expectedEpoch:-1},null,{},'x'.repeat(2100)]){const result=await call({socketPath:f.controlPath,path:'/control',method:'POST',headers:{'Content-Type':'application/json'}},JSON.stringify(body));assert.equal(result.status,400);}assert.equal((await f.state()).value().epoch,0);
});
test('drain timeout preserves holding state and shutdown cancels stalled streams',{timeout:5000},async t=>{
 const f=await fixture(t,{timeout:30}),runtime=await f.start();const req=http.get({host:'127.0.0.1',port:f.config.listenPort,path:'/hang',agent:false});req.on('error',()=>{});await new Promise(resolve=>req.once('response',res=>{res.on('error',()=>{});res.once('data',resolve);}));assert.equal((await f.command('hold',0)).status,200);assert.equal((await f.command('drain',1)).status,409);assert.equal((await f.state()).value().phase,'holding');await runtime.shutdown();assert.equal(runtime.state().phase,'closed');await assert.rejects(access(f.controlPath));
});
test('failed public bind removes owned control socket but never pre-existing control',{timeout:5000},async t=>{
 const f=await fixture(t),blocker=http.createServer();await listen(blocker,{host:'::',port:f.config.listenPort});t.after(()=>new Promise(resolve=>blocker.close(resolve)));await assert.rejects(f.start(),{code:'EADDRINUSE'});await assert.rejects(access(f.controlPath));await new Promise(resolve=>blocker.close(resolve));
 const owner=http.createServer();await listen(owner,f.controlPath);t.after(()=>new Promise(resolve=>owner.close(resolve)));const before=await lstat(f.controlPath);await assert.rejects(f.start(),{code:'EADDRINUSE'});assert.equal((await lstat(f.controlPath)).ino,before.ino);
});
test('process configuration rejects wildcard targets, privileged ports, extra fields and invalid limits',()=>{
 const config={listenPort:18096,slots:{legacy:'/run/old.sock',candidate:'/run/new.sock'},initialSlot:'legacy',limits,shutdownTimeoutMs:100};assert.deepEqual(validateGatewayEntryProcessConfig(config),config);for(const invalid of [{...config,listenPort:80},{...config,host:'0.0.0.0'},{...config,slots:{...config.slots,candidate:'http://example.test'}},{...config,limits:{...limits,maxWaitMs:0}}])assert.throws(()=>validateGatewayEntryProcessConfig(invalid));
});
test('accepted close advances epoch once and closes private state and public listener',{timeout:5000},async t=>{
 const f=await fixture(t),runtime=await f.start(),result=await f.command('close',0);assert.equal(result.status,202);assert.equal(result.value().epoch,1);await runtime.shutdown();assert.equal(runtime.state().epoch,1);assert.equal(runtime.state().phase,'closed');await assert.rejects(f.data());await assert.rejects(access(f.controlPath));
});
test('pending drain leaves state readable and rejects overlapping mutation',{timeout:5000},async t=>{
 const f=await fixture(t,{timeout:1000});await f.start();const req=http.get({host:'127.0.0.1',port:f.config.listenPort,path:'/hang',agent:false});req.on('error',()=>{});await new Promise(resolve=>req.once('response',res=>{res.on('error',()=>{});res.once('data',resolve);}));await f.command('hold',0);const draining=f.command('drain',1);assert.equal((await f.state()).value().inflight,1);assert.equal((await f.command('resume',1)).status,409);req.destroy();const result=await draining;assert.equal(result.status,200);assert.equal(result.value().phase,'drained');assert.equal((await f.command('resume',2)).status,200);
});

test('dual-stack peer normalization only unwraps actual IPv4-mapped IPv6',()=>{
 for(const source of ['::ffff:192.0.2.1','::ffff:c000:201','0:0:0:0:0:FFFF:c000:0201'])assert.equal(gatewayClientAddress({socket:{remoteAddress:source}}),'192.0.2.1');
 for(const source of ['192.0.2.1','2001:db8::1','::1','::ffff:0:c000:201','::192.0.2.1'])assert.equal(gatewayClientAddress({socket:{remoteAddress:source}}),source);
 assert.equal(gatewayClientAddress({socket:{remoteAddress:'::ffff:999.0.0.1'}}),null);
});

test('shutdown deadline includes a TCP client with incomplete HTTP headers',{timeout:5000},async t=>{
 const f=await fixture(t,{timeout:40}),runtime=await f.start(),client=net.connect({host:'127.0.0.1',port:f.config.listenPort});client.on('error',()=>{});t.after(()=>client.destroy());await once(client,'connect');client.write('GET / HTTP/1.1\r\nHost: incomplete');await new Promise(resolve=>setImmediate(resolve));assert.equal(runtime.state().inflight,0);const started=performance.now();await runtime.shutdown();assert.ok(performance.now()-started<1000,'partial headers must not wait for the Node default parser timeout');assert.equal(runtime.state().phase,'closed');await assert.rejects(access(f.controlPath));
});

for(const [event,request,status] of [
 ['connect','CONNECT example.test:443 HTTP/1.1\r\nHost: example.test\r\n\r\n',405],
 ['upgrade','GET / HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n',426]
])test(`rejected ${event} survives a peer reset during the response`,{timeout:5000},async t=>{
 const f=await fixture(t);const runtime=await f.start();
 // Reset the real accepted TCP socket during rejection, exercising its error
 // lifecycle deterministically instead of depending on packet scheduling.
 runtime.server.prependOnceListener(event,(_req,socket)=>{
  const end=socket.end;socket.end=function(...args){
   const result=end.apply(this,args);
   this.destroy(Object.assign(new Error('peer reset'),{code:'ECONNRESET'}));
   return result;
  };
 });
 const client=net.connect({host:'127.0.0.1',port:f.config.listenPort});client.on('error',()=>{});
 await once(client,'connect');client.resume();client.end(request);
 await new Promise(resolve=>client.once('close',resolve));
 assert.equal((await f.data()).text,'legacy');assert.equal((await f.state()).value().phase,'active');
 const normal=net.connect({host:'127.0.0.1',port:f.config.listenPort});let response='';normal.on('data',chunk=>response+=chunk);normal.end(request);
 await once(normal,'close');assert.match(response,new RegExp(`^HTTP/1.1 ${status} `));
});

async function untilState(check){const deadline=Date.now()+2000;while(!check()){assert.ok(Date.now()<deadline,'connection lifecycle reaches its terminal state');await new Promise(resolve=>setTimeout(resolve,5));}}

test('upstream SSE reset closes only its response, releases admission and never replays',{timeout:5000},async t=>{
 let calls=0,upstream,upstreamClosed=false;const prefix='data: synthetic partial\n\n';
 const f=await fixture(t,{handler(req,res,slot){req.resume();if(req.url!=='/reset-stream'){res.end(slot);return;}calls++;upstream=res;res.on('close',()=>{upstreamClosed=true;});res.writeHead(200,{'Content-Type':'text/event-stream'});res.write(prefix);}}),runtime=await f.start();
 let text='',aborted=false,completed=false,responses=0;
 await new Promise((resolve,reject)=>{const request=http.get({host:'127.0.0.1',port:f.config.listenPort,path:'/reset-stream',agent:false},response=>{responses++;response.on('data',chunk=>{text+=chunk;if(text.length>=prefix.length){assert.equal(text,prefix);upstream.destroy();}});response.once('aborted',()=>{aborted=true;resolve();});response.once('end',()=>{completed=true;reject(Error('truncated SSE must not look complete'));});response.on('error',()=>{});});request.on('error',reject);});
 await untilState(()=>runtime.state().inflight===0&&upstreamClosed);assert.equal(aborted,true);assert.equal(completed,false);assert.equal(text,prefix);assert.equal(calls,1);assert.equal(responses,1);assert.equal((await f.data()).text,'legacy');assert.equal((await f.state()).value().phase,'active');
});

test('TCP reset during an unfinished upload cancels the single upstream and keeps entry healthy',{timeout:5000},async t=>{
 let calls=0,body='',upstreamClosed=false,received;const started=new Promise(resolve=>received=resolve);
 const f=await fixture(t,{handler(req,res,slot){if(req.url!=='/upload-reset'){req.resume();res.end(slot);return;}calls++;req.on('data',chunk=>{body+=chunk;received();});req.on('error',()=>{});res.on('close',()=>{upstreamClosed=true;});}}),runtime=await f.start();
 const request=http.request({host:'127.0.0.1',port:f.config.listenPort,path:'/upload-reset',method:'POST',headers:{'Transfer-Encoding':'chunked'},agent:false});request.on('error',()=>{});request.write('synthetic upload prefix');await started;request.socket.resetAndDestroy();
 await untilState(()=>runtime.state().inflight===0&&upstreamClosed);assert.equal(body,'synthetic upload prefix');assert.equal(calls,1);assert.equal((await f.data()).text,'legacy');assert.equal((await f.state()).value().phase,'active');
});
