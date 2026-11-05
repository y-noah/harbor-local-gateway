import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createDeviceTlsServer,deviceTransportProof} from '../device-transport.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {capacityAgent,requestCapacityTarget,capacityHttpDefaultTimeoutMs} from '../tools/capacity/http-client.mjs';

const sockets=agent=>Object.values(agent.freeSockets).flat();
async function until(predicate){const end=Date.now()+10000;for(;;){if(await predicate())return;assert.ok(Date.now()<end,'owned keep-alive condition timed out');await delay(5);}}
async function fixture(t,operation){
 const directory=await mkdtemp(join(tmpdir(),'harbor-keepalive-test-')),runtime=createOwnedRuntime(),agents=[],calls=[],firstSockets=new Map(),closedCases=new Set();let service,releaseReady=false,releaseCheck=()=>{};
 try{
  const certificates=await syntheticCertificates(directory);
  service=createDeviceTlsServer({...certificates.server,ca:certificates.ca,handler:(req,res)=>{
   const url=new URL(req.url,'https://synthetic.invalid'),kind=url.searchParams.get('case')??'default';calls.push({kind,path:url.pathname,authorized:deviceTransportProof(req)?.authorized===true,socket:req.socket,sessionReused:req.socket.isSessionReused()});
   if(!firstSockets.has(kind)){firstSockets.set(kind,req.socket);req.socket.once('close',()=>{closedCases.add(kind);releaseCheck();});}
   if(url.pathname==='/no-hint'){res.setHeader('Connection','keep-alive');}
   if(url.pathname==='/short-hint'){res.setHeader('Connection','keep-alive');res.setHeader('Keep-Alive','timeout='+url.searchParams.get('hint'));}
   if(url.pathname==='/race'){res.setHeader('Connection','keep-alive');res.setHeader('Keep-Alive','timeout='+url.searchParams.get('hint'));}
   if(url.pathname==='/stream'){
    res.setHeader('content-type','text/event-stream');res.flushHeaders();const held=Number(url.searchParams.get('hold'));if(held>0){const timer=setTimeout(()=>res.end('data: {"type":"response.output_text.delta","delta":"synthetic"}\n\ndata: {"type":"response.completed","response":{"status":"completed","id":"synthetic-keepalive"}}\n\n'),held);res.once('close',()=>clearTimeout(timer));}return;
   }
   res.setHeader('content-type','application/json');res.end('{"ok":true}');
  }});
  const address=await service.listen(),origin='https://127.0.0.1:'+address.port;
  const agent=options=>{const value=capacityAgent(origin,{ca:certificates.ca,...certificates.client,...options});agents.push(value);return value;};
  await operation({directory,runtime,service,origin,agent,calls,firstSockets,closedCases,setReleaseCheck:callback=>{releaseCheck=callback;},setReleaseReady:()=>{releaseReady=true;},getReleaseReady:()=>releaseReady,call:(selected,path,options={})=>requestCapacityTarget({origin,path,agent:selected,timeoutMs:10000,signal:t.signal,...options})});
 }finally{for(const agent of agents)agent.destroy();await service?.close({force:true});await runtime.close();await rm(directory,{recursive:true,force:true});}
}

test('Agent default and explicit undefined share the existing ceiling while explicit values retain support',()=>{
 for(const [options,expected] of [[{},capacityHttpDefaultTimeoutMs],[{timeout:undefined},capacityHttpDefaultTimeoutMs],[{timeout:0},0],[{timeout:37},37]]){
  const agent=capacityAgent('https://127.0.0.1',options);try{assert.equal(agent.options.timeout,expected);assert.equal(agent.keepAlive,true);assert.equal(agent.maxCachedSessions,100);}finally{agent.destroy();}
 }
});

test('actual device TLS factory hint retires a free socket before the peer advertised idle deadline',{timeout:20000},t=>fixture(t,async f=>{
 const agent=f.agent(),freed=new Promise(resolve=>agent.once('free',socket=>resolve({socket,timeout:socket.timeout})));let header;const first=await f.call(agent,'/health',{onResponseHeaders:value=>{header=value['keep-alive'];}});assert.equal(first.success,true);const observed=await freed,socket=observed.socket,hint=Number(/^timeout=(\d+)/.exec(header)?.[1])*1000,selected=hint-agent.agentKeepAliveTimeoutBuffer;
 assert.equal(hint,f.service.server.keepAliveTimeout);assert.ok(selected>0);assert.equal(observed.timeout,selected);assert.ok(selected<hint);assert.equal(agent.options.timeout,capacityHttpDefaultTimeoutMs);await until(()=>socket.destroyed&&sockets(agent).length===0);assert.equal(f.calls.length,1);assert.equal(f.calls[0].authorized,true);
}));

test('absent hints retain the configured ceiling and short hints refuse unsafe idle reuse',{timeout:10000},t=>fixture(t,async f=>{
 const absent=f.agent();let missing='unset';assert.equal((await f.call(absent,'/no-hint?case=absent',{onResponseHeaders:value=>{missing=value['keep-alive'];}})).success,true);await until(()=>sockets(absent).length===1);assert.equal(missing,undefined);assert.equal(sockets(absent)[0].timeout,capacityHttpDefaultTimeoutMs);absent.destroy();
 const short=f.agent(),hint=Math.floor(short.agentKeepAliveTimeoutBuffer/1000);assert.equal((await f.call(short,'/short-hint?case=short&hint='+hint)).success,true);await until(()=>f.closedCases.has('short'));assert.equal(sockets(short).length,0);assert.equal((await f.call(short,'/short-hint?case=short&hint='+hint)).success,true);assert.equal(sockets(short).length,0);assert.notEqual(f.calls.at(-1).socket,f.calls.at(-2).socket);
}));

test('reuse and fresh TLS profiles retain connection and session behavior with the default ceiling',{timeout:10000},t=>fixture(t,async f=>{
 for(const connection of ['reuse-tls','new-tls']){
  const agent=f.agent({connection}),one=await f.call(agent,'/health?case='+connection);assert.equal(one.success,true);if(connection==='reuse-tls')await until(()=>sockets(agent).length===1);const two=await f.call(agent,'/health?case='+connection);assert.equal(two.success,true);const requests=f.calls.filter(row=>row.kind===connection);assert.equal(requests.length,2);assert.ok(requests.every(row=>row.authorized));assert.equal(new Set(requests.map(row=>row.socket)).size,connection==='reuse-tls'?1:2);assert.equal(two.reusedSocket,connection==='reuse-tls');if(connection==='new-tls'){assert.equal(agent.keepAlive,false);assert.equal(agent.maxCachedSessions,0);assert.ok(requests.every(row=>!row.sessionReused));}
  agent.destroy();
 }
}));

test('an active SSE survives its inherited free-socket timer and retains the original request deadline',{timeout:20000},t=>fixture(t,async f=>{
 const agent=f.agent();assert.equal((await f.call(agent,'/health')).success,true);await until(()=>sockets(agent).length===1);const socket=sockets(agent)[0],selected=socket.timeout,held=selected+Math.max(100,agent.agentKeepAliveTimeoutBuffer/2);let activeTimeouts=0,destroyedByTimeout=0;
 socket.on('timeout',()=>{if(Object.values(agent.sockets).some(values=>values.includes(socket))){activeTimeouts++;if(socket.destroyed)destroyedByTimeout++;}});
 const result=await f.call(agent,'/stream?hold='+held,{mode:'sse',timeoutMs:Math.ceil(held+5000)});assert.equal(result.success,true);assert.equal(result.httpEnded,true);assert.equal(result.reusedSocket,true);assert.ok(activeTimeouts>=1);assert.equal(destroyedByTimeout,0);assert.equal(f.calls.length,2);assert.equal(new Set(f.calls.map(row=>row.socket)).size,1);
}));

test('scaled Agent timeout is not a shorter active SSE total deadline',{timeout:10000},t=>fixture(t,async f=>{
 const timeout=40,agent=f.agent({timeout});let activeTimeouts=0,destroyedByTimeout=0;const create=agent.createConnection.bind(agent);agent.createConnection=(...args)=>{const socket=create(...args);socket.on('timeout',()=>{if(Object.values(agent.sockets).some(values=>values.includes(socket))){activeTimeouts++;if(socket.destroyed)destroyedByTimeout++;}});return socket;};
 const result=await f.call(agent,'/stream?hold='+(timeout*4),{mode:'sse',timeoutMs:2000});assert.equal(result.success,true);assert.equal(result.httpEnded,true);assert.ok(result.durationMs>timeout);assert.ok(activeTimeouts>=1);assert.equal(destroyedByTimeout,0);assert.equal(f.calls.length,1);
}));

test('explicit cancellation and the original short request deadline still terminate active SSE without retry',{timeout:10000},t=>fixture(t,async f=>{
 const cancelled=f.agent(),controller=new AbortController(),running=f.call(cancelled,'/stream?case=cancel',{mode:'sse',signal:controller.signal});await until(()=>f.calls.some(row=>row.kind==='cancel'));controller.abort();const cut=await running;assert.equal(cut.error,'client_cancelled');assert.equal(cut.success,false);assert.equal(cut.httpEnded,false);
 const deadline=f.agent(),expired=await f.call(deadline,'/stream?case=deadline',{mode:'sse',timeoutMs:80});assert.equal(expired.error,'client_deadline');assert.equal(expired.success,false);assert.equal(expired.httpEnded,false);assert.equal(f.calls.filter(row=>row.kind==='cancel').length,1);assert.equal(f.calls.filter(row=>row.kind==='deadline').length,1);
}));

// Block only the owned client event loop. The controller closes the real peer
// after both clients report readiness, then releases them via a private file.
// This deterministic close injection is distinct from the C8 unknown timing.
const raceChild=String.raw`
 import {readFile} from 'node:fs/promises';import {existsSync} from 'node:fs';import {join} from 'node:path';import {setTimeout as delay} from 'node:timers/promises';
 import {capacityAgent,requestCapacityTarget,capacityHttpDefaultTimeoutMs} from MODULE;
 const cfg=JSON.parse(process.argv[1]),agent=capacityAgent(cfg.origin,{ca:await readFile(join(cfg.directory,'ca.pem')),cert:await readFile(join(cfg.directory,'client.pem')),key:await readFile(join(cfg.directory,'client.key')),...(cfg.kind==='zero'?{timeout:0}:{})});
 const pool=()=>Object.values(agent.freeSockets).flat();let header,firstFree;agent.on('free',socket=>{firstFree??={socket,timeout:socket.timeout};});const call=()=>requestCapacityTarget({origin:cfg.origin,path:'/race?case='+cfg.kind+'&hint='+cfg.hint,agent,timeoutMs:10000,onResponseHeaders:value=>{header??=value['keep-alive'];}});
 try{if(!(await call()).success)throw Error('race_warmup_failed');const end=Date.now()+10000;while(!firstFree){if(Date.now()>end)throw Error('race_free_timeout');await delay(5);}const initialSocketTimeout=firstFree.timeout,retire=Number(/^timeout=(\d+)/.exec(header)?.[1])*1000-agent.agentKeepAliveTimeoutBuffer;await delay(retire+Math.max(20,agent.agentKeepAliveTimeoutBuffer/2));const freeBefore=pool().length;process.send({kind:'blocked',case:cfg.kind});const until=Date.now()+10000;while(!existsSync(cfg.release)){if(Date.now()>until)throw Error('race_release_timeout');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5);}const result=await call();await new Promise((resolve,reject)=>process.send({kind:'result',case:cfg.kind,initialSocketTimeout,agentTimeout:agent.options.timeout,freeBefore,second:{success:result.success,error:result.error,httpEnded:result.httpEnded,reusedSocket:result.reusedSocket,networkFailure:result.networkFailure??null}},error=>error?reject(error):resolve()));}finally{agent.destroy();process.disconnect();}
`;

test('default retires idle hinted sockets before a controlled peer-close race while explicit zero preserves opt-out',{timeout:20000},t=>fixture(t,async f=>{
 const reference=f.agent(),hint=Math.floor(reference.agentKeepAliveTimeoutBuffer/1000)+1,selected=hint*1000-reference.agentKeepAliveTimeoutBuffer,release=join(f.directory,'release');reference.destroy();f.service.server.keepAliveTimeout=capacityHttpDefaultTimeoutMs;
 const blocked=new Set();let releaseStarted=false,rejectRelease;const releaseFailure=new Promise((_,reject)=>{rejectRelease=reject;});const maybeRelease=()=>{if(!f.getReleaseReady()||releaseStarted||!['zero','default'].every(kind=>f.closedCases.has(kind)))return;releaseStarted=true;void writeFile(release,'peer-closed',{flag:'wx'}).catch(rejectRelease);};f.setReleaseCheck(maybeRelease);
 const jobs=['zero','default'].map(kind=>new Promise((resolve,reject)=>{
  const code=raceChild.replace('MODULE',JSON.stringify(new URL('../tools/capacity/http-client.mjs',import.meta.url).href)),child=f.runtime.start(process.execPath,['--input-type=module','-e',code,JSON.stringify({kind,directory:f.directory,origin:f.origin,hint,release})],{role:'helper',stdio:['ignore','ignore','ignore','ipc'],env:{NODE_OPTIONS:'',NODE_PATH:''}});let result;
  child.on('message',message=>{if(message.kind==='result')result=message;else if(message.kind==='blocked'){blocked.add(message.case);if(blocked.size===2){f.setReleaseReady();for(const socket of f.firstSockets.values())if(!socket.destroyed)socket.destroy();maybeRelease();}}});child.once('error',reject);child.once('exit',code=>code===0&&result?resolve(result):reject(Error('owned_idle_race_failed')));
 }));
 const results=await Promise.race([Promise.all(jobs),releaseFailure]),zero=results.find(row=>row.case==='zero'),normal=results.find(row=>row.case==='default');assert.equal(zero.agentTimeout,0);assert.equal(zero.initialSocketTimeout,0);assert.equal(zero.freeBefore,1);assert.equal(zero.second.success,false);assert.equal(zero.second.httpEnded,false);assert.equal(zero.second.networkFailure.reusedSocket,true);assert.ok(['ECONNRESET','EPIPE'].includes(zero.second.networkFailure.code));assert.equal(f.calls.filter(row=>row.kind==='zero').length,1);
 assert.equal(normal.agentTimeout,capacityHttpDefaultTimeoutMs);assert.equal(normal.initialSocketTimeout,selected);assert.equal(normal.freeBefore,0);assert.equal(normal.second.success,true);assert.equal(normal.second.reusedSocket,false);assert.equal(f.calls.filter(row=>row.kind==='default').length,2);
}));
