import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {EventEmitter} from 'node:events';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createGatewayProbe} from '../tools/capacity/gateway-probe.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createDeviceTlsServer} from '../device-transport.mjs';
import {capacityAgent,requestCapacityTarget} from '../tools/capacity/http-client.mjs';

const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const until=async predicate=>{const deadline=performance.now()+10000;for(;;){if(await predicate())return;assert.ok(performance.now()<deadline,'required handler condition was not observed');await delay(5);}};
const appFor=server=>({server,store:{observe:async()=>1,finish:async()=>({settled:true})},sessionHandoffs:{arrive:()=>({bind:async()=>{}})},status:()=>({})});

test('HTTP and device TLS cancellation does not become a handler settlement before its actual promise finishes',{timeout:30000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-handler-settlement-')),events=[],pending=new Map(),agents=[];let probe,plain,tls;
 try{
  const certificates=await syntheticCertificates(root),handler=async(req,res)=>{req.resume();const gate=deferred();pending.set(req.headers['x-capacity-trace'],gate);res.writeHead(200,{'content-type':'application/json'});res.write('{"ok":');await gate.promise;res.end('true}');};
  plain=http.createServer(handler);await new Promise(resolve=>plain.listen(0,'127.0.0.1',resolve));tls=createDeviceTlsServer({...certificates.server,ca:certificates.ca,handler});await tls.listen();
  probe=createGatewayProbe({sink:event=>events.push(event)});probe.attach({...appFor(plain),deviceTls:tls});
  for(const secure of [false,true]){
   const trace=secure?'handler-tls':'handler-http',origin=(secure?'https':'http')+'://127.0.0.1:'+(secure?tls.server.address().port:plain.address().port),agent=capacityAgent(origin,secure?{ca:certificates.ca,...certificates.client}:{});agents.push(agent);
   const abort=new AbortController(),request=requestCapacityTarget({origin,path:'/api/admin/status',agent,headers:{'x-capacity-trace':trace},signal:abort.signal});await until(()=>pending.has(trace));abort.abort();assert.equal((await request).success,false);
   await until(()=>events.some(event=>event.kind==='gateway-response-abort'&&event.trace===trace));assert.equal(events.some(event=>event.kind==='gateway-handler-settled'&&event.trace===trace),false);assert.equal(probe.status().pendingHandlers,1);
   pending.get(trace).resolve();await until(()=>events.some(event=>event.kind==='gateway-handler-settled'&&event.trace===trace));const start=events.find(event=>event.kind==='gateway-request-start'&&event.trace===trace),settled=events.filter(event=>event.kind==='gateway-handler-settled'&&event.trace===trace);assert.equal(settled.length,1);assert.equal(settled[0].wireId,start.wireId);assert.equal(settled[0].outcome,'resolved');assert.equal(probe.status().pendingHandlers,0);
  }
 }finally{for(const gate of pending.values())gate.resolve();for(const agent of agents)agent.destroy();await probe?.close();await tls?.close({force:true});if(plain){plain.closeAllConnections();await new Promise(resolve=>plain.close(resolve));}await rm(root,{recursive:true,force:true});}
});

test('unawaitable handler returns remain unknown without invoking arbitrary then getters',async()=>{
 let thenReads=0;const events=[],server=http.createServer((req,res)=>{req.resume();res.end('{}');return {get then(){thenReads++;throw new Error('SYNTHETIC_PRIVATE_THENABLE');}};}),probe=createGatewayProbe({sink:event=>events.push(event)});probe.attach(appFor(server));await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 try{const result=await requestCapacityTarget({origin:'http://127.0.0.1:'+server.address().port,path:'/health',headers:{'x-capacity-trace':'unknown-handler'}});assert.equal(result.success,true);assert.equal(thenReads,0);assert.equal(events.some(event=>event.kind==='gateway-handler-settled'),false);assert.equal(events.filter(event=>event.kind==='gateway-handler-unobservable').length,1);assert.equal(JSON.stringify(events).includes('SYNTHETIC_PRIVATE_THENABLE'),false);}finally{await probe.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});

test('promise return identity, receiver and rejection remain intact while observer failures stay outside the request',async()=>{
 const server=new EventEmitter(),events=[];let reject,expectedThis;const result=new Promise((_resolve,failed)=>reject=failed),original=function(){expectedThis=this;return result;};server.on('request',original);const probe=createGatewayProbe({sink:event=>{events.push(event);return Promise.reject(new Error('SYNTHETIC_PRIVATE_SINK'));}});probe.attach(appFor(server));
 try{const req=Object.assign(new EventEmitter(),{method:'GET',url:'/health',headers:{'x-capacity-trace':'rejected-handler'}}),res=Object.assign(new EventEmitter(),{statusCode:500,end(){}}),returned=server.listeners('request')[0].call(server,req,res);assert.equal(returned,result);assert.equal(expectedThis,server);const failure=new Error('SYNTHETIC_PRIVATE_HANDLER');reject(failure);await assert.rejects(returned,error=>error===failure);await until(()=>probe.status().pendingHandlers===0&&probe.status().dropped>0);assert.equal(events.filter(event=>event.kind==='gateway-handler-settled')[0].outcome,'rejected');assert.equal(JSON.stringify(events).includes('SYNTHETIC_PRIVATE_HANDLER'),false);assert.equal(JSON.stringify(events).includes('SYNTHETIC_PRIVATE_SINK'),false);}finally{await probe.close();}
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('real PostgreSQL row-lock cancellation settles only after the admin transaction finishes',{skip:!binDirectory?'需独立私有PG/gateway/worker，不能以socket关闭代事务终态':false,timeout:120000},async()=>{
 const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target,unlock,blocking;
 try{
  target=await createTarget({profile:'selfcheck',binDirectory,libraryDirectory,employeeCount:1});const memberId=target.employeeActors[0].memberId,locked=deferred(),gate=deferred();unlock=gate.resolve;
  blocking=target.store.transaction(async()=>{await target.store.get('SELECT id FROM harbor.members WHERE id=$1 FOR UPDATE',memberId);locked.resolve();await gate.promise;});await locked.promise;
  const trace='real-handler-row-lock',abort=new AbortController(),request=requestCapacityTarget({origin:target.gateway.ready.tlsOrigin,path:'/api/admin/members/'+memberId,method:'PATCH',agent:target.employeeActors[0].agent,headers:{...target.adminActors[0].headers,'content-type':'application/json','x-capacity-trace':trace},body:{name:'synthetic-committed-after-client-cancel'},signal:abort.signal});
  await until(async()=>{const row=await target.store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%harbor.members%' AND pid<>pg_backend_pid()");return row.n>0;});abort.abort();assert.equal((await request).success,false);
  await until(()=>target.events.some(event=>event.trace===trace&&event.kind==='gateway-response-abort'));assert.equal(target.events.some(event=>event.trace===trace&&event.kind==='gateway-handler-settled'),false);
  unlock();await blocking;await until(()=>target.events.some(event=>event.trace===trace&&event.kind==='gateway-handler-settled'));assert.equal((await target.store.get('SELECT name FROM harbor.members WHERE id=$1',memberId)).name,'synthetic-committed-after-client-cancel');const start=target.events.find(event=>event.trace===trace&&event.kind==='gateway-request-start'),settled=target.events.filter(event=>event.trace===trace&&event.kind==='gateway-handler-settled');assert.equal(settled.length,1);assert.equal(settled[0].wireId,start.wireId);
 }finally{unlock?.();await blocking?.catch(()=>{});await target?.close();}
});
