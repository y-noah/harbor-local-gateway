import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {randomBytes} from 'node:crypto';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {createGatewayProbe} from '../tools/capacity/gateway-probe.mjs';
import {requestCapacityTarget} from '../tools/capacity/http-client.mjs';

for(const retry of [false,true])test('queued settlements keep their admitted request context'+(retry?' across encrypted spool replay':' across a shared drain'),{timeout:15000},async()=>{
 const directory=await mkdtemp('/tmp/harbor-probe-recovery-'),events=[],settled=new Set(),both=Promise.withResolvers(),drained=Promise.withResolvers();let count=0,fail=retry,probe,recovery;
 const store={observe:async member=>'synthetic-request-'+member.id,finish:async requestId=>{if(fail)throw Error('synthetic-database-unavailable');const first=!settled.has(requestId);settled.add(requestId);return {settled:first};}};
 const server=http.createServer(async(req,res)=>{
  try{
   const member=req.url==='/first'?'first':'second',id=await store.observe({id:member,key_id:member},'synthetic','synthetic','gpt-6.1-sol');
   assert.equal(recovery.settle(id,{input:10,output:2,cached:0}),true);if(++count===2)both.resolve();
   if(member==='first'){await both.promise;await recovery.flush();if(retry){assert.equal(recovery.status().files,2);fail=false;await recovery.flush();}drained.resolve();await store.finish('synthetic-request-second');}
   else await drained.promise;
   res.end('{}');
  }catch(error){both.reject(error);drained.reject(error);res.writeHead(500);res.end('{}');}
 });
 // Register rejection handlers before either synthetic HTTP callback can fail.
 both.promise.catch(()=>{});drained.promise.catch(()=>{});
 try{
  recovery=await MeteringRecovery.create(store,{encryptionKey:randomBytes(32),directory,flushMs:10000});
  probe=createGatewayProbe({sink:event=>events.push(event)});probe.attach({server,store,meteringRecovery:recovery,sessionHandoffs:{arrive:()=>({bind:async()=>{}})},status:()=>({})});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const results=await Promise.all(['first','second'].map(side=>requestCapacityTarget({origin,path:'/'+side,headers:{'x-capacity-trace':'synthetic-'+side},timeoutMs:10000})));
  assert.ok(results.every(result=>result.success));
  const committed=events.filter(event=>event.kind==='gateway-settled'&&event.settled);
  assert.equal(committed.length,2);for(const side of ['first','second'])assert.ok(committed.some(event=>event.trace==='synthetic-'+side&&event.requestId==='synthetic-request-'+side&&event.contextMatches));
  const directWrong=events.find(event=>event.kind==='gateway-settled'&&!event.settled&&event.trace==='synthetic-first'&&event.requestId==='synthetic-request-second');
  assert.ok(directWrong);assert.equal(directWrong.contextMatches,false,'a direct cross-request finish must still fail association');
  assert.equal(probe.status().dropped,0);assert.equal(probe.status().pendingRecoveryBindings,0);assert.equal(recovery.status().files,0);
 }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await recovery?.close();await probe?.close();await rm(directory,{recursive:true,force:true});}
});
