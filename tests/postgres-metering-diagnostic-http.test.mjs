import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import http from 'node:http';
import {PostgresStore} from '../postgres-store.mjs';
import {hash,AppError} from '../store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const options=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};};
const until=async fn=>{const end=Date.now()+5000;while(!await fn()){assert.ok(Date.now()<end,'condition timeout');await new Promise(resolve=>setTimeout(resolve,5));}};

test('PG HTTP settlement and timing replay retain metering diagnosis without changing usage',{skip:!url&&!socket?'需真实隔离PG':false,timeout:30000},async()=>{
 const root=new PostgresStore(options()),db='harbor_metering_http_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-metering-http-'));let store,buffer,recovery,handler,server,created=false,mode='valid',failFinish=false,calls=0;const finishes=[],replays=[];
 try{
  await root.query('CREATE DATABASE '+db);created=true;store=new PostgresStore(options(db));await store.migrate();await store.initializePricing();const member=randomUUID(),key=randomUUID(),secret='synthetic-metering-http',at=Date.now();
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,hash(secret),at);
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES('synthetic','合成账号','synthetic','ready',$1)",JSON.stringify({plan:'synthetic',checked:at,remaining:50,windows:[{used:50,minutes:10080,reset:at+86400000}],limitReached:false}));
  const records=new RecordingStore(store,{encryptionKey:randomBytes(32)});buffer=await RecordingBuffer.create(records,{directory:join(directory,'records')});recovery=await MeteringRecovery.create(store,{encryptionKey:randomBytes(32),directory:join(directory,'metering')});
  const finish=store.finish.bind(store);store.finish=async(id,result,when)=>{finishes.push({id,result});if(failFinish){failFinish=false;throw Error('synthetic persistence failure');}return finish(id,result,when);};const enqueue=recovery.settle.bind(recovery);recovery.settle=(id,result,when)=>{replays.push({id,result});return enqueue(id,result,when);};
  const router=createPostgresRouter(store);
  handler=createPostgresResponsesHandler({store,recordings:records,buffer,meteringRecovery:recovery,resolveRoute:router.resolve,adminAuthenticated:async()=>false,checkLimits:async()=>{},transportForAccount:async()=>mode==='custom'?{run:async()=>{calls++;return {input:10,output:2,cached:0,usageKnown:true,response:{output:[]}};}}:createResponsesTransport({credentials:async()=>{if(mode==='credentials')throw new AppError(503,'合成凭据失败','upstream_authentication');return {token:'synthetic',accountId:'synthetic'};},fetchImpl:async()=>{calls++;if(mode==='reject')return new Response('{}',{status:429});if(mode==='disconnect')return new Response(new ReadableStream({pull(controller){controller.error(new TypeError('synthetic reset'));}}),{headers:{'content-type':'text/event-stream'}});return new Response('data: '+JSON.stringify({type:'response.completed',response:{status:'completed',model:'gpt-6.1-sol',output:[],...(mode==='missing'?{}:{usage:{input_tokens:mode==='invalid'?-1:10,output_tokens:2}})}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}})});
  server=http.createServer((req,res)=>{void handler.handle(req,res).catch(error=>{if(!res.headersSent)res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:error.code}}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  for(const [next,reason,known,status] of [['custom','usage_received',true,200],['valid','usage_received',true,200],['missing','terminal_usage_missing',false,200],['invalid','terminal_usage_invalid',false,200],['reject','upstream_rejected',true,429],['disconnect','interrupted_without_terminal',false,502],['credentials','not_dispatched',true,503],['spool','usage_received',true,200]]){
   mode=next;failFinish=next==='spool';const previous=finishes.length,previousReplays=replays.length,previousCalls=calls;
   const response=await fetch('http://127.0.0.1:'+server.address().port+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':randomUUID()},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成请求',stream:false})});assert.equal(response.status,status,await response.text());await until(()=>handler.status().active===0&&replays.length>previousReplays);
   const fact=finishes[previous];assert.ok(fact);assert.equal(fact.result.meteringDiagnostic.reason,reason);assert.equal(fact.result.meteringDiagnostic.usage_known,known);assert.equal(fact.result.usageKnown,known);assert.equal(calls-previousCalls,next==='credentials'?0:1);
   const timing=replays.slice(previousReplays).find(row=>row.result.streamTiming);assert.ok(timing,'HTTP finish persists timing receipt');assert.deepEqual(timing.result.meteringDiagnostic,fact.result.meteringDiagnostic);
   await recovery.flush();const saved=await store.get('SELECT status,usage_unknown,input,output,phase_history FROM harbor.requests WHERE id=$1',fact.id);assert.equal(saved.usage_unknown,known?0:1);assert.equal(saved.input,['custom','valid','spool'].includes(next)?10:0);assert.equal(saved.output,['custom','valid','spool'].includes(next)?2:0);
   const phases=typeof saved.phase_history==='string'?JSON.parse(saved.phase_history):saved.phase_history;assert.deepEqual(phases.at(-1).metering,fact.result.meteringDiagnostic);
  }
 }finally{handler?.abort();if(server)await new Promise(resolve=>server.close(resolve));await handler?.closeQueries();await recovery?.close();await buffer?.close();await store?.close();if(created)await root.query('DROP DATABASE '+db);await root.close();await rm(directory,{recursive:true,force:true});}
});

test('gateway startup records startup interruption for stale requests',{skip:!url&&!socket?'需真实隔离PG':false,timeout:30000},async()=>{
 const root=new PostgresStore(options()),db='harbor_metering_start_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-metering-start-'));let store,app,created=false;
 try{
  await root.query('CREATE DATABASE '+db);created=true;store=new PostgresStore(options(db));await store.migrate();await store.initializePricing();const member=randomUUID(),key=randomUUID(),at=Date.now();
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,hash('synthetic-startup'),at);
  const request=await store.observe({id:member,key_id:key},'synthetic','synthetic','gpt-6.1-sol',{at});
  const probe=async()=>[{total:1000000000000,free:900000000000}];probe.status=()=>({source:'controlled_test_measurement'});
  app=await createPostgresGateway({database:options(db),dataDir:directory,runtimeDir:join(directory,'runtime'),env:{},config:{port:43127,codexBinary:'synthetic',adminPasswordHash:passwordHash('synthetic')},recording:{enabled:false,storage:{probe}},rateReader:async()=>{throw Error('synthetic unavailable');}});
  const row=await store.get('SELECT status,usage_unknown,phase_history FROM harbor.requests WHERE id=$1',request);assert.equal(row.status,'interrupted');assert.equal(row.usage_unknown,1);assert.deepEqual(JSON.parse(row.phase_history).at(-1).metering,{version:1,reason:'startup_interrupted',terminal:'none',interruption:'startup',usage_known:false});
 }finally{await app?.close();await store?.close();if(created)await root.query('DROP DATABASE '+db);await root.close();await rm(directory,{recursive:true,force:true});}
});
