import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import http from 'node:http';
import {PostgresStore} from '../postgres-store.mjs';
import {hash} from '../store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const options=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};};
const until=async fn=>{const end=Date.now()+5000;while(!await fn()){assert.ok(Date.now()<end,'condition timeout');await new Promise(resolve=>setTimeout(resolve,5));}};
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
for(const phase of ['queued','running'])test('自动换绑等待旧'+phase+'完成，两条新请求继续且旧派发不重放',{skip:!url&&!socket?'需真实隔离PG':false,timeout:30000},async()=>{
 const root=new PostgresStore(options()),db='harbor_route_wait_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-route-wait-'));let store,buffer,recovery,handler,server,created=false;const gate=deferred(),oldStarted=deferred(),calls=[];let busyReads=0;
 try{
  await root.query('CREATE DATABASE '+db);created=true;store=new PostgresStore(options(db));await store.migrate();await store.initializePricing();const member=randomUUID(),key=randomUUID(),secret='synthetic-route-wait-key',at=Date.now();
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,hash(secret),at);
  for(const id of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES($1,$1,$1,'ready',$2)",id,JSON.stringify({plan:'synthetic-plan',checked:at,remaining:50,windows:[{used:50,minutes:10080,reset:at+86400000}],limitReached:false}));
  const put=(name,value)=>store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));await put('routing_strategy',{weighted:true});await put('routing_weights',{mode:'manual',weights:{a:100,b:0},expires:null,version:1});
  const records=new RecordingStore(store,{encryptionKey:randomBytes(32)});buffer=await RecordingBuffer.create(records,{directory:join(directory,'records')});recovery=await MeteringRecovery.create(store,{encryptionKey:randomBytes(32),directory:join(directory,'metering')});
  const router=createPostgresRouter(store),get=store.get.bind(store);store.get=async(sql,...args)=>{const result=await get(sql,...args);if(sql.includes("status IN ('queued','running')")&&sql.includes('LIMIT 1')&&result)busyReads++;return result;};
  const limits=createPostgresMemberLimits(store);
  handler=createPostgresResponsesHandler({store,recordings:records,buffer,meteringRecovery:recovery,resolveRoute:router.resolve,adminAuthenticated:async()=>false,checkLimits:(current,payload,options)=>limits.check(current.id,payload.model,{...options,tools:payload.tools}),transportForAccount:async account=>createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>{if(account.id==='a'&&phase==='queued'){oldStarted.resolve();await gate.promise;}return {token:'synthetic',accountId:account.id};},fetchImpl:async()=>{calls.push(account.id);if(account.id==='a'&&phase==='running'){oldStarted.resolve();await gate.promise;}return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:randomUUID(),model:'gpt-6.1-sol',output:[],usage:{input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}})});
  server=http.createServer((req,res)=>{void handler.handle(req,res).catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:error.code}}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const send=(signal,thread='same-session')=>fetch('http://127.0.0.1:'+server.address().port+'/v1/responses',{method:'POST',signal,headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':thread},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成完整上下文',stream:false})});
  const old=send();await oldStarted.promise;await put('routing_weights',{mode:'manual',weights:{a:0,b:100},expires:null,version:2});const next=[send(),send()];await until(()=>busyReads>=2);assert.equal((await get('SELECT COUNT(*) n FROM harbor.requests')).n,1);assert.equal((await get('SELECT account_id FROM harbor.sessions')).account_id,'a');assert.equal((await get('SELECT status FROM harbor.requests')).status,phase);
  const controller=new AbortController(),cancelled=send(controller.signal).catch(error=>error);await until(()=>busyReads>=3);controller.abort();assert.equal((await cancelled).name,'AbortError');await until(()=>handler.status().active===3);
  gate.resolve();const [oldResponse,...newResponses]=await Promise.all([old,...next]);assert.equal(oldResponse.status,phase==='running'?200:503,await oldResponse.text());for(const response of newResponses)assert.equal(response.status,200,await response.text());assert.deepEqual(calls,phase==='running'?['a','b','b']:['b','b']);await until(()=>handler.status().active===0);
  const rows=await store.all('SELECT account_id,status,route_version FROM harbor.requests ORDER BY query_sequence');assert.deepEqual(rows.map(row=>row.account_id),['a','b','b']);assert.deepEqual(rows.map(row=>row.status),[phase==='running'?'ok':'failed','ok','ok']);assert.deepEqual(rows.map(row=>row.route_version),[0,1,1]);assert.equal((await get('SELECT route_version FROM harbor.sessions')).route_version,1);
 }finally{gate.resolve();handler?.abort();if(server)await new Promise(resolve=>server.close(resolve));await handler?.closeQueries();await recovery?.close();await buffer?.close();await store?.close();if(created)await root.query('DROP DATABASE '+db);await root.close();await rm(directory,{recursive:true,force:true});}
});
