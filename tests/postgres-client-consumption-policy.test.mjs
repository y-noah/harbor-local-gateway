import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFileSync} from 'node:fs';
import http from 'node:http';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createPostgresClientCatalog} from '../postgres-client-catalog.mjs';
import {createPostgresSelfQuery} from '../postgres-self-query.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:30000};
const options=(database='postgres')=>{const u=new URL(url);u.pathname='/'+database;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:6,statementTimeoutMs:5000};};
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const condition=async predicate=>{const end=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<end,'synthetic catalog condition deadline');await new Promise(resolve=>setTimeout(resolve,5));}};
const listen=server=>new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const close=server=>new Promise(resolve=>{server.closeAllConnections();server.close(resolve);});
async function fixture(run,{install=true,policy='inherit',allowed=null,reader=true,catalogOptions={}}={}){
 const root=new PostgresStore(options()),database='harbor_csource_'+randomUUID().replaceAll('-','');let store,admin,backend,server,upstream,queue;
 const at=Date.now(),catalogs=new Map(),seen=[],transports=new Map(),sourceCalls=[];
 try{
  await root.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();if(install){await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});}
  admin=createPostgresMemberAdmin(store,{clock:()=>at,encryptionKey:randomBytes(32)});const member=(await admin.handle('/api/admin/members','POST',{name:'合成来源目录员工',quota:null,expires:null,allowed_models:allowed,...(install?{consumption_policy:policy}:{})})).value;
  const devices=createPostgresDeviceAccess(store,{clock:()=>at}),router=createPostgresRouter(store,{clock:()=>at});queue=createPostgresQueryQueue();const self=createPostgresSelfQuery(store,{clock:()=>at,queryQueue:queue});
  upstream=http.createServer((req,res)=>{void(async()=>{const id=req.url.slice(1);seen.push(id);let value=catalogs.get(id);if(typeof value==='function')value=await value();if(value instanceof Error){res.statusCode=502;res.end('{}');return;}const account=await store.get('SELECT auth_mode FROM harbor.accounts WHERE id=$1',id);res.setHeader('content-type','application/json');res.end(JSON.stringify(account.auth_mode==='openai_api'?{data:(value??[]).map(model=>({id:model.slug}))}:{models:value??[]}));})().catch(()=>{res.statusCode=502;res.end('{}');});});await listen(upstream);
  backend=createPostgresClientCatalog(store,{clock:()=>at,deviceAccess:devices,...(reader?{sourceAccounts:async args=>{sourceCalls.push(args);return router.catalogCandidates(args);}}:{}),transportForAccount:async account=>{
   if(!transports.has(account.id))transports.set(account.id,createResponsesTransport({apiKeyMode:account.auth_mode==='openai_api',credentials:async()=>({token:'synthetic-not-a-key',accountId:account.id}),version:async()=>'synthetic-version',fetchImpl:async(_url,init)=>{assert.equal(store.context.getStore(),undefined,'catalog upstream IO cannot retain database locks');return fetch('http://127.0.0.1:'+upstream.address().port+'/'+account.id,init);}}));
   return transports.get(account.id);
  },...catalogOptions});
  server=http.createServer((req,res)=>{void(async()=>{
   if(new URL(req.url,'http://localhost').pathname==='/api/me'){
    const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),current=await store.authenticate(secret,{at});await devices.authorize(current,req);const value=await self(secret);res.setHeader('content-type','application/json');res.end(JSON.stringify(value));return;
   }
   if(!await backend.handle(req,res)){res.statusCode=404;res.end('{}');}
  })().catch(error=>{if(res.headersSent){res.destroy();return;}res.statusCode=error.status||500;res.end(JSON.stringify({code:error.code}));});});await listen(server);
  const origin='http://127.0.0.1:'+server.address().port,call=(path='/v1/models',headers={})=>fetch(origin+path,{headers:{authorization:'Bearer '+member.secret,...headers}});
  const account=async({mode='subscription',weight=25,enabled=1,state='ready',block=null,last=0,models=[{slug:'synthetic-model'}]}={})=>{
   const id=randomUUID();catalogs.set(id,models);const quota=JSON.stringify({checked:at,windows:[{id:'primary',minutes:300,used:10,reset:at+18000000}]});
   await store.run('INSERT INTO harbor.accounts(id,name,home,auth_mode,api_weight,enabled,state,route_block,last_used,quota_json) VALUES($1,$2,$1,$3,$4,$5,$6,$7,$8,$9)',id,'合成目录来源',mode,mode==='openai_api'?weight:0,enabled,state,block,last,quota);return id;
  };
  const setPolicy=(consumption_policy,version)=>admin.handle('/api/admin/keys/'+member.keyId,'PATCH',{consumption_policy,expected_consumption_policy_version:version});
  await run({store,admin,backend,router,member,at,catalogs,seen,sourceCalls,call,account,setPolicy});
 }finally{await backend?.close();if(server)await close(server);if(upstream)await close(upstream);await queue?.close();await admin?.close();await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);if(install)await root.query('DROP ROLE IF EXISTS '+database+'_runtime');await root.close();}
}

test('me returns only server-owned source policy/version, ignores caller hints and keeps legacy DTO without schema',integration,async()=>{
 for(const install of [false,true])await fixture(async f=>{
  const original=f.store.authenticate.bind(f.store);f.store.authenticate=async(...args)=>{const member=await original(...args);delete member.consumption_policy;delete member.consumption_policy_version;return member;};
  const first=await f.call('/api/me?consumption_policy=subscription_only&consumption_policy_version=999',{'x-harbor-consumption-policy':'subscription_only'});assert.equal(first.status,200);const value=await first.json();
  if(!install){assert.equal(Object.hasOwn(value,'consumption_policy'),false);assert.equal(Object.hasOwn(value,'consumption_policy_version'),false);}else{
   assert.equal(value.consumption_policy,'api_only');assert.equal(value.consumption_policy_version,0);await f.setPolicy('subscription_only',0);const next=await(await f.call('/api/me')).json();assert.equal(next.consumption_policy,'subscription_only');assert.equal(next.consumption_policy_version,1);
  }
  assert.equal(Object.hasOwn(value,'secret'),false);assert.equal(Object.hasOwn(value,'digest'),false);assert.equal(JSON.stringify(value).includes(f.member.secret),false);assert.equal(f.seen.length,0);
 },{install,policy:'api_only'});
});
test('inherit preserves the old latest-subscription/mixed-API directory and never calls explicit routing',integration,async()=>{
 for(const install of [false,true])await fixture(async f=>{
  await f.account({last:1,models:[{slug:'old-subscription'}]});const recent=await f.account({last:2,models:[{slug:'recent-subscription'}]}),api=await f.account({mode:'openai_api',weight:0,models:[{slug:'manual-api'}]});
  const response=await f.call(),value=await response.json();assert.equal(response.status,200);assert.deepEqual(value.data.map(row=>row.id),['codex','claude','recent-subscription','manual-api']);assert.deepEqual(new Set(f.seen),new Set([recent,api]));assert.equal(f.sourceCalls.length,0);assert.deepEqual(value.catalog_status,{complete:true,sources:{total:2,successful:2,failed:0,changed:0}});assert.ok(value.models.every(model=>!Object.hasOwn(model,'consumption_sources')));
 },{install});
});
test('explicit only policies use real routing source groups and retain model permission intersection',integration,()=>fixture(async f=>{
 const api=await f.account({mode:'openai_api',models:[{slug:'common'},{slug:'api-only'},{slug:'not-allowed'}]}),sub=await f.account({models:[{slug:'common'},{slug:'subscription-only'},{slug:'not-allowed'}]});
 let response=await f.call('/v1/models?consumption_policy=subscription_only',{'x-harbor-consumption-policy':'subscription_only'}),value=await response.json();assert.equal(response.status,200);assert.deepEqual(value.data.map(row=>row.id),['common','api-only']);assert.deepEqual(f.seen,[api]);assert.ok(value.models.every(model=>model.consumption_sources.length===1&&model.consumption_sources[0]==='api'&&model.protocol_compatibility==='unknown'));
 await f.setPolicy('subscription_only',0);response=await f.call('/v1/codex/models');value=await response.json();assert.equal(response.status,200);assert.deepEqual(value.models.map(row=>row.slug),['common','subscription-only']);assert.ok(value.models.every(model=>model.consumption_sources[0]==='subscription'));assert.deepEqual(new Set(f.seen),new Set([api,sub]));assert.equal(value.catalog_status.consumption_policy_version,1);
},{policy:'api_only',allowed:['common','api-only','subscription-only']}));
test('api-first directory observes both sources without claiming fallback protocol support or hiding partial failure',integration,()=>fixture(async f=>{
 const api=await f.account({mode:'openai_api',weight:100,models:new Error('synthetic provider failure')}),sub=await f.account({models:[{slug:'common'},{slug:'subscription-only'}]});
 const response=await f.call(),value=await response.json();assert.equal(response.status,200);assert.equal(value.catalog_status.complete,false);assert.equal(value.catalog_status.source_support.api.failed,1);assert.equal(value.catalog_status.source_support.subscription.successful,1);assert.equal(value.catalog_status.fallback_compatibility,'unknown');assert.ok(value.models.every(model=>model.fallback_compatibility==='unknown'&&model.consumption_sources[0]==='subscription'));assert.deepEqual(new Set(f.seen),new Set([api,sub]));
 await f.store.run('UPDATE harbor.accounts SET api_weight=80 WHERE id=$1',api);const replacement=await f.account({mode:'openai_api',weight:10,models:[{slug:'common'},{slug:'api-only'}]});const next=await(await f.call('/v1/codex/models')).json();assert.deepEqual(next.models.find(model=>model.slug==='common').consumption_sources,['api','subscription']);assert.equal(next.models.find(model=>model.slug==='common').fallback_compatibility,'unknown');assert.ok(f.seen.includes(replacement));assert.equal(next.catalog_status.complete,false);
 assert.equal(JSON.stringify(next).includes(api),false);assert.equal(JSON.stringify(next).includes(sub),false);
},{policy:'api_first'}));
test('explicit candidates never include zero-weight, disabled, blocked or manually excluded accounts',integration,()=>fixture(async f=>{
 const good=await f.account({models:[{slug:'allowed-subscription'}]}),zero=await f.account({models:[{slug:'zero-subscription'}]}),off=await f.account({enabled:0,models:[{slug:'disabled-subscription'}]}),api=await f.account({mode:'openai_api',weight:100,models:[{slug:'allowed-api'}]});
 await f.account({mode:'openai_api',weight:0,models:[{slug:'zero-api'}]});await f.account({mode:'openai_api',enabled:0,models:[{slug:'disabled-api'}]});await f.account({mode:'openai_api',block:'login_expired',models:[{slug:'blocked-api'}]});
 await f.router.saveSettings({weighted:true});await f.router.saveWeights({mode:'manual',weights:{[good]:100,[zero]:0,[off]:0},expected_version:0});
 const response=await f.call(),value=await response.json();assert.equal(response.status,200);assert.deepEqual(new Set(value.models.map(row=>row.slug)),new Set(['allowed-subscription','allowed-api']));assert.deepEqual(new Set(f.seen),new Set([good,api]));
},{policy:'api_first'}));
test('source-policy version changes during provider IO reject the late catalog without expanding sources',integration,()=>fixture(async f=>{
 const gate=deferred(),api=await f.account({mode:'openai_api',models:async()=>{await gate.promise;return [{slug:'api-only'}];}});await f.account({models:[{slug:'subscription-only'}]});const pending=f.call();
 try{await condition(()=>f.seen.includes(api));await f.setPolicy('subscription_only',0);gate.resolve();const response=await pending;assert.equal(response.status,409);assert.equal((await response.json()).code,'client_catalog_policy_changed');assert.deepEqual(f.seen,[api]);}finally{gate.resolve();await pending.catch(()=>{});}
},{policy:'api_only'}));
test('removing source weight during provider IO is rechecked and a newly available account is not substituted',integration,()=>fixture(async f=>{
 const gate=deferred(),api=await f.account({mode:'openai_api',models:async()=>{await gate.promise;return [{slug:'old-source'}];}}),pending=f.call();
 try{await condition(()=>f.seen.includes(api));await f.store.run('UPDATE harbor.accounts SET api_weight=0 WHERE id=$1',api);await f.account({mode:'openai_api',models:[{slug:'new-source'}]});gate.resolve();const response=await pending;assert.equal(response.status,409);assert.equal((await response.json()).code,'client_catalog_sources_changed');assert.deepEqual(f.seen,[api]);}finally{gate.resolve();await pending.catch(()=>{});}
},{policy:'api_only'}));
test('new candidates cannot consume the fixed catalog source budget during send-time revalidation',integration,()=>fixture(async f=>{
 const gate=deferred(),api=await f.account({mode:'openai_api',models:async()=>{await gate.promise;return [{slug:'fixed-source'}];}}),pending=f.call();
 try{await condition(()=>f.seen.includes(api));await f.account({mode:'openai_api',models:[{slug:'new-unread-source'}]});gate.resolve();const response=await pending;assert.equal(response.status,200);const value=await response.json();assert.deepEqual(value.models.map(model=>model.slug),['fixed-source']);assert.deepEqual(value.catalog_status.sources,{total:1,successful:1,failed:0,changed:0});assert.deepEqual(f.seen,[api]);}finally{gate.resolve();await pending.catch(()=>{});}
},{policy:'api_only',catalogOptions:{maxSources:1}}));
test('MCP download works without model sources/reader and policy versions are rechecked before bytes leave',integration,()=>fixture(async f=>{
 const gate=deferred(),original=f.store.authenticate.bind(f.store);let calls=0;f.store.authenticate=async(...args)=>{if(++calls===3)await gate.promise;return original(...args);};
 const pending=f.call('/v1/codex/image-mcp.mjs');try{await condition(()=>calls>=3);await f.setPolicy('subscription_only',0);gate.resolve();const response=await pending;assert.equal(response.status,409);assert.equal((await response.json()).code,'client_catalog_policy_changed');}finally{gate.resolve();await pending.catch(()=>{});f.store.authenticate=original;}
 const response=await f.call('/v1/codex/image-mcp.mjs');assert.equal(response.status,200);assert.deepEqual(Buffer.from(await response.arrayBuffer()),readFileSync(new URL('../clients/image-mcp.mjs',import.meta.url)));assert.equal(f.seen.length,0);assert.equal(f.sourceCalls.length,0);
 const directory=await f.call();assert.equal(directory.status,503);assert.equal((await directory.json()).code,'client_catalog_configuration');
},{policy:'api_only',reader:false}));
test('me/catalog schema validation initializes once per lifecycle and ordinary reads never rescan schema',integration,()=>fixture(async f=>{
 const original=f.store.query.bind(f.store),calls=[];f.store.query=(sql,args)=>{calls.push({sql,marker:sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1'&&args[0]==='schema.key-consumption-policy'});return original(sql,args);};
 assert.equal((await f.call('/api/me')).status,200);assert.equal((await f.call('/v1/codex/image-mcp.mjs')).status,200);assert.equal(calls.filter(row=>row.marker).length,2);
 calls.length=0;assert.equal((await f.call('/api/me')).status,200);assert.equal((await f.call('/v1/codex/image-mcp.mjs')).status,200);assert.equal(calls.some(row=>row.marker||/pg_proc|pg_constraint|pg_attribute/.test(row.sql)),false);
}));
