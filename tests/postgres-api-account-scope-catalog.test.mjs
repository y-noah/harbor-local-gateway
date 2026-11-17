import {createPostgresApiAccountScope} from '../postgres-api-account-scope.mjs';
import {syntheticCodexProfile} from './codex-model-profile-fixture.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFileSync} from 'node:fs';
import http from 'node:http';
import {hash} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createPostgresClientCatalog} from '../postgres-client-catalog.mjs';
import {createPostgresSelfQuery} from '../postgres-self-query.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,backfillUsageRollups,reconcileUsageRollups} from '../postgres-usage-rollups.mjs';
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
   const id=randomUUID();catalogs.set(id,Array.isArray(models)&&mode==='subscription'?models.map(syntheticCodexProfile):models);const quota=JSON.stringify({checked:at,windows:[{id:'primary',minutes:300,used:10,reset:at+18000000}]});
   await store.run('INSERT INTO harbor.accounts(id,name,home,auth_mode,api_weight,enabled,state,route_block,last_used,quota_json) VALUES($1,$2,$1,$3,$4,$5,$6,$7,$8,$9)',id,'合成目录来源',mode,mode==='openai_api'?weight:0,enabled,state,block,last,quota);return id;
  };
  const setPolicy=(consumption_policy,version)=>admin.handle('/api/admin/keys/'+member.keyId,'PATCH',{consumption_policy,expected_consumption_policy_version:version});
  await run({store,admin,backend,router,member,at,catalogs,seen,sourceCalls,call,account,setPolicy});
 }finally{await backend?.close();if(server)await close(server);if(upstream)await close(upstream);await queue?.close();await admin?.close();await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);if(install)await root.query('DROP ROLE IF EXISTS '+database+'_runtime');await root.close();}
}


const configure=async(f,id,scope,weight)=>{const api=createPostgresApiAccountScope(f.store),saved=await api.read();return api.save(id,{api_usage_scope:scope,api_weight:weight,expected_api_scope_version:saved.version});};
test('explicit API catalog is naturally visible only to API policies; inherit and fixed inherit cannot expose it',integration,()=>fixture(async f=>{
 const extra=await f.account({mode:'openai_api',weight:0,models:[{slug:'gpt-6.1-sol'}]});await configure(f,extra,'explicit_api',100);
 const response=await f.call('/v1/codex/models');assert.equal(response.status,200);assert.deepEqual((await response.json()).models.map(x=>x.slug),['gpt-6.1-sol']);assert.deepEqual(f.seen,[extra]);
 await f.setPolicy('inherit',0);f.seen.length=0;const legacy=await f.call('/v1/codex/models',{'x-harbor-api-scope':'explicit_api'});assert.equal(legacy.status,200);assert.deepEqual((await legacy.json()).models,[]);assert.deepEqual(f.seen,[]);
 const session=randomUUID();await f.store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,$3,'responses',$4,$4)",session,f.member.memberId,extra,f.at);
 const fixed=await f.call('/v1/codex/models',{'x-harbor-session':session});assert.equal(fixed.status,409);assert.equal((await fixed.json()).code,'api_account_scope_conflict');assert.deepEqual(f.seen,[]);
},{policy:'api_only'}));
test('changing explicit scope during upstream directory IO rejects stale complete metadata',integration,()=>fixture(async f=>{
 const gate=deferred(),extra=await f.account({mode:'openai_api',weight:0,models:async()=>{await gate.promise;return [{slug:'gpt-6.1-sol'}];}});await configure(f,extra,'explicit_api',80);
 const pending=f.call('/v1/codex/models');try{await condition(()=>f.seen.includes(extra));await configure(f,extra,'explicit_api',70);gate.resolve();const response=await pending;assert.equal(response.status,409);assert.equal((await response.json()).code,'client_catalog_policy_changed');}finally{gate.resolve();await pending.catch(()=>{});}
},{policy:'api_only'}));
