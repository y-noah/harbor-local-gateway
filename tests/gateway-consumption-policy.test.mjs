import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createGateway} from '../server.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {passwordHash} from '../auth.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:30000};
const options=(database='postgres')=>{const target=new URL(url);target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:4,statementTimeoutMs:5000};};
const listen=server=>new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const config=()=>({port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-gateway-consumption'),reserveTokens:100});
const httpClient=origin=>async(path,{method='GET',body,cookie,secret}={})=>{
 const response=await fetch(origin+path,{method,headers:{...(cookie?{cookie}:{}),...(secret?{authorization:'Bearer '+secret}:{}),...(body===undefined?{}:{'content-type':'application/json'})},...(body===undefined?{}:{body:JSON.stringify(body)})});
 return {status:response.status,value:await response.json(),headers:response.headers};
};
async function login(call){const response=await call('/api/auth/login',{method:'POST',body:{password:'synthetic-gateway-consumption'}});assert.equal(response.status,200);return response.headers.get('set-cookie').split(';')[0];}
async function sqliteFixture(run){
 const directory=await mkdtemp(join(tmpdir(),'harbor-sqlite-consumption-'));let app;
 try{app=createGateway({dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:config(),responsesTransport:{models:async()=>[],run:async()=>{throw Error('no model call expected');}}});await listen(app.server);const call=httpClient('http://127.0.0.1:'+app.server.address().port),cookie=await login(call);await run({app,call,cookie});}
 finally{await app?.close();await rm(directory,{recursive:true,force:true});}
}
async function postgresFixture(run,{install=true,migrationOptions={}}={}){
 const root=new PostgresStore(options()),database='harbor_gwc_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-pg-consumption-gateway-'));let setup,app;
 const seen=[],transports={};
 try{
  await root.query('CREATE DATABASE '+database);setup=new PostgresStore(options(database));await setup.migrate(migrationOptions);if(install){await createUsageRollupRuntime(setup);await migrateConsumptionPolicy(setup,{apply:true});}
  for(const [source,apiKeyMode] of [['api',true],['subscription',false]])transports[source]=createResponsesTransport({apiKeyMode,credentials:async()=>({token:'synthetic-not-a-key',accountId:'synthetic-account'}),version:async()=>'synthetic-version',fetchImpl:async(_url,request)=>{
   seen.push({source,inference:request.method==='POST'});if(request.method!=='POST')return Response.json(apiKeyMode?{data:[{id:'gpt-6.1-sol'}]}:{models:[{slug:'gpt-6.1-sol',display_name:'合成模型'}]});
   return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-response',model:'gpt-6.1-sol',output:[],usage:{input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});
  }});
  const appOptions={env:{},database:options(database),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:config(),recording:{enabled:false},responsesTransport:transports.subscription,apiResponsesTransport:transports.api};
  const start=async()=>{app=await createPostgresGateway(appOptions);await listen(app.server);const call=httpClient('http://127.0.0.1:'+app.server.address().port),cookie=await login(call);return {app,call,cookie};};
  await run({setup,start,appOptions,seen,get app(){return app;}});
 }finally{await app?.close();await setup?.close();await root.query('DROP DATABASE IF EXISTS '+database);await root.query('DROP ROLE IF EXISTS '+database+'_runtime');await root.close();await rm(directory,{recursive:true,force:true});}
}

test('SQLite rejects non-inherit/illegal source settings and all untrusted versions without partial writes',async()=>sqliteFixture(async f=>{
 const create=async(extra={})=>f.call('/api/admin/members',{method:'POST',cookie:f.cookie,body:{name:'合成SQLite员工',quota:null,expires:null,...extra}}),member=await create();assert.equal(member.status,201);
 const count=()=>f.app.store.get('SELECT (SELECT count(*) FROM members) members,(SELECT count(*) FROM keys) keys,(SELECT count(*) FROM audit) audit');
 const before=count();for(const fields of [{consumption_policy:'api_only'},{consumption_policy:'subscription_only'},{consumption_policy:'api_first'},{consumption_policy:'bad'},{consumption_policy:null},{consumption_policy_version:0},{expected_consumption_policy_version:0},{consumption_policy:'inherit',expected_consumption_policy_version:0}]){
  assert.equal((await create(fields)).status,400);assert.equal((await f.call('/api/admin/keys',{method:'POST',cookie:f.cookie,body:{memberId:member.value.memberId,expires:null,...fields}})).status,400);assert.deepEqual(count(),before);
 }
 const row=f.app.store.get('SELECT * FROM keys WHERE id=?',member.value.keyId);for(const fields of [{consumption_policy:'api_only',expected_consumption_policy_version:0},{consumption_policy:'bad',expected_consumption_policy_version:0},{consumption_policy:'inherit'},{expected_consumption_policy_version:0},{consumption_policy_version:0},{consumption_policy:'inherit',expected_consumption_policy_version:'0'},{consumption_policy:'inherit',expected_consumption_policy_version:1}]){
  const changed=await f.call('/api/admin/keys/'+member.value.keyId,{method:'PATCH',cookie:f.cookie,body:{enabled:false,allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0,...fields}});assert.ok([400,409].includes(changed.status));assert.deepEqual(f.app.store.get('SELECT * FROM keys WHERE id=?',member.value.keyId),row);assert.deepEqual(count(),before);
 }
 assert.equal((await f.call('/api/admin/members/'+member.value.memberId,{method:'PATCH',cookie:f.cookie,body:{consumption_policy:'inherit',expected_consumption_policy_version:0,enabled:false}})).status,400);
 assert.equal((await f.call('/api/admin/keys',{method:'POST',secret:member.value.secret,body:{memberId:member.value.memberId,expires:null,consumption_policy:'api_only'}})).status,401);
 assert.equal((await f.call('/api/me',{secret:member.value.secret})).value.consumption_policy,undefined);assert.equal(f.app.store.all('PRAGMA table_info(keys)').some(column=>column.name==='consumption_policy'),false);
}));
test('SQLite old calls and explicit inherit retain original operation without claiming source capability',async()=>sqliteFixture(async f=>{
 const created=await f.call('/api/admin/members',{method:'POST',cookie:f.cookie,body:{name:'合成继承员工',quota:null,expires:null,consumption_policy:'inherit'}});assert.equal(created.status,201);assert.equal(Object.hasOwn(created.value,'consumption_policy_version'),false);
 const added=await f.call('/api/admin/keys',{method:'POST',cookie:f.cookie,body:{memberId:created.value.memberId,expires:null,consumption_policy:'inherit'}});assert.equal(added.status,201);
 assert.equal((await f.call('/api/admin/keys/'+added.value.keyId,{method:'PATCH',cookie:f.cookie,body:{consumption_policy:'inherit',expected_consumption_policy_version:0,enabled:false}})).status,200);assert.equal(f.app.store.get('SELECT enabled FROM keys WHERE id=?',added.value.keyId).enabled,0);
 assert.equal((await f.call('/api/admin/keys/'+added.value.keyId,{method:'PATCH',cookie:f.cookie,body:{enabled:true}})).status,200);assert.equal(f.app.store.get('SELECT enabled FROM keys WHERE id=?',added.value.keyId).enabled,1);
}));
test('PG gateway exposes verified capability, me and source catalog through the actual HTTP entry',integration,()=>postgresFixture(async f=>{
 const {app,call,cookie}=await f.start();const response=await call('/api/admin/members',{method:'POST',cookie,body:{name:'合成PG员工',quota:null,expires:null,consumption_policy:'api_only'}});assert.equal(response.status,201);const member=response.value;
 const state=await call('/api/admin/state?view=overview',{cookie});assert.equal(state.status,200);assert.equal(state.value.capabilities.consumptionPolicy,true);assert.equal(app.status().consumptionRouting.configured,true);assert.equal(app.status().consumptionRouting.sourceAccounts,true);
 const api=randomUUID(),sub=randomUUID();await app.store.run("INSERT INTO harbor.accounts(id,name,home,auth_mode,api_weight,state) VALUES($1,'合成API',$1,'openai_api',25,'ready'),($2,'合成订阅',$2,'subscription',0,'ready')",api,sub);
 const me=await call('/api/me?consumption_policy=subscription_only',{secret:member.secret});assert.equal(me.status,200);assert.equal(me.value.consumption_policy,'api_only');assert.equal(me.value.consumption_policy_version,0);
 const models=await call('/v1/models',{secret:member.secret});assert.equal(models.status,200);assert.deepEqual(models.value.models[0].consumption_sources,['api']);assert.equal(f.seen.filter(item=>item.source==='subscription').length,0);
 const policy=await call('/api/admin/keys/'+member.keyId+'/policy',{cookie});assert.equal(policy.value.consumption_policy,'api_only');assert.equal(policy.value.key_id,member.keyId);
 assert.equal((await call('/api/admin/keys/'+member.keyId,{method:'PATCH',secret:member.secret,body:{consumption_policy:'subscription_only',expected_consumption_policy_version:0}})).status,401);
 assert.equal((await app.store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,22);
}));
test('PG without source schema starts in legacy mode and rejects explicit settings',integration,()=>postgresFixture(async f=>{
 const {app,call,cookie}=await f.start(),created=await call('/api/admin/members',{method:'POST',cookie,body:{name:'合成旧入口员工',quota:null,expires:null}});assert.equal(created.status,201);
 const state=await call('/api/admin/state?view=overview',{cookie});assert.equal(state.status,200);assert.equal(state.value.capabilities.consumptionPolicy,false);
 const me=await call('/api/me',{secret:created.value.secret});assert.equal(me.status,200);assert.equal(Object.hasOwn(me.value,'consumption_policy'),false);
 const denied=await call('/api/admin/keys',{method:'POST',cookie,body:{memberId:created.value.memberId,expires:null,consumption_policy:'api_only'}});assert.equal(denied.status,503);assert.equal(denied.value.error.code,'database_migration_required');assert.equal((await app.store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,22);
},{install:false}));
test('PG startup rejects orphan source columns or checksum drift and releases its startup ownership',integration,async()=>{
 await postgresFixture(async f=>{
  await f.setup.query('ALTER TABLE harbor.keys ADD COLUMN consumption_policy TEXT');await assert.rejects(createPostgresGateway(f.appOptions),{code:'database_migration_conflict'});
  await f.setup.query('ALTER TABLE harbor.keys DROP COLUMN consumption_policy');const {call}=await f.start();assert.equal((await call('/health')).status,200);
 },{install:false});
 await postgresFixture(async f=>{
  const marker=await f.setup.get("SELECT value FROM harbor.gateway_settings WHERE name='schema.key-consumption-policy'");await f.setup.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.key-consumption-policy'");await assert.rejects(createPostgresGateway(f.appOptions),{code:'database_migration_conflict'});
  await f.setup.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='schema.key-consumption-policy'",marker.value);const {call}=await f.start();assert.equal((await call('/health')).status,200);
 });
});
test('PG source wiring does not enable or upgrade optional context migrations',integration,async()=>{
 for(const [version,migrationOptions] of [[23,{recordingContext:true}],[24,{recordingContextIndex:true}]])await postgresFixture(async f=>{const {app,call,cookie}=await f.start();assert.equal((await call('/api/admin/state?view=overview',{cookie})).value.capabilities.consumptionPolicy,true);assert.equal((await app.store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,version);},{migrationOptions});
});

test('running legacy gateway rejects explicit POST/PATCH after real online CLI install until controlled restart',integration,()=>postgresFixture(async f=>{
 const first=await f.start(),legacy=await first.call('/api/admin/members',{method:'POST',cookie:first.cookie,body:{name:'合成热安装旧Key',quota:null,expires:null}});assert.equal(legacy.status,201);
 assert.equal((await first.call('/api/admin/state?view=overview',{cookie:first.cookie})).value.capabilities.consumptionPolicy,false);
 assert.equal(Object.hasOwn((await first.call('/api/me',{secret:legacy.value.secret})).value,'consumption_policy'),false);
 assert.equal(Object.hasOwn((await first.call('/api/admin/keys/'+legacy.value.keyId+'/policy',{cookie:first.cookie})).value,'consumption_policy'),false);
 assert.equal((await first.call('/v1/models',{secret:legacy.value.secret})).status,200);
 const role=await createUsageRollupRuntime(f.setup),run=await promisify(execFile)(process.execPath,[new URL('../postgres-consumption-policy-cli.mjs',import.meta.url).pathname,'install','--runtime-role='+role],{env:{...process.env,HARBOR_DATABASE_URL:f.appOptions.database.connectionString},timeout:15000});assert.deepEqual(JSON.parse(run.stdout),{action:'install',installed:true,changed:true});assert.equal(run.stderr,'');
 const baseline=await f.setup.get('SELECT (SELECT count(*) FROM harbor.members) members,(SELECT count(*) FROM harbor.keys) keys,(SELECT count(*) FROM harbor.audit) audits'),key=await f.setup.get('SELECT * FROM harbor.keys WHERE id=$1',legacy.value.keyId);
 const denied=[await first.call('/api/admin/members',{method:'POST',cookie:first.cookie,body:{name:'不能留下半个员工',quota:null,expires:null,consumption_policy:'api_only'}}),await first.call('/api/admin/keys',{method:'POST',cookie:first.cookie,body:{memberId:legacy.value.memberId,expires:null,consumption_policy:'subscription_only'}}),await first.call('/api/admin/keys/'+legacy.value.keyId,{method:'PATCH',cookie:first.cookie,body:{allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0,consumption_policy:'api_only',expected_consumption_policy_version:0}})];
 for(const response of denied){assert.equal(response.status,503);assert.equal(response.value.error.code,'consumption_runtime_not_ready');}
 assert.deepEqual(await f.setup.get('SELECT (SELECT count(*) FROM harbor.members) members,(SELECT count(*) FROM harbor.keys) keys,(SELECT count(*) FROM harbor.audit) audits'),baseline);assert.deepEqual(await f.setup.get('SELECT * FROM harbor.keys WHERE id=$1',legacy.value.keyId),key);assert.equal(f.seen.filter(row=>row.inference).length,0);
 assert.equal((await first.call('/api/admin/keys/'+legacy.value.keyId,{method:'PATCH',cookie:first.cookie,body:{enabled:true}})).status,200,'legacy writes remain supported');
 await first.app.close();const restarted=await f.start();assert.equal((await restarted.call('/api/admin/state?view=overview',{cookie:restarted.cookie})).value.capabilities.consumptionPolicy,true);
 const created=await restarted.call('/api/admin/members',{method:'POST',cookie:restarted.cookie,body:{name:'合成已启用来源',quota:null,expires:null,consumption_policy:'api_only'}});assert.equal(created.status,201);
 const added=await restarted.call('/api/admin/keys',{method:'POST',cookie:restarted.cookie,body:{memberId:legacy.value.memberId,expires:null,consumption_policy:'api_only'}});assert.equal(added.status,201);
 const patched=await restarted.call('/api/admin/keys/'+legacy.value.keyId,{method:'PATCH',cookie:restarted.cookie,body:{allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0,consumption_policy:'api_only',expected_consumption_policy_version:0}});assert.equal(patched.status,200);assert.equal(patched.value.consumption_policy,'api_only');
 assert.equal((await restarted.call('/api/me',{secret:legacy.value.secret})).value.consumption_policy,'api_only');assert.equal((await restarted.call('/api/admin/keys/'+legacy.value.keyId+'/policy',{cookie:restarted.cookie})).value.consumption_policy,'api_only');
 const api=randomUUID(),sub=randomUUID();await f.setup.run("INSERT INTO harbor.accounts(id,name,home,auth_mode,api_weight,state) VALUES($1,'合成热启API',$1,'openai_api',25,'ready'),($2,'合成热启订阅',$2,'subscription',0,'ready')",api,sub);
 f.seen.length=0;const inference=await restarted.call('/v1/responses',{method:'POST',secret:legacy.value.secret,body:{model:'gpt-6.1-sol',input:'合成热安装验收',stream:false}});assert.equal(inference.status,200);assert.equal(inference.headers.get('x-harbor-consumption-source'),'api');assert.equal(f.seen.filter(row=>row.inference&&row.source==='api').length,1);assert.equal(f.seen.filter(row=>row.inference&&row.source==='subscription').length,0);
},{install:false}));

test('real transient read-backend termination retries schema initialization while successful reads stay cached',integration,()=>postgresFixture(async f=>{
 const {app,call,cookie}=await f.start(),created=await call('/api/admin/members',{method:'POST',cookie,body:{name:'合成连接恢复',quota:null,expires:null,consumption_policy:'api_only'}});assert.equal(created.status,201);
 let terminateNext=false,initializations=0;
 for(const store of new Set([app.store,app.queryStore])){const query=store.query.bind(store);store.query=async(sql,args)=>{if(sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1'&&args?.[0]==='schema.key-consumption-policy'){initializations++;if(terminateNext){terminateNext=false;return query('SELECT pg_terminate_backend(pg_backend_pid())');}}return query(sql,args);};}
 for(const path of ['/api/me','/v1/models','/api/admin/keys/'+created.value.keyId+'/policy']){
  const auth=path.startsWith('/api/admin')?{cookie}:{secret:created.value.secret};terminateNext=true;const failed=await call(path,auth);assert.equal(failed.status,503,path);assert.equal(failed.value.error.code,'database_unavailable');
  const prior=initializations,recovered=await call(path,auth);assert.equal(recovered.status,200,path);assert.equal(initializations,prior+1);const successful=initializations;assert.equal((await call(path,auth)).status,200,path);assert.equal(initializations,successful,'steady reads do not rescan schema');
 }
}));
