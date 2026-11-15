import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomBytes,randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresClientCatalog} from '../postgres-client-catalog.mjs';
import {createPostgresSelfQuery} from '../postgres-self-query.mjs';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {migrateUsageRollups,backfillUsageRollups,reconcileUsageRollups,disableUsageRollups} from '../postgres-usage-rollups.mjs';
import {usageWindow} from '../store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
const listen=server=>new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const close=server=>new Promise(resolve=>{server.closeAllConnections();server.close(resolve);});
async function condition(predicate){const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'目录HTTP条件等待超时');await new Promise(resolve=>setTimeout(resolve,10));}}
async function fixture(t){
 const database='harbor_dcl_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:6}),reader=new PostgresStore({...options(database),poolSize:3});let role,backend,server,upstream,admin;const queue=createPostgresQueryQueue();
 t.after(async()=>{await backend?.close();if(server)await close(server);if(upstream)await close(upstream);await queue.close();await admin?.close();await reader.close();await store.close();await root.query('DROP DATABASE '+database);if(role)await root.query('DROP ROLE '+role);await root.close();});await store.migrate();await store.initializePricing();role=await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});await migrateDepartmentIsolation(store,{apply:true});
 const at=Date.parse('2026-11-13T08:00:00Z'),clock=()=>at,issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer,clock}),a=await departments.createDepartment(superAdmin,{name:'同名部门'}),b=await departments.createDepartment(superAdmin,{name:'同名部门'}),scopeA=issuer.forDepartment(superAdmin,a.id),scopeB=issuer.forDepartment(superAdmin,b.id);
 admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32),clock});const create=async scope=>(await admin.handle('/api/admin/members','POST',{name:'同名员工',quota:null,expires:null},undefined,{scope})).value,ma=await create(scopeA),mb=await create(scopeB),models=new Map(),seen=[],sourceCalls=[];let gate=null,malicious=false;
 const addAccount=async(department,source,slug)=>{const id=randomUUID();await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,auth_mode,state,api_weight,last_used) VALUES($1,$2,'合成模型来源',$1,$3,'ready',20,$4)",id,department,source==='api'?'openai_api':'subscription',at);models.set(id,[{slug,display_name:slug},{slug:'common-model',display_name:'合成共享模型'}]);return id;};
 const apiA=await addAccount(a.id,'api','api-a'),subA=await addAccount(a.id,'subscription','sub-a'),apiB=await addAccount(b.id,'api','api-b'),subB=await addAccount(b.id,'subscription','sub-b'),router=createPostgresRouter(store,{clock}),policies=createPostgresConsumptionPolicy(store,{clock}),self=createPostgresSelfQuery(reader,{queryQueue:queue,clock}),devices=createPostgresDeviceAccess(store);
 upstream=http.createServer(async(req,res)=>{const id=req.url.slice(1);seen.push(id);if(gate)await gate;res.setHeader('Content-Type','application/json');res.end(JSON.stringify(models.get(id)??[]));});await listen(upstream);
 backend=createPostgresClientCatalog(store,{clock,maxSources:2,deviceAccess:devices,sourceAccounts:async opts=>{sourceCalls.push(opts);if(malicious){const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',apiB);return {policy:opts.policy,candidates:[{source:'api',account}]};}return router.catalogCandidates(opts);},transportForAccount:async account=>({models:async()=>{assert.equal(store.context.getStore(),undefined,'上游目录IO不可持有数据库事务');return (await fetch('http://127.0.0.1:'+upstream.address().port+'/'+account.id)).json();}})});
 server=http.createServer((req,res)=>{void(async()=>{if(req.url==='/api/me'){const secret=String(req.headers.authorization??'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at});await devices.authorize(member,req);const value=await self(secret,{scope:store.departmentScope(member)});res.setHeader('Content-Type','application/json');res.end(JSON.stringify(value));return;}if(!await backend.handle(req,res)){res.statusCode=404;res.end();}})().catch(error=>{res.statusCode=error.status??500;res.end(JSON.stringify({code:error.code}));});});await listen(server);
 const call=(path='/v1/codex/models',secret=ma.secret)=>fetch('http://127.0.0.1:'+server.address().port+path,{headers:{authorization:'Bearer '+secret}}),setPolicy=async policy=>{const current=await policies.read(ma.keyId);await policies.set(ma.keyId,{consumption_policy:policy,expected_consumption_policy_version:current.consumption_policy_version},{scope:scopeA});};
 return {store,reader,admin,departments,superAdmin,a,b,scopeA,scopeB,ma,mb,apiA,subA,apiB,subB,seen,sourceCalls,call,setPolicy,self,at,addAccount,gate:value=>{gate=value;},malicious:value=>{malicious=value;}};
}
test('inherit目录只访问自己部门源，另一部门账号不挤占容量，两种格式与固定MCP均可用',integration,async t=>{
 const f=await fixture(t);let response=await f.call(),value=await response.json();assert.equal(response.status,200);assert.deepEqual(new Set(value.models.map(model=>model.slug)),new Set(['sub-a','api-a','common-model']));assert.deepEqual(new Set(f.seen),new Set([f.subA,f.apiA]));
 response=await f.call('/v1/models',f.mb.secret);value=await response.json();assert.equal(response.status,200);assert.deepEqual(new Set(value.models.map(model=>model.slug)),new Set(['sub-b','api-b','common-model']));
 for(let index=0;index<3;index++)await f.addAccount(f.b.id,'api','unrelated-b-'+index);f.seen.length=0;assert.equal((await f.call()).status,200);assert.deepEqual(new Set(f.seen),new Set([f.subA,f.apiA]));
 await f.admin.handle('/api/admin/keys/'+f.ma.keyId,'PATCH',{allowed_models:['api-a'],expected_model_policy_version:0},undefined,{scope:f.scopeA});value=await (await f.call('/v1/models')).json();assert.deepEqual(value.data.map(model=>model.id),['api-a']);assert.equal((await f.call('/v1/codex/image-mcp.mjs')).status,200);
});

test('显式来源目录向真实router传员工scope，API耗尽不读取B源，异常来源跨部门在IO前拒绝',integration,async t=>{
 const f=await fixture(t);await f.setPolicy('api_only');let response=await f.call(),value=await response.json();assert.equal(response.status,200);assert.deepEqual(new Set(value.models.map(model=>model.slug)),new Set(['api-a','common-model']));assert.ok(f.sourceCalls.every(call=>call.scope.department_id===f.a.id));assert.deepEqual(f.seen,[f.apiA]);
 await f.setPolicy('subscription_only');f.seen.length=0;response=await f.call();assert.equal(response.status,200);assert.deepEqual(f.seen,[f.subA]);
 await f.setPolicy('api_first');await f.store.run("UPDATE harbor.accounts SET route_block='quota_exhausted' WHERE id=$1",f.apiA);f.seen.length=0;value=await (await f.call()).json();assert.deepEqual(new Set(value.models.map(model=>model.slug)),new Set(['sub-a','common-model']));assert.deepEqual(f.seen,[f.subA]);
 await f.setPolicy('api_only');f.malicious(true);f.seen.length=0;response=await f.call();assert.equal(response.status,503);assert.equal((await response.json()).code,'client_catalog_configuration');assert.equal(f.seen.length,0);
});

test('模型目录上游等待期间部门或成员停用再恢复，旧请求不得返回目录',integration,async t=>{
 for(const target of ['department','member']){const f=await fixture(t);let release;const gate=new Promise(resolve=>{release=resolve;});f.gate(gate);const pending=f.call();try{await condition(()=>f.seen.length>0);if(target==='department'){await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});}else{await f.store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',f.ma.memberId);await f.store.run('UPDATE harbor.members SET enabled=1 WHERE id=$1',f.ma.memberId);}release();const response=await pending;assert.equal(response.status,409);assert.equal((await response.json()).code,'client_catalog_policy_changed');}finally{release();f.gate(null);await pending.catch(()=>{});}}
});

test('api/me只汇总当前员工部门并核初始scope，停复旧scope与伪造scope拒绝',integration,async t=>{
 const f=await fixture(t),memberA=await f.store.authenticate(f.ma.secret,{at:f.at}),memberB=await f.store.authenticate(f.mb.secret,{at:f.at});for(const [member,count] of [[memberA,1],[memberB,2]])for(let n=0;n<count;n++){const request=await f.store.observe(member,'synthetic','synthetic','gpt-6.1-sol',{at:f.at});await f.store.finish(request,{status:'ok',input:100,output:20,cached:0},f.at+100);}
 let value=await (await f.call('/api/me')).json();assert.equal(value.department_id,f.a.id);assert.equal(value.standard.requests,1);assert.equal(value.keyStandard.requests,1);assert.equal(value.consumption_policy,'inherit');value=await (await f.call('/api/me',f.mb.secret)).json();assert.equal(value.department_id,f.b.id);assert.equal(value.standard.requests,2);
 const scopeA=f.store.departmentScope(memberA);await assert.rejects(f.self(f.ma.secret,{scope:f.store.departmentScope(memberB)}),{code:'department_forbidden'});await assert.rejects(f.self(f.ma.secret,{scope:JSON.parse(JSON.stringify(scopeA))}),{code:'department_scope_required'});
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});await assert.rejects(f.self(f.ma.secret,{scope:scopeA}),{code:'department_forbidden'});const current=await f.store.authenticate(f.ma.secret,{at:f.at});assert.equal((await f.self(f.ma.secret,{scope:f.store.departmentScope(current)})).standard.requests,1);
});


test('api/me小时汇总与原始查询完整DTO一致，受限角色下隔离部门及多Key且保留精确计价',integration,async t=>{
 const f=await fixture(t),window=usageWindow(f.at),extra=(await f.admin.handle('/api/admin/keys','POST',{memberId:f.ma.memberId,expires:null,consumption_policy:'subscription_only'},undefined,{scope:f.scopeA})).value;
 await f.setPolicy('api_only');
 const zero=(await f.admin.handle('/api/admin/members','POST',{name:'合成零请求',quota:null,expires:null},undefined,{scope:f.scopeA})).value;
 await f.store.run("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('synthetic-self-r1',0,'{}'),('synthetic-self-r2',0,'{}')");
 const points=[window.start-1,window.start,window.start+1,window.start+3600000,window.start+3600001,window.end-1,window.end,window.start+7200000,window.start+10800000,window.start+14400000];
 for(const [i,at] of points.entries()){
  const key=i%2?extra:f.ma,id='self-synthetic-'+i;
  await f.store.run("INSERT INTO harbor.requests(id,member_id,key_id,department_id,started,status,ip,price_version) VALUES($1,$2,$3,$4,$5,'ok','synthetic',$6)",id,f.ma.memberId,key.keyId,f.a.id,at,i%3?'synthetic-self-r1':'synthetic-self-r2');
  if(i===1)continue;
  const state=i===2?'unknown':i===3?'pending':i===4?'expired':'priced',amount=state==='priced'?String(900719925474099312345n+BigInt(i)):null;
  await f.store.run('INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,$4,0)',id,state,i===5?'synthetic-self-r2':i%3?'synthetic-self-r1':'synthetic-self-r2',amount);
 }
 await f.store.run("INSERT INTO harbor.requests(id,member_id,key_id,department_id,started,status,ip) VALUES('self-other-department',$1,$2,$3,$4,'ok','synthetic')",f.mb.memberId,f.mb.keyId,f.b.id,window.start+1);
 await migrateUsageRollups(f.store,{apply:true});await backfillUsageRollups(f.store);
 const {database}=await f.store.get('SELECT current_database() database'),role=database+'_runtime',restricted=new PostgresStore({...options(database),connection:{options:'-c role='+role},poolSize:1});
 const queue=createPostgresQueryQueue();try{const self=createPostgresSelfQuery(restricted,{clock:()=>f.at,queryQueue:queue});
 assert.equal((await restricted.get('SELECT current_user name')).name,role);
 const secrets=[f.ma.secret,extra.secret,zero.secret,f.mb.secret],raw=[];for(const secret of secrets)raw.push(await f.self(secret));
 assert.equal(raw[0].standard.requests,8);assert.equal(raw[0].keyStandard.requests,3);assert.equal(raw[1].keyStandard.requests,5);assert.deepEqual(raw[0].standard,raw[1].standard);assert.equal(raw[2].standard.requests,0);assert.equal(raw[3].standard.requests,1);
 assert.equal(raw[0].consumption_policy,'api_only');assert.equal(raw[0].consumption_policy_version,1);assert.equal(raw[1].consumption_policy,'subscription_only');
 assert.equal((await reconcileUsageRollups(f.store,{publishReady:true})).ready,true);
 const sqls=[],original=restricted.query.bind(restricted);restricted.query=(sql,args)=>{sqls.push(sql);return original(sql,args);};
 const ready=[await self(secrets[0])];assert.ok(sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));assert.ok(sqls.some(sql=>sql.includes('pg_constraint')),'cold client initializes its own schema attestation');sqls.length=0;
 for(const secret of secrets.slice(1))ready.push(await self(secret));assert.deepEqual(ready,raw);assert.ok(sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));assert.ok(!sqls.some(sql=>/pg_proc|pg_constraint/.test(sql)),'warm client does not repeat full schema attestation');
 const a=await restricted.authenticate(f.ma.secret,{at:f.at}),b=await restricted.authenticate(f.mb.secret,{at:f.at});await assert.rejects(self(f.ma.secret,{scope:restricted.departmentScope(b)}),{code:'department_forbidden'});assert.equal((await self(f.ma.secret,{scope:restricted.departmentScope(a)})).department_id,f.a.id);
 await disableUsageRollups(f.store);sqls.length=0;assert.deepEqual(await self(f.ma.secret),raw[0]);assert.ok(!sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));
 }finally{await queue.close();await restricted.close();}
});
