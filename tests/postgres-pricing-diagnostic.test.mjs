import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {hash,usageWindow} from '../store.mjs';
import {publicUsage} from '../pricing.mjs';
import {postgresRequestObservation} from '../postgres-observability.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需真实隔离PG':false,timeout:30000};
const options=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};};
const diagnostic=(calculation,projection)=>({version:1,calculation,projection});
async function fixture(t){
 const admin=new PostgresStore(options()),database='harbor_pricing_diag_'+randomUUID().replaceAll('-','');let created=false,store;
 t.after(async()=>{await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();});
 await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
 const member=randomUUID(),key=randomUUID(),at=Date.now();await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成费率诊断员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,hash(randomUUID()),at);
 const create=async({model='gpt-6.1-sol',started=at-1000,result={input:100,output:20,cached:10}}={})=>{const id=await store.observe({id:member,key_id:key},'synthetic-ip','synthetic-client',model,{at:started});await store.finish(id,result,started+1);return id;};
 const check=async(id,expected)=>{const raw=await store.get('SELECT * FROM harbor.requests WHERE id=$1',id),price=await store.get('SELECT * FROM harbor.request_prices WHERE request_id=$1',id),expectedAmount=publicUsage(await store.quote(raw));const detail=await postgresRequestObservation(store,id,at);assert.deepEqual(detail.pricingDiagnostic,expected);assert.deepEqual(detail.standard,expectedAmount,'diagnostic must preserve existing detail quote');assert.deepEqual(await store.get('SELECT * FROM harbor.requests WHERE id=$1',id),raw,'detail must not rewrite request');assert.deepEqual(await store.get('SELECT * FROM harbor.request_prices WHERE request_id=$1',id),price,'detail must not repair projection');return detail;};
 return {store,at,member,key,create,check};
}
test('PG详情诊断独立检测缺投影、版本、状态及每个金额分量且不修写历史',integration,async t=>{
 const {store,create,check}=await fixture(t),valid=await create(),baseline=await check(valid,diagnostic('priced','consistent'));
 for(const [projection,sql] of [
  ['missing','DELETE FROM harbor.request_prices WHERE request_id=$1'],
  ['revision_mismatch',"UPDATE harbor.request_prices SET revision='synthetic-wrong-revision' WHERE request_id=$1"],
  ['state_mismatch',"UPDATE harbor.request_prices SET state='unpriced' WHERE request_id=$1"],
  ['amount_mismatch','UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1'],
  ['amount_mismatch','UPDATE harbor.request_prices SET input_atoms=input_atoms+1,output_atoms=output_atoms-1 WHERE request_id=$1'],
  ['amount_mismatch','UPDATE harbor.request_prices SET cached_atoms=NULL WHERE request_id=$1']
 ]){const id=await create();await store.run(sql,id);const detail=await check(id,diagnostic('priced',projection));assert.deepEqual(detail.standard,baseline.standard);}
 const large=await create({result:{input:1000000001,output:1000000003,cached:1}});await check(large,diagnostic('priced','consistent'));await store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1',large);await check(large,diagnostic('priced','amount_mismatch'));
});
test('PG详情区分缺固定费率版本、未知模型及缓存费率缺失',integration,async t=>{
 const {store,at,create,check}=await fixture(t);
 const missingBook=await create();await store.run("UPDATE harbor.requests SET price_version='synthetic-missing-book' WHERE id=$1",missingBook);await store.run("UPDATE harbor.request_prices SET revision='synthetic-missing-book' WHERE request_id=$1",missingBook);await check(missingBook,diagnostic('price_book_missing','book_missing'));
 const unknown=await create({model:'synthetic-unpriced-model'});await check(unknown,diagnostic('model_rate_missing','consistent'));
 const revision='synthetic-null-cache',book={rates:{'synthetic-cached-model':{input:'2',output:'10',cached:null}},aliases:[]};await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3)',revision,at,JSON.stringify(book));await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active:revision}));
 const cached=await create({model:'synthetic-cached-model'});await check(cached,diagnostic('cached_rate_missing','consistent'));
 const uncached=await create({model:'synthetic-cached-model',result:{input:100,output:20,cached:0}});await check(uncached,diagnostic('priced','consistent'));
});
test('PG旧请求缺固定版本仍沿用active金额但明确报告版本缺失',integration,async t=>{
 const {store,create,check}=await fixture(t),id=await create(),before=await check(id,diagnostic('priced','consistent'));await store.run('UPDATE harbor.requests SET price_version=NULL WHERE id=$1',id);const after=await check(id,diagnostic('priced','request_version_missing'));assert.deepEqual(after.standard,before.standard);
 await store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='pricing.state'");await check(id,diagnostic('price_version_missing','request_version_missing'));
});
test('PG费率诊断详情沿用部门权限和近7天可见范围',integration,async t=>{
 const {store,at,create}=await fixture(t),visible=await create(),expired=await create({started:usageWindow(at).start-10});await migrateDepartmentIsolation(store,{apply:true});
 await store.run("INSERT INTO harbor.departments(id,name,created,updated) VALUES('other-department','合成其他部门',$1,$1)",at);const issuer=createDepartmentScopeIssuer(),own=issuer.superAdmin({departmentId:'department-default',credentialId:'synthetic-admin',credentialVersion:0}),other=issuer.departmentAdmin({departmentId:'other-department',credentialId:'synthetic-other',credentialVersion:0});
 assert.deepEqual((await postgresRequestObservation(store,visible,at,{scope:own})).pricingDiagnostic,diagnostic('priced','consistent'));
 await assert.rejects(postgresRequestObservation(store,visible,at,{scope:other}),{status:404});await assert.rejects(postgresRequestObservation(store,visible,at,{scope:{...own}}),{status:403});await assert.rejects(postgresRequestObservation(store,expired,at,{scope:own}),{status:404});assert.ok(await store.get('SELECT id FROM harbor.requests WHERE id=$1',expired));
});
