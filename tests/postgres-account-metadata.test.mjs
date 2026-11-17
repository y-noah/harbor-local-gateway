import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries,stateQuery} from '../postgres-state-query.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!url?'需独立合成PG测试库':false,timeout:30000};
const at=Date.parse('2026-11-17T12:00:00+08:00');
function options(database){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:1};}
async function fixture(run){
 const admin=new PostgresStore(options()),database='harbor_account_meta_'+randomUUID().replaceAll('-','');let store,created=false;const readers=[];
 try{await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();
  const reader=options=>{const value=createPostgresStateQueries(store,{clock:()=>at,...options});readers.push(value);return value;};await run({store,reader});
 }finally{for(const reader of readers)await reader.close();await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();}
}

test('账号元数据视图接受明确参数，重复或未知视图仍拒绝',()=>{
 assert.equal(stateQuery(new URLSearchParams('view=accounts-meta&people=none')).view,'accounts-meta');
 for(const value of ['view=accounts-meta&view=accounts','view=accounts-unknown'])assert.throws(()=>stateQuery(new URLSearchParams(value)),error=>error.status===400);
});

test('账号元数据保留完整配置与官方快照，但不读取用量、正文或计算权重，不伪造零费用',integration,()=>fixture(async({store,reader})=>{
 const quota=JSON.stringify({checked:at-1000,windows:[{minutes:10080,used:24.5,reset:at+86400000}],credits:{balance:123.45}});
 await store.run("INSERT INTO harbor.accounts(id,name,home,state,route_epoch,quota_json) VALUES('subscription','合成订阅','synthetic','ready',7,$1)",quota);
 await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_weight,api_secret_digest) VALUES('api','合成API','synthetic','ready','openai_api',25,$1)",'a'.repeat(64));
 await store.query("INSERT INTO harbor.accounts(id,name,home,removed_at) VALUES('removed','合成已移除','synthetic',1)");
 await store.query("INSERT INTO harbor.gateway_settings(name,value) VALUES('subscription_monthly_usd:subscription','{\"monthly_usd\":200}')");
 await store.run("INSERT INTO harbor.requests(id,account_id,started,ended,status,ip) VALUES('r','subscription',$1,$1,'ok','synthetic')",at);
 const complete=await reader({balancingReader:async()=>({known:true})}).read({view:'accounts'});
 const query=store.query.bind(store),forbidden=[];
 store.query=async(sql,args=[])=>{if(/\b(?:FROM|JOIN)\s+harbor\.(?:requests|request_prices|conversation_events|sessions|usage_hour_buckets)\b/i.test(sql)){forbidden.push(sql);throw Error('metadata must not scan historical data');}return query(sql,args);};
 let metadata;
 try{metadata=await reader({balancingReader:async()=>assert.fail('metadata must not compute routing weights')}).read({view:'accounts-meta'});}finally{store.query=query;}
 assert.deepEqual(forbidden,[]);
 assert.deepEqual(metadata.accounts,complete.accounts.map(({standard,...account})=>account));
 assert.equal(metadata.view,'accounts-meta');assert.equal(metadata.usagePending,true);assert.equal(metadata.balancingPending,true);
 assert.equal(Object.hasOwn(metadata,'balancing'),false);assert.equal(metadata.accounts.some(account=>Object.hasOwn(account,'standard')),false);
 assert.equal(metadata.accountPackagePolicy.enabled,false);assert.equal(typeof metadata.accountPackagePolicy.version,'number');
 const subscription=metadata.accounts.find(account=>account.id==='subscription'),api=metadata.accounts.find(account=>account.id==='api');
 assert.equal(subscription.subscription_monthly_usd,200);assert.equal(subscription.route_epoch,7);assert.equal(subscription.quota_json,quota);
 assert.equal(api.api_weight,25);assert.equal(api.api_key_configured,true);assert.equal(api.api_key_fingerprint,'aaaaaaaa');assert.equal(Object.hasOwn(api,'api_secret_digest'),false);
 await store.query('DELETE FROM harbor.requests');await store.query('DELETE FROM harbor.accounts');
 const empty=await reader().read({view:'accounts-meta'});assert.deepEqual(empty.accounts,[]);assert.equal(empty.usagePending,false);assert.equal(empty.balancingPending,false);
}));

test('账号元数据按可信团队过滤，不能通过伪造scope读取其他团队',integration,()=>fixture(async({store,reader})=>{
 await migrateDepartmentIsolation(store,{apply:true});const issuer=createDepartmentScopeIssuer();
 for(const suffix of ['a','b']){
  await store.run("INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,'合成团队',1,1)",'dept-'+suffix);
  await store.run("INSERT INTO harbor.department_admin_keys(id,department_id,digest,prefix,label,created) VALUES($1,$2,$3,'synthetic','合成管理员',1)",'admin-'+suffix,'dept-'+suffix,suffix.repeat(64));
  await store.run("INSERT INTO harbor.accounts(id,department_id,name,home) VALUES($1,$2,'同名账号','synthetic')",'account-'+suffix,'dept-'+suffix);
 }
 const read=reader(),scope=issuer.departmentAdmin({departmentId:'dept-a',credentialId:'admin-a',credentialVersion:0});
 const metadata=await read.read({view:'accounts-meta'},{scope});assert.deepEqual(metadata.accounts.map(account=>account.id),['account-a']);
 await assert.rejects(read.read({view:'accounts-meta'},{scope:{...scope,department_id:'dept-b'}}),error=>error.status===403);
 const global=await read.read({view:'accounts-meta'},{scope:issuer.superAdmin({credentialId:'root',credentialVersion:0})});assert.deepEqual(global.accounts.map(account=>account.id),['account-a','account-b']);
 await store.query("UPDATE harbor.department_admin_keys SET enabled=false WHERE id='admin-a'");await assert.rejects(read.read({view:'accounts-meta'},{scope}),error=>error.status===403);
}));
