import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需真实隔离PG':false,timeout:30000};
const options=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};};
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
async function fixture(t){
 const root=new PostgresStore(options()),name='harbor_member_read_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(options(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();
 const member=randomUUID(),key=randomUUID(),at=Date.now();await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,'synthetic-'+key,at);
 return {store,member,key,at};
}
const rollupRead=(sql,args)=>sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1'&&args[0]==='schema.usage-hour-rollups';

test('HTTP员工元数据不依赖汇总校验，瞬时用量初始化失败后下一请求可恢复',integration,async t=>{
 const {store}=await fixture(t),get=store.get.bind(store);let reads=0,reject=true;
 store.get=async(sql,...args)=>{if(rollupRead(sql,args)){reads++;if(reject){reject=false;throw Object.assign(Error('synthetic timeout'),{code:'57014'});}}return get(sql,...args);};
 const admin=createPostgresMemberAdmin(store),server=http.createServer((req,res)=>{void(async()=>{const u=new URL(req.url,'http://localhost'),result=await admin.handle(u.pathname,'GET',{},u.searchParams);res.writeHead(result.status,{'content-type':'application/json'});res.end(JSON.stringify(result.value));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});
 t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close();});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const call=async params=>{const response=await fetch('http://127.0.0.1:'+server.address().port+'/api/admin/members?'+params);return {status:response.status,value:await response.json()};};
 assert.equal((await call('view=list')).status,200);assert.equal(reads,0);const failed=await call('usage_profile=list');assert.equal(failed.status,503);assert.equal(failed.value.code,'query_timeout');assert.equal(reads,1);
 assert.equal((await call('view=list')).status,200);assert.equal(reads,1);assert.equal((await call('usage_profile=list')).status,200);assert.equal(reads,2);assert.equal((await call('usage_profile=list')).status,200);assert.equal(reads,2);
});
test('汇总契约错误仍阻断用量但不阻断无关元数据、选择和Key权限',integration,async t=>{
 const {store,key}=await fixture(t),get=store.get.bind(store);let reads=0;
 store.get=async(sql,...args)=>{if(rollupRead(sql,args)){reads++;return {value:JSON.stringify({version:999,checksum:'synthetic'})};}return get(sql,...args);};
 const queries=createPostgresMemberQueries(store),metadata=memberQuery(new URLSearchParams('view=list')),usage=memberQuery(new URLSearchParams());
 await assert.rejects(queries.list(usage),{status:503});assert.equal(reads,1);await assert.rejects(queries.list(usage),{status:503});assert.equal(reads,1);
 assert.equal((await queries.list(metadata)).rows.length,1);assert.equal((await queries.selection(metadata)).members.length,1);assert.ok(await queries.keyPolicy(key));assert.equal(reads,1);
});
test('取消后的员工查询保持执行槽直至当前SQL返回且不继续后续SQL',integration,async t=>{
 const {store}=await fixture(t),all=store.all.bind(store),get=store.get.bind(store),entered=deferred(),release=deferred(),queue=createPostgresQueryQueue({maxConcurrent:1}),controller=new AbortController();let blocked=true,memberCounts=0;
 store.all=async(sql,...args)=>{const rows=await all(sql,...args);if(blocked&&sql.startsWith('SELECT id,code,name,description,enabled FROM harbor.member_types')){blocked=false;entered.resolve();await release.promise;}return rows;};store.get=async(sql,...args)=>{if(sql.startsWith('SELECT COUNT(*) total FROM harbor.members'))memberCounts++;return get(sql,...args);};
 const queries=createPostgresMemberQueries(store,{queryQueue:queue}),query=memberQuery(new URLSearchParams('view=list'));const first=queries.list(query,{signal:controller.signal});const rejected=assert.rejects(first,{name:'AbortError'});await entered.promise;controller.abort();const second=queries.list(query);
 assert.deepEqual(queue.status(),{active:1,queued:1,closed:false});assert.equal(memberCounts,0);release.resolve();await rejected;assert.equal((await second).rows.length,1);assert.equal(memberCounts,1);await queue.close();
});
test('员工共享限额子读取取消后不再计算其他员工',integration,async t=>{
 const {store,member,at}=await fixture(t),other=randomUUID(),config=JSON.stringify({enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'10.00'}]});await store.run('UPDATE harbor.members SET usage_limits=$1 WHERE id=$2',config,member);await store.run("INSERT INTO harbor.members(id,name,created,usage_limits) VALUES($1,'其他合成员工',$2,$3)",other,at,config);
 const rows=await store.all('SELECT id,usage_limits,usage_limits_version FROM harbor.members ORDER BY id'),all=store.all.bind(store),controller=new AbortController();let aggregates=0;
 store.all=async(sql,...args)=>{const result=await all(sql,...args);if(sql.startsWith('WITH rules AS')){aggregates++;controller.abort();}return result;};const limits=createPostgresMemberLimits(store);
 await assert.rejects(store.transaction(()=>limits.statusForRows(rows,at,{signal:controller.signal}),{level:'REPEATABLE READ',readOnly:true}),{name:'AbortError'});assert.equal(aggregates,1);
});

test('账号与首页费用读共享瞬时初始化恢复规则，元数据不触发汇总验证',integration,async t=>{
 const {store}=await fixture(t),get=store.get.bind(store);let reads=0,reject=true;
 store.get=async(sql,...args)=>{if(rollupRead(sql,args)){reads++;if(reject){reject=false;throw Object.assign(Error('synthetic timeout'),{code:'57014'});}}return get(sql,...args);};
 const states=createPostgresStateQueries(store);t.after(()=>states.close());
 assert.equal((await states.read({view:'accounts-meta',people:'none'})).view,'accounts-meta');assert.equal(reads,0);
 await assert.rejects(states.read({view:'overview-cost'}),{code:'query_timeout'});assert.equal(reads,1);
 assert.equal((await states.read({view:'accounts',people:'none'})).view,'accounts');assert.equal(reads,2);
 assert.equal((await states.read({view:'overview-cost'})).view,'overview-cost');assert.equal(reads,2);
});
