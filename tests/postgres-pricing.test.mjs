import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {Readable} from 'node:stream';
import {readFileSync} from 'node:fs';
import {PostgresStore} from '../postgres-store.mjs';
import {usageWindow} from '../store.mjs';
import {createPostgresPricingRuntime} from '../postgres-pricing-runtime.mjs';
import {createPostgresPricingAdmin} from '../postgres-pricing-admin.mjs';
import {modelReleaseSource,modelReleaseSetting} from '../model-release-dates.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false},at=Date.parse('2026-11-09T12:00:00+08:00');
const rate={input:'3',cached:'0.3',output:'15',source:'synthetic official fixture'};
const page=model=>`# Pricing\n### Standard pricing data\n| Model | Input | Cached input | Output |\n| --- | --- | --- | --- |\n| ${model} | $3 | $0.3 | $15 |\n### Batch pricing data\n`;
test('PG官方分行上下文费率刷新发布短上下文价格且不为评审别名猜价',integration,()=>fixture(async({store,runtime})=>{
 await runtime.refresh({force:true});
 const state=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value),book=await store.pricingBook(state.active);
 assert.equal(state.error,null);assert.equal(state.lastSuccess,at);
 for(const [model,input,output,cached] of [['gpt-6-cyber','20.00','100.00','2.00'],['gpt-6-rosalind','15.00','75.00','1.50']]){
  assert.equal(book.rates[model].input,input);assert.equal(book.rates[model].output,output);assert.equal(book.rates[model].cached,cached);
 }
 assert.equal(Object.hasOwn(book.rates,'codex-auto-review'),false);
},{fetchImpl:async()=>new Response(readFileSync(new URL('./fixtures/pricing-specialized-context.md',import.meta.url),'utf8'))}));
function options(name='postgres'){const u=new URL(url);u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};}
async function fixture(run,optionsRuntime={}){
 const admin=new PostgresStore(options()),name='harbor_price_'+randomUUID().replaceAll('-','');let store,runtime;
 try{
  await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
  store=new PostgresStore(options(name));await store.migrate();await store.initializePricing();
  const member=randomUUID(),key=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成定价测试',at-10000);
  await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$3,$4)',key,member,'synthetic-pricing-'+key,at-10000);
  runtime=createPostgresPricingRuntime(store,{clock:()=>at,...optionsRuntime});
  const record=async({model='new-model',started=at-2000,ended=at-1000,unknown=false}={})=>{
   const id=await store.observe({id:member,key_id:key},'synthetic','synthetic',model,{at:started});
   if(ended!==null)await store.finish(id,{input:1000,output:50,cached:200,usageKnown:!unknown},ended);return id;
  };
  const row=id=>store.get('SELECT r.*,p.state price_state,p.total_atoms::text amount FROM harbor.requests r JOIN harbor.request_prices p ON p.request_id=r.id WHERE r.id=$1',id);
  await run({store,runtime,record,row,name});
 }finally{await runtime?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.close();}
}

test('PG新增费率只回算近7天旧未定价，不改已价、手调、未知、缺book、历史和真实tokens',integration,()=>fixture(async({store,runtime,record,row})=>{
 const eligible=await record(),priced=await record({model:'gpt-6.1-sol'}),unknown=await record({unknown:true}),manual=await record(),missing=await record(),historical=await record({started:usageWindow(at).start-2000,ended:usageWindow(at).start-1000}),review=await record({model:'codex-auto-review'});
 await store.run('UPDATE harbor.requests SET manual_cost_atoms=$1 WHERE id=$2','123',manual);await store.projectPrice({id:manual});
 await store.run('UPDATE harbor.requests SET price_version=$1 WHERE id=$2','missing-synthetic-book',missing);await store.projectPrice({id:missing});
 const before=new Map();for(const id of [eligible,priced,unknown,manual,missing,historical,review])before.set(id,await row(id));
 const initial=await runtime.status();await runtime.configure({expectedVersion:initial.settings.version,manualRates:{'new-model':rate,'gpt-6.1-sol':rate}});
 assert.equal((await runtime.backfill()).changed,1);
 const updated=await row(eligible);assert.equal(updated.price_state,'priced');assert.equal(updated.amount,'3210000000');assert.notEqual(updated.price_version,before.get(eligible).price_version);
 for(const id of [priced,unknown,manual,missing,historical,review])assert.deepEqual(await row(id),before.get(id));
 for(const id of before.keys())for(const field of ['input','output','cached'])assert.equal((await row(id))[field],before.get(id)[field]);
 assert.equal((await runtime.backfill()).changed,0);
}));

test('PG日期alias按请求时间生效，批次有界且连续，不猜测codex-auto-review价格',integration,()=>fixture(async({runtime,record,row})=>{
 const ids=[];for(let i=0;i<5;i++)ids.push(await record({model:'alias-model',started:at-2000+i,ended:at-1000+i}));
 const before=await record({model:'alias-model',started:at-4000}),review=await record({model:'codex-auto-review'});
 await runtime.configure({expectedVersion:0,aliases:[{name:'alias-model',target:'gpt-6.1-sol',from:at-2000,to:null,source:'synthetic documented dated mapping'}]});
 const results=[];for(let i=0;i<4;i++)results.push(await runtime.backfill());
 assert.deepEqual(results.map(r=>r.scanned),[2,2,2,1]);assert.equal(results.reduce((n,r)=>n+r.changed,0),5);assert.equal(results.at(-1).complete,true);
 for(const id of ids)assert.equal((await row(id)).price_state,'priced');assert.equal((await row(before)).price_state,'unpriced');assert.equal((await row(review)).price_state,'unpriced');
},{batchSize:2}));

test('PG并发配置expectedVersion只接受一个，官方刷新保留最新人工配置，失败保留有效版本',integration,()=>fixture(async({store,runtime})=>{
 const other=createPostgresPricingRuntime(store,{clock:()=>at});
 try{
  const changes=await Promise.allSettled([runtime.configure({expectedVersion:0,enabled:false}),other.configure({expectedVersion:0,refreshHours:5})]);
  assert.equal(changes.filter(r=>r.status==='fulfilled').length,1);assert.equal(changes.filter(r=>r.status==='rejected').length,1);
  const current=await runtime.status();let release;const gate=new Promise(resolve=>release=resolve);let entered;const started=new Promise(resolve=>entered=resolve);
  const syncing=createPostgresPricingRuntime(store,{clock:()=>at,fetchImpl:async()=>{entered();await gate;return new Response(page('new-official'));}});
  const pending=syncing.refresh({force:true});await started;await runtime.configure({expectedVersion:current.settings.version,manualRates:{'manual-only':rate}});release();await pending;
  const value=await runtime.status();assert.ok(value.rates.some(r=>r.model==='new-official'));assert.ok(value.rates.some(r=>r.model==='manual-only'&&r.manual));
  await syncing.close();const previous=value.state.active;
  const failing=createPostgresPricingRuntime(store,{clock:()=>at+1000,fetchImpl:async()=>new Response('changed format')});await failing.refresh({force:true});assert.equal((await runtime.status()).state.active,previous);assert.ok((await runtime.status()).state.error);await failing.close();
 }finally{await other.close();}
}));

test('PG发布存储失败整体回滚，旧state与settings不丢失',integration,()=>fixture(async({store,runtime})=>{
 const before=await runtime.status(),run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic failure');return run(sql,...args);};
 await assert.rejects(runtime.configure({expectedVersion:0,manualRates:{'new-model':rate}}),/synthetic failure/);store.run=run;
 const after=await runtime.status();assert.deepEqual(after.state,before.state);assert.deepEqual(after.settings,before.settings);
}));

test('PG结算与回算并发遵守请求行锁，不覆盖结算或将未知计价',integration,()=>fixture(async({store,runtime,record,row,name})=>{
 const id=await record(),blocker=new PostgresStore(options(name));let release,entered;
 const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
 const locked=blocker.transaction(async()=>{await blocker.get('SELECT id FROM harbor.requests WHERE id=$1 FOR UPDATE',id);entered();await gate;await blocker.run('UPDATE harbor.requests SET usage_unknown=1 WHERE id=$1',id);await blocker.projectPrice({id});});
 try{await started;await runtime.configure({expectedVersion:0,manualRates:{'new-model':rate}});const repair=runtime.backfill();release();await locked;await repair;assert.equal((await row(id)).price_state,'unmetered');assert.equal((await row(id)).usage_unknown,1);}
 finally{release();await locked;await blocker.close();}
}));

test('PG关闭取消官方请求并等待清理，关闭后不再发布',integration,()=>fixture(async({store,runtime})=>{
 let entered;const started=new Promise(resolve=>entered=resolve);
 const worker=createPostgresPricingRuntime(store,{clock:()=>at,fetchImpl:async(_url,{signal})=>{entered();return new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}));}});
 const before=(await runtime.status()).state,pending=worker.refresh({force:true});await started;await worker.close();await pending;assert.deepEqual((await runtime.status()).state,before);assert.throws(()=>worker.backfill(),{code:'service_stopping'});
}));

test('PG管理适配器鉴权、读取body和GET/PATCH/refresh入口',integration,()=>fixture(async({store})=>{
 let authenticated=false;const admin=createPostgresPricingAdmin(store,{clock:()=>at,adminAuthenticated:async()=>authenticated,fetchImpl:async()=>new Response(page('admin-model'))});
 const send=async(path,method='GET',payload)=>{
  const req=Readable.from(payload?[Buffer.from(JSON.stringify(payload))]:[]);req.url=path;req.method=method;req.headers={'content-type':'application/json'};
  const res={headers:{},getHeader(name){return this.headers[name];},setHeader(name,value){this.headers[name]=value;},writeHead(status,headers){this.status=status;Object.assign(this.headers,headers);},end(value){this.value=JSON.parse(value);}};const handled=await admin.handle(req,res);return {handled,...res};
 };
 try{assert.equal((await send('/other')).handled,false);await assert.rejects(send('/api/admin/pricing'),{status:401});authenticated=true;
  const initial=await send('/api/admin/pricing');assert.equal(initial.status,200);assert.equal(initial.headers['Cache-Control'],'no-store');assert.match(initial.headers['Server-Timing'],/^app;dur=\d+\.\d{2}$/);
  const configured=await send('/api/admin/pricing','PATCH',{expectedVersion:0,enabled:false});assert.equal(configured.value.settings.enabled,false);assert.equal(configured.headers['Server-Timing'],undefined);
  const refreshed=await send('/api/admin/pricing/refresh','POST');assert.ok(refreshed.value.rates.some(r=>r.model==='admin-model'));assert.equal(refreshed.headers['Server-Timing'],undefined);await assert.rejects(send('/api/admin/pricing','DELETE'),{status:405});
 }finally{await admin.close();}
}));

test('PG周期同步遵守开关与间隔，新增官方模型回算且保留既有模型',integration,()=>fixture(async({store,runtime,record,row})=>{
 const id=await record({model:'new-official'});let calls=0,time=at;
 const automatic=createPostgresPricingRuntime(store,{clock:()=>time,fetchImpl:async url=>{if(url===modelReleaseSource)return new Response('synthetic unavailable release metadata',{status:503});calls++;return new Response(page('new-official'));}});
 try{
  await runtime.configure({expectedVersion:0,enabled:false});await automatic.tick();assert.equal(calls,0);assert.equal((await row(id)).price_state,'unpriced');
  await runtime.configure({expectedVersion:1,enabled:true,refreshHours:1});await automatic.tick();assert.equal(calls,1);assert.equal((await row(id)).price_state,'priced');
  assert.ok((await runtime.status()).rates.some(r=>r.model==='gpt-6.1-sol'));
  await automatic.tick();assert.equal(calls,1);time+=3600001;await automatic.tick();assert.equal(calls,2);
 }finally{await automatic.close();}
}));

test('PG每批回算投影失败回滚请求版本与游标，重试仍完整恢复',integration,()=>fixture(async({store,runtime,record,row})=>{
 const ids=[await record(),await record()],before=await Promise.all(ids.map(row));
 await runtime.configure({expectedVersion:0,manualRates:{'new-model':rate}});
 const project=store.projectPrice.bind(store);let calls=0;store.projectPrice=async(...args)=>{if(++calls===2)throw Error('synthetic projection failure');return project(...args);};
 await assert.rejects(runtime.backfill(),/synthetic projection failure/);store.projectPrice=project;
 assert.deepEqual(await Promise.all(ids.map(row)),before);assert.equal((await runtime.backfill()).changed,2);
 for(const id of ids)assert.equal((await row(id)).price_state,'priced');
}));


test('PG回算批次上限100，拒绝超限配置',()=>{
 const valid=createPostgresPricingRuntime({}, {batchSize:100});void valid.close();
 for(const batchSize of [0,101,1000,1.5])assert.throws(()=>createPostgresPricingRuntime({}, {batchSize}),{code:'pricing_configuration'});
});

test('PG官方发布日期独立保存和失败保留，不改变计价book且状态读取不联网',integration,()=>fixture(async({store})=>{
 let releaseFailure=false,releaseDay='Nov 8',calls=0;
 const automatic=createPostgresPricingRuntime(store,{clock:()=>at,fetchImpl:async url=>{
  calls++;if(url!==modelReleaseSource)return new Response(page('synthetic-release-model'));
  return releaseFailure?new Response('synthetic unavailable',{status:503}):new Response(`# Changelog\n\n## November, 2026\n\n### ${releaseDay}\n\nFeature · Model: synthetic-release-model\n\nReleased [Synthetic](https://developers.openai.com/api/docs/models/synthetic-release-model) for testing.\n`);
 }});
 try{
  assert.equal((await automatic.status()).rates.find(row=>row.model==='gpt-6.1-sol').release_date,'2026-09-29');assert.equal(calls,0);
  await automatic.refresh({force:true});const first=await automatic.status(),revision=first.state.active,book=await store.pricingBook(revision);assert.equal(first.rates.find(row=>row.model==='synthetic-release-model').release_date,'2026-11-08');
  releaseDay='Nov 7';await automatic.refresh({force:true});assert.equal((await automatic.status()).state.active,revision);assert.deepEqual(await store.pricingBook(revision),book);assert.equal((await automatic.status()).rates.find(row=>row.model==='synthetic-release-model').release_date,'2026-11-07');
  releaseFailure=true;await automatic.refresh({force:true});const failed=await automatic.status();assert.equal(failed.state.active,revision);assert.equal(failed.rates.find(row=>row.model==='synthetic-release-model').release_date,'2026-11-07');assert.match(failed.modelReleaseState.error,/保留已核实/);assert.ok(await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',modelReleaseSetting));
  await automatic.configure({expectedVersion:0,enabled:false});const prior=calls;await automatic.refresh();await automatic.status();assert.equal(calls,prior);
 }finally{await automatic.close();}
}));

test('PG发布前已读旧active但迟提交的结算，在完成扫描后仍能回算',integration,()=>fixture(async({store,runtime,record,row})=>{
 const id=await record({ended:null}),get=store.get.bind(store);let release,entered,intercept=true;
 const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
 store.get=async(sql,...args)=>{
  const value=await get(sql,...args);
  if(intercept&&sql==="SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'"){intercept=false;entered();await gate;}
  return value;
 };
 const finishing=store.finish(id,{input:1000,output:50,cached:200},at-1000);
 try{
  await started;await runtime.configure({expectedVersion:0,manualRates:{'new-model':rate}});
  assert.deepEqual(await runtime.backfill(),{scanned:0,changed:0,complete:true});
  release();await finishing;assert.equal((await row(id)).price_state,'unpriced');
  const tokens=await store.get('SELECT input,output,cached FROM harbor.requests WHERE id=$1',id);
  assert.equal((await runtime.backfill()).changed,1);assert.equal((await row(id)).price_state,'priced');
  assert.deepEqual(await store.get('SELECT input,output,cached FROM harbor.requests WHERE id=$1',id),tokens);
 }finally{release();await finishing;store.get=get;}
}));


test('PG费率状态和写后返回均沿只读池与查询队列，不占核心池聚合',integration,()=>fixture(async({store,name})=>{
 const reader=new PostgresStore(options(name)),all=store.all.bind(store);let queued=0,reading=false,aggregates=0;
 const readAll=reader.all.bind(reader);reader.all=async(sql,...args)=>{assert.equal(reading,true);if(sql.includes('COUNT(*) requests'))aggregates++;return readAll(sql,...args);};
 store.all=async(sql,...args)=>{assert.ok(!sql.includes('COUNT(*) requests'),'后台聚合不得进入核心池');return all(sql,...args);};
 const runtime=createPostgresPricingRuntime(store,{clock:()=>at,readStore:reader,queryQueue:{run:async operation=>{queued++;reading=true;try{return await operation();}finally{reading=false;}}},fetchImpl:async()=>new Response(page('read-pool-model'))});
 try{
  await runtime.status();await runtime.configure({expectedVersion:0,enabled:false});await runtime.refresh({force:true});
  assert.equal(queued,3);assert.equal(aggregates,3);await runtime.close();
  assert.ok(await reader.get('SELECT 1 alive'));assert.ok(await store.get('SELECT 1 alive'));
 }finally{store.all=all;await runtime.close();await reader.close();}
}));
