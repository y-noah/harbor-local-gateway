import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {randomBytes,randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {usageWindow,AppError} from '../store.mjs';
import {createPostgresTypeStatistics,typeStatisticsQuery,typeStatisticsConditions} from '../postgres-type-statistics.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {requestsQuery} from '../observability.mjs';
import {recordingQuery,recordingScope} from '../postgres-recording-query.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPostgresRecordingExports} from '../postgres-recording-export.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {createPostgresTypeStatisticsAdmin} from '../postgres-type-statistics-admin.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需隔离PG；CI test_postgres执行真实链路':false};
function options(name='postgres'){
  if(url){const address=new URL(url);address.pathname='/'+name;return {connectionString:address.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database:name}};
}
const at=Date.parse('2026-11-03T12:00:00+08:00'),window=usageWindow(at);
async function fixture(run){
  const admin=new PostgresStore(options()),name='harbor_test_'+randomUUID().replaceAll('-','');let store,reader;
  try{
    await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");store=new PostgresStore(options(name));await store.migrate();await store.initializePricing();
    const ids=[];for(let index=0;index<3;index++){const id=randomUUID(),key=randomUUID();ids.push({id,key});await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',id,'合成统计员工'+index,at-10000);await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$3,$4)',key,id,'synthetic-stat-key-'+index,at-10000);}
    const record=async({member=0,type='usage-a',version=1,status='ok',model='gpt-6.1-sol',started=at-1000,ended=at-100,input=100,output=20,cached=50,known=true}={})=>{
      const owner=ids[member],request=await store.observe({id:owner.id,key_id:owner.key},'synthetic','synthetic',model,{at:started});const catalog=type?await store.get('SELECT code,name FROM harbor.member_types WHERE id=$1',type):null;
      await store.run('UPDATE harbor.requests SET usage_type_id=$1,usage_type_code=$2,usage_type_name=$3,usage_type_version=$4 WHERE id=$5',type,catalog?.code??null,catalog?.name??null,version,request);
      if(!['queued','running'].includes(status))await store.finish(request,{status,input,output,cached,usageKnown:known},ended);else await store.run('UPDATE harbor.requests SET status=$1,business_status=$1 WHERE id=$2',status,request);
      return request;
    };
    reader=createPostgresTypeStatistics(store,{clock:()=>at});await run({store,reader,record,ids,name});
  }finally{await reader?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.close();}
}
const atoms=exact=>{const [whole,fraction='']=exact.split('.');return BigInt(whole)*1000000000000n+BigInt(fraction.padEnd(12,'0'));};

test('type filters validate exact IDs, bounded dates, pagination, missing identities and SQL parameter offsets',()=>{
  const query=typeStatisticsQuery(new URLSearchParams({type:'usage-a',group_by:'model',page:'2',page_size:'10',from:String(window.start),to:String(window.end)}));assert.equal(query.groupBy,'model');assert.equal(query.pageSize,10);
  const scope=typeStatisticsConditions(query,{at,start:3,alias:'req'});assert.match(scope.where,/req.usage_at>=\$3/);assert.match(scope.where,/req.usage_type_id=\$5/);assert.deepEqual(scope.values,[window.start,window.end,'usage-a']);assert.ok(!scope.where.includes('members'));
  assert.match(typeStatisticsConditions({type:'historical_unknown'},{at}).where,/usage_type_version IS NULL/);assert.match(typeStatisticsConditions({type:'unclassified'},{at}).where,/usage_type_version IS NOT NULL/);
  for(const input of ['page=0','page_size=101','from=-1','to=no','group_by=account','type=bad%27','status=reconciled','member_missing=yes','model=x&model_missing=true','page=1&page=2','group_by=type&groupBy=model','anything=1'])assert.throws(()=>typeStatisticsQuery(new URLSearchParams(input)),{code:'type_statistics_invalid'});
  for(const q of [{from:window.start-1},{to:window.end+1},{from:at,to:at},{page:Infinity}])assert.throws(()=>typeStatisticsConditions(q,{at}),{code:'type_statistics_invalid'});
  assert.throws(()=>typeStatisticsConditions({}, {at,alias:'r;DROP'}),{code:'type_statistics_invalid'});
});

test('type version budget avoids redundant request scans without rejecting unused catalogue versions',integration,async()=>fixture(async({store,record})=>{
  const request=await record(),original=store.all.bind(store),queries=[];store.all=async(sql,...args)=>{queries.push(sql);return original(sql,...args);};
  const reader=createPostgresTypeStatistics(store,{clock:()=>at,maxVersions:1});
  try{
    const initial=await reader.read();assert.equal(initial.summary.requests,1);assert.equal(queries.filter(sql=>sql.startsWith('SELECT DISTINCT p.revision')).length,0);
    const version=await store.get('SELECT price_version FROM harbor.requests WHERE id=$1',request);
    await store.run('INSERT INTO harbor.pricing_versions SELECT $1,created,book FROM harbor.pricing_versions WHERE id=$2','synthetic-unused-version',version.price_version);
    queries.length=0;const withUnused=await reader.read();assert.deepEqual(withUnused.summary,initial.summary);assert.deepEqual(withUnused.rows,initial.rows);assert.equal(queries.filter(sql=>sql.startsWith('SELECT DISTINCT p.revision')).length,1);
    const second=await record();await store.run('UPDATE harbor.requests SET price_version=$1 WHERE id=$2','synthetic-unused-version',second);await store.run('UPDATE harbor.request_prices SET revision=$1 WHERE request_id=$2','synthetic-unused-version',second);
    await assert.rejects(reader.read(),{code:'type_statistics_capacity'});
  }finally{await reader.close();store.all=original;}
}));

test('query admission is bounded and shared queue remains available when the type reader closes',async()=>{
  let release;const gate=new Promise(resolve=>{release=resolve;}),queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:0}),store={get:async()=>{},all:async()=>{},transaction:async()=>{await gate;throw new AppError(503,'synthetic','fixture_stop');}},reader=createPostgresTypeStatistics(store,{queryQueue:queue});
  const first=assert.rejects(reader.read(),{code:'fixture_stop'});await assert.rejects(reader.read(),{code:'query_queue_full'});const closing=reader.close();await assert.rejects(reader.read(),{code:'query_closed'});release();await Promise.all([first,closing]);assert.equal(queue.status().closed,false);assert.equal(await queue.run(async()=>7),7);await queue.close();
});

test('types use request snapshots across classification/name changes, including disabled types and removed employees',integration,async()=>fixture(async({store,reader,record,ids})=>{
  const first=await record({type:'usage-a'});await store.run("UPDATE harbor.members SET usage_type_id='usage-b',usage_type_version=2 WHERE id=$1",ids[0].id);await record({type:'usage-b',version:2});await record({type:null,version:0,member:1});await record({type:null,version:null,member:2});await store.run("UPDATE harbor.member_types SET name='更新后的任务说明',enabled=0 WHERE id='usage-a'");await store.run('UPDATE harbor.members SET removed_at=$1 WHERE id=$2',at,ids[0].id);
  const result=await reader.read();assert.equal(result.summary.requests,4);assert.equal(result.rows.length,4);const a=result.rows.find(row=>row.typeId==='usage-a'),b=result.rows.find(row=>row.typeId==='usage-b');assert.equal(a.requests,1);assert.equal(b.requests,1);assert.equal(a.name,'更新后的任务说明');assert.equal(a.classificationSource,'request_snapshot');assert.equal(a.labelSource,'current_type_catalog');assert.equal(result.rows.find(row=>row.type==='unclassified').requests,1);assert.equal(result.rows.find(row=>row.type==='historical_unknown').requests,1);assert.equal((await store.get('SELECT usage_type_name FROM harbor.requests WHERE id=$1',first)).usage_type_name,'复杂研发');
  const filtered=await reader.read({...a.filters,groupBy:'member'});assert.equal(filtered.summary.requests,1);assert.equal(filtered.rows[0].memberId,ids[0].id);const condition=typeStatisticsConditions(a.filters,{at});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests r WHERE '+condition.where,...condition.values)).n,1);
}));

test('business outcomes remain independent of manual pricing and exact USD totals align across all type pages',integration,async()=>fixture(async({store,reader,record})=>{
  await record({type:'usage-a',status:'ok'});const manual=await record({type:'usage-a',status:'failed'});await store.run("UPDATE harbor.requests SET status='reconciled',manual_cost_atoms='9007199254740993' WHERE id=$1",manual);await store.projectPrice(await store.get('SELECT * FROM harbor.requests WHERE id=$1',manual),at);
  for(const [type,status] of [['usage-b','cancelled'],['usage-c','rejected'],['usage-d','interrupted'],['usage-e','unknown'],['usage-f','running']])await record({type,status,known:status!=='unknown'});await record({type:null,version:0,status:'queued'});await record({type:null,version:null,status:'ok',model:'synthetic-unpriced',cached:0});
  const result=await reader.read({pageSize:2});assert.equal(result.summary.requests,9);assert.equal(result.summary.outcomes.success,2);assert.equal(result.summary.outcomes.failed,1);assert.equal(result.summary.failureRate,1/3);assert.equal(result.summary.standard.pending,2);assert.equal(result.summary.standard.unmetered,1);assert.equal(result.summary.standard.unpriced,1);assert.equal(result.pagination.total,8);assert.ok(atoms(result.summary.standard.total_exact)>9007199254740993n);
  let count=0,total=0n;const seen=new Set();for(let page=1;page<=result.pagination.totalPages;page++){const value=await reader.read({pageSize:2,page});assert.equal(value.summary.requests,9);for(const row of value.rows){assert.equal(seen.has(row.id),false);seen.add(row.id);count+=row.requests;if(row.standard.total_exact!==null)total+=atoms(row.standard.total_exact);}}
  assert.equal(count,result.summary.requests);assert.equal(total,atoms(result.summary.standard.total_exact));assert.equal((await reader.read({status:'failed'})).summary.requests,1);assert.equal((await reader.read({status:'failed'})).summary.outcomes.failed,1);assert.equal((await reader.read({type:'usage-f'})).summary.failureRate,null);
  assert.equal(Object.hasOwn(result.summary,'input'),false);assert.equal(Object.hasOwn(result.summary,'output'),false);assert.equal(Object.hasOwn(result.summary,'cached'),false);assert.equal(result.summary.standard.currency,'USD');assert.equal(Number.isFinite(result.summary.standard.total),true);
}));

test('the seven Beijing-day window uses completion time, excludes exact upper bounds and preserves in-flight start ownership',integration,async()=>fixture(async({store,reader,record})=>{
  await record({started:window.start-1000,ended:window.start,type:'usage-a'});await record({started:window.start-2000,ended:window.start-1,type:'usage-b'});await record({started:at,ended:window.end,type:'usage-c'});await record({started:window.start,type:'usage-d',status:'running'});await record({started:window.start-1,type:'usage-e',status:'running'});
  const result=await reader.read();assert.equal(result.summary.requests,2);assert.deepEqual(result.rows.map(row=>row.type).sort(),['usage-a','usage-d']);const narrow=await reader.read({from:window.start,to:window.start+1});assert.equal(narrow.summary.requests,2);assert.equal(narrow.window.start,window.start);await assert.rejects(reader.read({from:window.start-1}),{code:'type_statistics_invalid'});
  const sql=store.all.bind(store),observed=[];store.all=async(statement,...args)=>{observed.push(statement);return sql(statement,...args);};await reader.read({pageSize:1});store.all=sql;assert.ok(observed.every(statement=>/LIMIT/.test(statement)));assert.ok(observed.every(statement=>!statement.includes('r.*')));assert.ok(observed.every(statement=>!statement.includes('conversation_events')));
}));

test('stale price projections are unpriced and all-unpriced groups retain null amounts instead of false zero',integration,async()=>fixture(async({store,reader,record})=>{
  const request=await record();await store.run("UPDATE harbor.request_prices SET revision='synthetic-stale-revision' WHERE request_id=$1",request);const result=await reader.read();assert.equal(result.summary.standard.state,'partial');assert.equal(result.summary.standard.unpriced,1);assert.equal(result.summary.standard.total_exact,null);assert.equal(result.rows[0].standard.total_exact,null);
  const empty=await reader.read({member:randomUUID()});assert.equal(empty.summary.requests,0);assert.equal(empty.summary.standard.total_exact,'0');assert.equal(empty.summary.failureRate,null);assert.equal(empty.rows.length,0);
}));

test('model/member drilldown preserves stable IDs and explicitly handles missing identities',integration,async()=>fixture(async({store,reader,record,ids})=>{
  await record({member:0,model:'gpt-6.1-sol'});await record({member:1,model:'gpt-6.1-sol'});const missing=await record({member:2,model:'gpt-6.1-sol'});await store.run('UPDATE harbor.requests SET member_id=NULL,key_id=NULL,model=NULL,billing_model=NULL WHERE id=$1',missing);
  const models=await reader.read({groupBy:'model'});assert.equal(models.rows.length,2);for(const row of models.rows){const detail=await reader.read({...row.filters,groupBy:'member'});assert.equal(detail.summary.requests,row.requests);}const members=await reader.read({groupBy:'member'});assert.equal(members.rows.length,3);assert.ok(members.rows.some(row=>row.memberId===ids[0].id));const unknown=members.rows.find(row=>row.memberId===null);assert.equal(unknown.filters.memberMissing,true);assert.equal((await reader.read(unknown.filters)).summary.requests,1);
}));

test('byte and price-version budgets reject complete results rather than returning partial statistics',integration,async()=>fixture(async({store,record})=>{
  const first=await record();await store.run("UPDATE harbor.member_types SET name=repeat('合成',2000) WHERE id='usage-a'");const bytes=createPostgresTypeStatistics(store,{clock:()=>at,pageBytes:1024});await assert.rejects(bytes.read(),{code:'type_statistics_capacity'});await bytes.close();
  await store.run("UPDATE harbor.member_types SET name='合成任务类型' WHERE id='usage-a'");const second=await record({type:'usage-b'}),row=await store.get('SELECT price_version FROM harbor.requests WHERE id=$1',first);await store.run('INSERT INTO harbor.pricing_versions SELECT $1,created,book FROM harbor.pricing_versions WHERE id=$2','synthetic-extra-version',row.price_version);await store.run('UPDATE harbor.requests SET price_version=$1 WHERE id=$2','synthetic-extra-version',second);await store.run('UPDATE harbor.request_prices SET revision=$1 WHERE request_id=$2','synthetic-extra-version',second);
  const versions=createPostgresTypeStatistics(store,{clock:()=>at,maxVersions:1});await assert.rejects(versions.read(),{code:'type_statistics_capacity'});await versions.close();
}));


test('company total and paged groups retain the same PostgreSQL snapshot while an in-flight request settles',integration,async()=>fixture(async({store,reader,record,name})=>{
  const request=await record({status:'running'}),other=new PostgresStore(options(name)),original=store.get.bind(store);let settled=false;
  try{
    store.get=async(sql,...args)=>{const value=await original(sql,...args);if(!settled&&sql==='SELECT pg_current_snapshot()::text visibility'){settled=true;await other.finish(request,{status:'ok',input:1000,output:100,cached:0},at);}return value;};
    const snapshot=await reader.read();assert.equal(snapshot.summary.active,1);assert.equal(snapshot.summary.standard.pending,1);assert.equal(snapshot.rows[0].active,1);assert.equal(snapshot.rows[0].standard.pending,1);assert.equal(snapshot.rows[0].standard.total_exact,snapshot.summary.standard.total_exact);
    store.get=original;const fresh=await reader.read();assert.equal(fresh.summary.active,0);assert.equal(fresh.summary.outcomes.success,1);assert.equal(fresh.summary.standard.pending,0);assert.equal(fresh.rows[0].typeId,'usage-a');
  }finally{store.get=original;await other.close();}
}));

test('database timeout uses the shared actionable error and releases the query slot',async()=>{
  const reader=createPostgresTypeStatistics({get:async()=>{},all:async()=>{},transaction:async()=>{throw Object.assign(new Error('synthetic'),{code:'57014'});}});await assert.rejects(reader.read(),{code:'query_timeout'});await reader.close();assert.equal(reader.status().active,0);
});


test('new unclassified requests preserve the observed classification version without relabelling old history',integration,async()=>fixture(async({store,ids})=>{
 const current=await store.observe({id:ids[0].id,key_id:ids[0].key},'synthetic','synthetic','gpt-6.1-sol',{at});let row=await store.get('SELECT usage_type_id,usage_type_version FROM harbor.requests WHERE id=$1',current);assert.deepEqual(row,{usage_type_id:null,usage_type_version:0});await store.run("UPDATE harbor.members SET usage_type_id='usage-a',usage_type_version=1 WHERE id=$1",ids[0].id);assert.deepEqual(await store.get('SELECT usage_type_id,usage_type_version FROM harbor.requests WHERE id=$1',current),row);
 const next=await store.observe({id:ids[0].id,key_id:ids[0].key},'synthetic','synthetic','gpt-6.1-sol',{at});row=await store.get('SELECT usage_type_id,usage_type_version FROM harbor.requests WHERE id=$1',next);assert.deepEqual(row,{usage_type_id:'usage-a',usage_type_version:1});
}));
test('request drilldowns and fixed export filters preserve request-time type, business outcome and missing identities',integration,async()=>fixture(async({store,reader,record,ids})=>{
 const failed=await record({type:'usage-a',status:'failed'});await store.run("UPDATE harbor.requests SET status='reconciled',manual_cost_atoms='1000000000000' WHERE id=$1",failed);await store.projectPrice(await store.get('SELECT * FROM harbor.requests WHERE id=$1',failed),at);await record({type:'usage-b'});await record({type:null,version:0});await record({type:null,version:null});const missing=await record({type:'usage-a'});await store.run("UPDATE harbor.requests SET member_id=NULL,key_id=NULL,billing_model='',model=NULL WHERE id=$1",missing);await store.run("UPDATE harbor.members SET usage_type_id='usage-f',usage_type_version=8 WHERE id=$1",ids[0].id);
 const queries=createPostgresRequestQueries(store,{clock:()=>at,businessOutcomes:true}),jobs=createPostgresJobs(store,{clock:()=>at}),exports=createPostgresRecordingExports(store,{jobs,crypto:createRecordingIndexCrypto(randomBytes(32)),recordings:{},clock:()=>at});
 try{
  for(const input of [{type:'usage-a',status:'failed'},{type:'unclassified'},{type:'historical_unknown'},{groupBy:'model',modelMissing:true},{groupBy:'member',memberMissing:true}]){
   const stats=await reader.read(input);assert.equal(stats.rows.length,1);const filters=stats.rows[0].requestFilters,params=new URLSearchParams(Object.fromEntries(Object.entries(filters).map(([key,value])=>[key,String(value)]))),query=requestsQuery(params,{usageTypes:true}),result=await queries.read(query);assert.equal(result.pagination.total,stats.summary.requests);assert.equal(result.summary.standard.total_exact,stats.summary.standard.total_exact);
   const recQuery=recordingQuery(params,{clock:()=>at}),scope=recordingScope(recQuery);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests r WHERE '+scope.where,...scope.values)).n,stats.summary.requests);
   const exported=await exports.enqueue({include_body:false,format:'jsonl',filters:Object.fromEntries(params)}),job=await jobs.claim(['recording.export']);assert.equal(job.id,exported.id);await jobs.complete(job,await exports.handlers[job.kind](job));let text='';for await(const chunk of exports.download(exported.id))text+=chunk.toString();const rows=text.trim().split('\n').map(JSON.parse);assert.equal(rows.length,stats.summary.requests);assert.ok(rows.every(row=>!Object.hasOwn(row,'input_tokens')&&!Object.hasOwn(row,'output_tokens')));if(input.type==='unclassified')assert.equal(rows[0].type_source,'unclassified');if(input.type==='historical_unknown')assert.equal(rows[0].type_source,'historical_unknown');if(input.status==='failed'){assert.equal(rows[0].request_id,failed);assert.equal(rows[0].business_status,'failed');}
  }
  const first=await queries.read(requestsQuery(new URLSearchParams({type:'usage-a',view:'list',page_size:'1'}),{usageTypes:true}));await assert.rejects(queries.read(requestsQuery(new URLSearchParams({type:'usage-b',snapshot:first.snapshot}),{usageTypes:true})),{code:'invalid_request_snapshot'});
 }finally{await queries.close();}
}));
test('PostgreSQL type filters are opt-in for the old parser and reject contradictory or unbounded drilldown inputs',()=>{
 assert.throws(()=>requestsQuery(new URLSearchParams({type:'usage-a'})));const good=requestsQuery(new URLSearchParams({type:'historical_unknown',outcome:'failed',model_missing:'true'}),{usageTypes:true});assert.equal(good.type,'historical_unknown');assert.equal(good.modelMissing,true);
 for(const value of ['type=bad%27','outcome=reconciled','outcome=failed&status=reconciled','member=x&member_missing=true','model=x&model_missing=true','model_missing=1','from=-1','to=x','type=usage-a&type=usage-b'])assert.throws(()=>requestsQuery(new URLSearchParams(value),{usageTypes:true}));
 for(const value of ['outcome=failed&status=reconciled','model=x&model_missing=true','member=x&member_missing=true','model_missing=1'])assert.throws(()=>recordingQuery(new URLSearchParams(value),{clock:()=>at}));
});
test('type statistics HTTP requires administrator authentication before parsing and returns bounded reference-only results',integration,async()=>fixture(async({store,record})=>{
 await record({type:'usage-a'});const adapter=createPostgresTypeStatisticsAdmin(store,{clock:()=>at,adminAuthenticated:async req=>req.headers.cookie==='synthetic-admin-cookie'}),server=createServer((req,res)=>{void adapter.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}},error=>{res.writeHead(error.status||500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:error.code}}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
 try{let response=await fetch(origin+'/api/admin/type-statistics?unknown=bad',{headers:{authorization:'Bearer synthetic-member-key'}});assert.equal(response.status,401);response=await fetch(origin+'/api/admin/type-statistics?group_by=model',{headers:{cookie:'synthetic-admin-cookie'}});assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');const result=await response.json();assert.equal(result.rows[0].model,'gpt-6.1-sol');assert.equal(result.summary.requests,1);assert.equal(result.classificationSource,'request_snapshot');assert.equal(Object.hasOwn(result.summary,'input'),false);assert.equal((await fetch(origin+'/api/admin/type-statistics?page_size=101',{headers:{cookie:'synthetic-admin-cookie'}})).status,400);assert.equal((await fetch(origin+'/api/admin/type-statistics',{method:'POST',headers:{cookie:'synthetic-admin-cookie'}})).status,405);}finally{await adapter.close();await new Promise(resolve=>server.close(resolve));}
}));

test('device request filters share list/statistics/snapshot identity and preserve observed names after device changes',integration,async()=>fixture(async({store,record,ids})=>{
 const device=randomUUID();await store.run("INSERT INTO harbor.employee_devices(id,member_id,name,system,hardware_state,fingerprint_source,public_key_sha256,csr,created,enrollment_digest,enrollment_expires) VALUES($1,$2,'合成新设备名','windows','unavailable','unavailable','synthetic','synthetic',$3,$1,$4)",device,ids[0].id,at,at+10000);
 const known=await record(),unknown=await record({type:null,version:null});await store.run("UPDATE harbor.requests SET device_id=$1,device_name='请求发生时设备名',device_system='windows',device_generation=2,device_auth_state='authenticated' WHERE id=$2",device,known);
 const queries=createPostgresRequestQueries(store,{clock:()=>at,businessOutcomes:true});try{
 const input=requestsQuery(new URLSearchParams({device}),{usageTypes:true}),result=await queries.read(input);assert.equal(result.rows.length,1);assert.equal(result.rows[0].id,known);assert.equal(result.rows[0].device_name,'请求发生时设备名');assert.equal(result.summary.requests,1);assert.equal(result.rows[0].device_generation,2);const missing=await queries.read(requestsQuery(new URLSearchParams({device:'unknown'}),{usageTypes:true}));assert.deepEqual(missing.rows.map(row=>row.id),[unknown]);assert.equal(missing.summary.requests,1);await assert.rejects(queries.read({...input,device:'unknown',snapshot:result.snapshot}),{code:'invalid_request_snapshot'});assert.throws(()=>requestsQuery(new URLSearchParams({device:'not-a-device'}),{usageTypes:true}));
 }finally{await queries.close();}
}));


test('R54 historical custom ranges retain cursor pagination until the current-day boundary or configured TTL',integration,async()=>fixture(async({store,record})=>{
 let now=at;await record({started:at-3600000,ended:at-3500000});await record({started:at-3700000,ended:at-3400000});const queries=createPostgresRequestQueries(store,{clock:()=>now,snapshotTtlMs:60000,businessOutcomes:true}),params=new URLSearchParams({from:String(at-7200000),to:String(at-1000),page_size:'1',view:'list',type:'usage-a'}),query=requestsQuery(params,{usageTypes:true});
 try{const first=await queries.read(query);assert.equal(first.pagination.total,2);assert.ok(first.pagination.nextCursor);const saved=await store.get('SELECT expires_at FROM harbor.request_query_snapshots WHERE id=$1',first.snapshot);assert.equal(saved.expires_at,at+60000);const next=await queries.read({...query,snapshot:first.snapshot,cursor:first.pagination.nextCursor,page:2});assert.equal(next.rows.length,1);assert.notEqual(next.rows[0].id,first.rows[0].id);await assert.rejects(queries.read({...query,to:at-2000,snapshot:first.snapshot}),{code:'invalid_request_snapshot'});now+=60000;await assert.rejects(queries.read({...query,snapshot:first.snapshot}),{code:'invalid_request_snapshot'});
 const beforeMidnight=Date.parse('2026-11-03T23:59:59+08:00');now=beforeMidnight;const last=await queries.read(query),expires=await store.get('SELECT expires_at FROM harbor.request_query_snapshots WHERE id=$1',last.snapshot);assert.equal(expires.expires_at,usageWindow(now).end);now=usageWindow(now).end;await assert.rejects(queries.read({...query,snapshot:last.snapshot}),{code:'invalid_request_snapshot'});
 }finally{await queries.close();}
}));
test('R55 slash model names preserve type statistics request, indexed search and encrypted export drilldowns',integration,async()=>fixture(async({store,record,reader})=>{
 const model='synthetic-vendor/custom-model',request=await record({model,type:'usage-a'}),key=randomBytes(32),jobs=createPostgresJobs(store,{clock:()=>at}),recordings=new RecordingStore(store,{encryptionKey:key,clock:()=>at}),backend=createPostgresRecordingJobs(store,{recordingKey:key,jobs,clock:()=>at,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});await recordings.start(request,{turnState:'known'});await recordings.append(request,[{sourceKey:'synthetic-slash-input',kind:'user',provenance:'client',content:{text:'斜杠模型合成问题'}}]);await recordings.finish(request,{state:'complete'});await backend.schedule();let job;while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers[job.kind](job));
 const stats=await reader.read({groupBy:'model',model}),filters=Object.fromEntries(Object.entries(stats.rows[0].requestFilters).map(([key,value])=>[key,String(value)])),params=new URLSearchParams(filters),queries=createPostgresRequestQueries(store,{clock:()=>at,businessOutcomes:true});try{const list=await queries.read(requestsQuery(params,{usageTypes:true}));assert.deepEqual(list.rows.map(row=>row.id),[request]);params.set('q','模型合成');const searched=await backend.queries.search(recordingQuery(params,{clock:()=>at,search:true}));assert.ok(searched.items.some(row=>row.id===request));const exported=await backend.exports.enqueue({include_body:true,format:'jsonl',filters}),claimed=await jobs.claim(['recording.export']);await jobs.complete(claimed,await backend.handlers[claimed.kind](claimed));let text='';for await(const chunk of backend.download(exported.id))text+=chunk.toString();const rows=text.trim().split('\n').map(JSON.parse);assert.equal(rows.filter(row=>row.kind==='request').length,1);assert.equal(rows.find(row=>row.kind==='request').model,model);assert.ok(rows.some(row=>row.kind==='recording_event'));}finally{await queries.close();}
}));
