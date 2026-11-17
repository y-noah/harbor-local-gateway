import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {usageWindow} from '../store.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL;
const options=name=>{const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,maxParallelWorkersPerGather:0,jit:false};};
const nodes=plan=>[plan,...(plan.Plans??[]).flatMap(nodes)];
test('部门会话覆盖索引在长寿命历史和跨部门同窗流量下保留结果并缩小候选读取',{skip:!url?'需独立合成PG':false,timeout:120000},async()=>{
 const root=new PostgresStore(options()),database='harbor_session_index_'+randomUUID().replaceAll('-','');let store,reader,created=false;
 try{
  await root.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
  const at=Date.parse('2026-11-17T12:00:00+08:00'),window=usageWindow(at),revision=(await store.get('SELECT id FROM harbor.pricing_versions LIMIT 1')).id;
  for(const suffix of ['a','b']){
   await store.run('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$1,$2,$2)','dept-'+suffix,at);
   await store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成员工',$2,$3)",'member-'+suffix,at,'dept-'+suffix);
   await store.run("INSERT INTO harbor.accounts(id,name,home,department_id) VALUES($1,'合成账号','synthetic',$2)",'account-'+suffix,'dept-'+suffix);
   await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,department_id) VALUES($1,$2,$3,$4,$4,$5)",'session-'+suffix,'member-'+suffix,'account-'+suffix,at,'dept-'+suffix);
  }
  // Same visible session has a long history; another department owns most
  // recent traffic. Neither population may enter this page's usage aggregate.
  for(const [prefix,suffix,count,time] of [['old','a',100000,window.start-86400000],['other','b',100000,at-1000],['visible','a',200,at-1000]]){
   for(let first=1;first<=count;first+=5000)await store.run(`WITH inserted AS (
    INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version,department_id)
    SELECT $1||n,$2,$3,$4,$5::bigint-1,$5,'ok','synthetic','gpt-6.1-sol',$6,$7 FROM generate_series($8::integer,$9::integer) n RETURNING id)
    INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
    SELECT id,'priced',$6,42,0,0,0,42,$5 FROM inserted`,prefix,'member-'+suffix,'account-'+suffix,'session-'+suffix,time,revision,'dept-'+suffix,first,Math.min(count,first+4999));
  }
  for(const table of ['requests','request_prices','sessions'])await store.query('VACUUM ANALYZE harbor.'+table);
  const scope=createDepartmentScopeIssuer().superAdmin({departmentId:'dept-a',credentialId:'synthetic-root',credentialVersion:0});reader=createPostgresStateQueries(store,{clock:()=>at});
  let selected;const all=store.all.bind(store);store.all=async(sql,...args)=>{if(sql.startsWith('WITH selected_requests AS'))selected={sql,args};return all(sql,...args);};
  const before=await reader.read({view:'sessions'},{scope});assert.equal(before.sessions.length,1);assert.equal(before.sessions[0].request_count,200);assert.ok(!JSON.stringify(before).includes('session-b'));assert.match(selected.sql,/requests\.department_id=\$4/);
  const explain=async()=>{const plan=(await all('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+selected.sql,...selected.args))[0]['QUERY PLAN'][0];return {executionMs:plan['Execution Time'],sharedBlocks:(plan.Plan['Shared Hit Blocks']??0)+(plan.Plan['Shared Read Blocks']??0),indexes:nodes(plan.Plan).map(n=>n['Index Name']).filter(Boolean),scans:nodes(plan.Plan).filter(n=>n['Relation Name']==='requests').map(n=>({type:n['Node Type'],index:n['Index Name'],rows:n['Actual Rows'],loops:n['Actual Loops'],removed:n['Rows Removed by Filter']??0,heapFetches:n['Heap Fetches']??null,buffers:(n['Shared Hit Blocks']??0)+(n['Shared Read Blocks']??0)}))};};
  const baseline=await explain();
  for(const sql of (await readFile(new URL('../ci/storage-recovery/session-usage-index.sql',import.meta.url),'utf8')).split('-- statement-boundary'))await store.query(sql);
  const existing=await reader.read({view:'sessions'},{scope});assert.deepEqual(existing,before);const existingCover=await explain();
  const writeProbe=async()=>{const marker=Error('synthetic rollback'),wal=(await store.get('SELECT pg_current_wal_insert_lsn() lsn')).lsn;let ms;await assert.rejects(store.transaction(async()=>{const start=performance.now();await store.run(`INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version,department_id) SELECT 'probe-'||n,'member-a','account-a','session-a',$1::bigint-1,$1,'ok','synthetic','gpt-6.1-sol',$2,'dept-a' FROM generate_series(1,500) n`,at,revision);ms=performance.now()-start;throw marker;}),error=>error===marker);return {ms,walBytes:Number((await store.get('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn) bytes',wal)).bytes)};};
  await writeProbe();const oldWrite=[];for(let i=0;i<3;i++)oldWrite.push(await writeProbe());
  const parts=(await readFile(new URL('../ci/storage-recovery/session-department-usage-index.sql',import.meta.url),'utf8')).split('-- statement-boundary');assert.equal(parts.length,3);
  await store.query('CREATE INDEX request_session_department_usage_cover ON harbor.requests(usage_at)');await assert.rejects(store.query(parts[0]),/harbor_session_usage_index_conflict/);await store.query('DROP INDEX harbor.request_session_department_usage_cover');
  for(let attempt=0;attempt<2;attempt++)for(const sql of parts)await store.query(sql);
  assert.ok((await store.get("SELECT to_regclass('harbor.request_session_usage_cover') present")).present,'existing index remains available');
  const indexBytes=Number((await store.get("SELECT pg_relation_size('harbor.request_session_department_usage_cover') bytes")).bytes);await writeProbe();const newWrite=[];for(let i=0;i<3;i++)newWrite.push(await writeProbe());
  const after=await reader.read({view:'sessions'},{scope});assert.deepEqual(after,before);const covered=await explain();
  await store.run("UPDATE harbor.requests SET ip='synthetic-updated' WHERE id LIKE 'visible%'");
  assert.deepEqual(await reader.read({view:'sessions'},{scope}),before);const recentlyUpdated=await explain();
  console.log(JSON.stringify({kind:'synthetic-department-session-index',oldSameSession:100000,otherDepartmentInWindow:100000,visible:200,baseline,existingCover,covered,recentlyUpdated,indexBytes,writeProbeRows:500,oldWrite,newWrite,rollbackStillWritesWal:true}));
  assert.ok(recentlyUpdated.indexes.includes('request_session_department_usage_cover'));assert.equal(recentlyUpdated.scans.reduce((sum,n)=>sum+(n.rows+n.removed)*n.loops,0),200);assert.ok(recentlyUpdated.sharedBlocks<baseline.sharedBlocks);
  assert.ok(covered.indexes.includes('request_session_department_usage_cover'));assert.equal(covered.scans.reduce((sum,n)=>sum+(n.rows+n.removed)*n.loops,0),200);assert.ok(covered.sharedBlocks<baseline.sharedBlocks,'cover should avoid reading unrelated session history or other departments');
 }finally{await reader?.close();await store?.close();if(created)await root.query('DROP DATABASE '+database);await root.close();}
});
