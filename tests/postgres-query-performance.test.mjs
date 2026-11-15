import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {usageWindow} from '../store.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';
import {readFile} from 'node:fs/promises';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,backfillUsageRollups,reconcileUsageRollups} from '../postgres-usage-rollups.mjs';
import {createPostgresTypeStatistics} from '../postgres-type-statistics.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
function options(database='postgres'){
  if(url){const address=new URL(url);address.pathname='/'+database;return {connectionString:address.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database}};
}

test('employee usage profile is opt-in and rejected for Key queries or invalid values',()=>{
 assert.equal(memberQuery(new URLSearchParams()).usageProfile,undefined);
 for(const profile of ['full','list'])assert.equal(memberQuery(new URLSearchParams({usage_profile:profile})).usageProfile,profile);
 for(const value of ['', 'summary','LIST'])assert.throws(()=>memberQuery(new URLSearchParams({usage_profile:value})),{status:400});
 assert.throws(()=>memberQuery(new URLSearchParams('usage_profile=list&usage_profile=full')),{status:400});
 assert.throws(()=>memberQuery(new URLSearchParams('usage_profile=list'),{keys:true}),{status:400});
});

test('shared-window employee and grouped statistics preserve empty periods, boundary requests, exact USD and weighted duration',{
  skip:!url&&!socket?'需隔离PG；CI test_postgres执行真实链路':false
},async()=>{
  const admin=new PostgresStore(options()),database='harbor_test_'+randomUUID().replaceAll('-','');let store,types,runtime;
  try{
    await admin.query('CREATE DATABASE '+database+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
    store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
    const at=Date.parse('2026-11-08T10:00:00+08:00'),window=usageWindow(at),ids=Array.from({length:3},()=>randomUUID()),revision=(await store.get('SELECT id FROM harbor.pricing_versions LIMIT 1')).id;
    for(const [index,id] of ids.entries())await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',id,'合成窗口员工'+index,at-index);
    for(const [owner,ended,duration,type] of [[0,window.start,100,'usage-a'],[0,at-12*3600000,100,'usage-a'],[0,at,100,'usage-a'],[1,at-13*3600000,500,'usage-b'],[0,window.start-1,900,'usage-a']]){
      const id=randomUUID();
      await store.run("INSERT INTO harbor.requests(id,member_id,started,ended,status,business_status,ip,model,price_version,usage_type_id,usage_type_version) VALUES($1,$2,$3,$4,'ok','ok','synthetic','synthetic',$5,$6,1)",id,ids[owner],ended-duration,ended,revision,type);
      await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,total_atoms,input_atoms,output_atoms,cached_atoms,adjustment_atoms,measured_at) VALUES($1,'priced',$2,9007199254740993,9007199254740993,0,0,0,$3)",id,revision,at);
    }
    const members=createPostgresMemberQueries(store,{clock:()=>at}),page=await members.list({page:1,pageSize:25,q:'',typeId:'',status:''}),a=page.rows.find(row=>row.id===ids[0]),b=page.rows.find(row=>row.id===ids[1]),empty=page.rows.find(row=>row.id===ids[2]);
    assert.equal(a.standard.requests,3);assert.equal(a.standard.total_exact,'27021.597764222979');
    assert.equal(a.standard_12h.requests,1);assert.equal(a.standard_12h.total_exact,'9007.199254740993');
    assert.equal(b.standard.requests,1);assert.equal(b.standard_12h.requests,0);assert.equal(b.standard_12h.total_exact,'0');assert.equal(b.standard_12h.state,'priced');
    assert.equal(empty.standard.requests,0);assert.deepEqual(empty.standard,empty.standard_12h);
    assert.deepEqual((await members.detail(ids[0])).standard_12h,a.standard_12h);
    const compareProfile=async()=>{
      const statements=[],original=store.all.bind(store);store.all=async(sql,...values)=>{statements.push(sql);return original(sql,...values);};
      let compact;try{compact=await createPostgresMemberQueries(store,{clock:()=>at}).list(memberQuery(new URLSearchParams('usage_profile=list')));}finally{store.all=original;}
      for(const row of compact.rows){const baseline=page.rows.find(item=>item.id===row.id);assert.deepEqual(row.standard,baseline.standard);assert.deepEqual(row.standard_12h,baseline.standard_12h);assert.equal(row.last_seen,baseline.last_seen);for(const field of ['ips','used_devices','unverified_requests'])assert.equal(Object.hasOwn(row,field),false);}
      const identity=statements.find(sql=>sql.startsWith('WITH identity AS'));assert.ok(identity);assert.equal(identity.includes('COUNT(DISTINCT r.ip)'),false);assert.equal(identity.includes('MAX(r.started) last_seen'),true);
      const full=await createPostgresMemberQueries(store,{clock:()=>at}).list(memberQuery(new URLSearchParams()));for(const field of ['ips','used_devices','unverified_requests'])assert.equal(Object.hasOwn(full.rows[0],field),true);
    };
    await compareProfile();
    const indexStatements=(await readFile(new URL('../ci/storage-recovery/member-last-seen-index.sql',import.meta.url),'utf8')).split('-- statement-boundary').map(value=>value.trim()).filter(Boolean);
    assert.equal(indexStatements.length,3);
    await store.query('CREATE INDEX request_member_last_seen_cover ON harbor.requests(started)');
    await assert.rejects(store.query(indexStatements[0]),/harbor_member_last_seen_index_conflict/);await store.query('DROP INDEX harbor.request_member_last_seen_cover');
    for(let iteration=0;iteration<2;iteration++)for(const sql of indexStatements)await store.query(sql);
    runtime=await createUsageRollupRuntime(store);await migrateUsageRollups(store,{apply:true});await backfillUsageRollups(store,{batchSize:100});assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,true);
    await compareProfile();

    types=createPostgresTypeStatistics(store,{clock:()=>at});
    const first=await types.read({pageSize:1}),last=await types.read({pageSize:1,page:999});
    assert.equal(first.summary.requests,4);assert.equal(first.summary.average_ms,200);assert.equal(first.summary.standard.total_exact,'36028.797018963972');
    assert.equal(first.rows[0].average_ms,100);assert.equal(last.rows[0].average_ms,500);
    assert.deepEqual(first.summary,last.summary);assert.equal(last.pagination.page,2);assert.equal(last.pagination.total,2);
  }finally{await types?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);if(runtime)await admin.query('DROP ROLE '+runtime);await admin.close();}
});
