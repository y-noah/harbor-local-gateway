import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresTypeStatistics} from '../postgres-type-statistics.mjs';
import {usageWindow} from '../store.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL;
function options(database){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:1};}
test('类型统计完整结果保持精确，价格访问不随窗口外历史增加而全表扫描',{skip:!url?'需独立合成PG测试库':false,timeout:60000},async()=>{
 const admin=new PostgresStore(options()),name='harbor_type_lookup_'+randomUUID().replaceAll('-','');let store,current,previous,created=false;
 try{
  await admin.query('CREATE DATABASE '+name);created=true;store=new PostgresStore(options(name));await store.migrate();
  const at=Date.parse('2026-11-17T12:30:00+08:00'),window=usageWindow(at);
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('v1',0,'{}');INSERT INTO harbor.members(id,name,created) SELECT 'm'||n,'合成'||n,0 FROM generate_series(0,19)n");
  for(let start=1;start<=107000;start+=1000)await store.run(`WITH inserted AS (
   INSERT INTO harbor.requests(id,member_id,model,started,ended,status,business_status,ip,price_version)
   SELECT 'r'||n,'m'||(n%20),'model'||(n%3),CASE WHEN n<=100000 THEN $1::bigint ELSE $2::bigint END,CASE WHEN n<=100000 THEN $1::bigint ELSE $2::bigint END,'ok','ok','synthetic','v1'
   FROM generate_series($3::integer,$4::integer)n RETURNING id)
   INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
   SELECT id,'priced','v1',42,0,0,0,42,$2 FROM inserted`,window.start-1,at-100,start,Math.min(start+999,107000));
  for(const table of ['requests','request_prices','members'])await store.query('VACUUM ANALYZE harbor.'+table);
  const all=store.all.bind(store),captured=[];
  store.all=async(sql,...args)=>{if(sql.startsWith('WITH data AS'))captured.push({sql,args});return all(sql,...args);};
  current=createPostgresTypeStatistics(store,{clock:()=>at});
  const legacy={get:store.get.bind(store),transaction:store.transaction.bind(store),all:(sql,...args)=>all(sql.replace(/LEFT JOIN LATERAL \(SELECT state,revision,[^)]* FROM harbor\.request_prices WHERE request_id=r\.id OFFSET 0\) p ON TRUE/,'LEFT JOIN harbor.request_prices p ON p.request_id=r.id'),...args)};
  previous=createPostgresTypeStatistics(legacy,{clock:()=>at});
  for(const groupBy of ['type','model','member']){
   const result=await current.read({groupBy}),baseline=await previous.read({groupBy});
   for(const field of ['summary','rows','pagination','filters'])assert.deepEqual(result[field],baseline[field]);
   assert.equal(result.summary.requests,7000);assert.equal(result.summary.standard.total_exact,'0.000000294');
  }
  const selected=captured[0],plan=(await all('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+selected.sql,...selected.args))[0]['QUERY PLAN'][0],scans=[];
  function visit(node){if(node['Relation Name']==='request_prices')scans.push(node);for(const child of node.Plans??[])visit(child);}visit(plan.Plan);
  assert.equal(scans.length,1);assert.equal(scans[0]['Index Name'],'request_prices_pkey');assert.match(scans[0]['Index Cond'],/request_id = r.id/);assert.equal(scans[0]['Actual Rows'],1);assert.equal(scans[0]['Actual Loops'],7000);
  await store.query("DELETE FROM harbor.request_prices WHERE request_id='r100001'");
  const changed=await current.read({groupBy:'model'}),baseline=await previous.read({groupBy:'model'});assert.deepEqual(changed.summary,baseline.summary);assert.deepEqual(changed.rows,baseline.rows);assert.equal(changed.summary.standard.unpriced,1);
 }finally{await current?.close();await previous?.close();await store?.close();if(created)await admin.query('DROP DATABASE '+name);await admin.close();}
});
