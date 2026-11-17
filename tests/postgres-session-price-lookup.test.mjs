import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {usageWindow} from '../store.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL;
function options(database){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:1};}

test('会话页仅按所选请求主键读取价格，不扫描全部历史价格且金额与完整视图一致',{
 skip:!url?'需独立合成PG测试库':false,timeout:60000
},async()=>{
 const admin=new PostgresStore(options()),database='harbor_session_prices_'+randomUUID().replaceAll('-','');let store,reader,created=false;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();
  const at=Date.parse('2026-11-13T12:00:00+08:00'),window=usageWindow(at);
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('v1',0,'{}');INSERT INTO harbor.members(id,name,created) VALUES('m','合成员工',0);INSERT INTO harbor.accounts(id,name,home) VALUES('a','合成账号','synthetic')");
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) SELECT 's-'||n,'m','a',0,$1::bigint-n FROM generate_series(0,20) n",at);
  await store.run(`WITH inserted AS (
    INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,price_version)
    SELECT 'r-'||n,'m','a',CASE WHEN n<=100000 THEN 's-20' ELSE 's-'||((n-1)%20) END,
      CASE WHEN n<=100000 THEN $1::bigint ELSE $2::bigint END,
      CASE WHEN n<=100000 THEN $1::bigint ELSE $2::bigint END,'ok','synthetic','v1'
    FROM generate_series(1,107000) n RETURNING id
  ) INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
    SELECT id,'priced','v1',42,0,0,0,42,$2 FROM inserted`,window.start-1,at-100);
  for(const table of ['requests','request_prices','sessions'])await store.query('VACUUM ANALYZE harbor.'+table);
  const all=store.all.bind(store);let selected;
  store.all=async(sql,...args)=>{if(sql.startsWith('WITH selected_requests AS'))selected={sql,args};return all(sql,...args);};
  reader=createPostgresStateQueries(store,{clock:()=>at});
  const page=await reader.read({view:'sessions',sessionPageSize:20}),full=await reader.read({people:'none',sessionPageSize:20});
  assert.deepEqual(page.sessions,full.sessions);assert.deepEqual(page.sessionsPagination,full.sessionsPagination);
  assert.equal(page.sessions.length,20);for(const row of page.sessions){assert.equal(row.request_count,350);assert.equal(row.standard.total_exact,'0.0000000147');}
  const plan=(await all('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+selected.sql,...selected.args))[0]['QUERY PLAN'][0],scans=[];
  function visit(node){if(node['Relation Name']==='request_prices')scans.push(node);for(const child of node.Plans??[])visit(child);}visit(plan.Plan);
  assert.equal(scans.length,1);const scan=scans[0];assert.equal(scan['Index Name'],'request_prices_pkey');
  assert.match(scan['Index Cond'],/request_id = r.id/);assert.equal(scan['Actual Loops'],7000);
  assert.equal(scan['Actual Rows'],1);assert.equal(scan['Rows Removed by Filter']??0,0);
  // Subsequent reads retain exact missing-price semantics instead of caching
  // a previously complete chart or converting an unknown price to zero.
  await store.query("DELETE FROM harbor.request_prices WHERE request_id='r-100001'");
  const changed=await reader.read({view:'sessions',sessionPageSize:20});
  assert.deepEqual(changed.sessions,(await reader.read({people:'none',sessionPageSize:20})).sessions);
  assert.equal(changed.sessions.reduce((sum,row)=>sum+row.standard.unpriced,0),1);
 }finally{await reader?.close();await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();}
});
