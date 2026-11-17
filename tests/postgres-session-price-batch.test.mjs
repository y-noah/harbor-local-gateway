import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {fields,validPrice,referencePartialAggregateSql,referenceRollupAggregateSql} from '../postgres-reference-query.mjs';
import {usageWindow} from '../store.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需独立合成PG测试库':false,timeout:120000};
const at=Date.parse('2026-11-17T12:00:00+08:00');
function options(database){
 if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:database||'postgres'},maxParallelWorkersPerGather:0};
 const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,maxParallelWorkersPerGather:0};
}
// Preserve the previous parameterized price read as an independent oracle.
const oldPriceSql=`WITH selected_requests AS MATERIALIZED (
 SELECT id,session_id,price_version FROM harbor.requests WHERE session_id=ANY($3::text[]) AND usage_at>=$1 AND usage_at<$2 AND department_id=$4
),source AS (
 SELECT r.session_id,CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
 CASE WHEN ${validPrice} THEN p.revision END revision,${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms END ${f}`).join(',')}
 FROM selected_requests r LEFT JOIN LATERAL (
 SELECT p.state,p.revision,${fields.map(f=>`p.${f}_atoms`).join(',')} FROM harbor.request_prices p WHERE p.request_id=r.id OFFSET 0
 ) p ON TRUE LEFT JOIN harbor.pricing_versions v ON v.id=r.price_version
),partial AS MATERIALIZED (SELECT session_id,revision,${referencePartialAggregateSql({standardOnly:true})} FROM source GROUP BY session_id,revision)
 SELECT session_id,${referenceRollupAggregateSql({standardOnly:true})} FROM partial GROUP BY session_id`;
async function fixture(run){
 const admin=new PostgresStore(options()),database='harbor_price_batch_'+randomUUID().replaceAll('-','');let created=false,store,current,baseline;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
  await store.run("INSERT INTO harbor.departments(id,name,created,updated) VALUES('other','合成其他部门',$1,$1)",at);
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('batch-v1',0,'{}'),('batch-v2',0,'{}')");
  for(const department of ['department-default','other']){
   await store.run('INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,$1,$2,$3)','member-'+department,at,department);
   await store.run("INSERT INTO harbor.accounts(id,name,home,department_id) VALUES($1,$1,'synthetic',$2)",'account-'+department,department);
  }
  const issuer=createDepartmentScopeIssuer(),scope=issuer.superAdmin({departmentId:'department-default',credentialId:'synthetic-admin',credentialVersion:0});
  current=createPostgresStateQueries(store,{clock:()=>at});
  baseline=createPostgresStateQueries({get:store.get.bind(store),transaction:store.transaction.bind(store),all:(sql,...args)=>store.all(sql.startsWith('WITH selected_requests AS')?oldPriceSql:sql,...args)},{clock:()=>at});
  const session=async(id,department='department-default',updated=at)=>store.run('INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,department_id) VALUES($1,$2,$3,$4,$5,$6)',id,'member-'+department,'account-'+department,updated,updated,department);
  const request=async(id,sessionId,{department='department-default',ended=at-100,revision='batch-v1',priceRevision=revision,state='priced',total='42',missing=false}={})=>{
   await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version,department_id) VALUES($1,$2,$3,$4,$5,$5,'ok','synthetic','synthetic',$6,$7)",id,'member-'+department,'account-'+department,sessionId,ended,revision,department);
   if(!missing)await store.run('INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,0,0,0,$4,$5)',id,state,priceRevision,total,at);
  };
  await run({store,current,baseline,scope,session,request,window:usageWindow(at)});
 }finally{await current?.close();await baseline?.close();await store?.close();try{if(created)await admin.query('DROP DATABASE '+database);}finally{await admin.close();}}
}

test('批量会话价格与原LATERAL逐字段一致，保留部门七天分页及未知金额',integration,()=>fixture(async({store,current,baseline,scope,session,request,window})=>{
 for(let i=0;i<23;i++)await session('own-'+String(i).padStart(2,'0'),'department-default',at-i);
 await session('other-session','other');await request('other-request','other-session',{department:'other',total:'999999999999999999999'});
 const variants=[{total:'900719925474099312345'},{missing:true},{priceRevision:'batch-v2'},
  {revision:'missing-book'},{state:'pending',total:null},{state:'unmetered',total:null},{state:'expired',total:null},
  {total:'0'},{total:'7',ended:window.start},{total:'999',ended:window.start-1},{total:'999',ended:window.end}];
 for(const [i,value] of variants.entries())await request('variant-'+i,'own-00',value);
 await request('last-page','own-22',{total:'123'});
 await request('unknown-only','own-01',{state:'unmetered',total:null});
 for(const query of [{sessionPage:1},{sessionPage:2},{sessionPage:999},{sessionPageSize:50},{sessionPageSize:100},{sessionQ:'does-not-exist'}]){
  const params={view:'sessions',sessionPageSize:20,...query};assert.deepEqual(await current.read(params,{scope}),await baseline.read(params,{scope}));
 }
 const first=await current.read({view:'sessions'},{scope});assert.equal(first.sessionsPagination.total,23);assert.equal(first.sessions.length,20);assert.ok(first.sessions.every(row=>row.id.startsWith('own-')));
 const row=first.sessions.find(row=>row.id==='own-00');assert.equal(row.request_count,9);assert.equal(row.standard.known,3);assert.equal(row.standard.unpriced,3);assert.equal(row.standard.pending,1);assert.equal(row.standard.expired,1);assert.equal(row.standard.unmetered,1);assert.equal(row.standard.total_exact,'900719925.474099312352');
 const unknown=first.sessions.find(row=>row.id==='own-01');assert.equal(unknown.standard.total_exact,null);assert.equal(unknown.request_count,1);
 const second=await current.read({view:'sessions',sessionPage:2},{scope});assert.equal(second.sessions.length,3);assert.equal(second.sessions.find(row=>row.id==='own-22').request_count,1);
 const original=store.all.bind(store);let prices=0;store.all=async(sql,...args)=>{if(sql.startsWith('WITH selected_requests AS'))prices++;return original(sql,...args);};
 try{assert.deepEqual((await current.read({view:'sessions',sessionQ:'does-not-exist'},{scope})).sessions,[]);assert.equal(prices,0);}finally{store.all=original;}
}));

test('代表性大价格表仅批量查找页内价格，避免逐请求循环或全价表扫描',integration,()=>fixture(async({store,current,scope,session,window})=>{
 await session('visible');await session('archive','department-default',window.start-1000);
 for(const [prefix,count,sessionId,time] of [['archive',60000,'archive',window.start-1000],['visible',3000,'visible',at-100]]){
  for(let first=1;first<=count;first+=1000)await store.run(`WITH inserted AS (
   INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version,department_id)
   SELECT $1||n,'member-department-default','account-department-default',$2,$3,$3,'ok','synthetic','synthetic','batch-v1','department-default' FROM generate_series($4::integer,$5::integer) n RETURNING id
  ) INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
   SELECT id,'priced','batch-v1',42,0,0,0,42,$3 FROM inserted`,prefix,sessionId,time,first,Math.min(count,first+999));
 }
 for(const table of ['requests','request_prices','sessions'])await store.query('ANALYZE harbor.'+table);
 const original=store.all.bind(store);let selected;store.all=async(sql,...args)=>{if(sql.startsWith('WITH selected_requests AS'))selected={sql,args};return original(sql,...args);};
 let result;try{result=await current.read({view:'sessions'},{scope});}finally{store.all=original;}
 assert.equal(result.sessions.length,1);assert.equal(result.sessions[0].request_count,3000);assert.equal(result.sessions[0].standard.known,3000);
 const plan=(await store.all('EXPLAIN (ANALYZE,FORMAT JSON) '+selected.sql,...selected.args))[0]['QUERY PLAN'][0],nodes=[];
 const visit=node=>{nodes.push(node);for(const child of node.Plans??[])visit(child);};visit(plan.Plan);
 const prices=nodes.filter(node=>node['Relation Name']==='request_prices');assert.ok(prices.length>0);
 for(const node of prices){assert.equal(node['Actual Loops'],1,'price retrieval must be batched');assert.notEqual(node['Node Type'],'Seq Scan','historical prices must not be fully scanned');}
 assert.equal(prices.reduce((sum,node)=>sum+node['Actual Rows']*node['Actual Loops'],0),3000);
}));
