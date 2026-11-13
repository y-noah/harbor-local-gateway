import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {fields,validPrice,joins,referencePartialAggregateSql,referenceRollupAggregateSql} from '../postgres-reference-query.mjs';
import {usageWindow} from '../store.mjs';
import {readFile} from 'node:fs/promises';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立合成PG测试库':false,timeout:120000};
const at=Date.parse('2026-11-13T12:00:00+08:00');
function connection(database){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,maxParallelWorkersPerGather:0};}
const originalSessionSql=`WITH source AS (
 SELECT r.session_id,r.status,r.started,r.ended,CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
 CASE WHEN ${validPrice} THEN p.revision END revision,${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms END ${f}`).join(',')}
 FROM harbor.requests r${joins} WHERE r.session_id=ANY($3::text[]) AND r.usage_at>=$1 AND r.usage_at<$2
),partial AS MATERIALIZED (SELECT session_id,revision,${referencePartialAggregateSql()} FROM source GROUP BY session_id,revision)
 SELECT session_id,${referenceRollupAggregateSql()} FROM partial GROUP BY session_id`;
async function fixture(run){
 const admin=new PostgresStore(connection()),database='harbor_session_read_'+randomUUID().replaceAll('-','');let created=false,store,reader;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(connection(database));await store.migrate();await store.initializePricing();
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('synthetic-v1',0,'{}'),('synthetic-v2',0,'{}')");
  const member=randomUUID(),account=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成会话员工',at);await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES($1,'合成账号','synthetic')",account);
  reader=createPostgresStateQueries(store,{clock:()=>at});await run({store,reader,member,account,window:usageWindow(at)});
 }finally{await reader?.close();await store?.close();try{if(created)await admin.query('DROP DATABASE '+database);}finally{await admin.close();}}
}

test('会话金额窄投影与旧完整DTO逐字段一致，保留缺价、错版、空值及七天分页语义',integration,()=>fixture(async({store,reader,member,account,window})=>{
 const sessions=Array.from({length:27},()=>randomUUID());for(const [i,id] of sessions.entries())await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol,enabled) VALUES($1,$2,$3,$4,$4,$5,$6)",id,member,account,at-i,i%2?'chat':'responses',i%3?1:0);
 const variants=[
  {price:'priced',total:'900719925474099312345',revision:'synthetic-v1'},
  {price:'priced',total:'42',revision:'synthetic-v2'},
  {missing:true},{price:'priced',total:'999',stale:true},
  {price:'priced',total:'999',revision:'synthetic-missing-book'},
  {price:'pending',total:null},{price:'unmetered',total:null},{price:'expired',total:null},
  {price:'priced',total:'0'},{price:'priced',total:'7',ended:window.start},
  {price:'priced',total:'500',ended:window.start-1},{price:'priced',total:'500',ended:window.end},
  {price:'priced',total:'3',status:'running',ended:null},
 ];
 for(const [i,item] of variants.entries()){
  const id=randomUUID(),ended=item.ended===undefined?at-100:item.ended,started=ended===null?at-100:ended-10,revision=item.revision??'synthetic-v1';
  await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) VALUES($1,$2,$3,$4,$5,$6,$7,'synthetic','gpt-6.1-sol',$8)",id,member,account,sessions[0],started,ended,item.status??'ok',revision);
  if(!item.missing)await store.run('INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,0,0,0,$4,$5)',id,item.price,item.stale?'synthetic-v2':revision,item.total??null,at);
 }
 const cases=[...[20,50,100].flatMap(sessionPageSize=>[1,2,999].map(sessionPage=>({sessionPage,sessionPageSize}))),...[{sessionQ:'合成会话员工'},{sessionQ:'不存在的合成员工'},{sessionStatus:'enabled'},{sessionStatus:'disabled'},{sessionType:'unclassified'},{sessionAccount:account}].map(filter=>({...filter,sessionPage:1,sessionPageSize:20}))];
 for(const query of cases){
  const legacy=await reader.read({...query,people:'none'}),current=await reader.read({...query,view:'sessions'});
  assert.deepEqual(current.sessions,legacy.sessions);assert.deepEqual(current.sessionsPagination,legacy.sessionsPagination);assert.deepEqual(current.usageWindow,legacy.usageWindow);
  assert.equal(current.peopleLoaded,false);for(const field of ['members','keys','requests','daily','audit'])assert.deepEqual(current[field],[]);
 }
 const visible=(await reader.read({view:'sessions',sessionPageSize:20})).sessions.find(r=>r.id===sessions[0]);assert.equal(visible.request_count,11);assert.equal(visible.standard.known,5);assert.equal(visible.standard.pending,1);assert.equal(visible.standard.unpriced,3);assert.equal(visible.standard.expired,1);assert.equal(visible.standard.unmetered,1);assert.deepEqual(visible.standard.revisions,['synthetic-v1','synthetic-v2']);
 const empty=createPostgresStateQueries(store,{clock:()=>at+15*86400000});try{assert.deepEqual((await empty.read({view:'sessions'})).sessions,[]);}finally{await empty.close();}
}));

test('会话页在大历史及稀疏多账号分布下保持完整精确用量与有界候选',integration,()=>fixture(async({store,reader,member,account,window})=>{
 const statements=(await readFile(new URL('../ci/storage-recovery/request-statistics-indexes.sql',import.meta.url),'utf8')).split('-- statement-boundary');for(const sql of statements)await store.query(sql);
 const sessions=Array.from({length:20},()=>randomUUID()),archive=randomUUID();for(const [i,id] of [...sessions,archive].entries())await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol) VALUES($1,$2,$3,$4,$5,'responses')",id,member,account,at,i===20?window.start-86400000:at-i);
 for(let start=1;start<=300000;start+=10000)await store.run("WITH inserted AS (INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) SELECT 'archive-'||n,$1,$2,$3,$4,$4,'ok','synthetic','gpt-6.1-sol','synthetic-v1' FROM generate_series($5::integer,$6::integer) n RETURNING id) INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) SELECT id,'priced','synthetic-v1',42,0,0,0,42,$4 FROM inserted",member,account,archive,window.start-86400000,start,start+9999);
 await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) SELECT 'current-'||n,$1,$2,($3::text[])[1+((n-1)%20)],$4,$4,'ok','synthetic','gpt-6.1-sol','synthetic-v1' FROM generate_series(1,7000) n",member,account,sessions,at-1000);
 await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) SELECT id,'priced','synthetic-v1',42,0,0,0,42,$1 FROM harbor.requests WHERE id LIKE 'current-%'",at);
 for(const table of ['requests','request_prices','sessions'])await store.query('VACUUM ANALYZE harbor.'+table);
 const original=store.all.bind(store);let selected;store.all=async(sql,...args)=>{if(sql.startsWith('WITH selected_requests AS'))selected={sql,args};return original(sql,...args);};let value,elapsedMs;try{const start=performance.now();value=await reader.read({view:'sessions',sessionPageSize:20});elapsedMs=performance.now()-start;}finally{store.all=original;}
 assert.equal(value.sessions.length,20);assert.equal(value.sessionsPagination.total,20);for(const row of value.sessions){assert.equal(row.request_count,350);assert.equal(row.standard.known,350);assert.equal(row.standard.total_exact,'0.0000000147');}
 const originalReader=createPostgresStateQueries({get:store.get.bind(store),transaction:store.transaction.bind(store),all:(sql,...args)=>store.all(sql.startsWith('WITH selected_requests AS')?originalSessionSql:sql,...args)},{clock:()=>at});
 const originalDenseStart=performance.now(),originalDense=await originalReader.read({view:'sessions',sessionPageSize:20}),originalDenseMs=performance.now()-originalDenseStart;assert.deepEqual(value,originalDense);
 const legacyDense=await reader.read({people:'none',sessionPageSize:20});assert.deepEqual(value.sessions,legacyDense.sessions);assert.deepEqual(value.sessionsPagination,legacyDense.sessionsPagination);
 const insertProbe=async()=>{const marker=Error('synthetic-write-probe-rollback'),walStart=(await store.get('SELECT pg_current_wal_insert_lsn() value')).value;let ms;await assert.rejects(store.transaction(async()=>{const start=performance.now();await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) SELECT 'write-probe-'||n,$1,$2,$3,$4,$4,'ok','synthetic','gpt-6.1-sol','synthetic-v1' FROM generate_series(1,5000) n",member,account,sessions[0],at);ms=performance.now()-start;throw marker;}),error=>error===marker);const walBytes=Number((await store.get('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn) value',walStart)).value);await store.query('VACUUM ANALYZE harbor.requests');return {ms,walBytes};};
 // Rollback undoes rows, not WAL/index maintenance. Discard one warm-up per
 // configuration and report raw samples; timings remain local observations.
 await insertProbe();const baselineWriteSamples=[];for(let n=0;n<3;n++)baselineWriteSamples.push(await insertProbe());
 const indexStatements=(await readFile(new URL('../ci/storage-recovery/session-usage-index.sql',import.meta.url),'utf8')).split('-- statement-boundary');assert.equal(indexStatements.length,3);const indexStart=performance.now();for(const sql of indexStatements)await store.query(sql);const indexCreateMs=performance.now()-indexStart,indexBytes=Number((await store.get("SELECT pg_relation_size('harbor.request_session_usage_cover') bytes")).bytes);
 await insertProbe();const coveredWriteSamples=[];for(let n=0;n<3;n++)coveredWriteSamples.push(await insertProbe());const coveredStart=performance.now(),covered=await reader.read({view:'sessions',sessionPageSize:20}),coveredDenseMs=performance.now()-coveredStart;assert.deepEqual(covered,value);
 const plan=(await store.all('EXPLAIN (ANALYZE,FORMAT JSON) '+selected.sql,...selected.args))[0]['QUERY PLAN'][0],nodes=[];function visit(n){nodes.push(n);for(const c of n.Plans??[])visit(c);}visit(plan.Plan);
 console.log(JSON.stringify({module:'synthetic-session-plan',nodes:nodes.map(n=>({type:n['Node Type'],relation:n['Relation Name'],index:n['Index Name'],rows:n['Actual Rows'],loops:n['Actual Loops'],removed:n['Rows Removed by Filter'],width:n['Plan Width'],heapFetches:n['Heap Fetches']}))}));
 const requestScan=nodes.filter(n=>n['Relation Name']==='requests');assert.ok(requestScan.length>0&&requestScan.every(n=>n['Node Type']==='Index Only Scan'&&n['Index Name']==='request_session_usage_cover'&&(n['Heap Fetches']??0)===0),'the session/time covering index should remove request heap fetches after vacuum');assert.equal(requestScan.reduce((sum,n)=>sum+n['Actual Rows']*n['Actual Loops'],0),7000,'unrelated historical requests must stay outside the metadata projection');
 // A different account exposes only one sparse session while the same seven-day
 // window contains many unrelated requests. It must keep both values and scope.
 const sparseAccount=randomUUID(),sparseSession=randomUUID();await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES($1,'合成稀疏账号','synthetic')",sparseAccount);await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol) VALUES($1,$2,$3,$4,$4,'responses')",sparseSession,member,sparseAccount,at);
 await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) VALUES('sparse-request',$1,$2,$3,$4,$4,'ok','synthetic','gpt-6.1-sol','synthetic-v1')",member,sparseAccount,sparseSession,at);
 await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES('sparse-request','priced','synthetic-v1',42,0,0,0,42,$1)",at);
 for(let start=1;start<=100000;start+=5000)await store.run("UPDATE harbor.requests SET ended=$1::bigint,started=$1::bigint-1 WHERE id IN (SELECT 'archive-'||n FROM generate_series($2::integer,$3::integer) n)",at-5000,start,start+4999);
 for(const table of ['requests','request_prices'])await store.query('VACUUM ANALYZE harbor.'+table);
 const start=performance.now(),sparse=await reader.read({view:'sessions',sessionAccount:sparseAccount,sessionPageSize:20}),sparseMs=performance.now()-start,legacySparse=await reader.read({people:'none',sessionAccount:sparseAccount,sessionPageSize:20});assert.deepEqual(sparse.sessions,legacySparse.sessions);assert.deepEqual(sparse.sessionsPagination,legacySparse.sessionsPagination);assert.equal(sparse.sessions.length,1);assert.equal(sparse.sessions[0].request_count,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,307001);const originalSparseStart=performance.now(),originalSparse=await originalReader.read({view:'sessions',sessionAccount:sparseAccount,sessionPageSize:20}),originalSparseMs=performance.now()-originalSparseStart;assert.deepEqual(sparse,originalSparse);await originalReader.close();
 console.log(JSON.stringify({module:'synthetic-session-price-lookup',historyPrices:300000,visibleRequests:7000,sessionRows:20,readerMs:elapsedMs,executionMs:plan['Execution Time'],sparseWindowRequests:107001,sparseVisibleRequests:1,sparseReaderMs:sparseMs,originalSparseMs,originalDenseMs,coveredDenseMs,indexCreateMs,indexBytes,writeProbeRows:5000,baselineWriteSamples,coveredWriteSamples,rollbackStillWritesWal:true,writeOrderBias:true}));
}));

test('会话覆盖索引显式安装可重入并拒绝同名错误结构',integration,()=>fixture(async({store})=>{
 const parts=(await readFile(new URL('../ci/storage-recovery/session-usage-index.sql',import.meta.url),'utf8')).split('-- statement-boundary');
 await store.query('CREATE INDEX request_session_usage_cover ON harbor.requests(usage_at)');await assert.rejects(store.query(parts[0]),/harbor_session_usage_index_conflict/);await store.query('DROP INDEX harbor.request_session_usage_cover');
 for(let attempt=0;attempt<2;attempt++)for(const sql of parts)await store.query(sql);
 const row=await store.get("SELECT indisvalid,indisready,indislive FROM pg_index WHERE indexrelid='harbor.request_session_usage_cover'::regclass");assert.deepEqual(row,{indisvalid:true,indisready:true,indislive:true});
}));
