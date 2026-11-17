import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {hasWorkerProgress,workerStartupResult} from '../postgres-worker-startup.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!url?'需隔离PostgreSQL':false,timeout:120000};
const sql=await readFile(new URL('../ci/storage-recovery/worker-progress-indexes.sql',import.meta.url),'utf8');
const statements=sql.split('-- statement-boundary');
function connection(name){const target=new URL(url);if(name)target.pathname='/'+name;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}
async function fixture(run){
 const admin=new PostgresStore(connection()),name='harbor_progress_indexes_'+randomUUID().replaceAll('-','');let store,created=false;
 try{await admin.query('CREATE DATABASE '+name);created=true;store=new PostgresStore(connection(name));await store.migrate();await run(store);}
 finally{await store?.close();try{if(created)await admin.query('DROP DATABASE '+name);}finally{await admin.close();}}
}
async function install(store){for(const statement of statements)await store.query(statement);}
const since=1800000000000;
async function insert(store,{kind='recording.index',status='completed',created=since,claimed=since,updated=since,result=null}={}){
 await store.query('INSERT INTO harbor.background_jobs(id,kind,payload,available_at,created,claimed_at,updated,status,result) VALUES($1,$2,\'{}\',$3,$3,$4,$5,$6,$7::jsonb)',[randomUUID(),kind,created,claimed,updated,status,result===null?null:JSON.stringify(result)]);
}
const nodes=plan=>[plan,...(plan.Plans||[]).flatMap(nodes)];
test('进度CASE保留全部分支语义，启动证明短路历史查询且generic语义一致',integration,()=>fixture(async store=>{
 await store.query("INSERT INTO harbor.background_jobs(id,kind,payload,available_at,created,claimed_at,updated,status,result) SELECT 'old-'||n,(ARRAY['worker.startup','recording.context.index','recording.index','routing.expiry','recording.purge','recording.export'])[1+n%6],'{}',1,1,1,1,CASE WHEN n%7=0 THEN 'running' ELSE 'completed' END,'{}' FROM generate_series(1,250000) n");
 await store.query('ANALYZE harbor.background_jobs');
 const variants=[];
 for(const kind of ['recording.purge','routing.expiry','recording.index','recording.export'])for(const status of ['running','completed'])variants.push({kind,status,expected:true});
 variants.push({kind:'worker.startup',result:workerStartupResult,expected:true});
 for(const patch of [{created:since-1},{claimed:since-1},{updated:since-1},{status:'running'},{result:{contract:'wrong'}}])variants.push({kind:'worker.startup',result:workerStartupResult,...patch,expected:false});
 variants.push({kind:'recording.context.index',expected:false},{kind:'recording.index',status:'running',claimed:since-1,expected:false},{kind:'recording.index',updated:since-1,expected:false});
 let captured;
 const probe=()=>hasWorkerProgress({get:async(query,...args)=>{captured={query,args};return store.get(query,...args);}},since);
 const evaluate=async()=>{const outcomes=[];for(const variant of variants){await store.query("DELETE FROM harbor.background_jobs WHERE created>1");await insert(store,variant);outcomes.push(await probe());}return outcomes;};
 const before=await evaluate();assert.deepEqual(before,variants.map(v=>v.expected));
 const explain=async()=>{const row=await store.get('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+captured.query,...captured.args);return row['QUERY PLAN'][0];};
 const baseline=await explain();await install(store);await install(store);await store.query('ANALYZE harbor.background_jobs');
 assert.deepEqual(await evaluate(),before);
 const plans=[];
 for(const variant of [{kind:'recording.index',status:'running'},{kind:'routing.expiry'},{kind:'worker.startup',result:workerStartupResult},{kind:'worker.startup',result:{contract:'wrong'}}]){
  await store.query('DELETE FROM harbor.background_jobs WHERE created>1');await insert(store,variant);await probe();const plan=await explain();plans.push(plan);
  const scans=nodes(plan.Plan);assert.ok(!scans.some(n=>n['Node Type']==='Seq Scan'&&n['Relation Name']==='background_jobs'),JSON.stringify(scans));
  assert.ok(scans.some(n=>n['Index Name']==='background_job_progress_claim'),JSON.stringify(scans));assert.ok(scans.some(n=>n['Index Name']==='background_job_progress_update'));
  if(variant.kind==='worker.startup'&&variant.result===workerStartupResult){const branches=plan.Plan.Plans;assert.equal(branches.length,3);assert.equal(branches[0]['Actual Loops'],1);assert.equal(branches[1]['Actual Loops'],0);assert.equal(branches[2]['Actual Loops'],0);}
 }
 // Generic estimates may choose a sequential scan for an unknown cutoff.
 // Verify its actual plan and all results without forcing planner settings.
 // The live store uses unnamed sql,args; its custom plans were checked above.
 await store.query('SET plan_cache_mode=force_generic_plan');await store.query('PREPARE harbor_progress(bigint,text,jsonb) AS '+captured.query);
 const generic=(await store.get(`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE harbor_progress(${since},'worker.startup','{"contract":"harbor-worker-startup-v1"}'::jsonb)`))['QUERY PLAN'][0];
 const genericNodes=nodes(generic.Plan),genericOutcomes=[];
 for(const variant of variants){await store.query('DELETE FROM harbor.background_jobs WHERE created>1');await insert(store,variant);genericOutcomes.push((await store.get(`EXECUTE harbor_progress(${since},'worker.startup','{"contract":"harbor-worker-startup-v1"}'::jsonb)`)).progress);}
 assert.deepEqual(genericOutcomes,before);
 await store.query('DELETE FROM harbor.background_jobs WHERE created>1');await insert(store,{kind:'worker.startup',result:workerStartupResult});const genericStartup=(await store.get(`EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE harbor_progress(${since},'worker.startup','{"contract":"harbor-worker-startup-v1"}'::jsonb)`))['QUERY PLAN'][0];assert.equal(genericStartup.Plan.Plans.length,3);assert.equal(genericStartup.Plan.Plans[0]['Actual Loops'],1);assert.equal(genericStartup.Plan.Plans[1]['Actual Loops'],0);assert.equal(genericStartup.Plan.Plans[2]['Actual Loops'],0);
 console.log(JSON.stringify({module:'worker-progress-indexes',syntheticRows:250000,branchCases:variants.length,baselineMs:baseline['Execution Time'],indexedMs:plans.map(p=>p['Execution Time']),genericMs:generic['Execution Time'],genericNodes:genericNodes.map(n=>({type:n['Node Type'],index:n['Index Name']}))}));
}));
test('同名错误索引拒绝且不删改，完成态复核拒绝缺失索引',integration,()=>fixture(async store=>{
 await store.query('CREATE INDEX background_job_progress_claim ON harbor.background_jobs(updated)');
 await assert.rejects(install(store),/harbor_worker_progress_index_conflict/);
 const index=await store.get("SELECT pg_get_indexdef('harbor.background_job_progress_claim'::regclass) definition");assert.match(index.definition,/\(updated\)/);
 await store.query('DROP INDEX harbor.background_job_progress_claim');await install(store);
 await store.query('DROP INDEX harbor.background_job_progress_update');await assert.rejects(store.query(statements.at(-1)),/harbor_worker_progress_index_unavailable/);
}));
