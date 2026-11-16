import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';
import {passwordHash} from '../auth.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需隔离真实PG':false,timeout:60000};
function connection(database='postgres'){
 if(url){const value=new URL(url);value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true};}
 return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database}};
}
async function fixture(t){
 const admin=new PostgresStore(connection()),database='harbor_jit_'+randomUUID().replaceAll('-',''),stores=[],apps=[];
 const directory=await mkdtemp(join(tmpdir(),'harbor-query-jit-'));
 t.after(async()=>{try{for(const app of apps)await app.close();for(const store of stores)await store.close();await admin.query('DROP DATABASE IF EXISTS '+database);}finally{await admin.close();await rm(directory,{recursive:true,force:true});}});
 await admin.query('CREATE DATABASE '+database);
 const store=options=>{const value=new PostgresStore({...connection(database),...options});stores.push(value);return value;};
 return {database,directory,store,apps};
}

test('JIT settings reject non-booleans and preserve all other connection startup options',async()=>{
 for(const jit of [null,0,1,'off','on',{},[]]){
  assert.throws(()=>new PostgresStore({connection:{host:'/tmp/synthetic-pg'},jit}),{code:'database_configuration'});
  await assert.rejects(createPostgresGateway({env:{},resources:{queryJit:jit}}),{code:'gateway_configuration'});
 }
 for(const jit of [undefined,false,true]){
  const store=new PostgresStore({connection:{host:'/tmp/synthetic-pg',options:'-c application_name=synthetic_jit -c jit=on'},maxParallelWorkersPerGather:0,...(jit===undefined?{}:{jit})});
  try{const options=store.pool.options.options;assert.ok(options.startsWith('-c application_name=synthetic_jit -c jit=on '));assert.match(options,/-c max_parallel_workers_per_gather=0/);if(jit!==undefined)assert.ok(options.endsWith('-c jit='+(jit?'on':'off')));else assert.ok(options.endsWith('-c max_parallel_workers_per_gather=0'));}finally{await store.close();}
 }
});

test('JIT is set for every physical pool connection including reconnect, without changing the server default',integration,async t=>{
 const f=await fixture(t),baseline=f.store({poolSize:1}),before=(await baseline.get("SELECT current_setting('jit') value")).value;
 for(const jit of [false,true]){
  const store=f.store({poolSize:1,jit,maxParallelWorkersPerGather:0}),first=await store.get("SELECT pg_backend_pid() pid,current_setting('jit') jit,current_setting('max_parallel_workers_per_gather') parallel");
  assert.equal(first.jit,jit?'on':'off');assert.equal(first.parallel,'0');
  const client=await store.pool.connect();client.release(true);
  const second=await store.get("SELECT pg_backend_pid() pid,current_setting('jit') jit");assert.notEqual(second.pid,first.pid);assert.equal(second.jit,first.jit);
  assert.equal((await baseline.get("SELECT current_setting('jit') value")).value,before);
 }
});

test('gateway defaults JIT off only for admin queries and permits a server-side opt-in',integration,async t=>{
 const f=await fixture(t),setup=f.store();await setup.migrate();await setup.initializePricing();
 const baseline=(await setup.get("SELECT current_setting('jit') value")).value,config={port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-jit-admin')},at=Date.parse('2026-11-08T10:00:00+08:00'),member=randomUUID(),revision=(await setup.get('SELECT id FROM harbor.pricing_versions ORDER BY id LIMIT 1')).id,results=[];
 await setup.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成JIT员工',$2)",member,at);
 for(let i=0;i<3;i++){
  const id=randomUUID();await setup.run("INSERT INTO harbor.requests(id,member_id,started,ended,status,ip,model,price_version) VALUES($1,$2,$3,$4,'ok','synthetic','synthetic',$5)",id,member,at-100-i,at,revision);
  if(i<2)await setup.run("INSERT INTO harbor.request_prices(request_id,state,revision,total_atoms,input_atoms,output_atoms,cached_atoms,adjustment_atoms,measured_at) VALUES($1,'priced',$2,9007199254740993,9007199254740993,0,0,0,$3)",id,revision,at);
 }
 for(const queryJit of [undefined,true]){
  const app=await createPostgresGateway({env:{},clock:()=>at,database:connection(f.database),dataDir:join(f.directory,'data'),runtimeDir:join(f.directory,'runtime'),config,resources:queryJit===undefined?{}:{queryJit},recording:{enabled:false}});f.apps.push(app);
  assert.equal((await app.queryStore.get("SELECT current_setting('jit') value")).value,queryJit===true?'on':'off');
  assert.equal((await app.store.get("SELECT current_setting('jit') value")).value,baseline);
  assert.equal((await app.recordingStore.get("SELECT current_setting('jit') value")).value,baseline);
  const result=await createPostgresMemberQueries(app.queryStore,{clock:()=>at}).list(memberQuery(new URLSearchParams('usage_profile=list'))),row=result.rows.find(value=>value.id===member);
  assert.equal(row.standard.requests,3);assert.equal(row.standard.total_exact,'18014.398509481986');results.push({standard:row.standard,recent:row.standard_12h,lastSeen:row.last_seen});
  await app.close();f.apps.pop();
 }
 assert.deepEqual(results[0],results[1]);
});
