import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {createWorkerStartupProbe,hasWorkerProgress,workerStartupKind,workerStartupResult} from '../postgres-worker-startup.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresWorker} from '../postgres-worker.mjs';

test('启动检查只入队一次，并发调度共享一次真实提交',async()=>{
 const calls=[],probe=createWorkerStartupProbe({enqueue:async(...args)=>{calls.push(args);return {created:true};}});
 await Promise.all([probe.schedule(),probe.schedule(),probe.schedule()]);await probe.schedule();assert.equal(calls.length,1);assert.equal(calls[0][0],workerStartupKind);
 assert.match(calls[0][1].id,/^worker\.startup:[a-f0-9-]{36}$/);assert.deepEqual(await probe.handlers[workerStartupKind](),workerStartupResult);
});

test('启动检查入队失败后保留原ID重试，丢失提交ACK不产生第二条检查',async()=>{
 const calls=[],probe=createWorkerStartupProbe({enqueue:async(...args)=>{calls.push(args);if(calls.length===1)throw Error('synthetic lost acknowledgement');return {created:false};}});
 await assert.rejects(probe.schedule(),/synthetic lost/);await probe.schedule();await probe.schedule();assert.equal(calls.length,2);assert.deepEqual(calls[0],calls[1]);
 assert.throws(()=>createWorkerStartupProbe({}),TypeError);
});

test('进展证明拒绝非法时间戳及非布尔数据库结果',async()=>{
 for(const since of [-1,NaN,Infinity,1.5,'1',null])await assert.rejects(hasWorkerProgress({get:()=>assert.fail('invalid boundary queried DB')},since),TypeError);
 for(const progress of [false,null,1,'true'])assert.equal(await hasWorkerProgress({get:async()=>({progress})},0),false);
});

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需隔离PostgreSQL':false,timeout:30000};
const database=name=>url?{connectionString:(()=>{const v=new URL(url);if(name)v.pathname='/'+name;return v.href;})(),allowPlaintextLocalNetwork:true}:{connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database:name||'postgres'}};
const until=async condition=>{const deadline=performance.now()+8000;while(!await condition()){assert.ok(performance.now()<deadline,'Worker startup condition timed out');await new Promise(resolve=>setTimeout(resolve,10));}};

test('手动单轮不新增自检，转为持续运行和同一分钟重启各只产生一次真实启动证明',integration,async t=>{
 const root=new PostgresStore(database()),name='harbor_worker_startup_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name)),workers=[];
 t.after(async()=>{for(const worker of workers)await worker.close();await store.close();await root.query('DROP DATABASE '+name);await root.close();});
 await store.migrate();await store.initializePricing();let at=Date.UTC(2026,10,10,0,0,10);const clock=()=>at;
 const start=async(running=true)=>{const worker=await createPostgresWorker({database:database(name),env:{},clock,recording:{enabled:false},runtime:{pollMs:100}});workers.push(worker);if(running)worker.start();return worker;};
 const old=await start(false);await old.runOnce();await until(()=>old.status().active===0);
 assert.equal(Number((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind=$1",workerStartupKind)).n),0);old.start();old.start();
 await until(async()=>Number((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind=$1 AND status='completed'",workerStartupKind)).n)===1);
 await until(async()=>Number((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind IN ('recording.purge','routing.expiry','recording.index','recording.export') AND status IN ('pending','running')")).n)===0);await old.close();
 const originalMinute=Math.floor(at/60000),since=at+1;at+=1000;assert.equal(Math.floor(at/60000),originalMinute);assert.equal(await hasWorkerProgress(store,since),false);
 const next=await start();await until(()=>hasWorkerProgress(store,since));await next.close();
 assert.equal(Number((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind=$1",workerStartupKind)).n),2);
 assert.equal(Number((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind IN ('recording.purge','routing.expiry','recording.index','recording.export') AND updated>=$1",since)).n),0,'proof must not rely on the next periodic job');
 const row=await store.get('SELECT id,created,claimed_at,updated FROM harbor.background_jobs WHERE kind=$1 AND created>=$2',workerStartupKind,since);assert.ok(row);assert.equal(await hasWorkerProgress(store,since),true);
 for(const patch of [{created:since-1},{claimed_at:since-1},{updated:since-1},{status:'cancelled'},{result:{contract:'unverified'}}]){
  const state={...row,status:'completed',result:workerStartupResult,...patch};await store.run('UPDATE harbor.background_jobs SET created=$2,claimed_at=$3,updated=$4,status=$5,result=$6::jsonb WHERE id=$1',row.id,state.created,state.claimed_at,state.updated,state.status,JSON.stringify(state.result));
  assert.equal(await hasWorkerProgress(store,since),false,JSON.stringify(patch));
 }
});
