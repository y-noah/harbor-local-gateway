import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {loadPostgresRecordingKey} from '../postgres-recording-storage.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
async function condition(predicate){const end=Date.now()+3000;while(!predicate()){assert.ok(Date.now()<end,'storage refresh must progress independently of the blocked maintenance schedule');await delay(10);}}

test('worker storage freshness and low-space rejection continue while maintenance scheduling is blocked',{
  skip:!binDirectory?'需独占PG18，不连接共享环境':false,timeout:30000
},async t=>{
  const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),store=new PostgresStore({connection:lab.connection});
  let worker,release;const gate=new Promise(resolve=>{release=resolve;});let entered=false,free=90,at=Date.now(),probes=0;
  t.after(async()=>{release();try{await worker?.close();}finally{try{await store.close();}finally{await lab.close();}}});
  await store.migrate();await store.initializePricing();
  const dataDir=join(lab.directory,'worker-data');await mkdir(dataDir,{mode:0o700});
  const key=await loadPostgresRecordingKey(store,{dataDir,createIfEmpty:true});key.fill(0);await mkdir(join(dataDir,'recording-spool'),{mode:0o700});
  const probe=async()=>{probes++;return [{total:100,free}];};
  worker=await createPostgresWorker({database:{connection:lab.connection},env:{},dataDir,clock:()=>at,
    recording:{enabled:true,storage:{probe,cacheMs:0,minFreeBytes:10,minFreeRatio:.1}}});
  // Hold the real schedule at an existing maintenance boundary; leave the
  // storage monitor, runtime pump and all other database work unchanged.
  worker.planning.schedule=async()=>{entered=true;await gate;};
  t.mock.timers.enable({apis:['setInterval']});worker.start();await condition(()=>entered);
  worker.start();const before=probes;
  at+=6000;assert.equal(worker.recordingStorage.captureAllowed(),false,'expired samples must never remain usable');
  t.mock.timers.tick(1000);await condition(()=>worker.recordingStorage.captureAllowed());
  assert.equal(probes,before+1,'repeated start must not duplicate independent sampling');
  assert.equal(worker.status().polling,true,'the maintenance schedule is still blocked');
  free=0;at++;t.mock.timers.tick(1000);await condition(()=>worker.recordingStorage.status().status==='low');
  assert.equal(worker.recordingStorage.captureAllowed(),false);
  assert.ok(worker.status().recording.pausedKinds.includes('recording.index'));
  release();await worker.close();const closedProbes=probes;t.mock.timers.tick(5000);await delay(0);
  assert.equal(probes,closedProbes,'closing the worker must stop independent sampling');
});
