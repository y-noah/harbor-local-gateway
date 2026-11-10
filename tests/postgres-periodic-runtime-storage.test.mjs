import test from 'node:test';
import assert from 'node:assert/strict';
import {createRecordingStorageMonitor} from '../postgres-recording-storage.mjs';
import {periodicRuntimeStorageStatus,assertPeriodicRuntimeStorage} from '../postgres-periodic-runtime-storage.mjs';

test('restored runtime distinguishes real monitor low space and stale cache without changing admission',async()=>{
  let at=1000,free=90;
  const probe=async()=>[{total:100,free}];probe.status=()=>({verified:true});
  const monitor=createRecordingStorageMonitor({}, {probe,clock:()=>at,cacheMs:0,minFreeBytes:10,minFreeRatio:.1});
  try{
    await monitor.refresh();
    assert.doesNotThrow(()=>assertPeriodicRuntimeStorage('gateway',periodicRuntimeStorageStatus(monitor)));
    free=5;at++;
    await monitor.refresh();
    assert.equal(monitor.captureAllowed(),false);
    assert.throws(()=>assertPeriodicRuntimeStorage('worker',periodicRuntimeStorageStatus(monitor)),{code:'periodic_application_runtime_storage_worker_low'});
    free=90;at++;
    await monitor.refresh();at+=5001;
    assert.throws(()=>assertPeriodicRuntimeStorage('gateway',periodicRuntimeStorageStatus(monitor)),{code:'periodic_application_runtime_storage_gateway_stale'});
  }finally{await monitor.close();}
});

test('unverified report categories survive IPC while arbitrary diagnostic data never crosses it',()=>{
  const secret='synthetic-secret-path-and-connection';
  const monitor={status:()=>({captureAllowed:false,binding:{verified:false,reason:'recording_storage_report_signature',path:secret},diagnostics:{reason:'probe_failed',message:secret}})};
  const status=periodicRuntimeStorageStatus(monitor);
  assert.deepEqual(status,{storage:false,captureAllowed:false,storageReason:'recording_storage_report_signature'});
  assert.throws(()=>assertPeriodicRuntimeStorage('gateway',JSON.parse(JSON.stringify(status))),{code:'periodic_application_runtime_storage_gateway_recording_storage_report_signature'});
  monitor.status=()=>({captureAllowed:false,binding:{verified:false,reason:secret},diagnostics:{reason:secret}});
  assert.deepEqual(periodicRuntimeStorageStatus(monitor),{storage:false,captureAllowed:false,storageReason:'binding_unverified'});
  assert.throws(()=>assertPeriodicRuntimeStorage('worker',{storage:false,storageReason:secret}),{code:'periodic_application_runtime_storage_worker_binding_unverified'});
  for(const malformed of [null,{}, {storage:1,captureAllowed:1}])assert.throws(()=>assertPeriodicRuntimeStorage('worker',malformed),{code:'periodic_application_runtime_storage_worker_binding_unverified'});
});

test('real report freshness loss remains rejected with a distinct fixed category',async()=>{
  let fresh=true;
  const probe=async()=>[{total:100,free:90}];probe.status=()=>({verified:true});probe.fresh=()=>fresh;
  const monitor=createRecordingStorageMonitor({}, {probe,minFreeBytes:0,minFreeRatio:0});
  try{
    await monitor.refresh();fresh=false;
    assert.equal(monitor.captureAllowed(),false);
    assert.throws(()=>assertPeriodicRuntimeStorage('worker',periodicRuntimeStorageStatus(monitor)),{code:'periodic_application_runtime_storage_worker_report_stale'});
  }finally{await monitor.close();}
});
