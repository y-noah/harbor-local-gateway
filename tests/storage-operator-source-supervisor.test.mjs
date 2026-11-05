import test from 'node:test';
import assert from 'node:assert/strict';
import {storageOperatorSourceSupervisor,fenceStorageOperatorSupervisor,verifyStorageOperatorSupervisor,verifyStorageOperatorSourceFence} from '../storage-operator-source-supervisor.mjs';

test('a fresh process must preserve the authenticated stopped container generation and start/finish epoch',()=>{
  const original={scope:'docker-systemd',sourceStopped:true,supervisorFenced:true,productionVerified:true,sourceImageId:'sha256:'+'a'.repeat(64),sourceContainerId:'b'.repeat(64),sourceStartedAt:'2026-11-05T00:00:00.000000000Z',sourceFinishedAt:'2026-11-05T01:00:00.000000000Z'};
  verifyStorageOperatorSourceFence(original,structuredClone(original));
  for(const [key,value] of [['sourceContainerId','c'.repeat(64)],['sourceStartedAt','2026-11-05T02:00:00.000000000Z'],['sourceFinishedAt','2026-11-05T03:00:00.000000000Z'],['sourceStopped',false],['sourceImageId','sha256:'+'d'.repeat(64)]])assert.throws(()=>verifyStorageOperatorSourceFence(original,{...original,[key]:value}),{code:'storage_operator_source_fence_changed'},key);
  for(const old of [undefined,{}, {...original,sourceContainerId:undefined},{...original,sourceStartedAt:''}])assert.throws(()=>verifyStorageOperatorSourceFence(old,original),{code:'storage_operator_source_fence_changed'});
});

test('existing source configuration retains systemd fencing without a new setting',async()=>{
  const source={supervisorUnit:'harbor-local-gateway.service'},calls=[];
  const exec=async(file,args,options)=>{calls.push({file,args,options});return {stdout:'LoadState=masked\nActiveState=inactive\nSubState=dead\n'};};
  assert.deepEqual(storageOperatorSourceSupervisor(source),{kind:'systemd',unit:source.supervisorUnit});
  await fenceStorageOperatorSupervisor(source,exec);await verifyStorageOperatorSupervisor(source,exec);
  assert.deepEqual(calls.map(row=>row.args),[['mask','--runtime','--now',source.supervisorUnit],['show',source.supervisorUnit,'--property=LoadState','--property=ActiveState','--property=SubState']]);
  assert.ok(calls.every(row=>row.file==='/usr/bin/systemctl'&&row.options.timeout>0));
});

test('Docker-only supervisor requires explicit configuration and does not call systemctl',async()=>{
  const source={supervisorKind:'docker-restart'},exec=()=>assert.fail('Docker-only configuration must not call systemctl');
  assert.deepEqual(storageOperatorSourceSupervisor(source),{kind:'docker-restart'});
  assert.ok(Object.isFrozen(storageOperatorSourceSupervisor(source)));
  await fenceStorageOperatorSupervisor(source,exec);await verifyStorageOperatorSupervisor(source,exec);
});

test('missing units, invalid modes and contradictory declarations fail without a fallback',async()=>{
  for(const source of [undefined,{}, {supervisorKind:null},{supervisorKind:false},{supervisorKind:''},{supervisorKind:'docker'},{supervisorKind:'systemd'},{supervisorUnit:'other.service'},{supervisorUnit:'harbor.service\n'},{supervisorKind:'docker-restart',supervisorUnit:'harbor.service'},{supervisorKind:'docker-restart',supervisorUnit:null}]){
    assert.throws(()=>storageOperatorSourceSupervisor(source),{code:'storage_operator_supervisor_configuration'});
    const exec=()=>assert.fail('invalid source configuration must not execute');
    await assert.rejects(fenceStorageOperatorSupervisor(source,exec),{code:'storage_operator_supervisor_configuration'});
    await assert.rejects(verifyStorageOperatorSupervisor(source,exec),{code:'storage_operator_supervisor_configuration'});
  }
});

test('systemd failure or an unfenced unit never falls back to Docker-only mode',async()=>{
  const source={supervisorUnit:'harbor.service'};
  for(const stdout of ['LoadState=not-found\nActiveState=inactive\n','LoadState=masked\nActiveState=active\n','LoadState=loaded\nActiveState=inactive\n',''])await assert.rejects(verifyStorageOperatorSupervisor(source,async()=>({stdout})),{code:'storage_operator_supervisor_not_fenced'});
  const error=Error('synthetic supervisor failure');
  for(const operation of [fenceStorageOperatorSupervisor,verifyStorageOperatorSupervisor])await assert.rejects(operation(source,async()=>{throw error;}),value=>value===error);
});
