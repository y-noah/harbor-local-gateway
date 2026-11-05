import {transferError,canonical} from './migration-artifacts.mjs';

const fail=code=>{throw transferError(code);};

export function verifyStorageOperatorSourceFence(expected,actual){
  if(!expected||expected.scope!=='docker-systemd'||expected.sourceStopped!==true||expected.supervisorFenced!==true||expected.productionVerified!==true||!/^sha256:[a-f0-9]{64}$/.test(expected.sourceImageId??'')||!/^[a-f0-9]{64}$/.test(expected.sourceContainerId??'')||typeof expected.sourceStartedAt!=='string'||!expected.sourceStartedAt||typeof expected.sourceFinishedAt!=='string'||!expected.sourceFinishedAt||canonical(expected)!==canonical(actual))fail('storage_operator_source_fence_changed');
}

// Only the installed root configuration can select Docker as the sole source
// supervisor. A missing systemd unit must never silently select this mode.
export function storageOperatorSourceSupervisor(source){
  const kind=source&&Object.hasOwn(source,'supervisorKind')?source.supervisorKind:'systemd';
  if(kind==='docker-restart'){
    if(Object.hasOwn(source,'supervisorUnit'))fail('storage_operator_supervisor_configuration');
    return Object.freeze({kind});
  }
  if(kind!=='systemd'||!/^harbor[-a-z0-9]{0,60}\.service$/.test(source?.supervisorUnit??''))fail('storage_operator_supervisor_configuration');
  return Object.freeze({kind,unit:source.supervisorUnit});
}

export async function fenceStorageOperatorSupervisor(source,exec){
  const supervisor=storageOperatorSourceSupervisor(source);
  if(supervisor.kind==='systemd')await exec('/usr/bin/systemctl',['mask','--runtime','--now',supervisor.unit],{timeout:330000,maxBuffer:16384});
}

export async function verifyStorageOperatorSupervisor(source,exec){
  const supervisor=storageOperatorSourceSupervisor(source);
  if(supervisor.kind==='systemd'){
    const result=await exec('/usr/bin/systemctl',['show',supervisor.unit,'--property=LoadState','--property=ActiveState','--property=SubState'],{timeout:15000,maxBuffer:16384});
    const unit=Object.fromEntries(result.stdout.trim().split('\n').map(line=>line.split('=')));
    if(unit.LoadState!=='masked'||unit.ActiveState!=='inactive')fail('storage_operator_supervisor_not_fenced');
  }
  // Both modes still require the caller's independent Docker generation,
  // restart=no, complete process exit, and no-other-source checks.
}
