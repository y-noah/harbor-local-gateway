import {setTimeout as delay} from 'node:timers/promises';
import {isOwnedRuntime,isOwnedDatabaseProxy,isOwnedLaboratory,ownedProcessProof} from './owned-runtime.mjs';
import {isOwnedUpstream} from './controlled-upstream.mjs';
const adapters=new WeakSet();
export const isOwnedFaultAdapter=value=>adapters.has(value);
export function createOwnedFaultAdapter({runtime,worker,gateway,databaseProxy,upstream,restartGateway,laboratory,restoreOwnedLab}={}){
  if(!isOwnedRuntime(runtime))throw new Error('capacity_fault_runtime_not_owned');if(worker&&ownedProcessProof(runtime,worker).role!=='worker'||gateway&&ownedProcessProof(runtime,gateway).role!=='gateway'||databaseProxy&&!isOwnedDatabaseProxy(databaseProxy)||upstream&&!isOwnedUpstream(upstream)||restoreOwnedLab&&(!isOwnedLaboratory(laboratory)||laboratory.runtime!==runtime||typeof restoreOwnedLab!=='function'))throw new Error('capacity_fault_target_not_owned');
  const supported=[...(worker?['worker-pause']:[]),...(databaseProxy?['database-disconnect']:[]),...(upstream?['upstream-429','upstream-disconnect']:[]),...(gateway&&typeof restartGateway==='function'?['gateway-drain','gateway-crash']:[]),...(restoreOwnedLab?['host-restore']:[])];
  const adapter={supported,ownership:'task-created-isolated-targets',run:async(id,{durationMs=60000,signal,onApplied=()=>{}}={})=>{
    if(typeof onApplied!=='function'||!supported.includes(id)||!Number.isSafeInteger(durationMs)||durationMs<1||durationMs>600000)throw new Error('capacity_fault_not_supported');let startedAt=Date.now(),result={};
    if(id==='worker-pause')result=await runtime.pauseWorker(worker,durationMs,{signal});
    else if(id==='database-disconnect'){await databaseProxy.disconnect();startedAt=Date.now();let original;try{await onApplied({id,startedAt});await delay(durationMs,undefined,{signal});}catch(error){original=error;}try{await databaseProxy.restore();}catch(error){if(original)throw new AggregateError([original,error],'capacity_fault_and_restore_failed',{cause:original});throw error;}if(original)throw original;}
    else if(id==='upstream-429'||id==='upstream-disconnect'){upstream.setFault(id==='upstream-429'?'429':'disconnect');try{await delay(durationMs,undefined,{signal});}finally{upstream.setFault('normal');}}
    else if(id==='gateway-drain'||id==='gateway-crash'){await runtime.stop(gateway,{abrupt:id==='gateway-crash'});gateway=await restartGateway({signal});if(ownedProcessProof(runtime,gateway).role!=='gateway')throw new Error('capacity_restarted_gateway_not_owned');}
    else result=await restoreOwnedLab({laboratory,signal});
    return {...result,id,startedAt,restoredAt:Date.now(),durationMs:Date.now()-startedAt,ownership:'task-created-isolated-targets'};
  }};adapters.add(adapter);return adapter;
}
