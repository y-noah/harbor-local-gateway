import {postgresGatewayShutdownExitCode} from './postgres-shutdown.mjs';
import {transferError} from './migration-artifacts.mjs';
// These are shutdown boundary checks, not restore or deployment capabilities.
export async function closeRestoredRuntimeApplication(role,app){
 if(!['gateway','worker'].includes(role))throw transferError('storage_restored_runtime_role');if(!app)return {closed:true,drained:true,notStarted:true};const result=await app.close(),drained=role==='gateway'?postgresGatewayShutdownExitCode(result)===0:result?.recordingStorage?.pending===false;if(!drained)throw transferError('storage_restored_runtime_drain_failed');return {closed:true,drained:true,notStarted:false};
}
export async function collectRestoredRuntimeCloses(clients,{timeoutMs}={}){
 if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1)throw transferError('storage_restored_runtime_close_budget');return Promise.allSettled(clients.map(async client=>{if(!client)return {closed:true,drained:true,notCreated:true};let timer;try{return await Promise.race([Promise.resolve().then(()=>client.close()),new Promise((_,reject)=>{timer=setTimeout(()=>reject(transferError('storage_restored_runtime_close_timeout')),timeoutMs);})]);}finally{clearTimeout(timer);}}));
}
export function restoredRuntimeClosesSucceeded(results,children){return results.every(row=>row.status==='fulfilled'&&row.value?.closed===true&&row.value.drained===true)&&children.every(child=>child.exitCode===0&&child.signalCode===null);}
