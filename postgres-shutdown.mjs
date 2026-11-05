import {AppError} from './store.mjs';
export const POSTGRES_SHUTDOWN_CONTRACT='postgres-shutdown-v1';
export function postgresShutdownConfiguration(env={}){
 const read=(name,fallback,minimum,maximum)=>{const raw=env[name];if(raw===undefined)return fallback;if(typeof raw!=='string'||!/^\d+$/.test(raw))throw new AppError(503,'PostgreSQL关闭配置无效','gateway_configuration');const value=Number(raw);if(!Number.isSafeInteger(value)||value<minimum||value>maximum)throw new AppError(503,'PostgreSQL关闭配置无效','gateway_configuration');return value;};
 return Object.freeze({contract:POSTGRES_SHUTDOWN_CONTRACT,gateway_drain_ms:read('HARBOR_GATEWAY_DRAIN_TIMEOUT_MS',300000,100,3600000),cleanup_grace_ms:read('HARBOR_SHUTDOWN_CLEANUP_GRACE_MS',30000,1000,300000),worker_stop_ms:read('HARBOR_WORKER_STOP_TIMEOUT_MS',30000,1000,3600000)});
}
export function postgresGatewayShutdownExitCode(result){
 if(!result||result.ownershipLost||result.metering?.queued||result.metering?.files||result.recordingStorage?.pending)return 1;
 return result.forced===true?2:result.forced===false?0:1;
}
export function installPostgresGatewayShutdown(app,{runtime=process}={}){
 let stopping=false;
 const remove=()=>{for(const signal of ['SIGINT','SIGTERM'])runtime.off(signal,stop);app.ownershipSignal?.removeEventListener('abort',stop);};
 const stop=()=>{if(stopping)return;stopping=true;void Promise.resolve().then(()=>app.close()).then(result=>{runtime.exitCode=app.ownershipSignal?.aborted?1:postgresGatewayShutdownExitCode(result);},()=>{runtime.exitCode=1;}).finally(remove);};
 for(const signal of ['SIGINT','SIGTERM'])runtime.on(signal,stop);
 app.ownershipSignal?.addEventListener('abort',stop,{once:true});
 if(app.ownershipSignal?.aborted)stop();
 return remove;
}
