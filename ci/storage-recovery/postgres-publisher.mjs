import {readFile,lstat} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createRecordingProbeStore} from '../../postgres-recording-storage.mjs';
import {publishRecordingStorageReport} from '../../postgres-storage-report.mjs';

// A report is issued by the real PG user against its Unix socket and /proc;
// another container's mounted PGDATA alone never qualifies this publisher.
export async function createInfrastructurePublisher({configuration,signal,intervalMs=5000}={}){
 if(typeof configuration!=='function'||!Number.isInteger(intervalMs)||intervalMs<100||intervalMs>5000)throw Error('infra_publisher_configuration');
 let store,closed=false,active=null,closePromise,last={state:'awaiting_database',published:false};
 const controller=new AbortController(),lifetime=AbortSignal.any([signal,controller.signal].filter(Boolean));
 async function tick(){
  lifetime.throwIfAborted();if(closed)throw Error('infra_publisher_closed');if(active)return active;
  active=(async()=>{const value=await configuration();lifetime.throwIfAborted();
   if(!value||value.version!==1||value.database!==null&&!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(value.database)||typeof value.pgDataDir!=='string'||typeof value.keyFile!=='string'||typeof value.socketDirectory!=='string'||!Number.isInteger(value.port))throw Error('infra_publisher_configuration');
   if(value.database===null)return last={state:'awaiting_database',published:false};
   store=createRecordingProbeStore({connection:{host:value.socketDirectory,port:value.port,user:value.user??'postgres',password:value.password,database:value.database,application_name:'harbor_infrastructure_publisher'}});
   try{const result=await publishRecordingStorageReport(store,{pgDataDir:value.pgDataDir,keyFile:value.keyFile,ttlMs:15000});lifetime.throwIfAborted();return last={state:'ready',published:result.published};}
   finally{const current=store;store=null;await current.close();}
  })();try{return await active;}catch{last={state:'unavailable',published:false};return last;}finally{active=null;}
 }
 const run=async()=>{while(!lifetime.aborted){await tick();try{await delay(intervalMs,undefined,{signal:lifetime});}catch{break;}}};
 return Object.freeze({tick,run,status:()=>Object.freeze({...last}),close:()=>closePromise??=(async()=>{closed=true;controller.abort();await active?.catch(()=>{});await store?.close();last={state:'closed',published:false};})()});
}
async function installedConfiguration(){
 const path='/run/harbor-infra-control/publisher.json',info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022||info.size>4096)throw Error('infra_publisher_untrusted_configuration');
 const value=JSON.parse(await readFile(path,'utf8'));
 if(Object.keys(value).sort().join(',')!=='database,keyFile,pgDataDir,port,socketDirectory,version'||value.pgDataDir!=='/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker'||value.socketDirectory!=='/var/run/postgresql'||value.keyFile!=='/run/harbor-report.key'||value.port!==5432)throw Error('infra_publisher_untrusted_configuration');return value;
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 let publisher;try{if(process.argv.length!==2||process.getuid()===0)throw Error('infra_publisher_entry');publisher=await createInfrastructurePublisher({configuration:installedConfiguration});for(const event of ['SIGTERM','SIGINT'])process.once(event,()=>{void publisher.close().catch(()=>{process.exitCode=1;});});await publisher.run();}catch{process.stderr.write('Harbor基础设施报告发布器已停止\n');process.exitCode=1;}finally{await publisher?.close();}
}
