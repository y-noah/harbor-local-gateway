import {createPostgresGateway} from './postgres-gateway.mjs';
import {createPostgresWorker} from './postgres-worker.mjs';
import {postgresMaterialsPreflight} from './ci/postgres-preflight.mjs';
import {closeRestoredRuntimeApplication} from './storage-restored-runtime-shutdown.mjs';
import {transferError} from './migration-artifacts.mjs';
import {periodicRuntimeStorageStatus} from './postgres-periodic-runtime-storage.mjs';

if(process.argv[2]!=='--periodic-restored-runtime'||!process.send)throw transferError('periodic_application_child_private');
let app,role,initialization,closing=false,closed;
const blocked=()=>{throw transferError('periodic_application_upstream_disabled');};
// Verification exercises the real gateway/worker, but never logs in, refreshes
// subscription credentials, invokes a CLI, or sends a model request upstream.
const transport=new Proxy({}, {get:(_target,key)=>key==='stageObserverStatus'?()=>({enabled:false}):blocked});
async function initialize({database,env,role:selected}){
  if(!['gateway','worker'].includes(selected)||closing)throw transferError('periodic_application_child_role');role=selected;
  if(role==='gateway'){
    app=await createPostgresGateway({database,env,rateReader:blocked,checker:blocked,claudeChecker:blocked,executor:blocked,claudeExecutor:blocked,loginStarter:blocked,platformTransport:transport,responsesTransport:transport,apiResponsesTransport:transport});
    const materials=await postgresMaterialsPreflight({database,env});if(closing)throw transferError('periodic_application_child_closed');
    await new Promise((resolve,reject)=>{app.server.once('error',reject);app.server.listen(0,'127.0.0.1',resolve);});
    const tls=await app.deviceTls.listen();return {role,httpPort:app.server.address().port,tlsPort:tls.port,materials,storage:app.recordingStorage.status().binding.verified};
  }
  const materials=await postgresMaterialsPreflight({database,env});if(closing)throw transferError('periodic_application_child_closed');
  app=await createPostgresWorker({database,env});if(closing)throw transferError('periodic_application_child_closed');app.start();return {role,materials,storage:app.recordingStorage.status().binding.verified};
}
const close=()=>closed??=(async()=>{closing=true;await initialization?.catch(()=>{});const result=await closeRestoredRuntimeApplication(role,app);app=null;return result;})();
for(const name of ['SIGTERM','SIGINT'])process.on(name,()=>{void close().then(result=>process.exit(result.drained?0:1),()=>process.exit(1));});
process.on('disconnect',()=>{void close().then(result=>process.exit(result.drained?0:1),()=>process.exit(1));});
process.on('message',message=>{void(async()=>{
  if(!Number.isSafeInteger(message?.id)||message.id<1)return;
  try{
    let result;
    if(message.operation==='initialize'){if(initialization||closing)throw transferError('periodic_application_initialize_once');initialization=initialize(message.configuration);result=await initialization;}
    else if(message.operation==='close')result=await close();
    else if(message.operation==='status'&&app&&!closing)result=periodicRuntimeStorageStatus(app.recordingStorage);
    else throw transferError('periodic_application_child_command');
    if(process.connected)process.send({id:message.id,ok:true,result},()=>{if(message.operation==='close')process.exit(result.drained?0:1);});
  }catch(error){if(process.connected)process.send({id:message.id,ok:false,error:typeof error?.code==='string'&&/^[a-z][a-z0-9_]+$/.test(error.code)?error.code:'periodic_application_child_failed'});}
})();});
