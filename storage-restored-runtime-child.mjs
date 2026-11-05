import {createPostgresGateway} from './postgres-gateway.mjs';
import {createPostgresWorker} from './postgres-worker.mjs';
import {closeRestoredRuntimeApplication} from './storage-restored-runtime-shutdown.mjs';
import {postgresMaterialsPreflight} from './ci/postgres-preflight.mjs';
if(process.argv[2]!=='--owned-restored-runtime'||!process.send)throw Error('restored_runtime_child_private');
let app,role,closing=false,closePromise,initialization;
async function initialize(configuration){
 const {database,env}=configuration;role=configuration.role;if(!['gateway','worker'].includes(role)||closing)throw Error('restored_runtime_role');let materials;
 if(role==='gateway'){app=await createPostgresGateway({env,database});materials=await postgresMaterialsPreflight({env,database});if(closing)throw Error('restored_runtime_closed');await new Promise((resolve,reject)=>{app.server.once('error',reject);app.server.listen(0,'127.0.0.1',resolve);});const tls=await app.deviceTls.listen();return {role,httpPort:app.server.address().port,tlsPort:tls.port,materials,storage:app.recordingStorage.status().binding.verified};}
 materials=await postgresMaterialsPreflight({env,database});if(closing)throw Error('restored_runtime_closed');app=await createPostgresWorker({env,database});if(closing)throw Error('restored_runtime_closed');app.start();return {role,materials,storage:app.recordingStorage.status().binding.verified};
}
const close=()=>closePromise??=(async()=>{closing=true;await initialization?.catch(()=>{});const result=await closeRestoredRuntimeApplication(role,app);app=null;return result;})();
for(const name of ['SIGTERM','SIGINT'])process.on(name,()=>{void close().then(result=>process.exit(result.drained?0:1),()=>process.exit(1));});
process.on('disconnect',()=>{void close().then(result=>process.exit(result.drained?0:1),()=>process.exit(1));});
process.on('message',message=>{void(async()=>{if(!Number.isSafeInteger(message?.id)||message.id<1||typeof message.operation!=='string')return;try{let result;if(message.operation==='initialize'){if(initialization||closing)throw Error('restored_runtime_initialize_once');initialization=initialize(message.configuration);result=await initialization;}else if(message.operation==='status'){if(!app||closing)throw Error('restored_runtime_not_started');const status=app.status();result={storage:app.recordingStorage.status().binding.verified,captureAllowed:app.recordingStorage.captureAllowed(),stopping:status.stopping??false};}else if(message.operation==='close')result=await close();else throw Error('restored_runtime_command');if(process.connected)process.send({id:message.id,ok:true,result},()=>{if(message.operation==='close')process.exit(result.drained?0:1);});}catch(error){if(process.connected)process.send({id:message.id,ok:false,error:typeof error?.code==='string'&&/^[a-z][a-z0-9_]+$/.test(error.code)?error.code:'restored_runtime_child_failed'});}})();});
