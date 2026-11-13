import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createAdminAuth} from './auth.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope} from './department-access.mjs';
import {createPostgresDepartments,verifyDepartmentIsolationAvailability} from './postgres-departments.mjs';
import {postgresShutdownConfiguration} from './postgres-shutdown.mjs';
import {createPostgresExtraUsageBatches} from './postgres-extra-usage-batches.mjs';
import {createPostgresAccountExtraUsage} from './postgres-account-extra-usage.mjs';
import {createPostgresExtraRecovery} from './postgres-extra-recovery.mjs';
import {loadPostgresRecordingKey,createRecordingProbeStore,createRecordingStorageMonitor,recordingStorageConfiguration} from './postgres-recording-storage.mjs';
import {createPostgresRecordingJobs} from './postgres-recording-jobs.mjs';
import {resolve,join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {AppError} from './store.mjs';
import {PostgresStore,postgresContextSchemaVersions} from './postgres-store.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';
import {createPostgresRouter} from './postgres-routing.mjs';
import {createPostgresRoutingMaintenance} from './postgres-routing-maintenance.mjs';
import {createPostgresWorkerRuntime} from './postgres-worker-runtime.mjs';
import {createWorkerStartupProbe} from './postgres-worker-startup.mjs';
import {createPostgresPlanningJobs} from './postgres-planning-jobs.mjs';
import {createPostgresMemberBatches} from './postgres-member-batches.mjs';
import {createPostgresMemberActions} from './postgres-member-actions.mjs';

const departmentWorkerKinds=Object.freeze(['worker.startup','recording.export','recording.retention','recording.index','recording.context.index','recording.purge','member.config','member.actions','routing.expiry','routing.nightly','routing.preview','account.extra.configure','account.extra.recover']);
async function workerAdminConfiguration(path){
 let file;try{
  file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);const info=await file.stat();
  if(!info.isFile()||info.size>1048576||process.platform!=='win32'&&(info.mode&0o077||info.uid!==process.getuid()))throw Error();
  const content=await file.readFile('utf8');if(Buffer.byteLength(content)>1048576)throw Error();const config=JSON.parse(content);
  if(!/^[a-f0-9]{32}$/i.test(config.adminPasswordHash?.salt)||!/^[a-f0-9]{64}$/i.test(config.adminPasswordHash?.digest))throw Error();
  return config;
 }catch{throw new AppError(503,'Worker管理身份配置缺失或不安全','worker_admin_configuration');}finally{await file?.close();}
}

export async function createPostgresWorker(options={}){
  const concurrency=options.runtime?.concurrency??2,poolSize=options.poolSize??concurrency*2;
  // Every handler can hold one transaction connection. Keep one additional
  // connection per handler: a heartbeat may itself wait for an effect row lock.
  if(!Number.isInteger(concurrency)||concurrency<1||concurrency>8||!Number.isInteger(poolSize)||poolSize<concurrency*2||poolSize>64)throw new AppError(503,'Worker连接池须至少为执行并发的两倍','worker_configuration');
  const env=options.env||process.env,store=new PostgresStore({...options.database,...(!options.database?{connectionString:env.HARBOR_DATABASE_URL}:{}),poolSize});let runtime,closePromise,recordingMonitor,recordingProbeStore,recordingBackend,recordingTimer;
  try{
    try{await store.verifySchema();}catch(error){if(error.code==='42P01')throw new AppError(503,'数据库尚未迁移，请先运行db:migrate','database_migration_required');throw error;}const departmentInstalled=await verifyDepartmentIsolationAvailability(store),scopeIssuer=createDepartmentScopeIssuer(),dataDir=resolve(options.dataDir||env.HARBOR_DATA_DIR||join(dirname(fileURLToPath(import.meta.url)),'data')),settingsPath=join(dataDir,'settings.json');
    if(departmentInstalled){await workerAdminConfiguration(settingsPath);if(['jobs','handlers','schedule','refill'].some(name=>Object.hasOwn(options.runtime??{},name)))throw new AppError(503,'部门Worker不能覆盖可信任务接线','worker_configuration');}
    const departmentActors=departmentInstalled?createPostgresDepartments(store,{issuer:scopeIssuer,...(options.clock?{clock:options.clock}:{}),authorizeSuperAdmin:async credential=>createAdminAuth(await workerAdminConfiguration(settingsPath),{scopeIssuer,...(options.clock?{clock:options.clock}:{})}).authorizeSuperAdmin(credential)}):undefined;
    const jobs=createPostgresJobs(store,{...(options.clock?{clock:options.clock}:{}),...options.jobs,departmentActors,scopedKinds:departmentWorkerKinds,reservedKinds:['recording.purge','account.extra.recover']}),router=createPostgresRouter(store,options.clock?{clock:options.clock}:{}),maintenance=createPostgresRoutingMaintenance(store,{jobs,router});
    const extraUsage=createPostgresAccountExtraUsage(store,{...options.extraUsage,...(options.clock?{clock:options.clock}:{})}),extraRecovery=createPostgresExtraRecovery(store,{service:extraUsage,jobs:departmentInstalled?{...jobs,enqueue:(kind,input)=>{if(kind!=='account.extra.recover')throw new AppError(403,'额外额度恢复用途无效','department_forbidden');return jobs.enqueue(kind,{...input,scope:scopeIssuer.system({purpose:kind})});}}:jobs,...options.extraRecovery});
    const extraBatches=createPostgresExtraUsageBatches(store,{service:extraUsage,jobs,...(options.clock?{clock:options.clock}:{})});
    const planning=createPostgresPlanningJobs(store,{jobs,...(options.clock?{clock:options.clock}:{}),...options.planning});
    const memberBatches=createPostgresMemberBatches(store,{jobs,...(options.clock?{clock:options.clock}:{})});
    const memberActions=createPostgresMemberActions(store,{jobs,...(options.clock?{clock:options.clock}:{})});
    if(env.HARBOR_RECORDING_WORKER_ENABLED!==undefined&&!['0','1'].includes(env.HARBOR_RECORDING_WORKER_ENABLED))throw new AppError(503,'正文Worker开关须为0或1','recording_configuration');
    const recordingEnabled=options.recording?.enabled??(env.HARBOR_RECORDING_WORKER_ENABLED!==undefined?env.HARBOR_RECORDING_WORKER_ENABLED==='1':!!(options.dataDir||env.HARBOR_DATA_DIR));if(typeof recordingEnabled!=='boolean')throw new AppError(503,'正文Worker开关须为布尔值','recording_configuration');
    if(recordingEnabled){const dataDir=resolve(options.dataDir||env.HARBOR_DATA_DIR||join(dirname(fileURLToPath(import.meta.url)),'data')),recordingKey=await loadPostgresRecordingKey(store,{dataDir,spoolLimits:options.recording?.spoolLimits});recordingProbeStore=createRecordingProbeStore(options.database??{connectionString:env.HARBOR_DATABASE_URL});recordingMonitor=createRecordingStorageMonitor(recordingProbeStore,{...recordingStorageConfiguration(options.recording?.storage,env),appPaths:[dataDir,join(dataDir,'recording-spool')],...(options.clock?{clock:options.clock}:{})});await recordingMonitor.refresh();const contextSchemaVersion=(await store.get('SELECT MAX(version) version FROM harbor.schema_migrations')).version;recordingBackend=createPostgresRecordingJobs(store,{...options.recording?.backend,contextIndex:contextSchemaVersion===postgresContextSchemaVersions.index,contextStorage:contextSchemaVersion>=postgresContextSchemaVersions.storage,recordingKey,jobs,storagePolicy:recordingMonitor.policy,devices:true,...(options.clock?{clock:options.clock}:{})});}
    const startup=createWorkerStartupProbe(departmentInstalled?{...jobs,enqueue:(kind,options)=>jobs.enqueue(kind,{...options,scope:scopeIssuer.system({purpose:kind})})}:jobs);let startupRequested=false;
    const legacyHandlers={...extraBatches.handlers,...extraRecovery.handlers,...maintenance.handlers,...planning.handlers},handlers={...startup.handlers,...memberBatches.handlers,...memberActions.handlers,...maintenance.handlers,...planning.handlers,...extraBatches.handlers,...extraRecovery.handlers,...(recordingBackend?.handlers||{})};
    if(departmentInstalled){const startupHandler=handlers['worker.startup'];handlers['worker.startup']=async(job,context)=>{const scope=requireDepartmentScope(await jobs.authorizePersistedActor(job),{allowGlobal:true,allowSystem:true,roles:['system']});if(scope.view!=='global'||scope.purpose!=='worker.startup')throw new AppError(403,'启动自检身份无效','department_forbidden');return startupHandler(job,context);};}
    if(departmentInstalled){const recover=handlers['account.extra.recover'];handlers['account.extra.recover']=async(job,context)=>{const scope=requireDepartmentScope(await jobs.authorizePersistedActor(job),{allowGlobal:true,allowSystem:true,roles:['system']});if(scope.view!=='global'||scope.purpose!=='account.extra.recover')throw new AppError(403,'额外额度恢复身份无效','department_forbidden');return recover(job,context);};}
    await jobs.verifyReadiness({handlers});
    const runtimeJobs={...jobs,claim:kinds=>{const allowed=recordingMonitor&&!recordingMonitor.captureAllowed()?kinds.filter(kind=>!['recording.index','recording.context.index','recording.export'].includes(kind)):kinds;return allowed.length?jobs.claim(allowed):Promise.resolve(null);}};
    runtime=createPostgresWorkerRuntime({jobs:runtimeJobs,refill:recordingBackend?async()=>{if(recordingMonitor.captureAllowed())await recordingBackend.refill();}:undefined,handlers,schedule:async()=>{if(recordingBackend)await recordingMonitor.refresh();const errors=[];const operations=[async()=>{if(startupRequested)await startup.schedule();},extraRecovery.schedule,...(recordingBackend?[async()=>jobs.enqueue('recording.purge',{id:'recording.purge:'+Math.floor(await jobs.now()/60000),payload:{},...(departmentInstalled?{scope:scopeIssuer.system({purpose:'recording.purge'})}:{})})]:[]),...(!departmentInstalled?[maintenance.schedule,planning.schedule]:[async()=>{const failures=[];for(const row of await store.all('SELECT id FROM harbor.departments WHERE enabled ORDER BY id')){const expiryScope=scopeIssuer.system({purpose:'routing.expiry',departmentId:row.id}),scope=scopeIssuer.system({purpose:'routing.nightly',departmentId:row.id});for(const operation of [()=>maintenance.schedule({scope:expiryScope}),()=>planning.schedule({scope,expiryScope})])try{await operation();}catch(error){failures.push(error);}}if(failures.length)throw failures[0];}]),...(recordingBackend?[recordingBackend.schedule]:[])];for(const operation of operations)try{await operation();}catch(error){errors.push(error);}if(errors.length)throw errors[0];},...options.runtime});
    const start=()=>{startupRequested=true;const result=runtime.start();if(recordingMonitor&&!recordingTimer){
      // A slow maintenance schedule must not prevent fresh storage samples.
      // The monitor retains its existing single-flight and freshness limits.
      recordingTimer=setInterval(()=>{void recordingMonitor.refresh();},1000);recordingTimer.unref();
    }return result;};
    return {store,jobs,planning,extraUsage,recordingBackend,recordingStorage:recordingMonitor,start,runOnce:runtime.runOnce,status:()=>({...runtime.status(),database:store.status(),departments:{installed:departmentInstalled,supportedKinds:Object.keys(handlers),unsupportedKinds:departmentInstalled?Object.keys(legacyHandlers).filter(kind=>!handlers[kind]):[]},recording:{enabled:!!recordingBackend,storage:recordingMonitor?.status()??null,pausedKinds:recordingMonitor&&!recordingMonitor.captureAllowed()?['recording.index',...(recordingBackend?.handlers['recording.context.index']?['recording.context.index']:[]),'recording.export']:[]}}),close:()=>closePromise??=(async()=>{clearInterval(recordingTimer);recordingTimer=null;await runtime.close();const recordingStorage=await recordingMonitor?.close();await recordingProbeStore?.close();await store.close();return {recordingStorage};})()};
  }catch(error){clearInterval(recordingTimer);await runtime?.close();await recordingMonitor?.close();await recordingProbeStore?.close();await store.close();throw error;}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{postgresShutdownConfiguration(process.env);const enabled=process.env.HARBOR_RECORDING_WORKER_ENABLED??'1';if(!['0','1'].includes(enabled))throw new AppError(503,'正文Worker开关须为0或1','recording_configuration');const worker=await createPostgresWorker({dataDir:process.env.HARBOR_DATA_DIR||join(dirname(fileURLToPath(import.meta.url)),'data'),recording:{enabled:enabled==='1'}});worker.start();console.log('Harbor PostgreSQL worker ready');let stopping=false;for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>{if(stopping)return;stopping=true;void worker.close().then(result=>{if(result.recordingStorage?.pending)process.exitCode=1;}).catch(()=>{process.exitCode=1;});});}
  catch{console.error('Harbor Worker未能启动，请核对数据库配置和迁移');process.exitCode=1;}
}
