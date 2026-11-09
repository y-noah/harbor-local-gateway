import {createReportedRecordingStorageProbe} from './postgres-storage-report.mjs';
import {constants} from 'node:fs';
import {open,readFile,writeFile,lstat,realpath,stat,statfs,readdir} from 'node:fs/promises';
import {join,isAbsolute} from 'node:path';
import {endianness} from 'node:os';
import {performance} from 'node:perf_hooks';
import {createHash,randomBytes} from 'node:crypto';
import {AppError} from './store.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {RecordingStore} from './recording-store.mjs';
import {createRecordingIndexCrypto} from './recording-search-index.mjs';
import {createRecordingStoragePolicy} from './postgres-recording-jobs.mjs';
const fail=(message,code='recording_storage_unverified')=>{throw new AppError(503,message,code);};
const absent=async path=>{try{await lstat(path);return false;}catch(error){if(error.code==='ENOENT')return true;throw error;}};
// Workers only read this material. Only the gateway may initialize an empty installation.
export async function loadPostgresRecordingKey(store,{dataDir,createIfEmpty=false,spoolLimits={}}={}){
 if(typeof dataDir!=='string'||!isAbsolute(dataDir)||typeof createIfEmpty!=='boolean')fail('正文密钥目录配置无效','recording_key_configuration');const path=join(dataDir,'recording-encryption.key');
 if(await absent(path)){const existing=await store.get("SELECT EXISTS(SELECT 1 FROM harbor.recordings) OR EXISTS(SELECT 1 FROM harbor.conversation_events) OR EXISTS(SELECT 1 FROM harbor.recording_export_chunks) OR EXISTS(SELECT 1 FROM harbor.gateway_settings WHERE name='recording.key.binding') present");if(!createIfEmpty||existing.present||!await absent(join(dataDir,'recording-spool')))fail('正文密钥缺失，禁止生成替代材料','recording_key_missing');await writeFile(path,randomBytes(32),{flag:'wx',mode:0o600,flush:true});}
 const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size!==32||process.platform!=='win32'&&((info.mode&0o077)!==0||info.uid!==process.getuid()))fail('正文密钥权限或格式无效','recording_key_configuration');const handle=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);let key;try{key=await handle.readFile();}finally{await handle.close();}if(key.length!==32)fail('正文密钥格式无效','recording_key_configuration');
 const binding=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.key.binding'");
 if(!binding&&!await absent(join(dataDir,'recording-spool'))){
  const directory=join(dataDir,'recording-spool'),directoryInfo=await lstat(directory),maxFile=(spoolLimits.maxMemoryBytes??4194304)+64,maxBytes=spoolLimits.maxDiskBytes??67108864,maxFiles=spoolLimits.maxFiles??1024;if(!directoryInfo.isDirectory()||directoryInfo.isSymbolicLink()||process.platform!=='win32'&&((directoryInfo.mode&0o077)!==0||directoryInfo.uid!==process.getuid()))fail('正文恢复目录权限不安全','recording_key_configuration');if(![maxFile,maxBytes,maxFiles].every(Number.isSafeInteger)||maxFile<1024||maxBytes<1024||maxFiles<1)fail('正文恢复验证预算无效','recording_key_configuration');
  const names=(await readdir(directory)).filter(name=>/^\d{20}-[a-f0-9-]{36}\.record(?:\.pending)?$/.test(name));if(names.length>maxFiles)fail('既有正文恢复文件超过验证预算','recording_key_configuration');const repository=new RecordingStore(store,{encryptionKey:key});let total=0;
  for(const name of names){const file=join(directory,name),info=await lstat(file);total+=info.size;if(!info.isFile()||info.isSymbolicLink()||info.size>maxFile||total>maxBytes||process.platform!=='win32'&&((info.mode&0o077)!==0||info.uid!==process.getuid()))fail('既有正文恢复文件权限或容量无效','recording_key_configuration');const source=await open(file,constants.O_RDONLY|constants.O_NOFOLLOW);try{const buffer=Buffer.alloc(info.size+1);let offset=0,read;while(offset<buffer.length&&(read=(await source.read(buffer,offset,buffer.length-offset,null)).bytesRead))offset+=read;const bytes=buffer.subarray(0,offset);if(bytes.length>info.size||bytes.length>maxFile)fail('既有正文恢复文件超过验证预算','recording_key_configuration');repository.open(bytes,name.endsWith('.pending')?name.slice(0,-8):name);}catch{fail('既有正文恢复文件无法由此密钥验证','recording_key_mismatch');}finally{await source.close();}}
 }
 const digest=createHash('sha256').update('harbor-recording-key-binding-v1\0').update(key).digest('hex'),crypto=createRecordingIndexCrypto(key),recordings=new RecordingStore(store,{encryptionKey:key});
 await store.transaction(async()=>{await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-recording-key-binding',0))");const saved=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.key.binding'");if(saved){let value;try{value=JSON.parse(saved.value);}catch{}if(value?.version!==1||value.digest!==digest)fail('正文密钥与此数据库绑定不匹配','recording_key_mismatch');return;}
  // Validate legacy material before recording a new binding, never bless a wrong key.
  try{const event=await store.get('SELECT request_id,sequence,source_key,content_cipher FROM harbor.conversation_events ORDER BY request_id,sequence LIMIT 1');if(event)recordings.open(event.content_cipher,event.request_id+':'+event.sequence+':'+event.source_key);const preview=await store.get('SELECT request_id,preview_cipher FROM harbor.recordings WHERE preview_cipher IS NOT NULL ORDER BY request_id LIMIT 1');if(preview)recordings.open(preview.preview_cipher,preview.request_id+':preview');const exported=await store.get('SELECT export_id,ordinal,payload_cipher FROM harbor.recording_export_chunks ORDER BY export_id,ordinal LIMIT 1');if(exported)crypto.open(exported.payload_cipher,`export:${exported.export_id}:${exported.ordinal}`);if(await store.get('SELECT request_id FROM harbor.recording_documents WHERE key_version<>$1 LIMIT 1',crypto.keyVersion))fail('正文派生数据密钥不匹配','recording_key_mismatch');}catch{fail('既有正文密文无法由此密钥验证','recording_key_mismatch');}
  await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.key.binding',$1)",JSON.stringify({version:1,digest}));
 });return key;
}
// Diagnostics expose only fixed categories, never driver messages or paths.
const diagnosticCount=value=>Math.min(Number.MAX_SAFE_INTEGER,value+1);
const elapsedSince=started=>Math.min(86400000,Math.max(0,Math.round(performance.now()-started)));
function probeFailureKind(error){
 if(error?.code==='57014')return 'sql_timeout';
 if(error?.message==='Query read timeout')return 'query_timeout';
 if(['timeout exceeded when trying to connect','Connection terminated due to connection timeout','Connection timeout'].includes(error?.message))return 'connection_timeout';
 if(['ECONNREFUSED','ECONNRESET','EPIPE','ENOTFOUND','EHOSTUNREACH','ETIMEDOUT'].includes(error?.code))return 'connection_failed';
 if(['42501','EACCES','EPERM'].includes(error?.code))return 'permission_denied';
 if(['ENOENT','ENOTDIR','ELOOP'].includes(error?.code))return 'filesystem_unavailable';
 return 'probe_failed';
}
export function createLocalRecordingStorageProbe(store,{pgDataDir,appPaths=[],verifyDatabaseDirectory}={}){
 let state={configured:!!pgDataDir,source:'local_postgres_filesystem',verified:false,reason:pgDataDir?'not_checked':'not_configured',phase:'idle',failureKind:null};
 if(verifyDatabaseDirectory!==undefined&&typeof verifyDatabaseDirectory!=='function')fail('正文存储目录验证器无效','recording_storage_configuration');
 if(pgDataDir!==undefined&&(typeof pgDataDir!=='string'||!isAbsolute(pgDataDir))||!Array.isArray(appPaths)||appPaths.some(path=>typeof path!=='string'||!isAbsolute(path)))fail('正文存储路径配置无效','recording_storage_configuration');
 const reject=reason=>{state={...state,verified:false,reason,failureKind:'binding_rejected'};fail('数据库实际存储空间尚未核实');};
 const probe=async()=>{state={...state,phase:'configuration',failureKind:null};try{
  if(!pgDataDir)return reject('not_configured');const host=store.pool.options.host;if(typeof host!=='string'||!isAbsolute(host))return reject('database_transport_not_local');
  state={...state,phase:'database_identity'};const identity=await store.get("SELECT current_setting('data_directory') directory,current_setting('server_version_num')::integer version,pg_backend_pid() pid,(SELECT system_identifier::text FROM pg_control_system()) identifier");state={...state,phase:'directory_binding'};const directory=await realpath(pgDataDir);if(verifyDatabaseDirectory?await verifyDatabaseDirectory(identity,directory)!==true:await realpath(identity.directory)!==directory||await realpath('/proc/'+identity.pid+'/cwd')!==directory)return reject('database_namespace_mismatch');
  state={...state,phase:'cluster_binding'};const major=(await readFile(join(directory,'PG_VERSION'),'utf8')).trim();if(major!==String(Math.floor(identity.version/10000)))return reject('database_version_mismatch');const control=await open(join(directory,'global','pg_control'),constants.O_RDONLY|constants.O_NOFOLLOW);let identifier;try{const info=await control.stat();if(!info.isFile()||info.size!==8192)return reject('database_control_invalid');const bytes=Buffer.alloc(8),result=await control.read(bytes,0,8,0);if(result.bytesRead!==8)return reject('database_control_invalid');identifier=(endianness()==='LE'?bytes.readBigUInt64LE():bytes.readBigUInt64BE()).toString();}finally{await control.close();}if(identifier!==identity.identifier)return reject('database_cluster_mismatch');
  state={...state,phase:'database_relations'};const files=await store.all(`WITH base AS (SELECT c.oid,c.reltoastrelid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND (c.relname LIKE 'recording\\_%' ESCAPE '\\' OR c.relname IN ('recordings','conversation_events')) AND c.relkind IN ('r','m')), targets AS (SELECT oid FROM base UNION SELECT reltoastrelid FROM base WHERE reltoastrelid<>0), all_relations AS (SELECT oid FROM targets UNION SELECT indexrelid FROM pg_index WHERE indrelid IN (SELECT oid FROM targets)) SELECT pg_relation_filepath(oid) path FROM all_relations LIMIT 257`);
  if(!files.length||files.length>256||files.some(file=>typeof file.path!=='string'||isAbsolute(file.path)||file.path.split('/').includes('..')))return reject('database_relations_unverified');
  state={...state,phase:'filesystem'};const paths=[directory,await realpath(join(directory,'pg_wal')),...appPaths];for(const file of files)paths.push(await realpath(join(directory,file.path)));const devices=new Map();for(const path of paths){const resolved=await realpath(path),info=await stat(resolved);if(!devices.has(info.dev))devices.set(info.dev,resolved);}const samples=[];for(const path of devices.values()){const value=await statfs(path,{bigint:true}),total=value.blocks*value.bsize,free=value.bavail*value.bsize;if(total>BigInt(Number.MAX_SAFE_INTEGER)||free>BigInt(Number.MAX_SAFE_INTEGER))return reject('filesystem_size_unrepresentable');samples.push({total:Number(total),free:Number(free)});}state={...state,verified:true,reason:null,phase:'complete',failureKind:null,relation_count:files.length};return samples;
 }catch(error){if(error.code!=='recording_storage_unverified')state={...state,verified:false,reason:'database_storage_probe_failed',failureKind:probeFailureKind(error)};throw error;}};probe.status=()=>({...state});return probe;
}
export function createRecordingStorageMonitor(store,{pgDataDir,appPaths,probe,mode='local',reportKeyFile,minFreeBytes=1073741824,minFreeRatio=.1,cacheMs=1000,staleAfterMs=5000,probeTimeoutMs=1000,clock=Date.now}={}){
 if(!['local','report'].includes(mode)||!Number.isInteger(probeTimeoutMs)||probeTimeoutMs<100||probeTimeoutMs>5000||!Number.isInteger(staleAfterMs)||staleAfterMs<100||staleAfterMs>60000||typeof clock!=='function')fail('正文存储采样有效期无效','recording_storage_configuration');const actual=probe??(mode==='report'?createReportedRecordingStorageProbe(store,{reportKeyFile,appPaths}):createLocalRecordingStorageProbe(store,{pgDataDir,appPaths}));
 let probeFailure=null,lastFailure=null,lastDurationMs=0,maxDurationMs=0;const counts={started:0,completed:0,failed:0,monitor_timeout:0};const observed=async()=>{probeFailure=null;counts.started=diagnosticCount(counts.started);const started=performance.now();try{const value=await actual();counts.completed=diagnosticCount(counts.completed);return value;}catch(error){probeFailure=probeFailureKind(error);lastFailure={kind:probeFailure,phase:phase()};counts.failed=diagnosticCount(counts.failed);throw error;}finally{lastDurationMs=elapsedSince(started);maxDurationMs=Math.max(maxDurationMs,lastDurationMs);}},policy=createRecordingStoragePolicy({probe:observed,minFreeBytes,minFreeRatio,cacheMs,clock});let stopped=false,pending=null,visible=null,last={checked:0,status:'unknown',captureAllowed:false,exportAllowed:false},lastReason='not_checked',lastTimeout=null;
 const reason=()=>stopped?'stopped':last.checked>clock()?'clock_reversed':clock()-last.checked>staleAfterMs?'stale':typeof actual.fresh==='function'&&!actual.fresh()?'report_stale':lastReason;
 const snapshot=()=>{const cause=reason();return ['stopped','clock_reversed','stale','report_stale'].includes(cause)?{...last,status:cause==='stopped'?'stopped':'unknown',captureAllowed:false,exportAllowed:false}:last;};
 const phase=()=>{let value;try{value=typeof actual.status==='function'?actual.status()?.phase:undefined;}catch{}return ['configuration','database_identity','directory_binding','cluster_binding','database_relations','filesystem','complete','idle'].includes(value)?value:'unavailable';};
 const refresh=()=>{if(stopped)return Promise.resolve(snapshot());if(pending)return visible;const started=performance.now(),work=policy();pending=work;let timer;
 visible=Promise.race([work.then(value=>({value,timeout:false})),new Promise(resolve=>{timer=setTimeout(()=>resolve({value:{checked:clock(),status:'unknown',captureAllowed:false,exportAllowed:false},timeout:true}),probeTimeoutMs);})]).then(({value,timeout})=>{last=value;lastReason=timeout?'monitor_timeout':value.status==='unknown'?'probe_failed':value.status;if(timeout){counts.monitor_timeout=diagnosticCount(counts.monitor_timeout);lastTimeout={phase:phase(),afterMs:elapsedSince(started)};}return value;}).finally(()=>clearTimeout(timer));
 void work.finally(()=>{if(pending===work)pending=null;});return visible;};
 return {refresh,policy:async()=>snapshot(),snapshot,captureAllowed:()=>snapshot().captureAllowed,status:()=>({...snapshot(),probePending:!!pending,binding:typeof actual.status==='function'?actual.status():{source:'trusted_adapter',configured:true},diagnostics:{reason:reason(),probeFailure,lastFailure:lastFailure?{...lastFailure}:null,counts:{...counts},lastDurationMs,maxDurationMs,lastTimeout:lastTimeout?{...lastTimeout}:null}}),close:async({timeoutMs=1000}={})=>{if(!Number.isInteger(timeoutMs)||timeoutMs<0||timeoutMs>60000)fail('正文存储关闭预算无效','recording_storage_configuration');stopped=true;if(!pending)return {pending:false};let timer;await Promise.race([pending,new Promise(resolve=>{timer=setTimeout(resolve,timeoutMs);})]);clearTimeout(timer);return {pending:!!pending};}};
}

export async function recordingCaptureDecision(enabled,policy=()=>true){
 if(!enabled)return {enabled:false,reason:'capture_disabled'};try{const value=await policy();if(value===true||value?.captureAllowed===true)return {enabled:true,reason:null};return {enabled:false,reason:value?.status==='low'?'storage_low':'storage_unknown'};}catch{return {enabled:false,reason:'storage_unknown'};}
}
// Each call carries its own server-side decision; concurrent captures never
// infer the cause from a shared last-rejection property.
export function recordingPolicyBuffer(buffer,monitor){
 const write=(method,args)=>{const snapshot=monitor.snapshot();if(!snapshot.captureAllowed)return Object.freeze({accepted:false,reason:snapshot.status==='low'?'storage_low':'storage_unknown'});const accepted=!!buffer[method](...args);return Object.freeze({accepted,reason:accepted?null:'queue_limit'});};
 return {directory:buffer.directory,startResult:(...args)=>write('start',args),appendResult:(...args)=>write('append',args),contextSnapshotResult:(...args)=>write('contextSnapshot',args),start:(...args)=>write('start',args).accepted,append:(...args)=>write('append',args).accepted,contextSnapshot:(...args)=>write('contextSnapshot',args).accepted,finish:(...args)=>buffer.finish(...args)};
}
export function recordingBufferWrite(buffer,method,...args){
 const result=typeof buffer[method+'Result']==='function'?buffer[method+'Result'](...args):{accepted:!!buffer[method](...args),reason:'queue_limit'},accepted=result?.accepted===true;
 return {accepted,reason:accepted?null:['storage_low','storage_unknown','queue_limit'].includes(result?.reason)?result.reason:'capture_failed'};
}
// The primary reason is readable by old gateways replaying encrypted spool.
// Only the new repository interprets this optional fixed server-generated cause.
export function recordingFinishReason(reason){return ['storage_low','storage_unknown'].includes(reason)?{reason:'capture_failed',storageReason:reason}:{reason};}

export function recordingStorageConfiguration(value={},env={}){return {...value,mode:value.mode??env.HARBOR_RECORDING_STORAGE_MODE??'local',reportKeyFile:value.reportKeyFile??env.HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE,pgDataDir:value.pgDataDir??env.HARBOR_RECORDING_PG_DATA_DIR,minFreeBytes:value.minFreeBytes??(env.HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES===undefined?undefined:Number(env.HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES)),minFreeRatio:value.minFreeRatio??(env.HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO===undefined?undefined:Number(env.HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO))};}

// Apply after pg URL normalization; a connection string cannot override this budget.
export function createRecordingProbeStore(database){const store=new PostgresStore({...database,poolSize:1,connectionTimeoutMs:1000,statementTimeoutMs:1000});store.pool.options.query_timeout=1000;return store;}
