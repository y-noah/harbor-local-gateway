import {createDepartmentScopeIssuer} from './department-access.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {randomUUID,createHash} from 'node:crypto';
import {statfs} from 'node:fs/promises';
import {AppError,usageWindow} from './store.mjs';
import {RecordingStore} from './recording-store.mjs';
import {RecordingContextStore} from './recording-context-store.mjs';
import {isRecordingRetentionDays,recordingRetentionDays,unlimitedRecordingExpiry,requireUnlimitedRecordingRetention} from './recording-retention.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';
import {createRecordingIndexCrypto,assembleRecording,recordingParts} from './recording-search-index.mjs';
import {createPostgresRecordingQueries,recordingQuery} from './postgres-recording-query.mjs';
import {createPostgresRecordingExports} from './postgres-recording-export.mjs';
import {createPostgresRecordingContextJobs} from './postgres-recording-context-jobs.mjs';
import {createPostgresRecordingContextQueries} from './postgres-recording-context-query.mjs';
import {createQueryDepartments,departmentPredicate} from './postgres-query-departments.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {verifyDepartmentIsolationAvailability} from './postgres-departments.mjs';

const fail=(status,message,code='recording_jobs_invalid')=>{throw new AppError(status,message,code);};
const stop=signal=>{if(signal?.aborted)fail(503,'正文后台任务已停止','worker_stopping');};
const idPattern=/^[a-zA-Z0-9._:-]{1,200}$/;
// Probe every configured storage mount (including the database mount when remote
// monitoring supplies it). Missing/failed probes do not claim healthy storage.
export function createRecordingStoragePolicy({paths=[],probe,clock=Date.now,cacheMs=1000,minFreeBytes=1073741824,minFreeRatio=.1}={}){
  if(!Array.isArray(paths)||paths.some(path=>typeof path!=='string'||!path)||!probe&&!paths.length||probe!==undefined&&typeof probe!=='function'||!Number.isInteger(cacheMs)||cacheMs<0||cacheMs>60000||!Number.isSafeInteger(minFreeBytes)||minFreeBytes<0||!Number.isFinite(minFreeRatio)||minFreeRatio<0||minFreeRatio>.5)fail(503,'正文存储保护配置无效','recording_storage_configuration');
  let cached=null,pending=null;
  return async()=>{if(cached&&clock()-cached.checked<cacheMs)return cached;if(pending)return pending;return pending=(async()=>{
    const checked=clock();let samples=[];try{samples=probe?await probe():await Promise.all(paths.map(async path=>{const value=await statfs(path,{bigint:true});return {total:Number(value.blocks*value.bsize),free:Number(value.bavail*value.bsize)};}));if(!Array.isArray(samples)||!samples.length||samples.some(value=>!Number.isSafeInteger(value.free)||!Number.isSafeInteger(value.total)||value.total<=0||value.free<0||value.free>value.total))throw Error();
      const healthy=samples.every(value=>value.free>=minFreeBytes&&value.free/value.total>=minFreeRatio);return cached={checked,status:healthy?'healthy':'low',captureAllowed:healthy,exportAllowed:healthy,min_free_bytes:Math.min(...samples.map(value=>value.free)),min_free_ratio:Math.min(...samples.map(value=>value.free/value.total))};
    }catch{return cached={checked,status:'unknown',captureAllowed:false,exportAllowed:false,min_free_bytes:null,min_free_ratio:null};}
  })().finally(()=>{pending=null;});};
}
export function createPostgresRecordingJobs(store,{departmentActors,jobs=createPostgresJobs(store,{departmentActors,scopedKinds:['worker.startup','recording.export','recording.retention','recording.index','recording.context.index','recording.purge']}),recordingKey,clock=Date.now,queryQueue,readStore=store,storagePolicy,storagePaths,storageProbe,devices=false,contextIndex=false,contextStorage=false,indexBatch=32,maxEvents=8192,maxEventBytes=4194304,maxDocumentBytes=2097152,partChars=2048,purgeBatch=32,exports:exportOptions={},queries:queryOptions={}}={}){
  if(typeof contextIndex!=='boolean'||typeof contextStorage!=='boolean')fail(503,'上下文索引能力配置无效','recording_jobs_configuration');
  if(typeof devices!=='boolean'||!Number.isInteger(indexBatch)||indexBatch<1||indexBatch>256||!Number.isInteger(maxEvents)||maxEvents<1||maxEvents>65536||!Number.isSafeInteger(maxEventBytes)||maxEventBytes<1024||maxEventBytes>16777216||!Number.isSafeInteger(maxDocumentBytes)||maxDocumentBytes<1024||maxDocumentBytes>8388608||!Number.isInteger(partChars)||partChars<256||partChars>8192||!Number.isInteger(purgeBatch)||purgeBatch<1||purgeBatch>256)fail(503,'正文后台资源配置无效','recording_jobs_configuration');
  const crypto=createRecordingIndexCrypto(recordingKey),recordings=new RecordingStore(store,{encryptionKey:recordingKey,clock}),policy=storagePolicy||createRecordingStoragePolicy({paths:storagePaths,probe:storageProbe,clock});
  const departments=createQueryDepartments(readStore),writeDepartments=createAdminDepartments(store),systemIssuer=createDepartmentScopeIssuer();
  async function systemScope(kind,departmentId=null){return (await jobs.verifyReadiness()).installed?systemIssuer.system({purpose:kind,departmentId}):undefined;}
  async function sourceScope(job){const scope=await jobScope(job);if(scope&&(scope.role!=='system'||scope.purpose!==job.kind||scope.view!=='department'))fail(403,'后台整理身份无效','department_forbidden');if(scope&&!await store.get('SELECT id FROM harbor.requests WHERE id=$1 AND department_id=$2',job.target,scope.department_id))fail(404,'请求正文不存在','recording_not_found');return scope;}
  async function purgeScope(job){const scope=await jobScope(job);if(scope&&(scope.role!=='system'||scope.purpose!=='recording.purge'||scope.view!=='global'))fail(403,'后台清理身份无效','department_forbidden');return scope;}
  async function maintenanceScope(scope,{write=true}={}){
    const trusted=await writeDepartments.resolve(scope,{write});
    if(trusted?.view==='global')fail(403,'请先选择要管理的部门','department_forbidden');return trusted;
  }
  async function jobScope(job){
    if(typeof jobs.authorizePersistedActor==='function')return jobs.authorizePersistedActor(job);
    if(await verifyDepartmentIsolationAvailability(store))fail(503,'后台身份尚未就绪','department_operation_unavailable');return null;
  }
  const retentionLock=authorization=>authorization?'harbor-recording-retention:'+authorization.department_id:'harbor-recording-retention';
  async function retentionSetting(authorization,reader=store){
    const row=authorization?await reader.get("SELECT value FROM harbor.department_settings WHERE department_id=$1 AND name='recording.retention'",authorization.department_id):await reader.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.retention'");
    if(authorization&&!row)fail(503,'部门正文保留期配置缺失','department_configuration');return row?(authorization?row.value:JSON.parse(row.value)):{days:30,version:0};
  }
  async function retentionItems(plan,days,at,authorization){
    const args=days===null?[plan,unlimitedRecordingExpiry,at]:[plan,days*86400000],predicate=departmentPredicate(authorization,'r',args);
    if(days===null)await store.run(`INSERT INTO harbor.recording_retention_items(plan_id,request_id,expected_expires,target_expires)
      SELECT $1,c.request_id,c.expires_at,$2 FROM harbor.recordings c JOIN harbor.requests r ON r.id=c.request_id
      WHERE c.expires_at>$3 AND c.expires_at<$2 AND r.recording_state<>'expired' AND ${predicate} ON CONFLICT DO NOTHING`,...args);
    else await store.run(`INSERT INTO harbor.recording_retention_items(plan_id,request_id,expected_expires,target_expires)
      SELECT $1,c.request_id,c.expires_at,r.started+$2 FROM harbor.recordings c JOIN harbor.requests r ON r.id=c.request_id
      WHERE c.expires_at>r.started+$2 AND ${predicate} ON CONFLICT DO NOTHING`,...args);
  }

  const exportContexts=contextIndex?new RecordingContextStore(store,{encryptionKey:recordingKey,clock}):undefined;
  const queries=createPostgresRecordingQueries(readStore,{...queryOptions,crypto,clock,queryQueue,devices}),exporter=createPostgresRecordingExports(store,{...exportOptions,jobs,crypto,recordings,contexts:exportContexts,clock,storagePolicy:policy,queryQueue,devices});
  const contextQueries=contextIndex?createPostgresRecordingContextQueries(readStore,{...queryOptions,crypto,clock,queryQueue,devices,scopeDigestForSource:source=>exportContexts.scopeDigestForSource(source)}):null;
  const contextJobs=contextIndex?createPostgresRecordingContextJobs(store,{jobs,recordingKey,crypto,clock,indexBatch,maxDocumentBytes,partChars}):null;
  const storedContexts=!contextJobs&&contextStorage?new RecordingContextStore(store,{encryptionKey:recordingKey,clock}):null;
  const contextGc=contextJobs?.gc??(storedContexts?options=>storedContexts.gc(options):null);
  async function schedule(){
    const at=await jobs.now();
    // Rebuilding a changed key is explicit in status; only derived encrypted
    // documents/indexes change, never source events or metering facts.
    await store.run(`UPDATE harbor.recording_work w SET revision=w.revision+1,changed=$1,last_error=NULL WHERE w.request_id IN (
      SELECT d.request_id FROM harbor.recording_documents d JOIN harbor.recording_work x ON x.request_id=d.request_id WHERE d.key_version<>$2 AND x.revision=x.indexed_revision ORDER BY d.request_id LIMIT $3)`,at,crypto.keyVersion,indexBatch);
    const scheduled=await refillBody();
    await jobs.enqueue('recording.purge',{id:'recording.purge:'+Math.floor(at/60000),payload:{},scope:await systemScope('recording.purge')});
    if(contextJobs)await contextJobs.schedule();
    return {scheduled};
  }
  async function refillBody(){
    const scoped=(await jobs.verifyReadiness()).installed,rows=await store.all('SELECT w.request_id,w.revision'+(scoped?',r.department_id':'')+' FROM harbor.recording_work w JOIN harbor.requests r ON r.id=w.request_id'+(scoped?' JOIN harbor.departments dept ON dept.id=r.department_id AND dept.enabled':'')+' WHERE w.revision>w.indexed_revision AND w.last_error IS NULL ORDER BY w.changed,w.request_id LIMIT $1',indexBatch);
    for(const row of rows){const id='recording.index:'+createHash('sha256').update(row.request_id+':'+row.revision).digest('hex');let result;try{result=await jobs.enqueue('recording.index',{id,target:row.request_id,payload:{request_id:row.request_id,revision:row.revision},scope:scoped?systemIssuer.system({purpose:'recording.index',departmentId:row.department_id}):undefined});}catch(error){if(scoped&&error.code==='department_forbidden'&&!await store.get('SELECT id FROM harbor.departments WHERE id=$1 AND enabled',row.department_id))continue;throw error;}if(!result.created){const task=await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',id);if(['failed','cancelled'].includes(task?.status))await store.run("UPDATE harbor.recording_work SET last_error='index_attempts_exhausted' WHERE request_id=$1 AND revision=$2 AND revision>indexed_revision",row.request_id,row.revision);}}
    return rows.length;
  }
  async function refill(){
    const scheduled=await refillBody();if(contextJobs)await contextJobs.refill();return {scheduled};
  }
  async function organize(job,{signal}={}){
    if(job.kind!=='recording.index'||!idPattern.test(job.target??'')||job.target!==job.payload?.request_id||!Number.isSafeInteger(job.payload.revision)||job.payload.revision<1||Object.keys(job.payload).length!==2||job.id!=='recording.index:'+createHash('sha256').update(job.target+':'+job.payload.revision).digest('hex'))fail(400,'正文整理任务参数无效');
    stop(signal);await sourceScope(job);const source=await store.transaction(async()=>{
      await sourceScope(job);
      const row=await store.get('SELECT w.revision,w.indexed_revision,c.persisted_sequence,c.expires_at,c.turn_state,r.recording_state FROM harbor.recording_work w JOIN harbor.recordings c ON c.request_id=w.request_id JOIN harbor.requests r ON r.id=w.request_id WHERE w.request_id=$1',job.target);
      if(!row||row.revision!==job.payload.revision||row.indexed_revision>=row.revision)return null;
      const sizes=await store.all('SELECT sequence,octet_length(content_cipher) bytes FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence LIMIT $2',job.target,maxEvents+1);
      if(sizes.length>maxEvents||sizes.reduce((total,value)=>total+value.bytes,0)>maxEventBytes)fail(503,'正文整理源事件超过容量','recording_index_capacity');
      const events=await store.all('SELECT sequence,source_key,kind,provenance,created,content_cipher FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence LIMIT $2',job.target,maxEvents);return {row,events};
    },{level:'REPEATABLE READ',readOnly:true});
    if(!source)return {request_id:job.target,skipped:true};stop(signal);
    if(source.row.expires_at<=clock())return {request_id:job.target,expired:true};
    // Decrypt/assemble/tokenize outside a database transaction. Bounds are the
    // repository's recorded bytes, not the original model request size.
    const events=source.events.map(event=>({...event,content:recordings.open(event.content_cipher,job.target+':'+event.sequence+':'+event.source_key)})),items=assembleRecording(events,{maxBytes:maxDocumentBytes}),parts=recordingParts(items,{partChars}),prepared=[];let bytes=0;
    const indexTerms=crypto.createTermsScope();
    for(const [index,part] of parts.entries()){stop(signal);const payload=JSON.stringify(part),size=Buffer.byteLength(payload);bytes+=size;prepared.push({ordinal:index+1,kind:part.kind,field:part.field,bytes:size,cipher:crypto.seal(part,`document:${job.target}:${source.row.revision}:${index+1}`),terms:indexTerms(part.searchText)});if(index%16===15)await new Promise(setImmediate);}
    return jobs.commitEffect(job,async()=>{
      await sourceScope(job);
      const recording=await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1 FOR SHARE',job.target);if(!recording||recording.expires_at<=clock())return {request_id:job.target,expired:true};
      const current=await store.get('SELECT revision,indexed_revision FROM harbor.recording_work WHERE request_id=$1 FOR UPDATE',job.target);if(!current||current.revision!==source.row.revision||current.indexed_revision>=current.revision)return {request_id:job.target,stale:true};stop(signal);
      await store.run('DELETE FROM harbor.recording_documents WHERE request_id=$1',job.target);
      await store.run('INSERT INTO harbor.recording_documents VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',job.target,source.row.revision,source.row.persisted_sequence,clock(),source.row.recording_state,source.row.turn_state,source.row.expires_at,prepared.length,bytes,crypto.keyVersion);
      for(let offset=0;offset<prepared.length;offset+=indexBatch){
        stop(signal);
        const batch=prepared.slice(offset,offset+indexBatch);
        await store.run(`INSERT INTO harbor.recording_document_parts(request_id,ordinal,kind,field_id,payload_cipher,bytes,search_terms)
          SELECT $1,v.ordinal,v.kind,v.field,v.cipher,v.bytes,ARRAY(SELECT decode(term,'hex') FROM jsonb_array_elements_text(v.terms) AS terms(term))
          FROM jsonb_to_recordset($2::jsonb) AS v(ordinal integer,kind text,field text,cipher bytea,bytes integer,terms jsonb)`,job.target,JSON.stringify(batch.map(part=>({ordinal:part.ordinal,kind:part.kind,field:part.field,cipher:'\\x'+part.cipher.toString('hex'),bytes:part.bytes,terms:part.terms.map(term=>term.toString('hex'))}))));
      }
      stop(signal);
      await store.run('UPDATE harbor.recording_work SET indexed_revision=$1,indexed_at=$2,last_error=NULL WHERE request_id=$3',source.row.revision,clock(),job.target);return {request_id:job.target,parts:prepared.length,source_sequence:source.row.persisted_sequence};
    });
  }
  async function purge(job,{signal}={}){
    if(job.kind!=='recording.purge'||Object.keys(job.payload||{}).length||!/^recording\.purge:\d+$/.test(job.id))fail(400,'正文清理任务参数无效');let requests=0,exports=0;
    for(let batch=0;batch<128;batch++){stop(signal);const count=await jobs.owned(job,async()=>{
      await purgeScope(job);
      const rows=await store.all('SELECT request_id FROM harbor.recordings WHERE request_id IN (SELECT request_id FROM harbor.recordings WHERE expires_at<=$1 ORDER BY expires_at,request_id LIMIT $2) ORDER BY request_id FOR UPDATE SKIP LOCKED',clock(),purgeBatch);if(!rows.length)return 0;
      await store.all('SELECT id FROM harbor.recording_exports WHERE id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=ANY($1::text[])) ORDER BY id FOR UPDATE',rows.map(row=>row.request_id));
      for(const row of rows){await store.run("UPDATE harbor.requests SET recording_state='expired',recording_reason=NULL WHERE id=$1",row.request_id);await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',row.request_id);await store.run('DELETE FROM harbor.recording_work WHERE request_id=$1',row.request_id);}return rows.length;
    });requests+=count;if(count<purgeBatch)break;}
    for(let batch=0;batch<128;batch++){stop(signal);const count=await jobs.owned(job,async()=>{
      await purgeScope(job);
      const rows=await store.all("SELECT id FROM harbor.recording_exports WHERE expires<=$1 AND status<>'expired' ORDER BY expires,id LIMIT $2",clock(),purgeBatch);for(const row of rows){await jobs.cancel(row.id);const current=await store.get('SELECT expires FROM harbor.recording_exports WHERE id=$1 FOR UPDATE',row.id);if(!current||current.expires>clock())continue;await store.run('DELETE FROM harbor.recording_export_chunks WHERE export_id=$1',row.id);await store.run("UPDATE harbor.recording_exports SET status='expired',error_code='retention_expired' WHERE id=$1",row.id);}return rows.length;
    });exports+=count;if(count<purgeBatch)break;}
    // Expired previews have no effect. Completed plan summaries remain, while
    // their potentially large frozen ID sets are reclaimed in bounded pages.
    await jobs.owned(job,async()=>{
      await purgeScope(job);
      await store.run("UPDATE harbor.recording_retention_plans SET status='cancelled' WHERE status='preview' AND expires<=$1",clock());
      await store.run("DELETE FROM harbor.recording_retention_items WHERE (plan_id,request_id) IN (SELECT i.plan_id,i.request_id FROM harbor.recording_retention_items i JOIN harbor.recording_retention_plans p ON p.id=i.plan_id WHERE p.status IN ('complete','cancelled') AND p.expires<=$1 ORDER BY i.plan_id,i.request_id LIMIT $2)",clock(),purgeBatch*8);
    });
    if(contextGc)for(let batch=0;batch<purgeBatch;batch++){stop(signal);const result=await jobs.owned(job,async()=>{await purgeScope(job);return contextGc({limit:purgeBatch});});if(result.cycleComplete)break;}
    return {expired_recordings:requests,expired_exports:exports};
  }
  async function status({scope}={}){const authorization=await departments.resolve(scope),args=[],predicate=departmentPredicate(authorization,'r',args),row=await readStore.get('SELECT COUNT(*) pending,COUNT(*) FILTER(WHERE w.last_error IS NOT NULL) failed,MIN(w.changed) oldest FROM harbor.recording_work w JOIN harbor.requests r ON r.id=w.request_id WHERE w.revision>w.indexed_revision AND '+predicate,...args),disk=await policy();return {index_pending:row.pending,index_failed:row.failed,oldest_pending_at:row.oldest,...(contextJobs?await contextJobs.status({scope}):{}),storage:authorization?.view==='department'?{status:disk.status,captureAllowed:disk.captureAllowed,exportAllowed:disk.exportAllowed}:disk,...(authorization?.view==='department'?{}:{key_version:crypto.keyVersion}),queryable_days:365,default_query_window:'seven_beijing_calendar_days'};}
  async function indexHandler(job,context){try{return await organize(job,context);}catch(error){if(['recording_index_capacity','recording_unreadable','recording_index_unreadable'].includes(error.code))await jobs.owned(job,()=>store.run('UPDATE harbor.recording_work SET last_error=$1 WHERE request_id=$2 AND revision=$3',error.code,job.target,job.payload.revision));throw error;}}
  async function rebuild(payload,{scope}={}){if(!payload||Object.keys(payload).some(key=>key!=='request_id')||typeof payload.request_id!=='string'||!idPattern.test(payload.request_id))fail(400,'请选择需要重新整理的请求');const authorization=await maintenanceScope(scope);return store.transaction(async()=>{if(authorization&&!await store.get('SELECT id FROM harbor.requests WHERE id=$1 AND department_id=$2',payload.request_id,authorization.department_id))fail(404,'请求正文不存在');const result=await store.run('UPDATE harbor.recording_work SET revision=revision+1,last_error=NULL,changed=$1 WHERE request_id=$2',clock(),payload.request_id);if(!result.changes)fail(404,'请求正文不存在');if(contextJobs)await contextJobs.rebuild(payload.request_id,{scope});return {request_id:payload.request_id,status:'pending'};});}
  async function retentionPreview(days,{scope}={}){
    const authorization=await maintenanceScope(scope);
    if(!isRecordingRetentionDays(days))fail(400,'正文保留期须为1至365天或无限期');
    if(days===null)await requireUnlimitedRecordingRetention(store);
    return store.transaction(async()=>{
      if(authorization)await writeDepartments.current(authorization,{lock:true});
      const at=clock(),plan=randomUUID();await store.run('INSERT INTO harbor.recording_retention_plans(id,created,expires,days,requests,bytes'+(authorization?',department_id':'')+') VALUES($1,$2,$3,$4,0,0'+(authorization?',$5':'')+')',plan,at,at+300000,days??0,...(authorization?[authorization.department_id]:[]));
      await retentionItems(plan,days,at,authorization);
      const totals=await store.get('SELECT COUNT(*) requests,COALESCE(SUM(c.bytes),0)::text bytes FROM harbor.recording_retention_items i JOIN harbor.recordings c ON c.request_id=i.request_id WHERE i.plan_id=$1',plan);
      await store.run('UPDATE harbor.recording_retention_plans SET requests=$1,bytes=$2 WHERE id=$3',totals.requests,totals.bytes,plan);
      const current=await retentionSetting(authorization);
      return {id:plan,days,previous_days:current.days===0?null:current.days,expected_version:current.version??0,at,expires:at+300000,affected_requests:totals.requests,affected_bytes:totals.bytes,requires_confirmation:true};
    },{level:'REPEATABLE READ'});
  }
  async function retentionApply(payload,{scope}={}){
    const authorization=await maintenanceScope(scope);
    if(!payload||Object.keys(payload).some(key=>!['id','expected_version','confirm'].includes(key))||typeof payload.id!=='string'||!/^[a-f0-9-]{36}$/.test(payload.id)||payload.confirm!==true||!Number.isSafeInteger(payload.expected_version)||payload.expected_version<0)fail(400,'须明确确认保留期影响预览');
    return store.transaction(async()=>{
      if(authorization)await writeDepartments.current(authorization,{lock:true});
      await store.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[retentionLock(authorization)]);
      const plan=await store.get('SELECT * FROM harbor.recording_retention_plans WHERE id=$1'+(authorization?' AND department_id=$2':'')+' FOR UPDATE',payload.id,...(authorization?[authorization.department_id]:[]));if(!plan)fail(404,'保留期预览不存在');if(plan.status!=='preview')return {id:plan.id,status:plan.status,job_id:plan.job_id};if(plan.expires<=clock())fail(410,'保留期预览已失效');
      const current=await retentionSetting(authorization);if((current.version??0)!==payload.expected_version)fail(409,'保留期配置已变化，请重新预览','recording_retention_conflict');
      const days=recordingRetentionDays(plan);
      if(days===null){
        // Confirmation fences legacy capture transactions too. Include their
        // newly committed live records so none fall between preview and save.
        await retentionItems(plan.id,days,clock(),authorization);
        const totals=await store.get('SELECT COUNT(*) requests,COALESCE(SUM(c.bytes),0)::text bytes FROM harbor.recording_retention_items i LEFT JOIN harbor.recordings c ON c.request_id=i.request_id WHERE i.plan_id=$1',plan.id);
        await store.run('UPDATE harbor.recording_retention_plans SET requests=$1,bytes=$2 WHERE id=$3',totals.requests,totals.bytes,plan.id);
      }
      const job=await jobs.enqueue('recording.retention',{id:'recording.retention:'+plan.id,target:plan.id,payload:{plan_id:plan.id},scope:authorization??undefined});
      const setting=JSON.stringify({days:days??0,...(days===null?{mode:'unlimited'}:{}),plan_id:plan.id,version:payload.expected_version+1,updated:clock()});
      if(authorization)await store.run("UPDATE harbor.department_settings SET value=$1::jsonb,version=$2 WHERE department_id=$3 AND name='recording.retention'",setting,payload.expected_version+1,authorization.department_id);
      else await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.retention',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",setting);
      await store.run("UPDATE harbor.recording_retention_plans SET status='queued',job_id=$1 WHERE id=$2",job.id,plan.id);
      await createPostgresAudit(store,{clock}).write(authorization,{action:'recording.retention.configure',detail:{days,affected:plan.requests,actor:authorization?.role??'admin',plan:plan.id}});
      return {id:plan.id,status:'queued',job_id:job.id,days,version:payload.expected_version+1,physical_cleanup:'worker',new_capture_retention_requires_reload:false};
    });
  }
  async function retentionExecute(job,{signal}={}){
    if(job.kind!=='recording.retention'||job.target!==job.payload?.plan_id||job.id!=='recording.retention:'+job.target||Object.keys(job.payload).length!==1)fail(400,'保留期任务参数无效');
    for(;;){stop(signal);const count=await jobs.owned(job,async()=>{
      const authorization=await jobScope(job);if(authorization?.view==='global')fail(403,'保留期任务缺少部门','department_forbidden');
      if(!await store.get('SELECT id FROM harbor.recording_retention_plans WHERE id=$1'+(authorization?' AND department_id=$2':''),job.target,...(authorization?[authorization.department_id]:[])))fail(404,'保留期计划不存在');
      await store.query('SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))',[retentionLock(authorization)]);
      const setting=await retentionSetting(authorization);
      if(setting.plan_id&&setting.plan_id!==job.target){await store.run("UPDATE harbor.recording_retention_items SET status='changed' WHERE plan_id=$1 AND status='pending'",job.target);return 0;}
      const rows=await store.all("SELECT i.request_id,i.expected_expires,i.target_expires"+(authorization?",r.department_id":"")+" FROM harbor.recording_retention_items i LEFT JOIN harbor.requests r ON r.id=i.request_id WHERE i.plan_id=$1 AND i.status='pending' ORDER BY i.request_id LIMIT $2",job.target,purgeBatch);if(!rows.length)return 0;
      if(authorization&&rows.some(row=>row.department_id!==null&&row.department_id!==authorization.department_id))fail(403,'保留期来源不属于当前部门','department_forbidden');
      const sources=await store.all('SELECT request_id,expires_at FROM harbor.recordings WHERE request_id=ANY($1::text[]) ORDER BY request_id FOR UPDATE',rows.map(row=>row.request_id)),byId=new Map(sources.map(row=>[row.request_id,row]));
      await store.all('SELECT id FROM harbor.recording_exports WHERE id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=ANY($1::text[])) ORDER BY id FOR UPDATE',rows.map(row=>row.request_id));
      for(const item of rows){
        // Follow the capture repository's recording -> request order. Changes
        // finite changes after preview are skipped; unlimited rechecks liveness.
        const current=byId.get(item.request_id);let state='missing';
        if(current){state=item.target_expires===unlimitedRecordingExpiry||current.expires_at===item.expected_expires?'applied':'changed';if(state==='applied'){
          // Unlimited never revives content that expired while its plan waited.
          if(item.target_expires===unlimitedRecordingExpiry&&current.expires_at<=clock())state='changed';
          else{
            await store.run('UPDATE harbor.recordings SET expires_at=$1,updated=$2 WHERE request_id=$3',item.target_expires,clock(),item.request_id);
            if(contextJobs||contextGc)await store.run('UPDATE harbor.recording_context_snapshots SET expires_at=$1 WHERE request_id=$2 AND expires_at>$3',item.target_expires,item.request_id,clock());
          }
        }}
        await store.run('UPDATE harbor.recording_retention_items SET status=$1 WHERE plan_id=$2 AND request_id=$3',state,job.target,item.request_id);
      }return rows.length;
    });if(count<purgeBatch)break;}
    return jobs.commitEffect(job,async()=>{const authorization=await jobScope(job);if(!await store.get('SELECT id FROM harbor.recording_retention_plans WHERE id=$1'+(authorization?' AND department_id=$2':''),job.target,...(authorization?[authorization.department_id]:[])))fail(404,'保留期计划不存在');const rows=await store.all('SELECT status,COUNT(*) count FROM harbor.recording_retention_items WHERE plan_id=$1 GROUP BY status',job.target),counts=Object.fromEntries(rows.map(row=>[row.status,row.count]));await store.run("UPDATE harbor.recording_retention_plans SET status='complete',completed=$1,summary=$3::jsonb WHERE id=$2",clock(),job.target,JSON.stringify(counts));return {id:job.target,counts};});
  }
  async function retentionStatus(planId,{scope}={}){
    const authorization=await maintenanceScope(scope,{write:false});
    if(typeof planId!=='string'||!/^[a-f0-9-]{36}$/.test(planId))fail(404,'保留期计划不存在');const at=clock(),window=usageWindow(at),plan=await readStore.get('SELECT p.id,p.created,p.expires,p.days,p.requests,p.bytes,p.status,p.job_id,p.completed,p.summary,j.status job_status,j.error_code FROM harbor.recording_retention_plans p LEFT JOIN harbor.background_jobs j ON j.id=p.job_id WHERE p.id=$1 AND p.created>=$2 AND p.created<$3'+(authorization?' AND p.department_id=$4':''),planId,window.start,window.end,...(authorization?[authorization.department_id]:[]));if(!plan)fail(404,'保留期计划不存在或超出近7天范围');const rows=await readStore.all('SELECT status,COUNT(*) count FROM harbor.recording_retention_items WHERE plan_id=$1 GROUP BY status',planId);return {...plan,days:recordingRetentionDays(plan),summary:undefined,counts:plan.status==='complete'?plan.summary:Object.fromEntries(rows.map(row=>[row.status,row.count]))};
  }
  async function handle(path,method,payload,params=new URLSearchParams(),{signal,scope}={}){
    if(path!=='/api/admin/recordings'&&!path.startsWith('/api/admin/recordings/'))return null;
    if(signal?.aborted)throw signal.reason;
    const authorization=await departments.resolve(scope),departmentRead=method==='GET'&&(['/api/admin/recordings/context/search','/api/admin/recordings/status','/api/admin/recordings/search','/api/admin/recordings/session'].includes(path)||/^\/api\/admin\/recordings\/requests\/[a-zA-Z0-9._:-]{1,200}$/.test(path));
    const scopedRebuild=path==='/api/admin/recordings/rebuild'&&jobs.supportsScopedKind?.('recording.index')===true&&(!contextJobs||jobs.supportsScopedKind?.('recording.context.index')===true);
    const exportRoute=path==='/api/admin/recordings/exports'||/^\/api\/admin\/recordings\/exports\/recording\.export:[a-f0-9-]{36}$/.test(path),scopedExport=exportRoute&&jobs.supportsScopedKind?.('recording.export')===true,scopedRetention=path.startsWith('/api/admin/recordings/retention')&&jobs.supportsScopedKind?.('recording.retention')===true;
    if(authorization!==null&&method!=='GET'&&!scopedExport&&!scopedRetention&&!scopedRebuild)fail(403,'此操作的部门后台权限链尚未开放','department_operation_unavailable');
    if(authorization?.view==='department'&&!departmentRead&&!scopedExport&&!scopedRetention&&!scopedRebuild)fail(403,'此部门操作尚未开放','department_operation_unavailable');
    if(path==='/api/admin/recordings/context/search'&&method==='GET'&&contextQueries){const value=await contextQueries.search(recordingQuery(params,{clock,search:true,devices}),{signal,scope}),backlog=await contextJobs.status({scope});return {status:200,value:{...value,index_backlog:backlog.context_index_pending,oldest_pending_at:backlog.context_oldest_pending_at,complete:!value.has_more&&backlog.context_index_pending===0}};}
    if(path==='/api/admin/recordings/status'&&method==='GET')return {status:200,value:await status({scope})};
    if(path==='/api/admin/recordings/rebuild'&&method==='POST')return {status:202,value:await rebuild(payload,{scope})};
    if(path==='/api/admin/recordings/search'&&method==='GET')return {status:200,value:await queries.search(recordingQuery(params,{clock,search:true,devices}),{signal,scope})};
    if(path==='/api/admin/recordings/session'&&method==='GET')return {status:200,value:await queries.session(recordingQuery(params,{clock,devices}),{signal,scope})};
    const document=path.match(/^\/api\/admin\/recordings\/requests\/([a-zA-Z0-9._:-]{1,200})$/);if(document&&method==='GET'){if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1))fail(400,'正文分页参数无效');return {status:200,value:await queries.document(document[1],{after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)},{signal,scope})};}
    if(path==='/api/admin/recordings/exports'&&method==='GET')return {status:200,value:await exporter.list(params,{signal,scope})};
    if(path==='/api/admin/recordings/exports'&&method==='POST')return {status:202,value:await exporter.enqueue(payload,{scope})};
    const exportPath=path.match(/^\/api\/admin\/recordings\/exports\/(recording\.export:[a-f0-9-]{36})$/);if(exportPath&&method==='GET')return {status:200,value:await exporter.status(exportPath[1],{signal,scope})};if(exportPath&&method==='DELETE')return {status:200,value:await exporter.cancel(exportPath[1],{scope})};
    if(path==='/api/admin/recordings/retention/preview'&&method==='POST')return {status:200,value:await retentionPreview(payload?.days,{scope})};
    if(path==='/api/admin/recordings/retention'&&method==='GET'){
      const value=await retentionSetting(await maintenanceScope(scope,{write:false}),readStore);
      return {status:200,value:{days:value.days===0?null:value.days,version:value.version??0}};
    }
    if(path==='/api/admin/recordings/retention'&&method==='PATCH')return {status:202,value:await retentionApply(payload,{scope})};
    const retention=path.match(/^\/api\/admin\/recordings\/retention\/([a-f0-9-]{36})$/);if(retention&&method==='GET')return {status:200,value:await retentionStatus(retention[1],{scope})};
    return null;
  }
  return {schedule,refill,handlers:{'recording.index':indexHandler,'recording.purge':purge,'recording.retention':retentionExecute,...exporter.handlers,...contextJobs?.handlers},handle,queries,contextQueries,exports:exporter,download:exporter.download,capturePolicy:policy,status,retentionPreview,retentionApply,retentionStatus,rebuild};
}
