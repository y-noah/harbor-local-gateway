import {randomUUID,createHash} from 'node:crypto';
import {statfs} from 'node:fs/promises';
import {AppError,usageWindow} from './store.mjs';
import {RecordingStore} from './recording-store.mjs';
import {RecordingContextStore} from './recording-context-store.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';
import {createRecordingIndexCrypto,assembleRecording,recordingParts} from './recording-search-index.mjs';
import {createPostgresRecordingQueries,recordingQuery} from './postgres-recording-query.mjs';
import {createPostgresRecordingExports} from './postgres-recording-export.mjs';
import {createPostgresRecordingContextJobs} from './postgres-recording-context-jobs.mjs';
import {createPostgresRecordingContextQueries} from './postgres-recording-context-query.mjs';

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
export function createPostgresRecordingJobs(store,{jobs=createPostgresJobs(store),recordingKey,clock=Date.now,queryQueue,readStore=store,storagePolicy,storagePaths,storageProbe,devices=false,contextIndex=false,contextStorage=false,indexBatch=32,maxEvents=8192,maxEventBytes=4194304,maxDocumentBytes=2097152,partChars=2048,purgeBatch=32,exports:exportOptions={},queries:queryOptions={}}={}){
  if(typeof contextIndex!=='boolean'||typeof contextStorage!=='boolean')fail(503,'上下文索引能力配置无效','recording_jobs_configuration');
  if(typeof devices!=='boolean'||!Number.isInteger(indexBatch)||indexBatch<1||indexBatch>256||!Number.isInteger(maxEvents)||maxEvents<1||maxEvents>65536||!Number.isSafeInteger(maxEventBytes)||maxEventBytes<1024||maxEventBytes>16777216||!Number.isSafeInteger(maxDocumentBytes)||maxDocumentBytes<1024||maxDocumentBytes>8388608||!Number.isInteger(partChars)||partChars<256||partChars>8192||!Number.isInteger(purgeBatch)||purgeBatch<1||purgeBatch>256)fail(503,'正文后台资源配置无效','recording_jobs_configuration');
  const crypto=createRecordingIndexCrypto(recordingKey),recordings=new RecordingStore(store,{encryptionKey:recordingKey,clock}),policy=storagePolicy||createRecordingStoragePolicy({paths:storagePaths,probe:storageProbe,clock});
  const exportContexts=contextIndex?new RecordingContextStore(store,{encryptionKey:recordingKey,clock}):undefined;
  const queries=createPostgresRecordingQueries(readStore,{...queryOptions,crypto,clock,queryQueue,devices}),exporter=createPostgresRecordingExports(store,{...exportOptions,jobs,crypto,recordings,contexts:exportContexts,clock,storagePolicy:policy,queryQueue,devices});
  const contextQueries=contextIndex?createPostgresRecordingContextQueries(readStore,{...queryOptions,crypto,clock,queryQueue,devices}):null;
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
    await jobs.enqueue('recording.purge',{id:'recording.purge:'+Math.floor(at/60000),payload:{}});
    if(contextJobs)await contextJobs.schedule();
    return {scheduled};
  }
  async function refillBody(){
    const rows=await store.all('SELECT request_id,revision FROM harbor.recording_work WHERE revision>indexed_revision AND last_error IS NULL ORDER BY changed,request_id LIMIT $1',indexBatch);
    for(const row of rows){const id='recording.index:'+createHash('sha256').update(row.request_id+':'+row.revision).digest('hex'),result=await jobs.enqueue('recording.index',{id,target:row.request_id,payload:{request_id:row.request_id,revision:row.revision}});if(!result.created){const task=await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',id);if(['failed','cancelled'].includes(task?.status))await store.run("UPDATE harbor.recording_work SET last_error='index_attempts_exhausted' WHERE request_id=$1 AND revision=$2 AND revision>indexed_revision",row.request_id,row.revision);}}
    return rows.length;
  }
  async function refill(){
    const scheduled=await refillBody();if(contextJobs)await contextJobs.refill();return {scheduled};
  }
  async function organize(job,{signal}={}){
    if(job.kind!=='recording.index'||!idPattern.test(job.target??'')||job.target!==job.payload?.request_id||!Number.isSafeInteger(job.payload.revision)||job.payload.revision<1||Object.keys(job.payload).length!==2||job.id!=='recording.index:'+createHash('sha256').update(job.target+':'+job.payload.revision).digest('hex'))fail(400,'正文整理任务参数无效');
    stop(signal);const source=await store.transaction(async()=>{
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
      const rows=await store.all('SELECT request_id FROM harbor.recordings WHERE request_id IN (SELECT request_id FROM harbor.recordings WHERE expires_at<=$1 ORDER BY expires_at,request_id LIMIT $2) ORDER BY request_id FOR UPDATE SKIP LOCKED',clock(),purgeBatch);if(!rows.length)return 0;
      await store.all('SELECT id FROM harbor.recording_exports WHERE id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=ANY($1::text[])) ORDER BY id FOR UPDATE',rows.map(row=>row.request_id));
      for(const row of rows){await store.run("UPDATE harbor.requests SET recording_state='expired',recording_reason=NULL WHERE id=$1",row.request_id);await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',row.request_id);await store.run('DELETE FROM harbor.recording_work WHERE request_id=$1',row.request_id);}return rows.length;
    });requests+=count;if(count<purgeBatch)break;}
    for(let batch=0;batch<128;batch++){stop(signal);const count=await jobs.owned(job,async()=>{
      const rows=await store.all("SELECT id FROM harbor.recording_exports WHERE expires<=$1 AND status<>'expired' ORDER BY expires,id LIMIT $2",clock(),purgeBatch);for(const row of rows){await jobs.cancel(row.id);const current=await store.get('SELECT expires FROM harbor.recording_exports WHERE id=$1 FOR UPDATE',row.id);if(!current||current.expires>clock())continue;await store.run('DELETE FROM harbor.recording_export_chunks WHERE export_id=$1',row.id);await store.run("UPDATE harbor.recording_exports SET status='expired',error_code='retention_expired' WHERE id=$1",row.id);}return rows.length;
    });exports+=count;if(count<purgeBatch)break;}
    // Expired previews have no effect. Completed plan summaries remain, while
    // their potentially large frozen ID sets are reclaimed in bounded pages.
    await jobs.owned(job,async()=>{
      await store.run("UPDATE harbor.recording_retention_plans SET status='cancelled' WHERE status='preview' AND expires<=$1",clock());
      await store.run("DELETE FROM harbor.recording_retention_items WHERE (plan_id,request_id) IN (SELECT i.plan_id,i.request_id FROM harbor.recording_retention_items i JOIN harbor.recording_retention_plans p ON p.id=i.plan_id WHERE p.status IN ('complete','cancelled') AND p.expires<=$1 ORDER BY i.plan_id,i.request_id LIMIT $2)",clock(),purgeBatch*8);
    });
    if(contextGc)for(let batch=0;batch<purgeBatch;batch++){stop(signal);const result=await jobs.owned(job,()=>contextGc({limit:purgeBatch}));if(result.cycleComplete)break;}
    return {expired_recordings:requests,expired_exports:exports};
  }
  async function status(){const row=await readStore.get('SELECT COUNT(*) pending,COUNT(*) FILTER(WHERE last_error IS NOT NULL) failed,MIN(changed) oldest FROM harbor.recording_work WHERE revision>indexed_revision'),disk=await policy();return {index_pending:row.pending,index_failed:row.failed,oldest_pending_at:row.oldest,...(contextJobs?await contextJobs.status():{}),storage:disk,key_version:crypto.keyVersion,queryable_days:365,default_query_window:'seven_beijing_calendar_days'};}
  async function indexHandler(job,context){try{return await organize(job,context);}catch(error){if(['recording_index_capacity','recording_unreadable','recording_index_unreadable'].includes(error.code))await jobs.owned(job,()=>store.run('UPDATE harbor.recording_work SET last_error=$1 WHERE request_id=$2 AND revision=$3',error.code,job.target,job.payload.revision));throw error;}}
  async function rebuild(payload){if(!payload||Object.keys(payload).some(key=>key!=='request_id')||typeof payload.request_id!=='string'||!idPattern.test(payload.request_id))fail(400,'请选择需要重新整理的请求');return store.transaction(async()=>{const result=await store.run('UPDATE harbor.recording_work SET revision=revision+1,last_error=NULL,changed=$1 WHERE request_id=$2',clock(),payload.request_id);if(!result.changes)fail(404,'请求正文不存在');if(contextJobs)await contextJobs.rebuild(payload.request_id);return {request_id:payload.request_id,status:'pending'};});}
  async function retentionPreview(days){
    if(!Number.isInteger(days)||days<1||days>365)fail(400,'正文保留期须为1至365天');
    return store.transaction(async()=>{
      const at=clock(),plan=randomUUID();await store.run('INSERT INTO harbor.recording_retention_plans(id,created,expires,days,requests,bytes) VALUES($1,$2,$3,$4,0,0)',plan,at,at+300000,days);
      await store.run(`INSERT INTO harbor.recording_retention_items(plan_id,request_id,expected_expires,target_expires)
        SELECT $1,c.request_id,c.expires_at,r.started+$2 FROM harbor.recordings c JOIN harbor.requests r ON r.id=c.request_id WHERE c.expires_at>r.started+$2`,plan,days*86400000);
      const totals=await store.get('SELECT COUNT(*) requests,COALESCE(SUM(c.bytes),0)::text bytes FROM harbor.recording_retention_items i JOIN harbor.recordings c ON c.request_id=i.request_id WHERE i.plan_id=$1',plan);
      await store.run('UPDATE harbor.recording_retention_plans SET requests=$1,bytes=$2 WHERE id=$3',totals.requests,totals.bytes,plan);
      const settings=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.retention'");const current=settings?JSON.parse(settings.value):{days:30,version:0};
      return {id:plan,days,previous_days:current.days,expected_version:current.version??0,at,expires:at+300000,affected_requests:totals.requests,affected_bytes:totals.bytes,requires_confirmation:true};
    },{level:'REPEATABLE READ'});
  }
  async function retentionApply(payload){
    if(!payload||Object.keys(payload).some(key=>!['id','expected_version','confirm'].includes(key))||typeof payload.id!=='string'||!/^[a-f0-9-]{36}$/.test(payload.id)||payload.confirm!==true||!Number.isSafeInteger(payload.expected_version)||payload.expected_version<0)fail(400,'须明确确认保留期影响预览');
    return store.transaction(async()=>{
      await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-recording-retention',0))");
      const plan=await store.get('SELECT * FROM harbor.recording_retention_plans WHERE id=$1 FOR UPDATE',payload.id);if(!plan)fail(404,'保留期预览不存在');if(plan.status!=='preview')return {id:plan.id,status:plan.status,job_id:plan.job_id};if(plan.expires<=clock())fail(410,'保留期预览已失效');
      const settings=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.retention'"),current=settings?JSON.parse(settings.value):{days:30,version:0};if((current.version??0)!==payload.expected_version)fail(409,'保留期配置已变化，请重新预览','recording_retention_conflict');
      const job=await jobs.enqueue('recording.retention',{id:'recording.retention:'+plan.id,target:plan.id,payload:{plan_id:plan.id}});
      await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.retention',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({days:plan.days,version:payload.expected_version+1,updated:clock()}));
      await store.run("UPDATE harbor.recording_retention_plans SET status='queued',job_id=$1 WHERE id=$2",job.id,plan.id);
      await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',randomUUID(),clock(),'recording.retention.configure',JSON.stringify({days:plan.days,affected:plan.requests,actor:'admin',plan:plan.id}));
      return {id:plan.id,status:'queued',job_id:job.id,days:plan.days,version:payload.expected_version+1,physical_cleanup:'worker',new_capture_retention_requires_reload:false};
    });
  }
  async function retentionExecute(job,{signal}={}){
    if(job.kind!=='recording.retention'||job.target!==job.payload?.plan_id||job.id!=='recording.retention:'+job.target||Object.keys(job.payload).length!==1)fail(400,'保留期任务参数无效');
    for(;;){stop(signal);const count=await jobs.owned(job,async()=>{
      const rows=await store.all("SELECT request_id,expected_expires,target_expires FROM harbor.recording_retention_items WHERE plan_id=$1 AND status='pending' ORDER BY request_id LIMIT $2",job.target,purgeBatch);if(!rows.length)return 0;
      const sources=await store.all('SELECT request_id,expires_at FROM harbor.recordings WHERE request_id=ANY($1::text[]) ORDER BY request_id FOR UPDATE',rows.map(row=>row.request_id)),byId=new Map(sources.map(row=>[row.request_id,row]));
      await store.all('SELECT id FROM harbor.recording_exports WHERE id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=ANY($1::text[])) ORDER BY id FOR UPDATE',rows.map(row=>row.request_id));
      for(const item of rows){
        // Follow the capture repository's recording -> request order. Changes
        // after the preview are reported instead of expanding the frozen scope.
        const current=byId.get(item.request_id);let state='missing';
        if(current){state=current.expires_at===item.expected_expires?'applied':'changed';if(state==='applied')await store.run('UPDATE harbor.recordings SET expires_at=$1,updated=$2 WHERE request_id=$3',item.target_expires,clock(),item.request_id);}
        await store.run('UPDATE harbor.recording_retention_items SET status=$1 WHERE plan_id=$2 AND request_id=$3',state,job.target,item.request_id);
      }return rows.length;
    });if(count<purgeBatch)break;}
    return jobs.commitEffect(job,async()=>{const rows=await store.all('SELECT status,COUNT(*) count FROM harbor.recording_retention_items WHERE plan_id=$1 GROUP BY status',job.target),counts=Object.fromEntries(rows.map(row=>[row.status,row.count]));await store.run("UPDATE harbor.recording_retention_plans SET status='complete',completed=$1,summary=$3::jsonb WHERE id=$2",clock(),job.target,JSON.stringify(counts));return {id:job.target,counts};});
  }
  async function retentionStatus(planId){
    if(typeof planId!=='string'||!/^[a-f0-9-]{36}$/.test(planId))fail(404,'保留期计划不存在');const at=clock(),window=usageWindow(at),plan=await readStore.get('SELECT p.id,p.created,p.expires,p.days,p.requests,p.bytes,p.status,p.job_id,p.completed,p.summary,j.status job_status,j.error_code FROM harbor.recording_retention_plans p LEFT JOIN harbor.background_jobs j ON j.id=p.job_id WHERE p.id=$1 AND p.created>=$2 AND p.created<$3',planId,window.start,window.end);if(!plan)fail(404,'保留期计划不存在或超出近7天范围');const rows=await readStore.all('SELECT status,COUNT(*) count FROM harbor.recording_retention_items WHERE plan_id=$1 GROUP BY status',planId);return {...plan,summary:undefined,counts:plan.status==='complete'?plan.summary:Object.fromEntries(rows.map(row=>[row.status,row.count]))};
  }
  async function handle(path,method,payload,params=new URLSearchParams()){
    if(path==='/api/admin/recordings/context/search'&&method==='GET'&&contextQueries){const value=await contextQueries.search(recordingQuery(params,{clock,search:true,devices})),backlog=await contextJobs.status();return {status:200,value:{...value,index_backlog:backlog.context_index_pending,oldest_pending_at:backlog.context_oldest_pending_at,complete:!value.has_more&&backlog.context_index_pending===0}};}
    if(path==='/api/admin/recordings/status'&&method==='GET')return {status:200,value:await status()};
    if(path==='/api/admin/recordings/rebuild'&&method==='POST')return {status:202,value:await rebuild(payload)};
    if(path==='/api/admin/recordings/search'&&method==='GET')return {status:200,value:await queries.search(recordingQuery(params,{clock,search:true,devices}))};
    if(path==='/api/admin/recordings/session'&&method==='GET')return {status:200,value:await queries.session(recordingQuery(params,{clock,devices}))};
    const document=path.match(/^\/api\/admin\/recordings\/requests\/([a-zA-Z0-9._:-]{1,200})$/);if(document&&method==='GET'){if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1))fail(400,'正文分页参数无效');return {status:200,value:await queries.document(document[1],{after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)})};}
    if(path==='/api/admin/recordings/exports'&&method==='GET')return {status:200,value:await exporter.list(params)};
    if(path==='/api/admin/recordings/exports'&&method==='POST')return {status:202,value:await exporter.enqueue(payload)};
    const exportPath=path.match(/^\/api\/admin\/recordings\/exports\/(recording\.export:[a-f0-9-]{36})$/);if(exportPath&&method==='GET')return {status:200,value:await exporter.status(exportPath[1])};if(exportPath&&method==='DELETE')return {status:200,value:await exporter.cancel(exportPath[1])};
    if(path==='/api/admin/recordings/retention/preview'&&method==='POST')return {status:200,value:await retentionPreview(payload?.days)};
    if(path==='/api/admin/recordings/retention'&&method==='PATCH')return {status:202,value:await retentionApply(payload)};
    const retention=path.match(/^\/api\/admin\/recordings\/retention\/([a-f0-9-]{36})$/);if(retention&&method==='GET')return {status:200,value:await retentionStatus(retention[1])};
    return null;
  }
  return {schedule,refill,handlers:{'recording.index':indexHandler,'recording.purge':purge,'recording.retention':retentionExecute,...exporter.handlers,...contextJobs?.handlers},handle,queries,contextQueries,exports:exporter,download:exporter.download,capturePolicy:policy,status,retentionPreview,retentionApply,retentionStatus,rebuild};
}
