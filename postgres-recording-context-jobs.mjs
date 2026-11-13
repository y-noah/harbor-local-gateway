import {createDepartmentScopeIssuer} from './department-access.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
import {RecordingContextStore} from './recording-context-store.mjs';
import {recordingParts} from './recording-search-index.mjs';
import {createQueryDepartments,departmentPredicate} from './postgres-query-departments.mjs';

const formatVersion=1,kind='recording.context.index';
const jobId=(id,revision)=>kind+':'+createHash('sha256').update(id+':'+revision).digest('hex');
const stop=signal=>{if(signal?.aborted)throw new AppError(503,'上下文后台任务已停止','worker_stopping');};
// The object is shared, but only its authenticated, unexpired request references
// make it eligible for indexing. Search must apply the same source conditions.
const liveSource=(references,department='TRUE')=>`SELECT 1 FROM ${references} ref
 JOIN harbor.recording_context_snapshots s ON s.request_id=ref.request_id
 JOIN harbor.recordings c ON c.request_id=s.request_id JOIN harbor.requests r ON r.id=s.request_id
 LEFT JOIN harbor.sessions session ON session.id=r.session_id
 WHERE ref.object_id=o.id AND s.scope_digest=o.scope_digest AND s.key_version=o.key_version
 AND s.member_id=r.member_id AND s.session_id IS NOT DISTINCT FROM r.session_id
 AND r.recording_state<>'expired' AND LEAST(s.expires_at,c.expires_at)>$1
 AND (r.session_id IS NULL OR session.member_id=r.member_id) AND ${department}`;
const live=`EXISTS(${liveSource('harbor.recording_context_references')})`;
function visibleText(content,maxBytes){
 const pending=[content],text=[];let bytes=0;
 while(pending.length){const value=pending.pop();if(value&&typeof value==='object'){
   if(Array.isArray(value)){for(let i=value.length-1;i>=0;i--)pending.push(value[i]);}
   else{const entries=Object.entries(value);for(let i=entries.length-1;i>=0;i--){pending.push(entries[i][1]);pending.push(entries[i][0]);}}
  }else{const part=typeof value==='string'?value:JSON.stringify(value);if(part!==undefined){bytes+=Buffer.byteLength(part)+1;if(bytes>maxBytes)throw new AppError(503,'上下文索引超过正文预算','recording_index_capacity');text.push(part);}}
 }
 return text.join('\n');
}
export function createPostgresRecordingContextJobs(store,{jobs,recordingKey,crypto,clock=Date.now,indexBatch=32,maxDocumentBytes=2097152,partChars=2048}={}){
 const departments=createQueryDepartments(store),writeDepartments=createAdminDepartments(store),systemIssuer=createDepartmentScopeIssuer();
 async function jobScope(job){const scope=await jobs.authorizePersistedActor(job);if(scope&&(scope.role!=='system'||scope.purpose!==kind||scope.view!=='department'))throw new AppError(403,'上下文整理身份无效','department_forbidden');return scope;}
 const contexts=new RecordingContextStore(store,{encryptionKey:recordingKey,clock,maxSnapshotBytes:maxDocumentBytes,partBytes:Math.min(65536,maxDocumentBytes)});
 async function schedule(){
  const at=clock();
  await store.run(`UPDATE harbor.recording_context_work w SET revision=w.revision+1,changed=$1,last_error=NULL
   FROM harbor.recording_context_documents d WHERE w.object_id=d.object_id AND w.revision=w.indexed_revision
   AND (d.key_version<>$2 OR d.format_version<>$3) AND w.object_id IN
   (SELECT object_id FROM harbor.recording_context_documents WHERE key_version<>$2 OR format_version<>$3 ORDER BY object_id LIMIT $4)`,at,crypto.keyVersion,formatVersion,indexBatch);
  return refill();
 }
 async function refill(){
  // Keep live-source checks local to each ordered work item. A pulled-up EXISTS
  // can aggregate all request references before applying the small batch limit.
  // Materialize this object’s indexed references before joining source snapshots;
  // an empty/expired prefix must not rescan every snapshot for each work item.
  const at=clock(),scoped=(await jobs.verifyReadiness()).installed,rows=await store.all(`SELECT w.object_id,w.revision${scoped?',source.department_id':''} FROM harbor.recording_context_work w
   JOIN harbor.recording_context_objects o ON o.id=w.object_id
   JOIN LATERAL (WITH object_refs AS MATERIALIZED (SELECT object_id,request_id FROM harbor.recording_context_references WHERE object_id=o.id)
    ${scoped?liveSource('object_refs','(r.session_id IS NULL OR session.department_id=r.department_id) AND EXISTS(SELECT 1 FROM harbor.departments dept WHERE dept.id=r.department_id AND dept.enabled)').replace('SELECT 1','SELECT r.department_id'):liveSource('object_refs')} LIMIT 1) source ON true
   WHERE w.revision>w.indexed_revision AND w.last_error IS NULL ORDER BY w.changed,w.object_id LIMIT $2`,at,indexBatch);
  for(const row of rows){const id=jobId(row.object_id,row.revision);let result;try{result=await jobs.enqueue(kind,{id,target:row.object_id,payload:{object_id:row.object_id,revision:row.revision},scope:scoped?systemIssuer.system({purpose:kind,departmentId:row.department_id}):undefined});}catch(error){if(scoped&&error.code==='department_forbidden'&&!await store.get('SELECT id FROM harbor.departments WHERE id=$1 AND enabled',row.department_id))continue;throw error;}
   if(!result.created){const task=await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',id);
    if(['failed','cancelled'].includes(task?.status))await store.run("UPDATE harbor.recording_context_work SET last_error='index_attempts_exhausted' WHERE object_id=$1 AND revision=$2 AND revision>indexed_revision",row.object_id,row.revision);
    // A claimed source can expire before commit, then gain a new reference.
    // A completed no-effect job must not permanently seal that object's work.
    else if(task?.status==='completed')await store.run('UPDATE harbor.recording_context_work SET revision=revision+1,changed=$1 WHERE object_id=$2 AND revision=$3 AND revision>indexed_revision',at,row.object_id,row.revision);
   }
  }
  return rows.length;
 }
 async function organize(job,{signal}={}){
  if(job.kind!==kind||typeof job.target!=='string'||job.target!==job.payload?.object_id||!Number.isSafeInteger(job.payload?.revision)||job.payload.revision<1||Object.keys(job.payload).length!==2||job.id!==jobId(job.target,job.payload.revision))throw new AppError(400,'上下文整理任务参数无效','recording_jobs_invalid');
  stop(signal);const scope=await jobScope(job);const work=await store.get('SELECT revision,indexed_revision FROM harbor.recording_context_work WHERE object_id=$1',job.target);
  if(!work||work.revision!==job.payload.revision||work.indexed_revision>=work.revision)return {object_id:job.target,skipped:true};
  let source;try{source=await contexts.readObject(job.target,{scope:scope??undefined});}catch(error){if(['recording_context_missing','recording_context_expired'].includes(error.code))return {object_id:job.target,expired:true};throw error;}
  const text=visibleText(source.content,maxDocumentBytes),parts=recordingParts([{kind:'context',field:'context:'+job.target,text}],{partChars}),prepared=[];let bytes=0;
  const indexTerms=crypto.createTermsScope();
  for(const [index,part] of parts.entries()){
   stop(signal);const size=Buffer.byteLength(JSON.stringify(part));bytes+=size;
   prepared.push({ordinal:index+1,bytes:size,cipher:crypto.seal(part,`context-document:${job.target}:${work.revision}:${formatVersion}:${index+1}`),terms:indexTerms(part.searchText)});
   if(index%16===15)await new Promise(setImmediate);
  }
  return jobs.commitEffect(job,async()=>{const currentScope=await jobScope(job);return contexts.withLiveObject(job.target,source,async()=>{
   const current=await store.get('SELECT revision,indexed_revision FROM harbor.recording_context_work WHERE object_id=$1 FOR UPDATE',job.target);
   if(!current||current.revision!==work.revision||current.indexed_revision>=current.revision)return {object_id:job.target,stale:true};stop(signal);
   await store.run('DELETE FROM harbor.recording_context_documents WHERE object_id=$1',job.target);
   await store.run('INSERT INTO harbor.recording_context_documents VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',job.target,work.revision,source.keyVersion,source.fingerprint,crypto.keyVersion,formatVersion,clock(),prepared.length,bytes);
   if(prepared.length)await store.run(`INSERT INTO harbor.recording_context_document_parts(object_id,ordinal,bytes,payload_cipher,search_terms)
    SELECT v.object_id,v.ordinal,v.bytes,v.cipher,ARRAY(SELECT decode(term,'hex') FROM jsonb_array_elements_text(v.terms) AS terms(term))
    FROM jsonb_to_recordset($1::jsonb) AS v(object_id text,ordinal integer,bytes integer,cipher bytea,terms jsonb)`,JSON.stringify(prepared.map(part=>({object_id:job.target,ordinal:part.ordinal,bytes:part.bytes,cipher:'\\x'+part.cipher.toString('hex'),terms:part.terms.map(term=>term.toString('hex'))}))));
   await store.run('UPDATE harbor.recording_context_work SET indexed_revision=$1,indexed_at=$2,last_error=NULL WHERE object_id=$3',work.revision,clock(),job.target);
   return {object_id:job.target,parts:prepared.length,source_revision:work.revision};
  },{scope:currentScope??undefined});});
 }
 async function handler(job,context){try{return await organize(job,context);}catch(error){if(['recording_index_capacity','recording_context_scope','recording_context_capacity','recording_context_incomplete','recording_context_unreadable'].includes(error.code))await jobs.owned(job,()=>store.run('UPDATE harbor.recording_context_work SET last_error=$1 WHERE object_id=$2 AND revision=$3',error.code,job.target,job.payload.revision));throw error;}}
 async function rebuild(requestId,{scope}={}){const authorization=await writeDepartments.resolve(scope,{write:true});if(authorization&&(authorization.view!=='department'||!await store.get('SELECT id FROM harbor.requests WHERE id=$1 AND department_id=$2',requestId,authorization.department_id)))throw new AppError(404,'请求正文不存在','recording_not_found');await store.run(`UPDATE harbor.recording_context_work SET revision=revision+1,last_error=NULL,changed=$1 WHERE object_id IN (SELECT object_id FROM harbor.recording_context_references WHERE request_id=$2)`,clock(),requestId);}
 async function status({scope}={}){const authorization=await departments.resolve(scope),args=[clock()],predicate=departmentPredicate(authorization,'r',args),visible=`EXISTS(${liveSource('harbor.recording_context_references',predicate)})`;const row=await store.get(`SELECT COUNT(*) pending,COUNT(*) FILTER(WHERE w.last_error IS NOT NULL) failed,MIN(w.changed) oldest FROM harbor.recording_context_work w JOIN harbor.recording_context_objects o ON o.id=w.object_id WHERE w.revision>w.indexed_revision AND ${visible}`,...args);return {context_index_pending:row.pending,context_index_failed:row.failed,context_oldest_pending_at:row.oldest};}
 return {schedule,refill,rebuild,status,gc:options=>contexts.gc(options),handlers:{[kind]:handler}};
}
