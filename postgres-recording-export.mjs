import {randomUUID} from 'node:crypto';
import {AppError,usageWindow} from './store.mjs';
import {recordingQuery,recordingScope} from './postgres-recording-query.mjs';
import {csvCell,csvHeader,metadataCsv,exportMetadata,referenceUsdDisplay} from './recording-export-format.mjs';
import {validPrice,joins} from './postgres-reference-query.mjs';
import {createPostgresRecordingContextExport} from './postgres-recording-context-export.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {departmentPredicate,queryDepartmentIdentity} from './postgres-query-departments.mjs';
import {verifyDepartmentIsolationAvailability} from './postgres-departments.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';

const fail=(status,message,code='recording_export_invalid')=>{throw new AppError(status,message,code);};
const isId=value=>typeof value==='string'&&/^[a-f0-9-]{36}$/.test(value);
const stopped=signal=>{if(signal?.aborted)fail(503,'导出已停止','worker_stopping');};
export function createPostgresRecordingExports(store,{jobs,crypto,recordings,contexts,clock=Date.now,storagePolicy=async()=>({exportAllowed:true}),maxRows=100000,maxBytes=1073741824,chunkBytes=65536,ttlMs=3600000,batchRows=25,queryQueue,devices=false,maxPageBytes=65536}={}){
  if(!jobs||!crypto||!recordings||!Number.isInteger(maxRows)||maxRows<1||maxRows>1000000||!Number.isSafeInteger(maxBytes)||maxBytes<1024||maxBytes>4294967296||!Number.isInteger(chunkBytes)||chunkBytes<1024||chunkBytes>1048576||!Number.isInteger(ttlMs)||ttlMs<60000||ttlMs>86400000||!Number.isInteger(batchRows)||batchRows<1||batchRows>100||!Number.isInteger(maxPageBytes)||maxPageBytes<2048||maxPageBytes>1048576)fail(503,'导出资源配置无效','recording_export_configuration');
  if(contexts!==undefined&&(!contexts||typeof contexts.describeSnapshot!=='function'||typeof contexts.readObject!=='function'))fail(503,'上下文导出依赖无效','recording_export_configuration');
  const contextExport=contexts?createPostgresRecordingContextExport(store,{contexts,chunkBytes}):null;
  const departments=createAdminDepartments(store);
  const authorize=scope=>departments.resolve(scope);
  async function owner(job,{requireLease=true}={}){
    if(typeof jobs.authorizePersistedActor==='function')return jobs.authorizePersistedActor(job,{requireLease});
    if(await verifyDepartmentIsolationAvailability(store))fail(503,'导出后台身份配置未就绪','department_operation_unavailable');
    return null;
  }
  const auditIdentity=scope=>scope?{actor:scope.role,credential_id:scope.credential_id,credential_version:scope.credential_version,department_id:scope.department_id}:{actor:'admin'};
  const read=(op,{signal}={})=>queryQueue?queryQueue.run(op,{signal}):op();
  async function enqueue(input,{scope}={}){
    const authorization=await authorize(scope);
    if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(key=>!['include_body','format','filters'].includes(key))||typeof input.include_body!=='boolean'||!['csv','jsonl'].includes(input.format)||input.include_body&&input.format!=='jsonl'||!input.filters||typeof input.filters!=='object'||Array.isArray(input.filters)||Object.values(input.filters).some(value=>typeof value!=='string'))fail(400,'导出须明确格式、是否包含正文和筛选');
    const query=recordingQuery(new URLSearchParams(input.filters),{clock,devices}),window=usageWindow(clock());if(!input.include_body&&(query.from<window.start||query.to>window.end))fail(400,'元数据导出仅允许近7个北京时间自然日');
    if(query.cursor)fail(400,'导出不能从页面游标隐式截取');delete query.cursor;delete query.limit;delete query.q;
    const policy=await storagePolicy();if(!policy.exportAllowed)fail(503,'磁盘或存储预算不足，暂不接受导出','recording_storage_low');
    return store.transaction(async()=>{
      if(authorization)await departments.current(authorization,{lock:true,write:true});
      const id=randomUUID(),at=clock(),job=await jobs.enqueue('recording.export',{id:'recording.export:'+id,target:id,payload:{export_id:id},scope:authorization??undefined}),visibility=(await store.get('SELECT pg_current_snapshot()::text value')).value;
      await store.run('INSERT INTO harbor.recording_exports(id,created,expires,actor,options,visibility) VALUES($1,$2,$3,$4,$5::jsonb,$6::pg_snapshot)',job.id,at,at+ttlMs,authorization?.role??'admin',JSON.stringify({...(authorization?{department_format:1}:{}),include_body:input.include_body,format:input.format,query,...(contextExport?{include_context:input.include_body,context_format:1}:{}),max_rows:maxRows,max_bytes:maxBytes,key_version:crypto.keyVersion}),visibility);
      await createPostgresAudit(store,{clock}).write(authorization,{action:'recording.export.requested',at,detail:{export:job.id,include_body:input.include_body,format:input.format,...auditIdentity(authorization)},subject:{kind:'export',id:job.id}});
      return {id:job.id,status:'queued',expires:at+ttlMs,max_rows:maxRows,max_bytes:maxBytes};
    });
  }
  async function row(id,{scope,verifyOwner=true}={}){
    if(typeof id!=='string'||!/^recording\.export:[a-f0-9-]{36}$/.test(id))fail(404,'导出任务不存在','recording_export_not_found');
    const authorization=await authorize(scope),args=[id],predicate=departmentPredicate(authorization,'j',args);
    const value=await store.get('SELECT e.*,j.status job_status,j.error_code job_error FROM harbor.recording_exports e JOIN harbor.background_jobs j ON j.id=e.id WHERE e.id=$1 AND '+predicate,...args);
    if(!value)fail(404,'导出任务不存在','recording_export_not_found');
    if(authorization&&verifyOwner)await owner({id},{requireLease:false});return value;
  }
  const publicState=(value,at)=>({id:value.id,status:value.expires<=at||value.source_expired?'expired':value.status,job_status:value.job_status,rows:value.rows,bytes:value.bytes,chunks:value.chunks,created:value.created,expires:value.expires,completed:value.completed,error_code:value.error_code||value.job_error,include_body:value.options.include_body,format:value.options.format,limits:{rows:value.options.max_rows,bytes:value.options.max_bytes},download_ready:value.status==='complete'&&value.expires>at&&!value.source_expired});
  async function status(id,{signal,scope}={}){return read(async()=>publicState(await row(id,{scope}),clock()),{signal});}
  async function list(params=new URLSearchParams(),{signal,scope}={}){return read(async()=>{
    const authorization=await authorize(scope),identity=queryDepartmentIdentity(authorization);
    if(!(params instanceof URLSearchParams)||[...params.keys()].some(key=>!['before','limit'].includes(key)||params.getAll(key).length!==1))fail(400,'导出历史分页参数无效');
    const limit=params.has('limit')?Number(params.get('limit')):20,at=clock(),window=usageWindow(at);if(!Number.isInteger(limit)||limit<1||limit>100)fail(400,'导出历史每页须为1至100条');let cursor=null;
    if(params.has('before'))try{const encoded=params.get('before');if(!encoded||encoded.length>1024||!/^[A-Za-z0-9_-]+$/.test(encoded))throw Error();cursor=JSON.parse(Buffer.from(encoded,'base64url').toString());if(Object.keys(cursor).sort().join(',')!==(authorization?'at,id,scope,start':'at,id,start')||authorization&&cursor.scope!==identity||cursor.start!==window.start||!Number.isSafeInteger(cursor.at)||cursor.at<window.start||cursor.at>=window.end||typeof cursor.id!=='string'||!/^recording\.export:[a-f0-9-]{36}$/.test(cursor.id))throw Error();}catch{fail(400,'导出历史游标已失效，请重新查询','recording_export_cursor_invalid');}
    const values=[window.start,window.end],seek=cursor?' AND (e.created,e.id)<($3,$4)':'';if(cursor)values.push(cursor.at,cursor.id);const predicate=departmentPredicate(authorization,'j',values);values.push(limit+1);
    // Select only fixed-size public metadata, never filters, snapshots or body chunks.
    const rows=await store.all(`SELECT e.id,e.status,e.rows,e.bytes,e.chunks,e.created,e.expires,e.completed,e.error_code,j.status job_status,j.error_code job_error,
      jsonb_build_object('include_body',e.options->'include_body','format',e.options->'format','max_rows',e.options->'max_rows','max_bytes',e.options->'max_bytes') options
      FROM harbor.recording_exports e JOIN harbor.background_jobs j ON j.id=e.id WHERE e.created>=$1 AND e.created<$2${seek} AND ${predicate} ORDER BY e.created DESC,e.id DESC LIMIT $${values.length}`,...values);
    const items=[];let bytes=512;for(const value of rows.slice(0,limit)){const item=publicState(value,at),size=Buffer.byteLength(JSON.stringify(item))+1;if(bytes+size>maxPageBytes){if(!items.length)fail(503,'导出历史元数据超过响应预算','recording_export_capacity');break;}bytes+=size;items.push(item);}
    const last=items.at(-1),next_cursor=last&&rows.length>items.length?Buffer.from(JSON.stringify({start:window.start,at:last.created,id:last.id,...(authorization?{scope:identity}:{})})).toString('base64url'):null;return {items,next_cursor};
  },{signal});}

  async function cancel(id,{scope}={}){return store.transaction(async()=>{const value=await row(id,{scope});if(['cancelled','expired'].includes(value.status))return {id,status:value.status};if(value.status==='complete')fail(409,'导出已完成，可等待到期清理');await jobs.cancel(id);await store.run("UPDATE harbor.recording_exports SET status='cancelled',error_code='admin_cancelled' WHERE id=$1",id);await store.run('DELETE FROM harbor.recording_export_chunks WHERE export_id=$1',id);if(contextExport)await contextExport.release(id);return {id,status:'cancelled'};});}
  async function execute(job,{signal}={}){
    if(job.kind!=='recording.export'||job.id!=='recording.export:'+job.payload?.export_id||job.target!==job.payload?.export_id||!isId(job.target)||Object.keys(job.payload).length!==1)fail(400,'导出任务参数无效');
    for(;;){
      stopped(signal);const authorization=await owner(job);let current=await jobs.owned(job,()=>row(job.id,{scope:authorization??undefined}));if(current.status==='complete')return {id:job.id,rows:current.rows,bytes:current.bytes};if(current.status==='cancelled'||current.expires<=clock())fail(410,'导出已取消或过期','recording_export_expired');if(current.options.key_version!==crypto.keyVersion)fail(503,'导出加密材料已变化','recording_export_key_changed');
      if(current.options.include_context&&(!contextExport||current.options.context_format!==1))fail(503,'上下文导出格式或能力不匹配','recording_export_configuration');
      if(!(await storagePolicy()).exportAllowed)fail(503,'存储不足，导出已暂停','recording_storage_low');
      const scope=recordingScope(current.options.query,{departmentScope:authorization}),args=[...scope.values,current.visibility],visibilityArg=args.length;let seek='';
      if(current.after_id){args.push(current.after_at,current.after_id);seek=` AND (r.started,r.id)>($${args.length-1},$${args.length})`;}
      args.push(batchRows);const rows=await store.all(`SELECT r.id${authorization?',r.department_id':''},r.started,r.ended,r.member_id,r.session_id,r.usage_type_id,r.usage_type_name,r.usage_type_version,r.model,r.billing_model,r.business_status,r.recording_state,r.price_version${devices?',r.device_id,r.device_name,r.device_system,r.device_generation,r.device_auth_state':''},
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,CASE WHEN ${validPrice} THEN p.total_atoms::text END total_atoms
        FROM harbor.requests r${joins} WHERE ${scope.where} AND pg_visible_in_snapshot(r.inserted_xid,$${visibilityArg}::pg_snapshot)${seek} ORDER BY r.started,r.id LIMIT $${args.length}`,...args);
      if(!rows.length)return jobs.commitEffect(job,async()=>{await owner(job);const final=await row(job.id,{scope:authorization??undefined});await store.run("UPDATE harbor.recording_exports SET status='complete',completed=$1 WHERE id=$2",clock(),job.id);return {id:job.id,rows:final.rows,bytes:final.bytes};});
      const chunks=[],sources=new Set(),contextBatch=current.options.include_context?contextExport.batch():null;let text=current.rows===0&&current.options.format==='csv'?(current.options.department_format===1?csvCell('department_id')+',':'')+csvHeader:'',written=0,expires=current.expires;
      const append=line=>{if(Buffer.byteLength(line)>chunkBytes)fail(413,'导出单行超过分块预算','recording_export_capacity');if(text&&Buffer.byteLength(text)+Buffer.byteLength(line)>chunkBytes){chunks.push(text);text='';}text+=line;};
      for(const request of rows){
        stopped(signal);await owner(job);if(current.rows+written>=current.options.max_rows)fail(413,'导出超过请求数量上限，请缩小范围','recording_export_capacity');
        const provenance=authorization?{department_id:request.department_id}:{},metadata={...exportMetadata({...request,total_exact:referenceUsdDisplay(request.total_atoms)}),...provenance};append(current.options.format==='csv'?(current.options.department_format===1?csvCell(request.department_id)+',':'')+metadataCsv(metadata):JSON.stringify({kind:'request',...metadata})+'\n');
        if(current.options.include_body){let after=0;for(;;){stopped(signal);await owner(job);const page=await recordings.read(request.id,{after,limit:8,scope:authorization??undefined});append(JSON.stringify({kind:'recording_state',...provenance,request_id:request.id,state:page.state,reason:page.reason??null,turn_state:page.turnState??null})+'\n');
          if(page.events.length&&page.expiresAt){expires=Math.min(expires,page.expiresAt);sources.add(request.id);}
          for(const event of page.events){const serialized=JSON.stringify({...event,event_kind:event.kind,kind:'recording_event',request_id:request.id,...provenance});if(Buffer.byteLength(serialized)<=chunkBytes)append(serialized+'\n');else{const encoded=Buffer.from(serialized).toString('base64');for(let offset=0,part=0;offset<encoded.length;offset+=Math.floor(chunkBytes/2),part++)append(JSON.stringify({kind:'recording_event_fragment',...provenance,request_id:request.id,sequence:event.sequence,part,encoding:'base64_json',data:encoded.slice(offset,offset+Math.floor(chunkBytes/2)),last:offset+Math.floor(chunkBytes/2)>=encoded.length})+'\n');}}
          if(!page.hasMore)break;if(page.next<=after)fail(503,'正文导出游标没有前进','recording_export_invalid');after=page.next;
        }
          if(contextBatch){const until=await contextExport.append(contextBatch,request.id,job.id,value=>{stopped(signal);append(JSON.stringify(value)+'\n');},{scope:authorization??undefined,...(authorization?{departmentId:request.department_id}:{})});if(until){expires=Math.min(expires,until);sources.add(request.id);}}
        }written++;
      }
      if(text)chunks.push(text);const bytes=chunks.reduce((total,value)=>total+Buffer.byteLength(value),0);if(current.bytes+bytes>current.options.max_bytes)fail(413,'导出超过字节上限，请缩小范围','recording_export_capacity');
      const encrypted=chunks.map((value,index)=>({ordinal:current.chunks+index+1,bytes:Buffer.byteLength(value),cipher:crypto.seal({text:value},`export:${job.id}:${current.chunks+index+1}`)}));
      await jobs.owned(job,async()=>{
        await owner(job);
        // Lock source expiries before export metadata, matching expiry triggers.
        // If a source disappeared after its page was read, no stale body file is published.
        if(sources.size){const currentSources=await store.all('SELECT request_id,expires_at FROM harbor.recordings WHERE request_id=ANY($1::text[]) ORDER BY request_id FOR SHARE',[...sources]);if(currentSources.length!==sources.size)fail(410,'导出正文已经过期','recording_export_expired');for(const source of currentSources)expires=Math.min(expires,source.expires_at);if(expires<=clock())fail(410,'导出正文已经过期','recording_export_expired');}
        if(contextBatch)await contextExport.verify(contextBatch,{scope:authorization??undefined});
        const locked=await store.get('SELECT status,chunks,expires FROM harbor.recording_exports WHERE id=$1 FOR UPDATE',job.id);if(locked.status==='cancelled'||locked.expires<=clock())fail(410,'导出已取消或过期','recording_export_expired');if(locked.chunks!==current.chunks)fail(409,'导出进度已变化','job_lease_lost');
        for(const chunk of encrypted)await store.run('INSERT INTO harbor.recording_export_chunks VALUES($1,$2,$3,$4)',job.id,chunk.ordinal,chunk.bytes,chunk.cipher);
        for(const requestId of sources)await store.run('INSERT INTO harbor.recording_export_sources VALUES($1,$2) ON CONFLICT DO NOTHING',job.id,requestId);
        if(contextBatch)await contextExport.persist(contextBatch,job.id);
        const last=rows.at(-1);await store.run("UPDATE harbor.recording_exports SET status='running',after_at=$1,after_id=$2,rows=rows+$3,bytes=bytes+$4,chunks=chunks+$5,expires=LEAST(expires,$7) WHERE id=$6",last.started,last.id,written,bytes,chunks.length,job.id,expires);
      });
    }
  }
  async function* download(id,{signal,scope,revalidateScope}={}){
    let authorization=await authorize(scope);
    const validate=async()=>{
      if(revalidateScope){const current=await revalidateScope();if(!current)fail(403,'下载访问权限已失效','department_forbidden');requireDepartmentScope(current,{allowGlobal:true});if(!authorization||['role','view','department_id','credential_id','credential_version'].some(key=>authorization[key]!==current[key]))fail(403,'下载访问范围已变化','department_forbidden');authorization=current;}
      return row(id,{scope:authorization??undefined});
    };
    const initial=await validate();if(initial.status!=='complete'||initial.expires<=clock()||initial.source_expired)fail(410,'导出未完成或已经过期','recording_export_unavailable');if(initial.options.key_version!==crypto.keyVersion)fail(503,'导出加密材料已变化','recording_export_key_changed');
    await createPostgresAudit(store,{clock}).write(authorization,{action:'recording.export.download',detail:{export:id,...auditIdentity(authorization)},subject:{kind:'export',id}});
    for(let ordinal=1;ordinal<=initial.chunks;ordinal++){
      stopped(signal);const current=await validate();if(current.status!=='complete'||current.expires<=clock()||current.source_expired)fail(410,'导出已过期','recording_export_unavailable');
      const chunk=await store.get("SELECT c.payload_cipher,e.expires FROM harbor.recording_export_chunks c JOIN harbor.recording_exports e ON e.id=c.export_id WHERE c.export_id=$1 AND c.ordinal=$2 AND e.status='complete' AND e.expires>$3",id,ordinal,clock());if(!chunk)fail(410,'导出分块已过期或不可读取','recording_export_unavailable');
      const value=crypto.open(chunk.payload_cipher,`export:${id}:${ordinal}`),verified=await validate();stopped(signal);if(verified.status!=='complete'||verified.expires<=clock()||chunk.expires<=clock())fail(410,'导出已过期','recording_export_unavailable');yield Buffer.from(value.text);
    }
  }
  return {enqueue,status,list,cancel,download,handlers:{'recording.export':execute}};
}
