import {createHash} from 'node:crypto';
import {AppError,usageWindow} from './store.mjs';
import {normalizeSearch} from './recording-search-index.mjs';
import {createQueryDepartments,departmentPredicate,queryDepartmentIdentity} from './postgres-query-departments.mjs';

const fail=(status,message,code='recording_query_invalid')=>{throw new AppError(status,message,code);};
const identifier=value=>typeof value==='string'&&/^[a-zA-Z0-9._:-]{1,200}$/.test(value);
export function recordingQuery(params,{clock=Date.now,search=false,devices=false}={}){
  const allowed=['from','to','member','type','model','status','outcome','member_missing','model_missing','session','limit','cursor',...(devices?['device']:[]),...(search?['q']:[])];
  if([...params.keys()].some(key=>!allowed.includes(key)||params.getAll(key).length!==1))fail(400,'正文查询参数无效');
  const at=clock(),window=usageWindow(at),from=params.has('from')?Number(params.get('from')):window.start,to=params.has('to')?Number(params.get('to')):window.end;
  if(!Number.isSafeInteger(from)||!Number.isSafeInteger(to)||from<0||to<=from||to>window.end||from<at-365*86400000)fail(400,'正文查询范围须在可保留期限内');
  const result={from,to,limit:params.has('limit')?Number(params.get('limit')):25,member:params.get('member')||'',type:params.get('type')||'',model:params.get('model')||'',status:params.get('status')||'',outcome:params.get('outcome')||'',memberMissing:params.get('member_missing')==='true',modelMissing:params.get('model_missing')==='true',session:params.get('session')||'',device:params.get('device')||'',cursor:params.get('cursor')||'',q:params.get('q')||''};
  if(!Number.isInteger(result.limit)||result.limit<1||result.limit>100||['member','type','status','session','device'].some(key=>result[key]&&!identifier(result[key]))||result.model.length>200||/[\x00-\x1f]/.test(result.model)||result.cursor.length>4096)fail(400,'正文筛选或分页参数无效');
  if(['member_missing','model_missing'].some(key=>params.has(key)&&!['true','false'].includes(params.get(key)))||result.memberMissing&&result.member||result.modelMissing&&result.model||result.outcome&&(!['queued','running','ok','failed','unknown','interrupted','cancelled','rejected'].includes(result.outcome)||result.status))fail(400,'正文业务结果或缺失身份筛选无效');
  if(search){result.q=normalizeSearch(result.q.trim());const length=Array.from(result.q).length;if(length<2||length>128||/[\x00-\x1f]/.test(result.q))fail(400,'正文搜索须为2至128个字符，不支持正则或布尔表达式');}
  return result;
}
export function recordingScope(query,{offset=0,alias='r',departmentScope=null}={}){
  const values=[query.from,query.to],where=[`${alias}.usage_at>=$${offset+1}`,`${alias}.usage_at<$${offset+2}`];
  if(query.memberMissing)where.push(alias+'.member_id IS NULL');
  if(query.modelMissing)where.push(`COALESCE(NULLIF(${alias}.billing_model,''),NULLIF(${alias}.model,'')) IS NULL`);
  if(query.outcome){if(query.outcome==='unknown')where.push(`${alias}.business_status NOT IN ('ok','failed','queued','running','cancelled','rejected','interrupted')`);else{values.push(query.outcome);where.push(alias+'.business_status=$'+(offset+values.length));}}
  for(const [field,column] of [['member','member_id'],['type','usage_type_id'],['model','COALESCE'],['status','status'],['session','session_id'],['device','device_id']])if(query[field]){
    if(field==='type'&&['unclassified','historical_unknown'].includes(query[field])){where.push(alias+'.usage_type_id IS NULL AND '+alias+'.usage_type_version IS '+(query[field]==='unclassified'?'NOT ':'')+'NULL');continue;}
    if(['type','device'].includes(field)&&query[field]==='unknown'){where.push(alias+'.'+column+' IS NULL');continue;}
    values.push(query[field]);where.push((column==='COALESCE'?`COALESCE(NULLIF(${alias}.billing_model,''),NULLIF(${alias}.model,''))`:`${alias}.${column}`)+'=$'+(offset+values.length));
  }
  if(departmentScope!==null)where.push(departmentPredicate(departmentScope,alias,values,offset));
  return {values,where:where.join(' AND ')};
}
const identity=(query,authorization)=>createHash('sha256').update((authorization===null?'':queryDepartmentIdentity(authorization)+'\0')+JSON.stringify(Object.fromEntries(Object.entries(query).filter(([key])=>!['cursor','limit'].includes(key))))).digest('hex');
const encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
function decode(value,key){try{const cursor=JSON.parse(Buffer.from(value,'base64url').toString());if(cursor.key!==key||!Number.isSafeInteger(cursor.at)||typeof cursor.id!=='string'||!identifier(cursor.id)||!Number.isSafeInteger(cursor.ordinal)||cursor.ordinal<0||!Number.isSafeInteger(cursor.watermark))throw Error();return cursor;}catch{fail(400,'正文分页已失效，请重新查询','recording_cursor_invalid');}}
export function createPostgresRecordingQueries(store,{crypto,clock=Date.now,queryQueue,maxCandidates=500,maxPageBytes=131072,devices=false}={}){
  if(!crypto||!Number.isInteger(maxCandidates)||maxCandidates<1||maxCandidates>10000||!Number.isInteger(maxPageBytes)||maxPageBytes<1024||maxPageBytes>2097152)fail(503,'正文查询资源配置无效','recording_query_configuration');
  const read=(operation,{signal}={})=>queryQueue?queryQueue.run(operation,{signal}):operation(),deviceColumns=devices?',r.device_id,r.device_name,r.device_system,r.device_generation,r.device_auth_state':'';
  const departments=createQueryDepartments(store);
  async function validateScope(query,authorization){
    if(query.device&&!devices)fail(400,'设备正文筛选尚未启用');
    for(const [field,table] of [['member','members'],['type','member_types'],['session','sessions'],...(devices?[['device','employee_devices']]:[])])if(query[field]&&!['unknown',...(field==='type'?['unclassified','historical_unknown']:[])].includes(query[field])){
      const args=[query[field]],predicate=table==='employee_devices'&&authorization?.view==='department'?'EXISTS(SELECT 1 FROM harbor.members m WHERE m.id=employee_devices.member_id AND '+departmentPredicate(authorization,'m',args)+')':departmentPredicate(authorization,table,args);
      if(!await store.get(`SELECT id FROM harbor.${table} WHERE id=$1 AND ${predicate}`,...args))fail(400,'正文筛选对象不存在');
    }
  }
  async function search(query,{signal,scope:accessScope}={}){
    return read(async()=>{
      const authorization=await departments.resolve(accessScope),key=identity(query,authorization),at=clock(),cursor=query.cursor?decode(query.cursor,key):null,watermark=cursor?.watermark??at,scope=recordingScope(query,{departmentScope:authorization}),values=[...scope.values,at,watermark,crypto.keyVersion,crypto.terms(query.q)],n=values.length;
      await validateScope(query,authorization);let seek='';if(cursor){values.push(cursor.at,cursor.id,cursor.ordinal);seek=` AND (r.started,r.id,p.ordinal)<($${n+1},$${n+2},$${n+3})`;}
      values.push(maxCandidates+1);
      // HMAC bigram intersection is an index prefilter, never a plaintext LIKE scan.
      const candidates=await store.all(`SELECT r.id,r.started,r.ended,r.member_id,r.session_id,r.model,r.billing_model,r.status,r.usage_type_id,r.usage_type_code,r.usage_type_name,r.usage_type_version${deviceColumns},
        p.ordinal,p.payload_cipher,d.source_revision,d.state,d.turn_state,d.indexed_at,d.expires_at,w.revision current_revision
        FROM harbor.recording_document_parts p JOIN harbor.recording_documents d ON d.request_id=p.request_id JOIN harbor.recordings c ON c.request_id=d.request_id JOIN harbor.requests r ON r.id=d.request_id
        LEFT JOIN harbor.recording_work w ON w.request_id=r.id
        WHERE ${scope.where} AND c.expires_at>$${scope.values.length+1} AND d.expires_at>$${scope.values.length+1} AND d.indexed_at<=$${scope.values.length+2} AND d.key_version=$${scope.values.length+3}
          AND p.search_terms @> $${scope.values.length+4}::bytea[]${seek} ORDER BY r.started DESC,r.id DESC,p.ordinal DESC LIMIT $${values.length}`,...values);
      const items=[];let bytes=0,last=null,checked=0;
      for(const row of candidates.slice(0,maxCandidates)){
        if(items.length>=query.limit)break;const value=crypto.open(row.payload_cipher,`document:${row.id}:${row.source_revision}:${row.ordinal}`);checked++;last=row;
        const normalized=normalizeSearch(value.searchText);if(!normalized.includes(query.q))continue;
        const location=normalized.indexOf(query.q),snippet=value.searchText.slice(Math.max(0,location-80),location+query.q.length+160),{payload_cipher,current_revision,source_revision,...metadata}=row;
        const item={...metadata,kind:value.kind,field:value.field,snippet,index_pending:current_revision!==source_revision};const size=Buffer.byteLength(JSON.stringify(item));
        if(items.length&&bytes+size>maxPageBytes){checked--;last=candidates[checked-1]??cursor;break;}bytes+=size;items.push(item);
      }
      const hasMore=!!last&&checked<candidates.length,next=hasMore?encode({key,at:last.started??last.at,id:last.id,ordinal:last.ordinal,watermark}):null;
      const backlogArgs=[],backlogDepartment=departmentPredicate(authorization,'r',backlogArgs),backlog=await store.get('SELECT COUNT(*) count,MIN(w.changed) oldest FROM harbor.recording_work w JOIN harbor.requests r ON r.id=w.request_id WHERE w.revision>w.indexed_revision AND '+backlogDepartment,...backlogArgs);
      return {items,next,has_more:hasMore,candidate_limited:candidates.length>maxCandidates&&checked>=maxCandidates,index_backlog:backlog.count,oldest_pending_at:backlog.oldest,observed_at:at,index_watermark:watermark,range:{from:query.from,to:query.to},search_kind:'case_insensitive_literal_indexed',complete:!hasMore&&backlog.count===0};
    },{signal});
  }
  async function session(query,{signal,scope:accessScope}={}){return read(()=>store.transaction(async()=>{
    const authorization=await departments.resolve(accessScope);await validateScope(query,authorization);const at=clock(),key=identity(query,authorization),cursor=query.cursor?decode(query.cursor,key):null,scope=recordingScope(query,{departmentScope:authorization}),values=[...scope.values,at];let seek='';
    if(!query.session)fail(400,'会话记录必须指定会话');if(cursor){values.push(cursor.at,cursor.id);seek=` AND (r.started,r.id)<($${values.length-1},$${values.length})`;}
    values.push(query.limit+1);const rows=await store.all(`SELECT r.id,r.member_id,r.session_id,r.started,r.ended,r.status,r.model,r.billing_model,r.usage_type_id,r.usage_type_code,r.usage_type_name,r.usage_type_version,r.recording_state${deviceColumns},
      d.state organized_state,d.source_sequence,d.indexed_at,d.expires_at,d.parts,d.source_revision,d.key_version,w.revision current_revision
      FROM harbor.requests r LEFT JOIN harbor.recording_documents d ON d.request_id=r.id AND d.expires_at>$${scope.values.length+1} LEFT JOIN harbor.recording_work w ON w.request_id=r.id
      WHERE ${scope.where}${seek} ORDER BY r.started DESC,r.id DESC LIMIT $${values.length}`,...values);
    const items=rows.slice(0,query.limit).map(({source_revision,current_revision,key_version,...row})=>({...row,index_pending:source_revision!==current_revision||key_version!==crypto.keyVersion})),last=items.at(-1),hasMore=rows.length>items.length;
    return {items,has_more:hasMore,next:hasMore?encode({key,at:last.started,id:last.id,ordinal:0,watermark:cursor?.watermark??at}):null,observed_at:at,ordering:'newest_first',association:'request_session_identity_only',range:{from:query.from,to:query.to}};
  },{level:'REPEATABLE READ',readOnly:true}),{signal});}
  async function document(requestId,{after=0,limit=25}={},{signal,scope}={}){return read(()=>store.transaction(async()=>{
    if(!identifier(requestId)||!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)fail(400,'整理正文分页无效');
    const authorization=await departments.resolve(scope),args=[requestId],predicate=departmentPredicate(authorization,'r',args);
    const at=clock(),document=await store.get('SELECT d.*,LEAST(d.expires_at,c.expires_at) actual_expires,w.revision current_revision FROM harbor.recording_documents d JOIN harbor.recordings c ON c.request_id=d.request_id JOIN harbor.requests r ON r.id=d.request_id LEFT JOIN harbor.recording_work w ON w.request_id=d.request_id WHERE d.request_id=$1 AND '+predicate,...args);
    if(!document){const request=await store.get('SELECT r.recording_state FROM harbor.requests r WHERE r.id=$1 AND '+predicate,...args);if(!request)fail(404,'请求不存在');return {state:request.recording_state,index_pending:true,items:[],next:after,has_more:false};}
    if(document.actual_expires<=at)return {state:'expired',items:[],next:after,has_more:false};if(document.key_version!==crypto.keyVersion)fail(503,'正文索引材料已变化，等待重建','recording_index_rebuild');
    const sizes=await store.all('SELECT ordinal,bytes FROM harbor.recording_document_parts WHERE request_id=$1 AND ordinal>$2 ORDER BY ordinal LIMIT $3',requestId,after,limit+1),selected=[];let bytes=0;
    for(const row of sizes.slice(0,limit)){if(selected.length&&bytes+row.bytes>maxPageBytes)break;selected.push(row.ordinal);bytes+=row.bytes;}
    const rows=selected.length?await store.all('SELECT ordinal,payload_cipher FROM harbor.recording_document_parts WHERE request_id=$1 AND ordinal=ANY($2::integer[]) ORDER BY ordinal',requestId,selected):[];
    const items=rows.map(row=>{const {searchText,...value}=crypto.open(row.payload_cipher,`document:${requestId}:${document.source_revision}:${row.ordinal}`);return {ordinal:row.ordinal,...value};});
    return {state:document.state,turn_state:document.turn_state,source_sequence:document.source_sequence,indexed_at:document.indexed_at,index_pending:document.current_revision!==document.source_revision,expires_at:document.expires_at,items,next:selected.at(-1)??after,has_more:sizes.length>selected.length};
  },{level:'REPEATABLE READ',readOnly:true}),{signal});}
  return {search,session,document};
}
