import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
import {recordingScope} from './postgres-recording-query.mjs';
import {normalizeSearch} from './recording-search-index.mjs';

const fail=()=>{throw new AppError(400,'上下文分页已失效，请重新查询','recording_cursor_invalid');};
export function createPostgresRecordingContextQueries(store,{crypto,clock=Date.now,queryQueue,devices=false,maxCandidates=500,maxPageBytes=131072}={}){
 if(!crypto||!Number.isInteger(maxCandidates)||maxCandidates<1||maxCandidates>10000||!Number.isInteger(maxPageBytes)||maxPageBytes<1024||maxPageBytes>2097152)throw new AppError(503,'上下文查询预算无效','recording_query_configuration');
 const read=operation=>queryQueue?queryQueue.run(operation):operation();
 async function search(query){return read(async()=>{
  const at=clock(),key=createHash('sha256').update('context:'+JSON.stringify(Object.fromEntries(Object.entries(query).filter(([name])=>!['limit','cursor'].includes(name))))).digest('hex');let cursor;
  if(query.cursor)try{cursor=JSON.parse(Buffer.from(query.cursor,'base64url').toString());if(Object.keys(cursor).sort().join(',')!=='at,id,key,ordinal,watermark'||cursor.key!==key||!Number.isSafeInteger(cursor.at)||!Number.isSafeInteger(cursor.watermark)||typeof cursor.id!=='string'||!/^[a-f0-9-]{36}$/.test(cursor.id)||!Number.isSafeInteger(cursor.ordinal)||cursor.ordinal<1)fail();}catch{fail();}
  if(query.device&&!devices)throw new AppError(400,'设备正文筛选尚未启用','recording_query_invalid');
  for(const [field,table] of [['member','members'],['type','member_types'],['session','sessions'],...(devices?[['device','employee_devices']]:[])])if(query[field]&&!['unknown',...(field==='type'?['unclassified','historical_unknown']:[])].includes(query[field])&&!await store.get(`SELECT id FROM harbor.${table} WHERE id=$1`,query[field]))throw new AppError(400,'正文筛选对象不存在','recording_query_invalid');
  const scope=recordingScope(query),watermark=cursor?.watermark??at,values=[...scope.values,at,watermark,crypto.keyVersion,crypto.terms(query.q)],n=scope.values.length;let seek='';
  if(cursor){values.push(cursor.at,cursor.id,cursor.ordinal);seek=` AND (d.indexed_at,o.id,p.ordinal)<($${values.length-2},$${values.length-1},$${values.length})`;}
  values.push(maxCandidates+1);
  const rows=await store.all(`SELECT o.id object_id,o.kind context_kind,p.ordinal,p.payload_cipher,d.source_revision,d.format_version,d.indexed_at,w.revision current_revision,source.*
   FROM harbor.recording_context_document_parts p JOIN harbor.recording_context_documents d ON d.object_id=p.object_id
   JOIN harbor.recording_context_objects o ON o.id=d.object_id LEFT JOIN harbor.recording_context_work w ON w.object_id=o.id
   JOIN LATERAL (SELECT r.id request_id,r.member_id,r.session_id,r.started,r.model,r.billing_model,r.usage_type_id,r.usage_type_name${devices?',r.device_id,r.device_name,r.device_system,r.device_auth_state':''}
    FROM harbor.recording_context_references ref JOIN harbor.recording_context_snapshots s ON s.request_id=ref.request_id
    JOIN harbor.recordings c ON c.request_id=s.request_id JOIN harbor.requests r ON r.id=s.request_id LEFT JOIN harbor.sessions session ON session.id=r.session_id
    WHERE ref.object_id=o.id AND s.scope_digest=o.scope_digest AND s.key_version=o.key_version
    AND s.member_id=r.member_id AND s.session_id IS NOT DISTINCT FROM r.session_id AND (r.session_id IS NULL OR session.member_id=r.member_id)
    AND LEAST(s.expires_at,c.expires_at)>$${n+1} AND r.recording_state<>'expired' AND ${scope.where}
    ORDER BY r.started DESC,r.id DESC LIMIT 1) source ON true
   WHERE d.key_version=$${n+3} AND d.format_version=1 AND d.indexed_at<=$${n+2}
   AND o.state='complete' AND d.source_key_version=o.key_version AND d.source_fingerprint=o.fingerprint
   AND p.search_terms @> $${n+4}::bytea[]${seek}
   ORDER BY d.indexed_at DESC,o.id DESC,p.ordinal DESC LIMIT $${values.length}`,...values);
  const items=[];let checked=0,last=null,bytes=0;
  for(const row of rows.slice(0,maxCandidates)){
   if(items.length>=query.limit)break;
   const value=crypto.open(row.payload_cipher,`context-document:${row.object_id}:${row.source_revision}:${row.format_version}:${row.ordinal}`),text=normalizeSearch(value.searchText);checked++;last=row;
   const location=text.indexOf(query.q);if(location<0)continue;
   const {payload_cipher,source_revision,current_revision,...metadata}=row,item={...metadata,association:'shared_context',snippet:text.slice(Math.max(0,location-80),location+query.q.length+160),index_pending:source_revision!==current_revision},size=Buffer.byteLength(JSON.stringify(item));
   if(bytes+size>maxPageBytes){if(!items.length)throw new AppError(503,'上下文查询结果超过响应预算','recording_query_capacity');checked--;last=rows[checked-1]??null;break;}bytes+=size;items.push(item);
  }
  const hasMore=Boolean(last)&&checked<rows.length,next=hasMore?Buffer.from(JSON.stringify({key,at:last.indexed_at,id:last.object_id,ordinal:last.ordinal,watermark})).toString('base64url'):null;
  return {items,next,has_more:hasMore,candidate_limited:rows.length>maxCandidates&&checked>=maxCandidates,observed_at:at,index_watermark:watermark,range:{from:query.from,to:query.to},search_kind:'case_insensitive_literal_context_indexed',association:'shared_object_once_per_part'};
 });}
 return {search};
}
