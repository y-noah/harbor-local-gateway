import {AppError} from './store.mjs';

const fail=()=>{throw new AppError(410,'导出上下文来源已经变化或过期','recording_export_expired');};
// This helper holds only one export batch. The database dependency set, not an
// in-memory cache, is authoritative for cross-batch/restart deduplication.
export function createPostgresRecordingContextExport(store,{contexts,chunkBytes}={}){
 const batch=()=>({snapshots:[],objects:new Set()});
 async function append(current,requestId,exportId,emit){
  const snapshot=await contexts.describeSnapshot(requestId);
  const {requestId:sourceRequestId,references,scopeDigest,fingerprint,keyVersion,...metadata}=snapshot;
  emit({kind:'context_snapshot',request_id:sourceRequestId,...metadata});
  if(!scopeDigest)return null;
  current.snapshots.push(snapshot);
  const ids=[...new Set(references.map(ref=>ref.object_id))],existing=ids.length?await store.all('SELECT object_id FROM harbor.recording_context_export_objects WHERE export_id=$1 AND object_id=ANY($2::text[])',exportId,ids):[],seen=new Set(existing.map(row=>row.object_id));
  for(const ref of references){
   if(!seen.has(ref.object_id)&&!current.objects.has(ref.object_id)){
    const object=await contexts.readObject(ref.object_id);
    if(!object.scopeDigest.equals(scopeDigest)||!object.fingerprint.equals(ref.fingerprint)||object.keyVersion!==keyVersion||object.kind!==ref.kind)fail();
    const value={kind:'context_object',object_id:ref.object_id,context_kind:object.kind,content:object.content},serialized=JSON.stringify(value);
    if(Buffer.byteLength(serialized)+1<=chunkBytes)emit(value);
    else{const encoded=Buffer.from(serialized).toString('base64'),step=Math.floor(chunkBytes/2);for(let offset=0,part=0;offset<encoded.length;offset+=step,part++)emit({kind:'context_object_fragment',object_id:ref.object_id,part,encoding:'base64_json',data:encoded.slice(offset,offset+step),last:offset+step>=encoded.length});}
    current.objects.add(ref.object_id);
   }
   emit({kind:'context_reference',request_id:requestId,ordinal:ref.ordinal,object_id:ref.object_id,context_kind:ref.kind,truncated:ref.truncated});
  }
  return snapshot.expiresAt;
 }
 async function verify(current){
  // Source recording rows are already locked by the caller. A consistent
  // scope order lets multi-session exports coexist with capture and GC.
  for(const expected of [...current.snapshots].sort((a,b)=>Buffer.compare(a.scopeDigest,b.scopeDigest)||a.requestId.localeCompare(b.requestId))){
   const actual=await contexts.describeSnapshot(expected.requestId,{lockScope:true});
   if(!actual.scopeDigest||!actual.scopeDigest.equals(expected.scopeDigest)||actual.keyVersion!==expected.keyVersion||!actual.fingerprint.equals(expected.fingerprint)||actual.expiresAt!==expected.expiresAt)fail();
  }
 }
 async function persist(current,exportId){if(current.objects.size)await store.run('INSERT INTO harbor.recording_context_export_objects(export_id,object_id) SELECT $1,unnest($2::text[]) ON CONFLICT DO NOTHING',exportId,[...current.objects]);}
 return {batch,append,verify,persist,release:exportId=>store.run('DELETE FROM harbor.recording_context_export_objects WHERE export_id=$1',exportId)};
}
