import {RecordingStore} from './recording-store.mjs';
import {RecordingContextStore} from './recording-context-store.mjs';
import {createRecordingIndexCrypto} from './recording-search-index.mjs';
import {transferError} from './migration-artifacts.mjs';
export async function hasStoredRecordingCiphertexts(store){
 if((await store.get('SELECT EXISTS(SELECT 1 FROM harbor.recordings WHERE preview_cipher IS NOT NULL) OR EXISTS(SELECT 1 FROM harbor.conversation_events) OR EXISTS(SELECT 1 FROM harbor.recording_document_parts) OR EXISTS(SELECT 1 FROM harbor.recording_export_chunks) present')).present)return true;
 if((await store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name&&(await store.get('SELECT EXISTS(SELECT 1 FROM harbor.recording_context_objects) OR EXISTS(SELECT 1 FROM harbor.recording_context_snapshots) present')).present)return true;
 return Boolean((await store.get("SELECT to_regclass('harbor.recording_context_documents') name")).name&&(await store.get('SELECT EXISTS(SELECT 1 FROM harbor.recording_context_documents) present')).present);
}
async function verifyContextDerived(store,crypto,{signal,maxBytes}={}){
 const broken=await store.get(`SELECT EXISTS(SELECT 1 FROM harbor.recording_context_objects o LEFT JOIN harbor.recording_context_work w ON w.object_id=o.id LEFT JOIN harbor.recording_context_documents d ON d.object_id=o.id WHERE w.object_id IS NULL OR w.revision<1 OR w.indexed_revision<0 OR w.indexed_revision>w.revision OR w.indexed_revision>0 AND (d.object_id IS NULL OR d.source_revision<>w.indexed_revision)) invalid`);
 if(broken.invalid)throw transferError('storage_operator_application_context_index_incomplete');
 let after='',documents=0,parts=0;
 for(;;){signal?.throwIfAborted();const rows=await store.all(`SELECT d.*,o.key_version object_key,o.fingerprint object_fingerprint,w.revision,w.indexed_revision
  FROM harbor.recording_context_documents d LEFT JOIN harbor.recording_context_objects o ON o.id=d.object_id LEFT JOIN harbor.recording_context_work w ON w.object_id=d.object_id WHERE d.object_id>$1 ORDER BY d.object_id LIMIT 8`,after);
  for(const doc of rows){signal?.throwIfAborted();
   if(doc.parts<1||doc.bytes<1||doc.key_version!==crypto.keyVersion||doc.format_version!==1||doc.source_key_version!==doc.object_key||!Buffer.isBuffer(doc.source_fingerprint)||!Buffer.isBuffer(doc.object_fingerprint)||!doc.source_fingerprint.equals(doc.object_fingerprint)||doc.source_revision!==doc.indexed_revision||doc.source_revision<1||doc.source_revision>doc.revision)throw transferError('storage_operator_application_context_index_mismatch');
   const shape=await store.get('SELECT COUNT(*) parts,COALESCE(SUM(bytes),0) bytes,MAX(ordinal) last FROM harbor.recording_context_document_parts WHERE object_id=$1',doc.object_id);
   if(shape.parts!==doc.parts||Number(shape.bytes)!==doc.bytes||(shape.last??0)!==doc.parts)throw transferError('storage_operator_application_context_index_incomplete');
   let ordinal=0;const indexTerms=crypto.createTermsScope();
   for(;;){signal?.throwIfAborted();const values=await store.all('SELECT ordinal,bytes,cardinality(search_terms) term_count,CASE WHEN octet_length(payload_cipher)<=$3 THEN payload_cipher END cipher FROM harbor.recording_context_document_parts WHERE object_id=$1 AND ordinal>$2 ORDER BY ordinal LIMIT 8',doc.object_id,ordinal,maxBytes);
    for(const part of values){signal?.throwIfAborted();if(!part.cipher)throw transferError('storage_operator_application_context_index_capacity');const value=crypto.open(part.cipher,`context-document:${doc.object_id}:${doc.source_revision}:${doc.format_version}:${part.ordinal}`);if(typeof value.text!=='string'||typeof value.searchText!=='string'||value.kind!=='context'||value.field!=='context:'+doc.object_id||Buffer.byteLength(JSON.stringify(value))!==part.bytes)throw transferError('storage_operator_application_context_index_incomplete');const expected=indexTerms(value.searchText).map(term=>term.toString('hex')).sort(),termLimit=Math.floor(maxBytes/32);
     if(expected.length>termLimit||part.term_count>termLimit)throw transferError('storage_operator_application_context_index_capacity');
     if(part.term_count!==expected.length)throw transferError('storage_operator_application_context_search_mismatch');
     // Never return an unchecked array to Node. The expected count comes from
     // the authenticated plaintext; every digest must be exactly SHA-256 size.
     const safe=await store.get(`SELECT CASE WHEN cardinality(search_terms)=$3 AND ($3=0 OR array_ndims(search_terms)=1 AND array_lower(search_terms,1)=1) AND NOT EXISTS(SELECT 1 FROM unnest(search_terms) term WHERE term IS NULL OR octet_length(term)<>32) THEN search_terms END terms FROM harbor.recording_context_document_parts WHERE object_id=$1 AND ordinal=$2`,doc.object_id,part.ordinal,expected.length),actual=Array.isArray(safe?.terms)?safe.terms.map(term=>term.toString('hex')).sort():null;
     if(JSON.stringify(actual)!==JSON.stringify(expected))throw transferError('storage_operator_application_context_search_mismatch');ordinal=part.ordinal;parts++;}if(values.length<8)break;
   }
   documents++;after=doc.object_id;
  }if(rows.length<8)break;
 }
 const invalid=await store.get(`SELECT EXISTS(SELECT 1 FROM harbor.recording_context_export_objects d LEFT JOIN harbor.recording_exports e ON e.id=d.export_id LEFT JOIN harbor.recording_context_objects o ON o.id=d.object_id WHERE e.id IS NULL OR o.id IS NULL) invalid`);
 if(invalid.invalid)throw transferError('storage_operator_application_context_export_incomplete');
 return {documents,indexParts:parts,exportDependencies:(await store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects')).n};
}
// These are the current recording repository AAD formats, not user-supplied SQL
// or callbacks. Oversized rows remain explicitly unverified within its default
// per-record budget; they are never silently treated as successful plaintext.
export async function verifyRestoredRecordingCiphertexts(store,key,{signal}={}){
 const repository=new RecordingStore(store,{encryptionKey:key}),derived=createRecordingIndexCrypto(key),maxBytes=repository.maxBytes+28,formats=[
  {from:'harbor.recordings r',id:'r.request_id',ordinal:'0::bigint',cipher:'r.preview_cipher',extra:"''::text source",where:'r.preview_cipher IS NOT NULL',aad:row=>row.id+':preview',open:(bytes,aad)=>repository.open(bytes,aad)},
  {from:'harbor.conversation_events r',id:'r.request_id',ordinal:'r.sequence',cipher:'r.content_cipher',extra:'r.source_key source',where:'TRUE',aad:row=>row.id+':'+row.ordinal+':'+row.source,open:(bytes,aad)=>repository.open(bytes,aad)},
  {from:'harbor.recording_document_parts r JOIN harbor.recording_documents d ON d.request_id=r.request_id',id:'r.request_id',ordinal:'r.ordinal',cipher:'r.payload_cipher',extra:'d.source_revision::text source',where:'TRUE',aad:row=>'document:'+row.id+':'+row.source+':'+row.ordinal,open:(bytes,aad)=>derived.open(bytes,aad)},
  {from:'harbor.recording_export_chunks r',id:'r.export_id',ordinal:'r.ordinal',cipher:'r.payload_cipher',extra:"''::text source",where:'TRUE',aad:row=>'export:'+row.id+':'+row.ordinal,open:(bytes,aad)=>derived.open(bytes,aad)}
 ];let verified=0,unverified=0,contexts;
 const contextStore=new RecordingContextStore(store,{encryptionKey:key,maxSnapshotBytes:8388608,maxComponents:16384});
 try{return await store.transaction(async()=>{if(await store.get('SELECT request_id FROM harbor.recording_documents WHERE key_version<>$1 LIMIT 1',derived.keyVersion))throw transferError('storage_operator_application_derived_key_mismatch');for(const format of formats){let after=null,ordinal='0';for(;;){signal?.throwIfAborted();const rows=await store.all('SELECT '+format.id+' id,('+format.ordinal+')::text ordinal,'+format.extra+',CASE WHEN octet_length('+format.cipher+')<=$3 THEN '+format.cipher+' END cipher FROM '+format.from+' WHERE '+format.where+' AND ($1::text IS NULL OR ('+format.id+','+format.ordinal+')>($1::text,$2::bigint)) ORDER BY '+format.id+','+format.ordinal+' LIMIT 8',after,ordinal,maxBytes);for(const row of rows){signal?.throwIfAborted();if(!row.cipher)unverified++;else{format.open(row.cipher,format.aad(row));verified++;}after=row.id;ordinal=row.ordinal;}if(rows.length<8)break;}}if((await store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name){contexts=await contextStore.verifyStoredObjects({signal});verified+=contexts.parts;if((await store.get("SELECT to_regclass('harbor.recording_context_documents') name")).name){const index=await verifyContextDerived(store,derived,{signal,maxBytes});contexts={...contexts,...index};verified+=index.indexParts;}}return {verified,unverified,allVerified:unverified===0,maxCipherBytes:maxBytes,...(contexts?{contexts}:{})};},{level:'REPEATABLE READ',readOnly:true});}finally{repository.key.fill(0);contextStore.key.fill(0);contextStore.fingerprintKey.fill(0);}
}
