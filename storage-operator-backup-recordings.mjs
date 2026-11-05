import {RecordingStore} from './recording-store.mjs';
import {createRecordingIndexCrypto} from './recording-search-index.mjs';
import {transferError} from './migration-artifacts.mjs';
// These are the current recording repository AAD formats, not user-supplied SQL
// or callbacks. Oversized rows remain explicitly unverified within its default
// per-record budget; they are never silently treated as successful plaintext.
export async function verifyRestoredRecordingCiphertexts(store,key,{signal}={}){
 const repository=new RecordingStore(store,{encryptionKey:key}),derived=createRecordingIndexCrypto(key),maxBytes=repository.maxBytes+28,formats=[
  {from:'harbor.recordings r',id:'r.request_id',ordinal:'0::bigint',cipher:'r.preview_cipher',extra:"''::text source",where:'r.preview_cipher IS NOT NULL',aad:row=>row.id+':preview',open:(bytes,aad)=>repository.open(bytes,aad)},
  {from:'harbor.conversation_events r',id:'r.request_id',ordinal:'r.sequence',cipher:'r.content_cipher',extra:'r.source_key source',where:'TRUE',aad:row=>row.id+':'+row.ordinal+':'+row.source,open:(bytes,aad)=>repository.open(bytes,aad)},
  {from:'harbor.recording_document_parts r JOIN harbor.recording_documents d ON d.request_id=r.request_id',id:'r.request_id',ordinal:'r.ordinal',cipher:'r.payload_cipher',extra:'d.source_revision::text source',where:'TRUE',aad:row=>'document:'+row.id+':'+row.source+':'+row.ordinal,open:(bytes,aad)=>derived.open(bytes,aad)},
  {from:'harbor.recording_export_chunks r',id:'r.export_id',ordinal:'r.ordinal',cipher:'r.payload_cipher',extra:"''::text source",where:'TRUE',aad:row=>'export:'+row.id+':'+row.ordinal,open:(bytes,aad)=>derived.open(bytes,aad)}
 ];let verified=0,unverified=0;
 try{if(await store.get('SELECT request_id FROM harbor.recording_documents WHERE key_version<>$1 LIMIT 1',derived.keyVersion))throw transferError('storage_operator_application_derived_key_mismatch');for(const format of formats){let after=null,ordinal='0';for(;;){signal?.throwIfAborted();const rows=await store.all('SELECT '+format.id+' id,('+format.ordinal+')::text ordinal,'+format.extra+',CASE WHEN octet_length('+format.cipher+')<=$3 THEN '+format.cipher+' END cipher FROM '+format.from+' WHERE '+format.where+' AND ($1::text IS NULL OR ('+format.id+','+format.ordinal+')>($1::text,$2::bigint)) ORDER BY '+format.id+','+format.ordinal+' LIMIT 8',after,ordinal,maxBytes);for(const row of rows){signal?.throwIfAborted();if(!row.cipher)unverified++;else{format.open(row.cipher,format.aad(row));verified++;}after=row.id;ordinal=row.ordinal;}if(rows.length<8)break;}}return {verified,unverified,allVerified:unverified===0,maxCipherBytes:maxBytes};}finally{repository.key.fill(0);}
}
