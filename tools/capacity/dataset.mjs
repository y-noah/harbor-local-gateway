import {createHash,randomUUID,randomBytes} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {PostgresStore} from '../../postgres-store.mjs';
import {isOwnedLaboratory} from './owned-runtime.mjs';
import {RecordingStore} from '../../recording-store.mjs';
import {RecordingContextStore} from '../../recording-context-store.mjs';
import {capacityHistoricalContext} from './context-payload.mjs';
import {usageWindow,hash,token,encryptStoredKey,revealStoredKey} from '../../store.mjs';
import {quoteReference} from '../../pricing.mjs';
import {fixtureTimestamp,capacitySeedWindowCounts,capacitySeedActiveBodyRanges} from './dataset-time.mjs';
import {initializeCapacityPricing} from './pricing-source.mjs';
export {fixtureTimestamp} from './dataset-time.mjs';

const digest=value=>createHash('sha256').update(value).digest('hex');
const generatorHash=digest(readFileSync(new URL(import.meta.url)));
export const fixtureId=(runId,kind,index)=>{const hex=createHash('md5').update(runId+':'+kind+':'+index).digest('hex');return [hex.slice(0,8),hex.slice(8,12),hex.slice(12,16),hex.slice(16,20),hex.slice(20)].join('-');};
export function datasetManifest({profile='formal',runId=randomUUID(),ownerNonce=randomUUID(),anchor=Date.now(),metadata30d=profile==='formal'?5400000:120,metadata7d=profile==='formal'?1260000:28,bodyRequests=profile==='formal'?5400000:32,batchSize=500,currentSessionLayout='balanced',historicalPrimaryPercent=70}={}){
  const sessionsPerEmployee=10;
  if(!['balanced','historical-skew'].includes(currentSessionLayout)||!Number.isInteger(historicalPrimaryPercent)||historicalPrimaryPercent<0||historicalPrimaryPercent>100||historicalPrimaryPercent*sessionsPerEmployee%100!==0||!['formal','selfcheck'].includes(profile)||!/^[-a-f0-9]{36}$/.test(runId)||!/^[-a-f0-9]{36}$/.test(ownerNonce)||![anchor,metadata30d,metadata7d,bodyRequests,batchSize].every(Number.isSafeInteger)||anchor<0||metadata7d<1||metadata30d<metadata7d||bodyRequests<1||bodyRequests>metadata30d||batchSize<1||batchSize>1000)throw new Error('capacity_dataset_manifest_invalid');
  const value={schema:'harbor-capacity-dataset-v1',generatorHash,profile,runId,ownerNonce,anchor,metadata30d,metadata7d,bodyRequests,batchSize,employees:60,accounts:8,sessionsPerEmployee,currentSessionLayout,historicalPrimaryPercent,models:['gpt-6.1-sol','gpt-6-astra'],bodyDistribution:[{percent:70,bytes:8192},{percent:20,bytes:32768},{percent:9,bytes:131072},{percent:1,bytes:1048576}],synthetic:true};return {...value,manifestHash:digest(JSON.stringify(value))};
}
function validateManifest(manifest){const canonical=datasetManifest(manifest);if(canonical.manifestHash!==manifest.manifestHash||JSON.stringify(canonical)!==JSON.stringify(manifest))throw new Error('capacity_dataset_manifest_mismatch');return canonical;}
export const fixtureDatabase=manifest=>'harbor_capacity_'+manifest.runId.replaceAll('-','');
async function owned(store,manifest){validateManifest(manifest);const database=await store.get('SELECT current_database() name');if(database.name!==fixtureDatabase(manifest))throw new Error('capacity_dataset_database_not_owned');const marker=await store.get('SELECT run_id,owner_nonce,manifest_hash,metadata_cursor,body_cursor,recording_key_digest,pricing_revision,pricing_book_hash FROM harbor_capacity.fixture WHERE singleton=1');if(!marker||marker.run_id!==manifest.runId||marker.owner_nonce!==manifest.ownerNonce||marker.manifest_hash!==manifest.manifestHash)throw new Error('capacity_dataset_owner_mismatch');return marker;}
export async function createDatasetDatabase(admin,manifest,{connectionFor,encryptionKey,recordingKey,laboratory}={}){
  validateManifest(manifest);if(!isOwnedLaboratory(laboratory))throw new Error('capacity_dataset_laboratory_not_owned');if(typeof connectionFor!=='function'||!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32||!Buffer.isBuffer(recordingKey)||recordingKey.length!==32)throw new Error('capacity_dataset_configuration');const name=fixtureDatabase(manifest),ownerConnection=new PostgresStore({connection:laboratory.connection});let expectedCluster;try{expectedCluster=(await ownerConnection.get('SELECT system_identifier::text id FROM pg_control_system()')).id;}finally{await ownerConnection.close();}if((await admin.get('SELECT system_identifier::text id FROM pg_control_system()')).id!==expectedCluster)throw new Error('capacity_dataset_admin_cluster_mismatch');await admin.query('CREATE DATABASE '+name);let store;
  try{store=new PostgresStore({...connectionFor(name),statementTimeoutMs:600000});const actual=await store.get('SELECT current_database() name,(SELECT system_identifier::text FROM pg_control_system()) cluster');if(actual.name!==name||actual.cluster!==expectedCluster)throw new Error('capacity_dataset_connection_mismatch');await store.migrate({recordingContextIndex:true});await initializeCapacityPricing(store);await store.query('CREATE SCHEMA harbor_capacity');await store.query('CREATE TABLE harbor_capacity.fixture(singleton INTEGER PRIMARY KEY CHECK(singleton=1),run_id TEXT NOT NULL,owner_nonce TEXT NOT NULL,manifest_hash TEXT NOT NULL,recording_key_digest TEXT NOT NULL,pricing_revision TEXT NOT NULL,pricing_book_hash TEXT NOT NULL,metadata_cursor BIGINT NOT NULL DEFAULT 0,body_cursor BIGINT NOT NULL DEFAULT 0)');const pricing=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active,book=await store.pricingBook(pricing);await store.run('INSERT INTO harbor_capacity.fixture(singleton,run_id,owner_nonce,manifest_hash,recording_key_digest,pricing_revision,pricing_book_hash) VALUES(1,$1,$2,$3,$4,$5,$6)',manifest.runId,manifest.ownerNonce,manifest.manifestHash,digest(recordingKey),pricing,digest(JSON.stringify(book)));
    await store.query('CREATE TABLE harbor_capacity.body_receipts(request_id TEXT PRIMARY KEY,fixture_index BIGINT UNIQUE NOT NULL,source_bytes BIGINT NOT NULL,encoded_bytes BIGINT NOT NULL,event_count INTEGER NOT NULL,content_hash TEXT NOT NULL,cipher_hash TEXT NOT NULL)');
    await bootstrapActors(store,manifest,encryptionKey);return {store,close:()=>store.close(),destroy:async()=>{await owned(store,manifest);await store.close();await admin.query('DROP DATABASE '+name);}};
  }catch(error){await store?.close();await admin.query('DROP DATABASE '+name);throw error;}
}
export function fixtureSessionAccount(manifest,employee,slot,{historical=false}={}){
 if(!Number.isInteger(employee)||employee<0||employee>=manifest.employees||!Number.isInteger(slot)||slot<0||slot>=manifest.sessionsPerEmployee)throw new Error('capacity_dataset_session_coordinates');
 if(!historical&&manifest.currentSessionLayout==='balanced')return (employee+slot)%manifest.accounts;
 const index=employee*manifest.sessionsPerEmployee+slot;
 return slot*100<manifest.sessionsPerEmployee*manifest.historicalPrimaryPercent?0:1+index%(manifest.accounts-1);
}
export async function bootstrapActors(store,manifest,encryptionKey){
  await owned(store,manifest);const actors=[];
  await store.transaction(async()=>{
    for(let index=0;index<manifest.employees;index++){
      const member=fixtureId(manifest.runId,'member',index),keyId=fixtureId(manifest.runId,'key',index),type='usage-'+String.fromCharCode(97+index%6);await store.run('INSERT INTO harbor.members(id,name,created,usage_type_id,usage_type_version,default_models_set) VALUES($1,$2,$3,$4,1,1) ON CONFLICT(id) DO NOTHING',member,'容量合成员工'+index,manifest.anchor,type);
      const existing=await store.get('SELECT id,digest,secret_cipher FROM harbor.keys WHERE id=$1',keyId);let secret=existing?revealStoredKey(encryptionKey,existing):token();if(!secret)throw new Error('capacity_actor_key_unreadable');if(!existing)await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created,secret_cipher,allowed_models) VALUES($1,$2,$3,$4,$5,$6,$7)',keyId,member,hash(secret),secret.slice(0,12),manifest.anchor,encryptStoredKey(encryptionKey,secret,keyId),JSON.stringify(manifest.models));actors.push({memberId:member,keyId,secret,synthetic:true});
    }
    for(let index=0;index<manifest.accounts;index++)await store.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,state,quota_json) VALUES($1,$2,$3,'codex','subscription','ready',$4) ON CONFLICT(id) DO NOTHING",fixtureId(manifest.runId,'account',index),'容量受控账号'+index,'capacity-synthetic-'+index,JSON.stringify({plan:'synthetic-capacity-plan',checked:manifest.anchor,limitReached:false,windows:[{minutes:10080,used:50,reset:manifest.anchor+7*86400000}]}));
    for(let employee=0;employee<manifest.employees;employee++)for(let slot=0;slot<manifest.sessionsPerEmployee;slot++){const index=employee*manifest.sessionsPerEmployee+slot,account=fixtureSessionAccount(manifest,employee,slot);await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,client_key,client_session_id,upstream_thread) VALUES($1,$2,$3,'responses',$4,$4,$5,$6,$1) ON CONFLICT(id) DO NOTHING",fixtureId(manifest.runId,'session',index),actors[employee].memberId,fixtureId(manifest.runId,'account',account),manifest.anchor,hash('capacity-thread-'+index),'capacity-thread-'+index);}
  });return actors;
}
function bodyBytes(manifest,index){const point=index*37%100;let sum=0;for(const bin of manifest.bodyDistribution){sum+=bin.percent;if(point<sum)return bin.bytes;}throw new Error('capacity_body_distribution_invalid');}
function syntheticText(bytes,index,part){const head=Buffer.from('容量合成正文 request_'+index+' part_'+part+'\nfunction capacityScenario(){ return "合成代码与工具输出"; }\n'),result=Buffer.alloc(bytes,32);head.copy(result,0,0,Math.min(head.length,bytes));const offset=Math.min(head.length,bytes),remaining=bytes-offset;if(remaining){const text=createHash('shake256',{outputLength:Math.ceil(remaining/2)}).update(index+':'+part).digest('hex').replace(/.{64}/g,'$&\n');Buffer.from(text).copy(result,offset,0,remaining);}return result.toString('utf8');}
function fixtureBody(manifest,index){
  const requestId=fixtureId(manifest.runId,'request',index),size=bodyBytes(manifest,index),at=fixtureTimestamp(manifest,index),events=[];
  for(let offset=0,part=0;offset<size;offset+=32768,part++)events.push({sourceKey:'capacity-body-'+part,kind:part===0?'user':part%3===0?'tool_result':'assistant',provenance:part===0?'client':'upstream',content:{text:syntheticText(Math.min(32768,size-offset),index,part)}});
  return {requestId,index,size,at,events,encodedBytes:events.reduce((n,event)=>n+Buffer.byteLength(JSON.stringify(event.content)),0),contentHash:digest(JSON.stringify(events.map(event=>[event.kind,event.provenance,event.sourceKey,event.content]))),state:index%1000>=990?'partial':'complete'};
}
async function seedNewBodyBatch(store,manifest,first,until,recordingKey,signal){
  const recordings=new RecordingStore(store,{encryptionKey:recordingKey,maxBytes:2*1024*1024}),bodies=[],events=[];
  for(let index=first;index<until;index++){
    signal?.throwIfAborted();const body=fixtureBody(manifest,index);body.preview=recordings.seal({text:body.events[0].content.text.slice(0,200)},body.requestId+':preview');bodies.push(body);
    for(const [offset,event] of body.events.entries()){const sequence=offset+1;events.push({requestId:body.requestId,sequence,sourceKey:event.sourceKey,kind:event.kind,provenance:event.provenance,at:body.at,cipher:recordings.seal(event.content,body.requestId+':'+sequence+':'+event.sourceKey)});}
  }
  signal?.throwIfAborted();
  await store.run(`INSERT INTO harbor.recordings(request_id,turn_state,bytes,next_sequence,persisted_sequence,preview_cipher,updated,expires_at)
    SELECT v.id,'known',v.bytes,v.events+1,v.events,v.preview,v.at,v.expires FROM unnest($1::text[],$2::bigint[],$3::bigint[],$4::bytea[],$5::bigint[],$6::bigint[]) AS v(id,bytes,events,preview,at,expires)`,bodies.map(b=>b.requestId),bodies.map(b=>b.encodedBytes),bodies.map(b=>b.events.length),bodies.map(b=>b.preview),bodies.map(b=>b.at),bodies.map(b=>b.at+recordings.retentionDays*86400000));
  await store.run(`INSERT INTO harbor.conversation_events(request_id,sequence,source_key,kind,provenance,created,content_cipher)
    SELECT * FROM unnest($1::text[],$2::bigint[],$3::text[],$4::text[],$5::text[],$6::bigint[],$7::bytea[])`,events.map(e=>e.requestId),events.map(e=>e.sequence),events.map(e=>e.sourceKey),events.map(e=>e.kind),events.map(e=>e.provenance),events.map(e=>e.at),events.map(e=>e.cipher));
  const metadata=JSON.stringify(bodies.map(b=>({id:b.requestId,index:b.index,size:b.size,bytes:b.encodedBytes,events:b.events.length,hash:b.contentHash,state:b.state})));
  await store.run(`UPDATE harbor.requests r SET recording_state=v.state,recording_reason=NULL FROM jsonb_to_recordset($1::jsonb) AS v(id text,state text) WHERE r.id=v.id`,metadata);
  await store.run(`INSERT INTO harbor_capacity.body_receipts SELECT v.id,v.index,v.size,v.bytes,v.events,v.hash,md5(string_agg(md5(e.content_cipher),'' ORDER BY e.sequence))
    FROM jsonb_to_recordset($1::jsonb) AS v(id text,index bigint,size bigint,bytes bigint,events integer,hash text) JOIN harbor.conversation_events e ON e.request_id=v.id GROUP BY v.id,v.index,v.size,v.bytes,v.events,v.hash`,metadata);
}
export async function seedDataset(store,manifest,{recordingKey,signal,onProgress=async()=>{}}={}){
  let cursor=await owned(store,manifest);if(!Buffer.isBuffer(recordingKey)||recordingKey.length!==32||cursor.recording_key_digest!==digest(recordingKey))throw new Error('capacity_dataset_recording_key_invalid');const active=cursor.pricing_revision,book=await store.pricingBook(active);if(!book||digest(JSON.stringify(book))!==cursor.pricing_book_hash)throw new Error('capacity_dataset_pricing_changed');
  for(let first=cursor.metadata_cursor;first<manifest.metadata30d;first+=manifest.batchSize){signal?.throwIfAborted();const until=Math.min(first+manifest.batchSize,manifest.metadata30d),rows=[];
    for(let index=first;index<until;index++){const employee=index%manifest.employees,session=employee*manifest.sessionsPerEmployee+Math.floor(index/manifest.employees)%manifest.sessionsPerEmployee,account=fixtureSessionAccount(manifest,employee,session%manifest.sessionsPerEmployee,{historical:true}),at=fixtureTimestamp(manifest,index),model=manifest.models[index%4===0?1:0],state=index%1000>=996?'unknown':index%1000===995?'interrupted':index%1000===994?'rejected':index%1000===993?'cancelled':index%1000>=990?'failed':'ok',request={id:fixtureId(manifest.runId,'request',index),member_id:fixtureId(manifest.runId,'member',employee),key_id:fixtureId(manifest.runId,'key',employee),account_id:fixtureId(manifest.runId,'account',account),session_id:fixtureId(manifest.runId,'session',session),started:at-1000,ended:at,status:state,model,input:1000+index%1000,output:200+index%500,cached:500,usage_unknown:state==='unknown'?1:0,price_version:active,usage_type_id:'usage-'+String.fromCharCode(97+employee%6),usage_type_code:String.fromCharCode(65+employee%6),usage_type_name:'容量合成任务类型',usage_type_version:1},quote=quoteReference(request,book,active);rows.push({...request,price_state:quote.state,price_model:quote.model,price_model_source:quote.modelSource??null,...Object.fromEntries(['input','output','cached','adjustment','total'].map(field=>[field+'_atoms',quote[field+'Atoms']===null?null:String(quote[field+'Atoms'])]))});}
    await store.transaction(async()=>{await owned(store,manifest);await store.run(`INSERT INTO harbor.requests(id,member_id,key_id,account_id,session_id,started,ended,status,business_status,ip,model,input,output,cached,usage_unknown,price_version,route_version,source_channel,recording_state,usage_type_id,usage_type_code,usage_type_name,usage_type_version,handoff_context,handoff_input)
      SELECT p.id,p.member_id,p.key_id,p.account_id,p.session_id,p.started,p.ended,p.status,p.status,'capacity-synthetic',p.model,p.input,p.output,p.cached,p.usage_unknown,p.price_version,0,'responses','historical',p.usage_type_id,p.usage_type_code,p.usage_type_name,p.usage_type_version,'replayable','replayable' FROM jsonb_to_recordset($1::jsonb) AS p(id text,member_id text,key_id text,account_id text,session_id text,started bigint,ended bigint,status text,model text,input bigint,output bigint,cached bigint,usage_unknown int,price_version text,usage_type_id text,usage_type_code text,usage_type_name text,usage_type_version bigint) ON CONFLICT(id) DO NOTHING`,JSON.stringify(rows));
      await store.run(`INSERT INTO harbor.request_prices(request_id,state,model,model_source,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) SELECT p.id,p.price_state,p.price_model,p.price_model_source,p.price_version,p.input_atoms::numeric,p.output_atoms::numeric,p.cached_atoms::numeric,p.adjustment_atoms::numeric,p.total_atoms::numeric,$2 FROM jsonb_to_recordset($1::jsonb) AS p(id text,price_state text,price_model text,price_model_source text,price_version text,input_atoms text,output_atoms text,cached_atoms text,adjustment_atoms text,total_atoms text) ON CONFLICT(request_id) DO NOTHING`,JSON.stringify(rows),manifest.anchor);
      await store.run('UPDATE harbor_capacity.fixture SET metadata_cursor=GREATEST(metadata_cursor,$1) WHERE singleton=1',until);});await onProgress({kind:'metadata',completed:until,total:manifest.metadata30d});
  }
  cursor=await owned(store,manifest);
  for(let first=cursor.body_cursor;first<manifest.bodyRequests;first+=manifest.batchSize){
    signal?.throwIfAborted();const until=Math.min(first+manifest.batchSize,manifest.bodyRequests);
    await store.transaction(async()=>{
      await owned(store,manifest);await store.get('SELECT singleton FROM harbor_capacity.fixture WHERE singleton=1 FOR UPDATE');
      const ids=Array.from({length:until-first},(_,offset)=>fixtureId(manifest.runId,'request',first+offset)),existing=await store.get('SELECT request_id FROM harbor.recordings WHERE request_id=ANY($1::text[]) LIMIT 1',ids);
      if(!existing)await seedNewBodyBatch(store,manifest,first,until,recordingKey,signal);
      else for(let index=first;index<until;index++){
        signal?.throwIfAborted();const body=fixtureBody(manifest,index),{requestId,size,at,events}=body,recordings=new RecordingStore(store,{encryptionKey:recordingKey,maxBytes:2*1024*1024,clock:()=>at});
        // A published receipt fixes the sample. Only unfinished seed records
        // may acquire their canonical retention period during resumption.
        if(await store.get('SELECT request_id FROM harbor_capacity.body_receipts WHERE request_id=$1',requestId))continue;
        await recordings.start(requestId,{turnState:'known'});
        await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',at+recordings.retentionDays*86400000,requestId);
        await recordings.append(requestId,events);await recordings.finish(requestId,{state:body.state});
        const cipher=await store.get("SELECT md5(string_agg(md5(content_cipher),'' ORDER BY sequence)) checksum FROM harbor.conversation_events WHERE request_id=$1",requestId);
        await store.run('INSERT INTO harbor_capacity.body_receipts VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(request_id) DO NOTHING',requestId,index,size,body.encodedBytes,events.length,body.contentHash,cipher.checksum);
      }
      signal?.throwIfAborted();await store.run('UPDATE harbor_capacity.fixture SET body_cursor=GREATEST(body_cursor,$1) WHERE singleton=1',until);
    });
    await onProgress({kind:'body',completed:until,total:manifest.bodyRequests});
  }
  return verifyDataset(store,manifest,{recordingKey});
}
export async function verifyDataset(store,manifest,{recordingKey,at=Date.now(),seedOnly=false}={}){
  if(typeof seedOnly!=='boolean')throw Error('capacity_dataset_verification_scope_invalid');
  if(seedOnly&&!store.context.getStore())return store.transaction(()=>verifyDataset(store,manifest,{recordingKey,at,seedOnly}),{level:'REPEATABLE READ',readOnly:true});
  if(seedOnly&&!['repeatable read','serializable'].includes((await store.get("SELECT current_setting('transaction_isolation') isolation")).isolation))throw Error('capacity_dataset_verification_snapshot_required');
  // Body receipts cover original IDs below bodyRequests. Remaining original
  // metadata IDs come from the same generator, never from runtime traffic.
  const hasMetadataTail=manifest.bodyRequests<manifest.metadata30d,seedArgs=seedOnly&&hasMetadataTail?[manifest.runId,manifest.bodyRequests,manifest.metadata30d-1]:[];
  const metadataTail=(column,start)=>`${column} IN (SELECT md5($${start}::text||':request:'||slot::text)::uuid::text FROM generate_series($${start+1}::bigint,$${start+2}::bigint) slot)`;
  const selection=(column,start)=>!seedOnly?'TRUE':`(EXISTS(SELECT 1 FROM harbor_capacity.body_receipts original WHERE original.request_id=${column})${hasMetadataTail?' OR '+metadataTail(column,start):''})`;
  // The price scan already joins its receipt. Do not rebuild the full receipt
  // membership set for every bounded page of the original metadata scan.
  const priceSelection=!seedOnly?'TRUE':`(b.request_id IS NOT NULL${hasMetadataTail?' OR '+metadataTail('r.id',3):''})`;
  const marker=await owned(store,manifest),visible=usageWindow(at),retainedStart=visible.start-23*86400000;
  const expected=capacitySeedWindowCounts(manifest,at),activeRanges=capacitySeedActiveBodyRanges(manifest,at),expectedActiveBodies=activeRanges.reduce((n,[first,end])=>n+end-first,0),activeArgs=activeRanges.flat(),activePredicate=activeRanges.map((_,i)=>'(b.fixture_index>=$'+(i*2+1)+' AND b.fixture_index<$'+(i*2+2)+')').join(' OR ')||'FALSE';
  const counts=await store.get(`SELECT COUNT(*) request_rows,COUNT(*) FILTER(WHERE usage_at>=$3 AND usage_at<$2) metadata30d,COUNT(*) FILTER(WHERE usage_at>=$1 AND usage_at<$2) metadata7d,COUNT(*) FILTER(WHERE p.request_id IS NULL OR p.revision IS DISTINCT FROM r.price_version OR v.id IS NULL) missing_prices FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id LEFT JOIN harbor.pricing_versions v ON v.id=r.price_version WHERE ${selection('r.id',4)}`,visible.start,visible.end,retainedStart,...seedArgs),bodies=await store.get(`SELECT COUNT(*) body_requests,COUNT(*) FILTER(WHERE expires_at>$1) active_bodies,COALESCE(SUM(bytes),0)::text body_bytes,COUNT(*) FILTER(WHERE expires_at>$1 AND persisted_sequence=0) empty_bodies FROM harbor.recordings c WHERE ${selection('c.request_id',2)}`,at,...seedArgs),events=await store.get(`SELECT COUNT(*) events,COALESCE(SUM(octet_length(content_cipher)),0)::text encrypted_bytes FROM harbor.conversation_events e WHERE ${selection('e.request_id',1)}`,...seedArgs);
  const receiptArgs=[...activeArgs,manifest.runId,manifest.bodyRequests],runParameter='$'+(activeArgs.length+1),countParameter='$'+(activeArgs.length+2);
  const receipts=await store.get(`SELECT COUNT(*) receipts,COUNT(*) FILTER(WHERE c.request_id IS NULL AND NOT (${activePredicate})) expired_missing,COUNT(*) FILTER(WHERE b.fixture_index<0 OR b.fixture_index>=${countParameter} OR r.id IS NULL OR b.request_id<>md5(${runParameter}||':request:'||b.fixture_index::text)::uuid::text OR (${activePredicate}) AND (c.request_id IS NULL OR c.bytes<>b.encoded_bytes OR c.persisted_sequence<>b.event_count OR b.cipher_hash IS DISTINCT FROM (SELECT md5(string_agg(md5(e.content_cipher),'' ORDER BY e.sequence)) FROM harbor.conversation_events e WHERE e.request_id=b.request_id))) mismatches FROM harbor_capacity.body_receipts b LEFT JOIN harbor.requests r ON r.id=b.request_id LEFT JOIN harbor.recordings c ON c.request_id=b.request_id`,...receiptArgs),distribution=await store.all('SELECT source_bytes,COUNT(*) requests,SUM(encoded_bytes)::text encoded_bytes FROM harbor_capacity.body_receipts GROUP BY source_bytes ORDER BY source_bytes');let bodyReadable=false;
  if(Buffer.isBuffer(recordingKey)&&recordingKey.length===32&&marker.recording_key_digest===digest(recordingKey)){bodyReadable=true;const recordings=new RecordingStore(store,{encryptionKey:recordingKey}),samples=await store.all(`SELECT DISTINCT ON(source_bytes) request_id,content_hash FROM harbor_capacity.body_receipts b WHERE ${activePredicate} ORDER BY source_bytes,fixture_index`,...activeArgs);bodyReadable=samples.length>0;for(const sample of samples){const rows=await store.all('SELECT sequence,source_key,kind,provenance,content_cipher FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence',sample.request_id),decoded=rows.map(row=>[row.kind,row.provenance,row.source_key,recordings.open(row.content_cipher,sample.request_id+':'+row.sequence+':'+row.source_key)]);if(digest(JSON.stringify(decoded))!==sample.content_hash)bodyReadable=false;}}
  const book=await store.pricingBook(marker.pricing_revision);let pricingVerified=!!book&&digest(JSON.stringify(book))===marker.pricing_book_hash,pricingRows=0,bodyMetadataVerified=true,bodyDistributionMappingVerified=true,priceCursor='';
  while(pricingVerified){const rows=await store.all(`SELECT r.*,p.state projection_state,p.model projection_model,p.model_source projection_model_source,p.revision projection_revision,p.input_atoms::text,p.output_atoms::text,p.cached_atoms::text,p.adjustment_atoms::text,p.total_atoms::text,b.fixture_index seed_index,b.source_bytes seed_source_bytes,c.expires_at seed_expires_at FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id LEFT JOIN harbor_capacity.body_receipts b ON b.request_id=r.id LEFT JOIN harbor.recordings c ON c.request_id=r.id WHERE r.id>$1 AND ${priceSelection} ORDER BY r.id LIMIT $2`,priceCursor,manifest.batchSize,...seedArgs);if(!rows.length)break;for(const row of rows){pricingRows++;if(row.seed_index!==null){const index=row.seed_index;if(!Number.isSafeInteger(index)||index<0||index>=manifest.bodyRequests||row.id!==fixtureId(manifest.runId,'request',index)||row.usage_at!==fixtureTimestamp(manifest,index)||(row.seed_expires_at!==fixtureTimestamp(manifest,index)+30*86400000&&!(row.seed_expires_at===null&&fixtureTimestamp(manifest,index)+30*86400000<=at)))bodyMetadataVerified=false;if(row.seed_source_bytes!==bodyBytes(manifest,index))bodyDistributionMappingVerified=false;}const quote=quoteReference(row,book,marker.pricing_revision);if(row.price_version!==marker.pricing_revision||row.projection_revision!==marker.pricing_revision||row.projection_state!==quote.state||row.projection_model!==quote.model||row.projection_model_source!==(quote.modelSource??null)||['input','output','cached','adjustment','total'].some(field=>row[field+'_atoms']!==(quote[field+'Atoms']===null?null:String(quote[field+'Atoms']))))pricingVerified=false;}priceCursor=rows.at(-1).id;}
  pricingVerified=pricingVerified&&pricingRows===manifest.metadata30d;
  const verified=pricingVerified&&bodyMetadataVerified&&counts.request_rows===manifest.metadata30d&&marker.metadata_cursor===manifest.metadata30d&&marker.body_cursor===manifest.bodyRequests&&counts.metadata30d===expected.metadata30d&&counts.metadata7d===expected.metadata7d&&counts.missing_prices===0&&bodies.body_requests+receipts.expired_missing===manifest.bodyRequests&&bodies.empty_bodies===0&&bodies.active_bodies===expectedActiveBodies&&receipts.receipts===manifest.bodyRequests&&receipts.mismatches===0&&bodyReadable;
  return {manifestHash:manifest.manifestHash,source:'isolated-synthetic-dataset',verificationScope:seedOnly?'original-seed-only':'entire-isolated-database',pricingRevision:marker.pricing_revision,pricingBookHash:marker.pricing_book_hash,observedAt:at,verified,pricingVerified,pricingRows,bodyMetadataVerified,bodyDistributionMappingVerified,totalMetadata:counts.request_rows,expectedActiveBodyRequests:expectedActiveBodies,bodyVerificationScope:'active-seed-records',metadata30d:counts.metadata30d,metadata7d:counts.metadata7d,bodyRequests:bodies.body_requests,seedBodyRequests:receipts.receipts,expiredSeedBodyRequests:receipts.expired_missing,activeBodyRequests:bodies.active_bodies,bodyBytes:Number(bodies.body_bytes),encryptedBytes:Number(events.encrypted_bytes),events:events.events,bodyDistribution:distribution,declaredBodyDistribution:manifest.bodyDistribution,bodyReadable,bodyCoverage:manifest.bodyRequests/manifest.metadata30d,formalBodyFootprintVerified:verified&&manifest.bodyRequests===manifest.metadata30d&&bodies.active_bodies>=counts.metadata30d&&distribution.length===manifest.bodyDistribution.length,bodyDistributionVerified:verified&&distribution.length===manifest.bodyDistribution.length,progress:{metadata:marker.metadata_cursor,body:marker.body_cursor},formalScale:verified&&counts.metadata30d>=5400000&&counts.metadata7d>=1260000};
}


async function contextCipherHashes(store,ids,contextKeyVersion){
 return new Map((await store.all(`SELECT s.request_id,CASE WHEN BOOL_AND(COALESCE(o.key_version=$2 AND o.state='complete' AND o.scope_digest=s.scope_digest AND p.object_id IS NOT NULL AND p.ordinal>=1 AND p.ordinal<=o.parts,false)) AND COUNT(DISTINCT ref.ordinal)=MAX(s.components) AND MIN(ref.ordinal)=1 AND MAX(ref.ordinal)=COUNT(DISTINCT ref.ordinal) THEN md5(string_agg(md5(jsonb_build_array(r.member_id,r.session_id,session.member_id,r.recording_state,c.expires_at,s.member_id,s.session_id,encode(s.scope_digest,'hex'),s.key_version,s.state,s.reason,s.coverage,s.seen,s.components,s.bytes,s.expires_at,s.created,encode(s.fingerprint,'hex'),ref.ordinal,ref.truncated,o.id,encode(o.scope_digest,'hex'),encode(o.fingerprint,'hex'),o.key_version,o.state,o.kind,o.bytes,o.parts,p.ordinal,p.bytes,md5(p.content_cipher))::text),'' ORDER BY ref.ordinal,p.ordinal)) ELSE NULL END value
  FROM harbor.recording_context_snapshots s JOIN harbor.requests r ON r.id=s.request_id LEFT JOIN harbor.sessions session ON session.id=r.session_id JOIN harbor.recordings c ON c.request_id=s.request_id LEFT JOIN harbor.recording_context_references ref ON ref.request_id=s.request_id LEFT JOIN harbor.recording_context_objects o ON o.id=ref.object_id LEFT JOIN harbor.recording_context_parts p ON p.object_id=o.id WHERE s.request_id=ANY($1::text[]) AND s.key_version=$2 GROUP BY s.request_id`,ids,contextKeyVersion)).map(row=>[row.request_id,row.value]));
}

// Source coverage is separate from index/storage/runtime qualification. This
// verifier never creates missing receipts or repairs the original corpus.
export async function verifyDatasetContexts(store,manifest,{recordingKey,at=Date.now(),signal}={}){
 if(!Number.isSafeInteger(at)||at<0)throw Error('capacity_dataset_window_invalid');
 if(!store.context.getStore())return store.transaction(()=>verifyDatasetContexts(store,manifest,{recordingKey,at,signal}),{level:'REPEATABLE READ',readOnly:true});
 if(!['repeatable read','serializable'].includes((await store.get("SELECT current_setting('transaction_isolation') isolation")).isolation))throw Error('capacity_dataset_verification_snapshot_required');
 signal?.throwIfAborted();const marker=await owned(store,manifest);
 if(!Buffer.isBuffer(recordingKey)||recordingKey.length!==32||marker.recording_key_digest!==digest(recordingKey))throw Error('capacity_dataset_recording_key_invalid');
 const report={manifestHash:manifest.manifestHash,observedAt:at,scope:'original-context-source-only',verified:false,receipts:0,active:0,expired:0,mismatches:0,readableSizes:[],missing:[],formalQualified:false};
 if(!(await store.get("SELECT to_regclass('harbor_capacity.context_receipts') present")).present)return {...report,missing:['context_receipts_missing']};
 const contexts=new RecordingContextStore(store,{encryptionKey:recordingKey,clock:()=>at}),generator=digest(readFileSync(new URL('./context-payload.mjs',import.meta.url))),samples=new Map();let cursor=-1;
 for(;;){
  signal?.throwIfAborted();
  const rows=await store.all(`SELECT b.*,r.id source_id,r.member_id source_member,r.session_id source_session,session.member_id session_member,c.expires_at body_expires,s.request_id snapshot_id,s.member_id snapshot_member,s.session_id snapshot_session,s.state,s.reason,s.bytes,s.components,s.seen,s.expires_at snapshot_expires
   FROM harbor_capacity.context_receipts b LEFT JOIN harbor.requests r ON r.id=b.request_id LEFT JOIN harbor.sessions session ON session.id=r.session_id LEFT JOIN harbor.recordings c ON c.request_id=r.id LEFT JOIN harbor.recording_context_snapshots s ON s.request_id=r.id WHERE b.fixture_index>$1 ORDER BY b.fixture_index LIMIT $2`,cursor,manifest.batchSize);
  if(!rows.length)break;
  const live=rows.filter(row=>Number.isSafeInteger(row.fixture_index)&&row.fixture_index>=0&&row.fixture_index<manifest.bodyRequests&&fixtureTimestamp(manifest,row.fixture_index)+30*86400000>at),hashes=await contextCipherHashes(store,live.map(row=>row.request_id),contexts.keyVersion);
  for(const row of rows){
   report.receipts++;const index=row.fixture_index;
   if(!Number.isSafeInteger(index)||index<0||index>=manifest.bodyRequests){report.mismatches++;continue;}
   const expires=fixtureTimestamp(manifest,index)+30*86400000,employee=index%manifest.employees,member=fixtureId(manifest.runId,'member',employee),session=fixtureId(manifest.runId,'session',employee*manifest.sessionsPerEmployee+Math.floor(index/manifest.employees)%manifest.sessionsPerEmployee),bytes=bodyBytes(manifest,index),active=expires>at;
   if(active)report.active++;else report.expired++;
   if(row.request_id!==fixtureId(manifest.runId,'request',index)||row.source_id!==row.request_id||row.source_member!==member||row.source_session!==session||row.session_member!==member||row.generator_hash!==generator||row.source_bytes!==bytes||!/^[a-f0-9]{64}$/.test(row.content_hash)||row.body_expires!==expires&&!(row.body_expires===null&&!active)){report.mismatches++;continue;}
   if(!active)continue;
   if(row.snapshot_id!==row.request_id||row.snapshot_member!==member||row.snapshot_session!==session||row.snapshot_expires!==expires||row.state!=='complete'||row.reason!==null||row.components!==5||row.seen!==5||row.bytes!==bytes||!hashes.get(row.request_id)||hashes.get(row.request_id)!==row.cipher_hash){report.mismatches++;continue;}
   if(!samples.has(bytes))samples.set(bytes,row);
  }
  cursor=rows.at(-1).fixture_index;
 }
 // The positive keyset cursor must not conceal corrupt negative receipts.
 if(await store.get('SELECT request_id FROM harbor_capacity.context_receipts WHERE fixture_index<0 LIMIT 1'))report.mismatches++;
 for(const [bytes,row] of samples){
  signal?.throwIfAborted();const components=new Map();let after={component:0,part:0};
  for(;;){signal?.throwIfAborted();const page=await contexts.read(row.request_id,{after});if(page.state!=='complete')throw Error('capacity_context_source_unreadable');for(const part of page.parts){const item=components.get(part.component)??{kind:part.kind,text:''};item.text+=part.text;components.set(part.component,item);}if(!page.hasMore)break;after=page.next;}
  const decoded=[...components.values()].map(row=>({kind:row.kind,content:JSON.parse(row.text)})),expected=capacityHistoricalContext(row.fixture_index,bytes);
  if(JSON.stringify(decoded)!==JSON.stringify(expected.components)||expected.contentHash!==row.content_hash){report.mismatches++;continue;}
  report.readableSizes.push(bytes);
 }
 const expectedActive=capacitySeedActiveBodyRanges(manifest,at).reduce((n,[first,end])=>n+end-first,0);
 if(report.receipts!==manifest.bodyRequests)report.missing.push('context_receipt_coverage_incomplete');
 if(report.active!==expectedActive)report.missing.push('context_active_coverage_incomplete');
 if(report.mismatches)report.missing.push('context_source_mismatch');
 signal?.throwIfAborted();report.readableSizes.sort((a,b)=>a-b);report.verified=!report.missing.length;return report;
}

// Explicit construction stage only. It does not extend verifyDataset or mint a
// formal profile until context coverage, indexes and storage are also checked.
export async function seedDatasetContexts(store,manifest,{recordingKey,signal,onProgress=async()=>{},first=0,until=manifest.bodyRequests}={}){
 const marker=await owned(store,manifest);
 if(!Buffer.isBuffer(recordingKey)||recordingKey.length!==32||marker.recording_key_digest!==digest(recordingKey))throw Error('capacity_dataset_recording_key_invalid');
 if(marker.body_cursor!==manifest.bodyRequests||!Number.isSafeInteger(first)||!Number.isSafeInteger(until)||first<0||until<first||until>manifest.bodyRequests)throw Error('capacity_context_seed_range_invalid');
 signal?.throwIfAborted();
 await store.query('CREATE TABLE IF NOT EXISTS harbor_capacity.context_receipts(request_id text PRIMARY KEY,fixture_index bigint UNIQUE NOT NULL,generator_hash text NOT NULL,source_bytes bigint NOT NULL,content_hash text NOT NULL,cipher_hash text NOT NULL)');
 const contextGenerator=digest(readFileSync(new URL('./context-payload.mjs',import.meta.url))),contexts=new RecordingContextStore(store,{encryptionKey:recordingKey,clock:()=>manifest.anchor}),contextKeyVersion=contexts.keyVersion;
 const cipherHashes=ids=>contextCipherHashes(store,ids,contextKeyVersion);
 let snapshots=0,reused=0;
 for(let batch=first;batch<until;batch+=manifest.batchSize){
  const end=Math.min(until,batch+manifest.batchSize);signal?.throwIfAborted();
  const result=await store.transaction(async()=>{
   await owned(store,manifest);await store.get('SELECT singleton FROM harbor_capacity.fixture WHERE singleton=1 FOR UPDATE');let saved=0,existing=0;
   const ids=Array.from({length:end-batch},(_,offset)=>fixtureId(manifest.runId,'request',batch+offset)),sources=await store.all(`SELECT b.request_id,b.fixture_index body_index,c.request_id receipt_id,c.fixture_index receipt_index,c.generator_hash,c.source_bytes,c.content_hash,c.cipher_hash FROM harbor_capacity.body_receipts b LEFT JOIN harbor_capacity.context_receipts c ON c.request_id=b.request_id WHERE b.request_id=ANY($1::text[])`,ids),byId=new Map(sources.map(row=>[row.request_id,row])),fresh=[],prior=[];
   for(let index=batch;index<end;index++){
    signal?.throwIfAborted();const requestId=fixtureId(manifest.runId,'request',index),source=byId.get(requestId);if(source?.body_index!==index)throw Error('capacity_context_seed_source_missing');
    const payload=capacityHistoricalContext(index,bodyBytes(manifest,index));
    if(source.receipt_id){if(source.receipt_index!==index||source.generator_hash!==contextGenerator||source.source_bytes!==payload.bytes||source.content_hash!==payload.contentHash)throw Error('capacity_context_seed_receipt_mismatch');prior.push(source);}else fresh.push({requestId,index,...payload});
   }
   if(prior.length){const hashes=await cipherHashes(prior.map(row=>row.request_id));for(const row of prior)if(!hashes.get(row.request_id)||hashes.get(row.request_id)!==row.cipher_hash)throw Error('capacity_context_seed_cipher_mismatch');existing=prior.length;}
   let pending=[],pendingBytes=0;
   const write=async()=>{
    if(!pending.length)return;const receipts=await contexts.writeSnapshotBatch(pending.map(row=>({requestId:row.requestId,components:row.components,coverage:{state:'complete',reasons:[],opaqueReasoningExcluded:0}})),{signal});
    if(receipts.length!==pending.length||receipts.some((row,index)=>row.requestId!==pending[index].requestId||row.state!=='complete'||row.bytes!==pending[index].bytes||row.components!==pending[index].components.length))throw Error('capacity_context_seed_incomplete');pending=[];pendingBytes=0;
   };
   for(const row of fresh){signal?.throwIfAborted();if(pendingBytes+row.bytes>contexts.maxSnapshotBytes||pending.reduce((count,entry)=>count+entry.components.length,0)+row.components.length>contexts.maxComponents)await write();pending.push(row);pendingBytes+=row.bytes;}await write();
   if(fresh.length){
    const hashes=await cipherHashes(fresh.map(row=>row.requestId));if(fresh.some(row=>!hashes.get(row.requestId)))throw Error('capacity_context_seed_cipher_mismatch');
    await store.run('INSERT INTO harbor_capacity.context_receipts SELECT * FROM unnest($1::text[],$2::bigint[],$3::text[],$4::bigint[],$5::text[],$6::text[])',fresh.map(row=>row.requestId),fresh.map(row=>row.index),fresh.map(()=>contextGenerator),fresh.map(row=>row.bytes),fresh.map(row=>row.contentHash),fresh.map(row=>hashes.get(row.requestId)));saved=fresh.length;
   }
   signal?.throwIfAborted();return {saved,existing};
  });
  snapshots+=result.saved;reused+=result.existing;await onProgress({kind:'context',completed:end,total:until});
 }
 return {scope:'explicit-historical-context-construction',first,until,snapshots,reused,generatorHash:contextGenerator,formalQualified:false};
}
