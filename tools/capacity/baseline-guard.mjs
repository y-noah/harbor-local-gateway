import {randomUUID,createHash} from 'node:crypto';
import {PostgresStore} from '../../postgres-store.mjs';
import {datasetManifest,verifyDataset,verifyDatasetContexts} from './dataset.mjs';
import {createRecordingIndexCrypto} from '../../recording-search-index.mjs';
import {capacitySeedWindowCounts,capacitySeedWindowIndexRanges} from './dataset-time.mjs';

const guards=new WeakMap(),fail=code=>{throw Error('capacity_baseline_'+code);};
const sources=[['harbor.requests','source'],['harbor.recordings','source'],['harbor.conversation_events','source'],['harbor.request_prices','source'],['harbor.recording_work','index'],['harbor.recording_documents','index'],['harbor.recording_document_parts','index'],['harbor_capacity.body_receipts','receipt'],['harbor.gateway_settings','settings'],['harbor.pricing_versions','pricing']];
const contextSources=[['harbor.sessions','context_session'],['harbor.recording_context_snapshots','source'],['harbor.recording_context_references','source'],['harbor.recording_context_objects','context_object'],['harbor.recording_context_parts','context_part'],['harbor.recording_context_work','context_index'],['harbor.recording_context_documents','context_index'],['harbor.recording_context_document_parts','context_index'],['harbor_capacity.context_receipts','receipt']];
const rowFunction=`CREATE FUNCTION harbor_capacity.capture_baseline_loss() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE slots bigint[]; refs text[]; sealed boolean; revision text; changed boolean;
BEGIN
 IF TG_ARGV[0] IN ('index','context_index') THEN
  SELECT is_sealed INTO sealed FROM harbor_capacity.baseline_guard WHERE singleton=1 FOR SHARE;
  IF NOT sealed THEN RETURN NULL; END IF;
 END IF;
 IF TG_ARGV[0]='settings' THEN
  IF TG_OP='UPDATE' THEN
   IF OLD.name<>'pricing.state' AND NEW.name<>'pricing.state' THEN RETURN NULL; END IF;
   BEGIN changed:=OLD.name IS DISTINCT FROM NEW.name OR (OLD.value::jsonb->>'active') IS DISTINCT FROM (NEW.value::jsonb->>'active'); EXCEPTION WHEN OTHERS THEN changed:=true; END;
  ELSIF TG_OP='DELETE' THEN changed:=OLD.name='pricing.state'; ELSE changed:=NEW.name='pricing.state'; END IF;
  IF changed THEN UPDATE harbor_capacity.baseline_guard SET invalidated=true WHERE singleton=1; END IF; RETURN NULL;
 ELSIF TG_ARGV[0]='pricing' THEN
  SELECT pricing_revision INTO revision FROM harbor_capacity.baseline_guard WHERE singleton=1;
  IF TG_OP='DELETE' THEN changed:=OLD.id=revision;
  ELSIF TG_OP='INSERT' THEN changed:=NEW.id=revision;
  ELSE changed:=OLD.id=revision AND (OLD.id IS DISTINCT FROM NEW.id OR OLD.book IS DISTINCT FROM NEW.book); END IF;
  IF changed THEN UPDATE harbor_capacity.baseline_guard SET invalidated=true WHERE singleton=1; END IF; RETURN NULL;
 ELSIF TG_ARGV[0]='context_session' THEN
  IF TG_OP='UPDATE' AND OLD.id IS NOT DISTINCT FROM NEW.id AND OLD.member_id IS NOT DISTINCT FROM NEW.member_id THEN RETURN NULL; END IF;
  IF TG_OP='INSERT' THEN refs:=ARRAY[NEW.id]; ELSIF TG_OP='DELETE' THEN refs:=ARRAY[OLD.id]; ELSE refs:=ARRAY[OLD.id,NEW.id]; END IF;
  SELECT array_agg(DISTINCT b.fixture_index) INTO slots FROM harbor_capacity.body_receipts b JOIN harbor.requests r ON r.id=b.request_id WHERE r.session_id=ANY(refs);
 ELSIF TG_ARGV[0] IN ('context_object','context_part','context_index') THEN
  IF TG_ARGV[0]='context_object' THEN
   IF TG_OP='INSERT' THEN refs:=ARRAY[NEW.id]; ELSIF TG_OP='DELETE' THEN refs:=ARRAY[OLD.id]; ELSE refs:=ARRAY[OLD.id,NEW.id]; END IF;
  ELSE
   IF TG_OP='INSERT' THEN refs:=ARRAY[NEW.object_id]; ELSIF TG_OP='DELETE' THEN refs:=ARRAY[OLD.object_id]; ELSE refs:=ARRAY[OLD.object_id,NEW.object_id]; END IF;
  END IF;
  SELECT array_agg(DISTINCT b.fixture_index) INTO slots FROM harbor_capacity.body_receipts b JOIN harbor.recording_context_references ref ON ref.request_id=b.request_id WHERE ref.object_id=ANY(refs);
 ELSIF TG_ARGV[0]='receipt' THEN
  IF TG_OP='INSERT' THEN slots:=ARRAY[NEW.fixture_index]; ELSIF TG_OP='DELETE' THEN slots:=ARRAY[OLD.fixture_index]; ELSE slots:=ARRAY[OLD.fixture_index,NEW.fixture_index]; END IF;
 ELSE
  IF TG_TABLE_NAME='requests' THEN
   IF TG_OP='INSERT' THEN refs:=ARRAY[NEW.id]; ELSIF TG_OP='DELETE' THEN refs:=ARRAY[OLD.id]; ELSE refs:=ARRAY[OLD.id,NEW.id]; END IF;
  ELSE
   IF TG_OP='INSERT' THEN refs:=ARRAY[NEW.request_id]; ELSIF TG_OP='DELETE' THEN refs:=ARRAY[OLD.request_id]; ELSE refs:=ARRAY[OLD.request_id,NEW.request_id]; END IF;
  END IF;
  SELECT array_agg(DISTINCT fixture_index) INTO slots FROM harbor_capacity.body_receipts WHERE request_id=ANY(refs);
 END IF;
 IF slots IS NULL THEN RETURN NULL; END IF;
 IF EXISTS(SELECT 1 FROM unnest(slots) slot WHERE slot<0 OR slot>=(SELECT seed_rows FROM harbor_capacity.baseline_guard WHERE singleton=1)) THEN
  UPDATE harbor_capacity.baseline_guard SET invalidated=true WHERE singleton=1; RETURN NULL;
 END IF;
 INSERT INTO harbor_capacity.baseline_losses(fixture_index) SELECT DISTINCT slot FROM unnest(slots) slot ON CONFLICT DO NOTHING;
 RETURN NULL;
END $$`;
const truncateFunction=`CREATE FUNCTION harbor_capacity.capture_baseline_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN UPDATE harbor_capacity.baseline_guard SET invalidated=true WHERE singleton=1; RETURN NULL; END $$`;
const identity=store=>store.get("SELECT current_database() database,(SELECT system_identifier::text FROM pg_control_system()) cluster,extract(epoch FROM pg_postmaster_start_time())::text epoch,to_regclass('harbor_capacity.baseline_guard')::oid::text guard_oid,to_regclass('harbor_capacity.baseline_losses')::oid::text losses_oid");
async function protection(store,tables=sources){
 const rows=await store.all("SELECT tgrelid::text relation,tgname,tgenabled,pg_get_triggerdef(oid) definition,pg_get_functiondef(tgfoid) function FROM pg_trigger WHERE tgname IN ('capacity_baseline_row','capacity_baseline_truncate') AND tgrelid=ANY($1::regclass[]) ORDER BY tgrelid,tgname",tables.map(([name])=>name));
 if(rows.length!==tables.length*2||rows.some(row=>row.tgenabled!=='A'))return null;
 return createHash('sha256').update(JSON.stringify(rows)).digest('hex');
}
// Install inside the source-verification transaction, before traffic starts.
// The handle measures preservation; it does not authorize formal capacity.
export async function installCapacityBaselineGuard(store,manifest,pricingRevision,{recordingContext=false,recordingKey}={}){
 const canonical=datasetManifest(manifest);
 if(typeof recordingContext!=='boolean'||!store.context.getStore()||JSON.stringify(canonical)!==JSON.stringify(manifest)||manifest.bodyRequests!==manifest.metadata30d||typeof pricingRevision!=='string')fail('configuration');
 manifest=Object.freeze(canonical);
 const tables=recordingContext?[...sources,...contextSources]:sources;let contextIndexKey=null;
 if(recordingContext){const verified=await verifyDatasetContexts(store,manifest,{recordingKey});if(!verified.verified)fail('context_source_unverified');contextIndexKey=createRecordingIndexCrypto(recordingKey).keyVersion;}
 const at=Date.now();if(manifest.anchor>at)fail('future_seed');const nonce=randomUUID();
 await store.query('CREATE TABLE harbor_capacity.baseline_guard(singleton integer PRIMARY KEY CHECK(singleton=1),nonce text NOT NULL,seed_rows bigint NOT NULL,pricing_revision text NOT NULL,is_sealed boolean NOT NULL DEFAULT false,sealed_at bigint,invalidated boolean NOT NULL DEFAULT false,context_enabled boolean NOT NULL DEFAULT false,context_index_key text)');
 await store.query('CREATE TABLE harbor_capacity.baseline_losses(fixture_index bigint PRIMARY KEY)');
 await store.run('INSERT INTO harbor_capacity.baseline_guard(singleton,nonce,seed_rows,pricing_revision,context_enabled,context_index_key) VALUES(1,$1,$2,$3,$4,$5)',nonce,manifest.metadata30d,pricingRevision,recordingContext,contextIndexKey);
 const current=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'");let active;try{active=JSON.parse(current?.value).active;}catch{}
 if(active!==pricingRevision)await store.run('UPDATE harbor_capacity.baseline_guard SET invalidated=true WHERE singleton=1');
 await store.query(rowFunction);await store.query(truncateFunction);
 for(const [name,mode] of tables){
  await store.query(`CREATE TRIGGER capacity_baseline_row AFTER INSERT OR UPDATE OR DELETE ON ${name} FOR EACH ROW EXECUTE FUNCTION harbor_capacity.capture_baseline_loss('${mode}')`);
  await store.query(`CREATE TRIGGER capacity_baseline_truncate BEFORE TRUNCATE ON ${name} FOR EACH STATEMENT EXECUTE FUNCTION harbor_capacity.capture_baseline_truncate()`);
  await store.query(`ALTER TABLE ${name} ENABLE ALWAYS TRIGGER capacity_baseline_row`);await store.query(`ALTER TABLE ${name} ENABLE ALWAYS TRIGGER capacity_baseline_truncate`);
 }
 const handle=Object.freeze(Object.create(null));guards.set(handle,{store,manifest,nonce,pricingRevision,recordingContext,contextIndexKey,tables,at,identity:await identity(store),protection:await protection(store,tables),unreliable:false,invalidated:false,losses:Buffer.alloc(Math.ceil(manifest.metadata30d/8)),segments:[],segmentFrom:null,sealedAt:null,lastFacts:null,suspended:false,pending:Promise.resolve()});return handle;
}
async function checked(handle){
 const record=guards.get(handle);if(!record)fail('not_owned');
 const actual=await identity(record.store);if(JSON.stringify(actual)!==JSON.stringify(record.identity))fail('source_changed');
 if(record.unreliable||!record.protection||await protection(record.store,record.tables)!==record.protection){record.unreliable=true;fail('protection_changed');}
 const state=await record.store.get('SELECT * FROM harbor_capacity.baseline_guard WHERE singleton=1');if(state?.nonce!==record.nonce||state.seed_rows!==record.manifest.metadata30d||state.pricing_revision!==record.pricingRevision||state.context_enabled!==record.recordingContext||state.context_index_key!==record.contextIndexKey){record.unreliable=true;fail('binding_changed');}return {record,state};
}
export async function sealCapacityBaselineGuard(handle){
 const record=guards.get(handle);if(!record)fail('not_owned');
 return record.store.transaction(async()=>{
  await record.store.query('SELECT singleton FROM harbor_capacity.baseline_guard WHERE singleton=1 FOR UPDATE');const {state}=await checked(handle);if(state.is_sealed)return {sealed:true,at:state.sealed_at};const at=Date.now();
  await record.store.run('UPDATE harbor_capacity.baseline_guard SET is_sealed=true,sealed_at=$1 WHERE singleton=1',at);
  await captureIndexLosses(record.store,at);
  if(record.recordingContext)await captureContextIndexLosses(record.store,at,record.contextIndexKey);
  return {sealed:true,at};
 });
}
// Bounded parent-owned bitmap survives a physical rollback of the database.
// Each bit denotes one original row; runtime rows cannot compensate a loss.
const popcount=Uint8Array.from({length:256},(_,value)=>{let count=0;for(;value;value&=value-1)count++;return count;});
const mark=(bits,index)=>{bits[Math.floor(index/8)]|=1<<(index&7);};
function rangeLosses(bits,ranges){let count=0;for(const [first,end] of ranges){let index=first;while(index<end&&(index&7))count+=(bits[Math.floor(index/8)]>>(index++&7))&1;while(index+8<=end){count+=popcount[bits[Math.floor(index/8)]];index+=8;}while(index<end)count+=(bits[Math.floor(index/8)]>>(index++&7))&1;}return count;}
function serialized(handle,operation){
 const record=guards.get(handle);if(!record)fail('not_owned');if(record.store.context.getStore())fail('nested_transaction');
 const result=record.pending.then(()=>operation(record));record.pending=result.catch(()=>{});return result;
}
async function captureIndexLosses(store,at){
  await store.run(`INSERT INTO harbor_capacity.baseline_losses(fixture_index)
   SELECT b.fixture_index FROM harbor_capacity.body_receipts b JOIN harbor.requests r ON r.id=b.request_id
   JOIN harbor.recordings c ON c.request_id=r.id LEFT JOIN harbor.recording_work w ON w.request_id=r.id LEFT JOIN harbor.recording_documents d ON d.request_id=r.id
   LEFT JOIN LATERAL(SELECT COUNT(*) parts,MIN(ordinal) first,MAX(ordinal) last,COALESCE(SUM(bytes),0) bytes,COALESCE(BOOL_OR(bytes<0 OR octet_length(payload_cipher)=0),false) invalid FROM harbor.recording_document_parts p WHERE p.request_id=d.request_id) p ON true
   WHERE c.expires_at>$1 AND NOT COALESCE(w.indexed_revision>=w.revision AND w.last_error IS NULL AND d.source_revision=w.revision AND d.source_sequence=c.persisted_sequence AND d.state=r.recording_state AND d.turn_state=c.turn_state AND d.expires_at=c.expires_at AND d.parts=p.parts AND d.bytes=p.bytes AND NOT p.invalid AND (d.parts=0 OR p.first=1 AND p.last=d.parts),false)
   ON CONFLICT DO NOTHING`,at);
}
async function captureContextIndexLosses(store,at,keyVersion){
 await store.run(`INSERT INTO harbor_capacity.baseline_losses(fixture_index)
  SELECT DISTINCT b.fixture_index FROM harbor_capacity.body_receipts b
  JOIN harbor.recording_context_snapshots s ON s.request_id=b.request_id JOIN harbor.recordings c ON c.request_id=s.request_id
  LEFT JOIN harbor.recording_context_references ref ON ref.request_id=s.request_id
  LEFT JOIN harbor.recording_context_objects o ON o.id=ref.object_id
  LEFT JOIN harbor.recording_context_work w ON w.object_id=o.id LEFT JOIN harbor.recording_context_documents d ON d.object_id=o.id
  LEFT JOIN LATERAL(SELECT COUNT(*) parts,MIN(ordinal) first,MAX(ordinal) last,COALESCE(SUM(bytes),0) bytes,COALESCE(BOOL_OR(bytes<0 OR octet_length(payload_cipher)<>bytes+28),false) invalid FROM harbor.recording_context_document_parts p WHERE p.object_id=d.object_id) p ON true
  WHERE LEAST(s.expires_at,c.expires_at)>$1 AND NOT COALESCE(
   o.state='complete' AND w.indexed_revision>=w.revision AND w.last_error IS NULL
   AND d.source_revision=w.revision AND d.source_key_version=o.key_version AND d.source_fingerprint=o.fingerprint
   AND d.key_version=$2 AND d.format_version=1 AND d.parts>0 AND d.parts=p.parts AND d.bytes=p.bytes
   AND NOT p.invalid AND p.first=1 AND p.last=d.parts,false)
  ON CONFLICT DO NOTHING`,at,keyVersion);
}
async function captureLosses(record){
 const bits=Buffer.from(record.losses);let cursor=-1;
 for(;;){const rows=await record.store.all('SELECT fixture_index FROM harbor_capacity.baseline_losses WHERE fixture_index>$1 ORDER BY fixture_index LIMIT 4096',cursor);if(!rows.length)break;for(const row of rows){if(!Number.isSafeInteger(row.fixture_index)||row.fixture_index<0||row.fixture_index>=record.manifest.metadata30d)fail('losses_invalid');mark(bits,row.fixture_index);}cursor=rows.at(-1).fixture_index;}
 // The positive cursor cannot conceal a negative index.
 if(await record.store.get('SELECT fixture_index FROM harbor_capacity.baseline_losses WHERE fixture_index<0 LIMIT 1'))fail('losses_invalid');
 return bits;
}
async function readRecord(handle,record){
 if(record.suspended)return {protected:false,available:false,sealed:true,reason:'baseline_suspended',segments:record.segments.map(row=>({...row})),formalQualified:false};
 // Bound the claim before any query can establish the transaction snapshot.
 const at=Date.now();
 const result=await record.store.transaction(async()=>{
  const {state}=await checked(handle);const bits=await captureLosses(record);
  return {state,bits};
 },{level:'REPEATABLE READ',readOnly:true});
 record.losses=result.bits;record.invalidated ||= result.state.invalidated;const state=result.state;if(state.is_sealed)record.sealedAt??=state.sealed_at;
 if(!state.is_sealed||record.invalidated)return {protected:true,sealed:state.is_sealed,available:false,reason:record.invalidated?'baseline_invalidated':'baseline_unsealed',segments:record.segments.map(row=>({...row})),formalQualified:false};
 const from=record.segmentFrom??state.sealed_at;if(at<from)fail('clock_reversed');
 const expected=capacitySeedWindowCounts(record.manifest,at),lost7=rangeLosses(record.losses,capacitySeedWindowIndexRanges(record.manifest,at,'7d')),lost30=rangeLosses(record.losses,capacitySeedWindowIndexRanges(record.manifest,at,'30d'));
 const lost=record.losses.reduce((count,byte)=>count+popcount[byte],0);
 const facts={protected:true,sealed:true,available:true,method:'monotone-seed-loss-lower-bound',...(record.recordingContext?{recordingContextProtected:true}:{}),manifestHash:record.manifest.manifestHash,sourceCluster:record.identity.cluster,sourceDatabase:record.identity.database,from,through:at,pricingRevision:state.pricing_revision,lostSeedRows:lost,metadata7d:expected.metadata7d-lost7,metadata30d:expected.metadata30d-lost30,segments:record.segments.map(row=>({...row})),formalQualified:false};
 const {segments,...interval}=facts;record.lastFacts=Object.freeze(interval);return facts;
}
export async function readCapacityBaselineGuard(handle){return serialized(handle,record=>readRecord(handle,record));}
// Called before the fault while requests are still running; never drains or
// flushes WAL. The final observed snapshot closes only the old interval.
export async function suspendCapacityBaselineGuard(handle){return serialized(handle,async record=>{
 if(record.suspended)fail('already_suspended');const facts=await readRecord(handle,record);const {segments,...segment}=facts.available?facts:record.lastFacts??{};
 if(segment.available)record.segments.push(Object.freeze(segment));record.suspended=true;return {...segment,available:facts.available,suspended:true};
});}
// Recheck actual recovered seed and trigger/index protection, never a caller's
// verification DTO. Source identity may change only its postmaster epoch.
export async function resumeCapacityBaselineGuard(handle,{recordingKey,store}={}){return serialized(handle,async record=>{
 if(!record.suspended)fail('not_suspended');if(record.unreliable)fail('protection_changed');
 const target=store??record.store;if(!(target instanceof PostgresStore)||target.context.getStore())fail('restore_store_invalid');
 const result=await target.transaction(async()=>{
  const actual=await identity(target),previous=record.identity;
  if(actual.epoch===previous.epoch||Object.keys(previous).some(key=>key!=='epoch'&&actual[key]!==previous[key]))fail('restore_source_mismatch');
  if(!record.protection||await protection(target,record.tables)!==record.protection){record.unreliable=true;fail('protection_changed');}
  const state=await target.get('SELECT * FROM harbor_capacity.baseline_guard WHERE singleton=1');
  if(state?.nonce!==record.nonce||state.seed_rows!==record.manifest.metadata30d||state.pricing_revision!==record.pricingRevision||state.context_enabled!==record.recordingContext||state.context_index_key!==record.contextIndexKey||state.is_sealed!==true||state.sealed_at!==record.sealedAt)fail('binding_changed');
  const verified=await verifyDataset(target,record.manifest,{recordingKey,seedOnly:true});if(!verified.verified||verified.pricingRevision!==record.pricingRevision)fail('restored_seed_unverified');
  if(record.recordingContext){const contexts=await verifyDatasetContexts(target,record.manifest,{recordingKey});if(!contexts.verified||createRecordingIndexCrypto(recordingKey).keyVersion!==record.contextIndexKey)fail('restored_context_unverified');await captureContextIndexLosses(target,Date.now(),record.contextIndexKey);}
  await captureIndexLosses(target,Date.now());const bits=await captureLosses({...record,store:target});
  return {actual,state,bits};
 },{level:'REPEATABLE READ'});
 record.store=target;record.identity=result.actual;record.losses=result.bits;record.invalidated ||= result.state.invalidated;record.segmentFrom=Date.now();record.suspended=false;
 return {resumed:true,from:record.segmentFrom,previousThrough:record.segments.at(-1)?.through??null,formalQualified:false};
});}
