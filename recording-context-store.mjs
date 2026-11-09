import {createCipheriv,createDecipheriv,createHash,createHmac,hkdfSync,randomBytes,randomUUID,timingSafeEqual} from 'node:crypto';
import {AppError} from './store.mjs';
import {sanitizeRecord,restoreAuthenticatedRecord} from './record-redaction.mjs';
import {requestContextReasons} from './recording-request-context.mjs';

const kinds=new Set(['instruction','history','tool_schema','context']);
const captureReasons=new Set(requestContextReasons);
const fail=(status,message,code='recording_context_invalid')=>{throw new AppError(status,message,code);};
const equal=(a,b)=>Buffer.isBuffer(a)&&Buffer.isBuffer(b)&&a.length===b.length&&timingSafeEqual(a,b);
// Each component is JSON, split only at UTF-8 boundaries. The source passed to
// this repository is already filtered; this second boundary remains defensive.
function chunks(bytes,limit){const result=[];for(let start=0;start<bytes.length;){let end=Math.min(start+limit,bytes.length);if(end<bytes.length)while(end>start&&(bytes[end]&0xc0)===0x80)end--;result.push(bytes.subarray(start,end));start=end;}return result;}
export class RecordingContextStore{
 constructor(store,{encryptionKey,clock=Date.now,maxSnapshotBytes=2097152,maxComponents=4096,partBytes=65536,maxReadBytes=131072}={}){
  if(!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32||typeof clock!=='function'||!Number.isSafeInteger(maxSnapshotBytes)||maxSnapshotBytes<256||maxSnapshotBytes>8388608||!Number.isInteger(maxComponents)||maxComponents<1||maxComponents>16384||!Number.isInteger(partBytes)||partBytes<256||partBytes>131072||partBytes>maxSnapshotBytes||!Number.isInteger(maxReadBytes)||maxReadBytes<partBytes+28||maxReadBytes>2097152)fail(503,'上下文仓储配置无效','recording_context_configuration');
  this.store=store;this.clock=clock;this.maxSnapshotBytes=maxSnapshotBytes;this.maxComponents=maxComponents;this.partBytes=partBytes;this.maxReadBytes=maxReadBytes;this.gcScope=null;
  this.key=Buffer.from(hkdfSync('sha256',encryptionKey,Buffer.alloc(0),'harbor-recording-context-encryption-v1',32));
  this.fingerprintKey=Buffer.from(hkdfSync('sha256',encryptionKey,Buffer.alloc(0),'harbor-recording-context-fingerprint-v1',32));
  this.keyVersion=createHash('sha256').update(this.key).digest('hex');
 }
 #digest(domain,value){return createHmac('sha256',this.fingerprintKey).update(domain+'\0').update(value).digest();}
 #seal(bytes,id,ordinal,scope){const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',this.key,iv);cipher.setAAD(Buffer.from(`context:v1:${this.keyVersion}:${scope.toString('hex')}:${id}:${ordinal}`));return Buffer.concat([iv,cipher.update(bytes),cipher.final(),cipher.getAuthTag()]);}
 #open(bytes,id,ordinal,scope){try{const decipher=createDecipheriv('aes-256-gcm',this.key,bytes.subarray(0,12));decipher.setAAD(Buffer.from(`context:v1:${this.keyVersion}:${scope.toString('hex')}:${id}:${ordinal}`));decipher.setAuthTag(bytes.subarray(-16));return Buffer.concat([decipher.update(bytes.subarray(12,-16)),decipher.final()]);}catch{fail(503,'上下文密文无法验证','recording_context_unreadable');}}
 async #authority(requestId,{lock=false,allowMissing=false}={}){
  if(typeof requestId!=='string'||!requestId||requestId.length>200)fail(400,'上下文请求标识无效');
  const row=await this.store.get(`SELECT r.id,r.member_id,r.session_id,r.recording_state,r.recording_reason,c.expires_at,s.member_id session_member FROM harbor.requests r ${allowMissing?'LEFT JOIN':'JOIN'} harbor.recordings c ON c.request_id=r.id LEFT JOIN harbor.sessions s ON s.id=r.session_id WHERE r.id=$1${lock?' FOR SHARE OF r,c':''}`,requestId);
  if(!row)fail(404,'请求正文尚不存在','recording_context_missing');
  if(allowMissing&&row.expires_at===null)return {...row,scope:null};
  if(!row.member_id||row.session_id&&row.session_member!==row.member_id)fail(503,'请求上下文归属不一致','recording_context_scope');
  const scope=this.#digest('scope',JSON.stringify([row.member_id,row.session_id?['session',row.session_id]:['request',row.id]]));
  return {...row,scope};
 }
 async #lock(scope){await this.store.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",['harbor-recording-context:'+scope.toString('hex')]);}
 #prepare(components){
  if(!Array.isArray(components))fail(400,'上下文组件须为有序数组');
  const values=[];let bytes=0,truncated=components.length>this.maxComponents;
  for(const component of components.slice(0,this.maxComponents)){
   if(!component||typeof component!=='object'||Array.isArray(component)||Object.keys(component).some(key=>!['kind','content'].includes(key))||!kinds.has(component.kind)||component.content===undefined)fail(400,'上下文组件字段无效');
   let clipped=false,json,budget=this.maxSnapshotBytes-bytes;
   // JSON punctuation and truncation markers also consume the storage budget.
   // Tighten the filter rather than silently dropping an otherwise useful prefix.
   for(;;){const safe=sanitizeRecord(component.content,{maxBytes:budget,onTruncate:()=>{clipped=true;}});json=JSON.stringify(safe);if(typeof json!=='string')fail(400,'上下文组件内容无效');if(Buffer.byteLength(json)<=this.maxSnapshotBytes-bytes||budget===0)break;clipped=true;budget=Math.floor(budget/2);}
   const content=Buffer.from(json);if(bytes+content.length>this.maxSnapshotBytes){truncated=true;break;}
   bytes+=content.length;truncated||=clipped;values.push({kind:component.kind,content,clipped});
   if(bytes===this.maxSnapshotBytes&&values.length<components.length){truncated=true;break;}
  }
  return {values,bytes,state:truncated?'truncated':'complete',seen:components.length};
 }
 #receipt(row){return {requestId:row.request_id,state:row.state,reason:row.reason,coverage:row.coverage??null,seen:row.seen,components:row.components,bytes:row.bytes,expiresAt:row.expires_at};}
 async #validObjects(objects){
  if(!objects.length)return;
  const shapes=new Map((await this.store.all('SELECT object_id,COUNT(*) parts,SUM(bytes) bytes,MAX(ordinal) last FROM harbor.recording_context_parts WHERE object_id=ANY($1::text[]) GROUP BY object_id',objects.map(object=>object.id))).map(shape=>[shape.object_id,shape]));
  for(const object of objects){
   if(object.state!=='complete'||object.key_version!==this.keyVersion)fail(503,'上下文对象尚不完整','recording_context_incomplete');
   const shape=shapes.get(object.id);if(!shape||shape.parts!==object.parts||Number(shape.bytes)!==object.bytes||shape.last!==object.parts)fail(503,'上下文对象分块缺失','recording_context_incomplete');
  }
 }
 async writeSnapshot(requestId,components,{coverage=null}={}){
   if(coverage!==null){
    if(!coverage||typeof coverage!=='object'||Array.isArray(coverage)||Object.keys(coverage).some(key=>!['state','reasons','opaqueReasoningExcluded'].includes(key))||!['complete','incomplete'].includes(coverage.state)||!Array.isArray(coverage.reasons)||coverage.reasons.length>captureReasons.size||coverage.reasons.some(reason=>!captureReasons.has(reason))||!Number.isSafeInteger(coverage.opaqueReasoningExcluded)||coverage.opaqueReasoningExcluded<0||coverage.state==='complete'&&coverage.reasons.length||coverage.state==='incomplete'&&!coverage.reasons.length)fail(400,'上下文采集覆盖信息无效');
    coverage={state:coverage.state,reasons:[...new Set(coverage.reasons)].sort(),opaqueReasoningExcluded:coverage.opaqueReasoningExcluded};
   }
  const prepared=this.#prepare(components);
   if(coverage?.state==='incomplete'&&prepared.state==='complete')prepared.state=coverage.reasons.includes('size_limit')?'truncated':'partial';
  return this.store.transaction(async()=>{
   const authority=await this.#authority(requestId,{lock:true});await this.#lock(authority.scope);
   if(authority.expires_at<=this.clock()||authority.recording_state==='expired')fail(410,'请求正文已过期','recording_context_expired');
   for(const value of prepared.values)value.fingerprint=this.#digest('object:'+authority.scope.toString('hex'),Buffer.concat([Buffer.from(value.kind+'\0'),value.content]));
   const fingerprint=this.#digest('snapshot',JSON.stringify({state:prepared.state,coverage,seen:prepared.seen,objects:prepared.values.map(value=>[value.fingerprint.toString('hex'),value.clipped])}));
   const existing=await this.store.get('SELECT * FROM harbor.recording_context_snapshots WHERE request_id=$1 FOR UPDATE',requestId);
   if(existing){if(!equal(existing.scope_digest,authority.scope)||existing.key_version!==this.keyVersion)fail(503,'上下文归属或密钥版本不一致','recording_context_scope');if(!equal(existing.fingerprint,fingerprint))fail(409,'本次上下文已持久化且不可覆盖','recording_context_conflict');await this.#validateSnapshot(existing);return this.#receipt({...existing,expires_at:Math.min(existing.expires_at,authority.expires_at)});}
   // The snapshot byte/component limits also bound these arrays. Keep database
   // round trips independent of history length, including the reuse path.
   const unique=new Map(prepared.values.map(value=>[value.fingerprint.toString('hex'),value]));
   const existingObjects=unique.size?await this.store.all('SELECT * FROM harbor.recording_context_objects WHERE scope_digest=$1 AND key_version=$2 AND fingerprint=ANY($3::bytea[]) FOR UPDATE',authority.scope,this.keyVersion,[...unique.values()].map(value=>value.fingerprint)):[];
   await this.#validObjects(existingObjects);
   const objects=new Map(existingObjects.map(object=>[object.fingerprint.toString('hex'),object])),created=[],parts=[];
   for(const [fingerprint,value] of unique){
    const object=objects.get(fingerprint);
    if(object){if(object.kind!==value.kind||object.bytes!==value.content.length)fail(503,'上下文对象元数据不一致','recording_context_incomplete');continue;}
    const id=randomUUID(),blocks=chunks(value.content,this.partBytes),metadata={id,fingerprint:value.fingerprint,kind:value.kind,bytes:value.content.length,parts:blocks.length};created.push(metadata);objects.set(fingerprint,metadata);
    for(const [index,part] of blocks.entries())parts.push({id,ordinal:index+1,bytes:part.length,cipher:this.#seal(part,id,index+1,authority.scope)});
   }
   if(created.length){
    await this.store.run(`INSERT INTO harbor.recording_context_objects(id,scope_digest,fingerprint,key_version,kind,state,bytes,parts,created)
     SELECT v.id,$1,v.fingerprint,$2,v.kind,'writing',v.bytes,v.parts,$3 FROM unnest($4::text[],$5::bytea[],$6::text[],$7::bigint[],$8::integer[]) AS v(id,fingerprint,kind,bytes,parts)`,authority.scope,this.keyVersion,this.clock(),created.map(value=>value.id),created.map(value=>value.fingerprint),created.map(value=>value.kind),created.map(value=>value.bytes),created.map(value=>value.parts));
    await this.store.run(`INSERT INTO harbor.recording_context_parts(object_id,ordinal,bytes,content_cipher)
     SELECT * FROM unnest($1::text[],$2::integer[],$3::integer[],$4::bytea[])`,parts.map(part=>part.id),parts.map(part=>part.ordinal),parts.map(part=>part.bytes),parts.map(part=>part.cipher));
    await this.store.run("UPDATE harbor.recording_context_objects SET state='complete' WHERE id=ANY($1::text[])",created.map(value=>value.id));
   }
   const references=prepared.values.map(value=>({id:objects.get(value.fingerprint.toString('hex')).id,truncated:value.clipped}));
   const snapshot=await this.store.get('INSERT INTO harbor.recording_context_snapshots(request_id,member_id,session_id,scope_digest,fingerprint,key_version,state,reason,coverage,seen,components,bytes,expires_at,created) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14) RETURNING *',requestId,authority.member_id,authority.session_id,authority.scope,fingerprint,this.keyVersion,prepared.state,prepared.state==='truncated'?'size_limit':prepared.state==='partial'?'capture_incomplete':null,coverage===null?null:JSON.stringify(coverage),prepared.seen,references.length,prepared.bytes,authority.expires_at,this.clock());
   if(references.length)await this.store.run('INSERT INTO harbor.recording_context_references(request_id,ordinal,object_id,truncated) SELECT $1,v.ordinal,v.object_id,v.truncated FROM unnest($2::integer[],$3::text[],$4::boolean[]) AS v(ordinal,object_id,truncated)',requestId,references.map((_,index)=>index+1),references.map(value=>value.id),references.map(value=>value.truncated));
   return this.#receipt(snapshot);
  });
 }
 async #validateSnapshot(snapshot){
  // One database round trip, including every distinct object's part shape.
  // Do not turn a one-page read into thousands of sequential validations.
  const shape=await this.store.get(`WITH refs AS (
   SELECT r.ordinal,o.* FROM harbor.recording_context_references r JOIN harbor.recording_context_objects o ON o.id=r.object_id WHERE r.request_id=$1
  ), shapes AS (
   SELECT object_id,COUNT(*) parts,COALESCE(SUM(bytes),0) bytes,MAX(ordinal) last FROM harbor.recording_context_parts WHERE object_id IN (SELECT id FROM refs) GROUP BY object_id
  ) SELECT COUNT(*) components,COALESCE(SUM(r.bytes),0)::text bytes,COALESCE(MAX(r.ordinal),0) last,
   COALESCE(BOOL_AND(r.scope_digest=$2 AND r.key_version=$3),true) scope_valid,
   COALESCE(BOOL_AND(r.state='complete' AND p.object_id IS NOT NULL AND p.parts=r.parts AND p.bytes=r.bytes AND p.last=r.parts),true) complete
   FROM refs r LEFT JOIN shapes p ON p.object_id=r.id`,snapshot.request_id,snapshot.scope_digest,this.keyVersion);
  if(!shape.scope_valid)fail(503,'上下文对象归属不一致','recording_context_scope');
  if(shape.components!==snapshot.components||shape.last!==snapshot.components||Number(shape.bytes)!==snapshot.bytes||!shape.complete)fail(503,'上下文对象或引用不完整','recording_context_incomplete');
 }
 async read(requestId,{after={component:0,part:0},limit=32}={}){
  if(!after||Object.keys(after).some(key=>!['component','part'].includes(key))||![after.component,after.part].every(value=>Number.isSafeInteger(value)&&value>=0)||!Number.isInteger(limit)||limit<1||limit>100)fail(400,'上下文分页无效');
  return this.store.transaction(async()=>{
   const authority=await this.#authority(requestId,{allowMissing:true});
   if(authority.expires_at===null){const known=['disabled','historical','expired','pending'].includes(authority.recording_state);return {state:known?authority.recording_state:'unavailable',reason:known?authority.recording_reason:'recording_missing',parts:[],next:after,hasMore:false};}
   const snapshot=await this.store.get('SELECT * FROM harbor.recording_context_snapshots WHERE request_id=$1',requestId);
   if(!snapshot)return {state:authority.expires_at<=this.clock()||authority.recording_state==='expired'?'expired':'unavailable',parts:[],next:after,hasMore:false};
   if(!equal(snapshot.scope_digest,authority.scope)||snapshot.key_version!==this.keyVersion)fail(503,'上下文归属或密钥版本不一致','recording_context_scope');
   const expiresAt=Math.min(snapshot.expires_at,authority.expires_at);
   if(expiresAt<=this.clock()||authority.recording_state==='expired')return {state:'expired',parts:[],next:after,hasMore:false,expiresAt};
   await this.#validateSnapshot(snapshot);
   const sizes=await this.store.all('SELECT r.ordinal component,p.ordinal part,octet_length(p.content_cipher) cipher_bytes FROM harbor.recording_context_references r JOIN harbor.recording_context_parts p ON p.object_id=r.object_id WHERE r.request_id=$1 AND (r.ordinal,p.ordinal)>($2,$3) ORDER BY r.ordinal,p.ordinal LIMIT $4',requestId,after.component,after.part,limit+1);
   let bytes=0,count=0;for(const row of sizes.slice(0,limit)){if(bytes+row.cipher_bytes>this.maxReadBytes){if(!count)fail(503,'上下文分块超过读取预算','recording_context_capacity');break;}bytes+=row.cipher_bytes;count++;}
   const end=count?sizes[count-1]:after;
   const rows=count?await this.store.all('SELECT r.ordinal component,r.truncated,o.id object_id,o.kind,o.parts,p.ordinal part,p.bytes,p.content_cipher FROM harbor.recording_context_references r JOIN harbor.recording_context_objects o ON o.id=r.object_id JOIN harbor.recording_context_parts p ON p.object_id=o.id WHERE r.request_id=$1 AND (r.ordinal,p.ordinal)>($2,$3) AND (r.ordinal,p.ordinal)<=($4,$5) ORDER BY r.ordinal,p.ordinal',requestId,after.component,after.part,end.component,end.part):[];
   const parts=rows.map(row=>{const plain=this.#open(row.content_cipher,row.object_id,row.part,authority.scope);if(plain.length!==row.bytes)fail(503,'上下文分块长度不一致','recording_context_incomplete');return {component:row.component,part:row.part,parts:row.parts,objectId:row.object_id,kind:row.kind,truncated:row.truncated,text:plain.toString('utf8')};});
   const last=parts.at(-1),next=last?{component:last.component,part:last.part}:after;
   return {...this.#receipt({...snapshot,expires_at:expiresAt}),encoding:'json_utf8',parts,next,hasMore:sizes.length>parts.length};
  },{level:'REPEATABLE READ',readOnly:true});
 }
 // Internal derived-data reader. It is not exposed as an employee endpoint.
 // A live source authorizes the scope; ciphertext is fetched only after its
 // full size is checked. Workers must recheck source liveness when committing.
 async #liveSource(object){
   return this.store.get(`SELECT r.id,r.member_id,r.session_id FROM harbor.recording_context_references ref
    JOIN harbor.recording_context_snapshots s ON s.request_id=ref.request_id
    JOIN harbor.recordings c ON c.request_id=s.request_id JOIN harbor.requests r ON r.id=s.request_id
    LEFT JOIN harbor.sessions session ON session.id=r.session_id
    WHERE ref.object_id=$1 AND s.scope_digest=$2 AND s.key_version=$3 AND s.member_id=r.member_id
     AND s.session_id IS NOT DISTINCT FROM r.session_id AND r.recording_state<>'expired'
     AND LEAST(s.expires_at,c.expires_at)>$4 AND (r.session_id IS NULL OR session.member_id=r.member_id)
    ORDER BY r.id LIMIT 1`,object.id,object.scope_digest,object.key_version,this.clock());
 }
 async withLiveObject(objectId,expected,operation){
  if(typeof operation!=='function'||!expected||expected.objectId!==objectId||expected.keyVersion!==this.keyVersion||!Buffer.isBuffer(expected.fingerprint)||!Buffer.isBuffer(expected.scopeDigest))fail(503,'上下文派生提交依赖无效','recording_context_configuration');
  return this.store.transaction(async()=>{
   let object=await this.store.get('SELECT * FROM harbor.recording_context_objects WHERE id=$1',objectId);
   if(!object)return {object_id:objectId,expired:true};
   const source=await this.#liveSource(object);if(!source)return {object_id:objectId,expired:true};
   // Match capture's source -> scope -> object/work lock order. In particular,
   // never wait on a recording row while holding the scope GC lock.
   let authority;try{authority=await this.#authority(source.id,{lock:true});}catch(error){if(error.code==='recording_context_missing')return {object_id:objectId,expired:true};throw error;}await this.#lock(authority.scope);
   object=await this.store.get('SELECT * FROM harbor.recording_context_objects WHERE id=$1 FOR SHARE',objectId);
   if(!object||authority.expires_at<=this.clock()||authority.recording_state==='expired'||!await this.#liveSource(object))return {object_id:objectId,expired:true};
   if(object.state!=='complete'||object.key_version!==expected.keyVersion||!equal(object.scope_digest,expected.scopeDigest)||!equal(authority.scope,expected.scopeDigest)||!equal(object.fingerprint,expected.fingerprint))fail(503,'上下文派生来源已变化','recording_context_scope');
   return operation(object);
  });
 }
 async readObject(objectId){
  if(typeof objectId!=='string'||!objectId||objectId.length>200)fail(400,'上下文对象标识无效');
  const {object,scope,rows}=await this.store.transaction(async()=>{
   const object=await this.store.get('SELECT * FROM harbor.recording_context_objects WHERE id=$1',objectId);
   if(!object)fail(404,'上下文对象不存在','recording_context_missing');
   const source=await this.#liveSource(object);
   if(!source)fail(410,'上下文对象已无有效来源','recording_context_expired');
   const scope=this.#digest('scope',JSON.stringify([source.member_id,source.session_id?['session',source.session_id]:['request',source.id]]));
   if(!equal(scope,object.scope_digest)||object.key_version!==this.keyVersion)fail(503,'上下文对象归属或密钥不一致','recording_context_scope');
   if(object.state!=='complete')fail(503,'上下文对象尚不完整','recording_context_incomplete');
   const shape=await this.store.get('SELECT COUNT(*) parts,COALESCE(SUM(bytes),0) bytes,COALESCE(SUM(octet_length(content_cipher)),0) cipher_bytes,MAX(ordinal) last FROM harbor.recording_context_parts WHERE object_id=$1',objectId);
   // The smallest supported part is 256 bytes; UTF-8 alignment can retreat
   // three bytes. Do not assume the reader uses the writer's partBytes.
   if(!Number.isSafeInteger(object.bytes)||object.bytes>this.maxSnapshotBytes||object.parts>Math.ceil(this.maxSnapshotBytes/(256-3))+1||Number(shape.cipher_bytes)>this.maxSnapshotBytes+28*object.parts)fail(503,'上下文对象超过读取预算','recording_context_capacity');
   if(shape.parts!==object.parts||shape.last!==object.parts||Number(shape.bytes)!==object.bytes)fail(503,'上下文对象分块缺失','recording_context_incomplete');
   const rows=await this.store.all('SELECT ordinal,bytes,content_cipher FROM harbor.recording_context_parts WHERE object_id=$1 ORDER BY ordinal',objectId);
   return {object,scope,rows};
  },{level:'REPEATABLE READ',readOnly:true});
   // Release the database connection before decrypting/decoding the bounded
   // object. Derived writes subsequently recheck its source under locks.
   const parts=[];
   for(const [index,row] of rows.entries()){
    if(row.ordinal!==index+1)fail(503,'上下文对象分块顺序无效','recording_context_incomplete');
    const plain=this.#open(row.content_cipher,objectId,row.ordinal,scope);
    if(plain.length!==row.bytes)fail(503,'上下文对象分块长度不一致','recording_context_incomplete');parts.push(plain);
   }
   const bytes=Buffer.concat(parts);
   if(!equal(this.#digest('object:'+scope.toString('hex'),Buffer.concat([Buffer.from(object.kind+'\0'),bytes])),object.fingerprint))fail(503,'上下文对象指纹不一致','recording_context_unreadable');
   let content;try{content=JSON.parse(bytes.toString('utf8'));}catch{fail(503,'上下文对象格式无效','recording_context_unreadable');}
   return {objectId,kind:object.kind,content,bytes:object.bytes,keyVersion:object.key_version,fingerprint:object.fingerprint,scopeDigest:scope};
 }
 async gc({limit=100}={}){
  if(!Number.isInteger(limit)||limit<1||limit>1000)fail(400,'上下文清理批次无效');
  // Visit one scope at a time, retaining a keyset cursor across calls. Live
  // scopes must not permanently starve expired data in later scopes.
  const candidate=await this.store.get('SELECT scope_digest FROM (SELECT scope_digest FROM harbor.recording_context_objects UNION SELECT scope_digest FROM harbor.recording_context_snapshots) s WHERE ($1::bytea IS NULL OR scope_digest>$1) ORDER BY scope_digest LIMIT 1',this.gcScope);
  if(!candidate){this.gcScope=null;return {snapshots:0,objects:0,cycleComplete:true};}
  const scope=candidate.scope_digest;
  const result=await this.store.transaction(async()=>{
   await this.#lock(scope);
   const derived=Boolean((await this.store.get("SELECT to_regclass('harbor.recording_context_export_objects') name")).name);
   // Export dependencies are committed together with their chunks. Release
   // only terminal/expired exports, in bounded batches under the source lock.
   const expiredExports=`(e.expires<=$2 OR e.status IN ('cancelled','failed','expired'))`;
   if(derived)await this.store.run(`DELETE FROM harbor.recording_context_export_objects d USING (
    SELECT d.export_id,d.object_id FROM harbor.recording_context_export_objects d
    JOIN harbor.recording_context_objects o ON o.id=d.object_id JOIN harbor.recording_exports e ON e.id=d.export_id
    WHERE o.scope_digest=$1 AND ${expiredExports} ORDER BY d.export_id,d.object_id LIMIT $3 FOR UPDATE OF d
   ) stale WHERE d.export_id=stale.export_id AND d.object_id=stale.object_id`,scope,this.clock(),limit);
   const unreferenced=`NOT EXISTS(SELECT 1 FROM harbor.recording_context_references r WHERE r.object_id=o.id)${derived?' AND NOT EXISTS(SELECT 1 FROM harbor.recording_context_export_objects d WHERE d.object_id=o.id)':''}`;
   const stale=await this.store.all("SELECT s.request_id FROM harbor.recording_context_snapshots s JOIN harbor.recordings c ON c.request_id=s.request_id JOIN harbor.requests r ON r.id=s.request_id WHERE s.scope_digest=$1 AND (LEAST(s.expires_at,c.expires_at)<=$2 OR r.recording_state='expired') ORDER BY s.request_id LIMIT $3 FOR UPDATE OF s",scope,this.clock(),limit);
   for(const row of stale)await this.store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',row.request_id);
   const orphan=await this.store.all(`SELECT o.id FROM harbor.recording_context_objects o WHERE o.scope_digest=$1 AND ${unreferenced} ORDER BY o.id LIMIT $2 FOR UPDATE OF o`,scope,limit);
   for(const row of orphan)await this.store.run('DELETE FROM harbor.recording_context_objects WHERE id=$1',row.id);
   const remaining=await this.store.get(`SELECT EXISTS(SELECT 1 FROM harbor.recording_context_snapshots s JOIN harbor.recordings c ON c.request_id=s.request_id JOIN harbor.requests r ON r.id=s.request_id WHERE s.scope_digest=$1 AND (LEAST(s.expires_at,c.expires_at)<=$2 OR r.recording_state='expired')) OR EXISTS(SELECT 1 FROM harbor.recording_context_objects o WHERE o.scope_digest=$1 AND ${unreferenced})${derived?` OR EXISTS(SELECT 1 FROM harbor.recording_context_export_objects d JOIN harbor.recording_context_objects o ON o.id=d.object_id JOIN harbor.recording_exports e ON e.id=d.export_id WHERE o.scope_digest=$1 AND ${expiredExports})`:''} pending`,scope,this.clock());
   return {snapshots:stale.length,objects:orphan.length,pending:remaining.pending};
  });
  if(!result.pending)this.gcScope=scope;
  return {snapshots:result.snapshots,objects:result.objects,cycleComplete:false};
 }
}
