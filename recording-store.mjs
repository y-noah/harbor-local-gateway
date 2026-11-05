import {createCipheriv,createDecipheriv,randomBytes} from 'node:crypto';
import {AppError} from './store.mjs';
import {sanitizeRecord,restoreAuthenticatedRecord} from './record-redaction.mjs';

const kinds=new Set(['user','assistant','tool_call','tool_result','attachment','boundary']);
const origins=new Set(['client','upstream','gateway']);
const states=new Set(['complete','partial','truncated']);
const atomicCommits=new WeakMap();
export function consumeRecordingAtomicCommit(receipt,commands,repository){const saved=receipt&&atomicCommits.get(receipt);if(!saved||saved.commands!==commands||saved.repository!==repository)return false;atomicCommits.delete(receipt);return true;}
const validateTurnState=value=>{if(!['known','uncertain'].includes(value))throw new AppError(400,'本轮识别状态无效');};
export class RecordingStore{
  constructor(store,{encryptionKey,retentionDays=30,maxBytes=2*1024*1024,maxEventBytes=128*1024,maxPageBytes=128*1024,clock=Date.now}={}){
    if(!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32)throw new AppError(503,'正文加密材料未配置','recording_configuration');
    if(!Number.isInteger(retentionDays)||retentionDays<1||retentionDays>365||!Number.isSafeInteger(maxBytes)||maxBytes<256||!Number.isSafeInteger(maxEventBytes)||maxEventBytes<256||maxEventBytes>maxBytes||!Number.isSafeInteger(maxPageBytes)||maxPageBytes<256||maxPageBytes>2*1024*1024)throw new AppError(503,'正文保留与容量配置无效','recording_configuration');
    this.store=store;this.key=Buffer.from(encryptionKey);this.retentionDays=retentionDays;this.maxBytes=maxBytes;this.maxEventBytes=maxEventBytes;this.maxPageBytes=maxPageBytes;this.clock=clock;
  }
  seal(value,aad){
    const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',this.key,iv);cipher.setAAD(Buffer.from(aad));
    let clipped=false;const safe=sanitizeRecord(value,{maxBytes:this.maxBytes,onTruncate:()=>{clipped=true;}});
    if(clipped)throw new AppError(503,'正文加密对象超过采集边界','recording_buffer_invalid');
    return Buffer.concat([iv,cipher.update(JSON.stringify(safe),'utf8'),cipher.final(),cipher.getAuthTag()]);
  }
  open(buffer,aad){
    try{
      const cipher=createDecipheriv('aes-256-gcm',this.key,buffer.subarray(0,12));cipher.setAAD(Buffer.from(aad));cipher.setAuthTag(buffer.subarray(-16));
      return restoreAuthenticatedRecord(JSON.parse(Buffer.concat([cipher.update(buffer.subarray(12,-16)),cipher.final()]).toString('utf8')));
    }catch{throw new AppError(503,'正文无法解密，需核查加密材料与数据完整性','recording_unreadable');}
  }
  async start(requestId,{turnState='uncertain'}={}){
    validateTurnState(turnState);
    const at=this.clock();
    return this.store.transaction(()=>this.#startInTransaction(requestId,turnState,at));
  }
  async #startInTransaction(requestId,turnState,at){
      const request=await this.store.get('SELECT id,recording_state FROM harbor.requests WHERE id=$1 FOR UPDATE',requestId);
      if(!request)throw new AppError(404,'请求不存在');
      const row=await this.store.get(`INSERT INTO harbor.recordings(request_id,turn_state,expires_at,updated)
        VALUES($1,$2,$3,$4) ON CONFLICT(request_id) DO NOTHING RETURNING request_id`,requestId,turnState,at+this.retentionDays*86400000,at);
      if(row)await this.store.run("UPDATE harbor.requests SET recording_state='pending',recording_reason=NULL WHERE id=$1 AND recording_state IN ('disabled','historical')",requestId);
      return {started:!!row};
  }
  #prepareEvents(events){
    if(!Array.isArray(events)||events.length>128)throw new AppError(400,'正文批次大小无效');
    // This is a second defensive boundary. Never give original payloads to this repository.
    return events.map(event=>{
      if(!event||typeof event.sourceKey!=='string'||event.sourceKey.length<1||event.sourceKey.length>200||!kinds.has(event.kind)||!origins.has(event.provenance))throw new AppError(400,'正文事件无效');
      const content=sanitizeRecord(event.content);return {...event,sourceKey:event.sourceKey,content,bytes:Buffer.byteLength(JSON.stringify(content))};
    });
  }
  async append(requestId,events){const safe=this.#prepareEvents(events);return this.store.transaction(()=>this.#appendInTransaction(requestId,safe));}
  async #appendInTransaction(requestId,safe){
      const row=await this.store.get(`SELECT c.*,r.recording_state FROM harbor.recordings c JOIN harbor.requests r ON r.id=c.request_id WHERE c.request_id=$1 FOR UPDATE OF c,r`,requestId);
      if(!row)throw new AppError(404,'正文采集尚未初始化');
      // Missing content and accepting an already acknowledged prefix are separate
      // facts. A queue-limit marker must not discard preceding durable commands.
      if(row.expires_at<=this.clock()||!['pending','partial'].includes(row.recording_state))return {accepted:0,state:row.expires_at<=this.clock()?'expired':row.recording_state};
      const existing=new Set(safe.length?(await this.store.all('SELECT source_key FROM harbor.conversation_events WHERE request_id=$1 AND source_key=ANY($2::text[])',requestId,safe.map(event=>event.sourceKey))).map(event=>event.source_key):[]),insertions=[];
      let bytes=row.bytes,sequence=row.next_sequence,accepted=0,preview=row.preview_cipher,truncated=false;
      for(const event of safe){
        if(existing.has(event.sourceKey))continue;
        if(event.bytes>this.maxEventBytes||bytes+event.bytes>this.maxBytes){truncated=true;break;}
        const aad=requestId+':'+sequence+':'+event.sourceKey,cipher=this.seal(event.content,aad);
        insertions.push({sequence,sourceKey:event.sourceKey,kind:event.kind,provenance:event.provenance,created:this.clock(),cipher});existing.add(event.sourceKey);
        if(!preview&&event.kind==='user'&&typeof event.content?.text==='string')preview=this.seal({text:event.content.text.slice(0,200)},requestId+':preview');
        bytes+=event.bytes;sequence++;accepted++;
      }
      if(insertions.length)await this.store.run(`INSERT INTO harbor.conversation_events(request_id,sequence,source_key,kind,provenance,created,content_cipher)
        SELECT $1,v.sequence,v.source_key,v.kind,v.provenance,v.created,v.content_cipher
        FROM unnest($2::bigint[],$3::text[],$4::text[],$5::text[],$6::bigint[],$7::bytea[]) AS v(sequence,source_key,kind,provenance,created,content_cipher)`,
        requestId,insertions.map(event=>event.sequence),insertions.map(event=>event.sourceKey),insertions.map(event=>event.kind),insertions.map(event=>event.provenance),insertions.map(event=>event.created),insertions.map(event=>event.cipher));
      await this.store.run('UPDATE harbor.recordings SET next_sequence=$1,persisted_sequence=$2,bytes=$3,updated=$4,preview_cipher=$5 WHERE request_id=$6',sequence,sequence-1,bytes,this.clock(),preview,requestId);
      if(truncated)await this.store.run("UPDATE harbor.requests SET recording_state='truncated',recording_reason='size_limit' WHERE id=$1",requestId);
      return {accepted,state:truncated?'truncated':'pending',persistedSequence:sequence-1};
  }
  // Only this closed command loop may omit per-command savepoints. A nested
  // RELEASE is not a durable commit and must never produce a dequeue receipt.
  async writeAtomicCommands(commands){
    if(typeof this.store.context?.getStore!=='function'||this.store.context.getStore())throw new AppError(503,'正文原子写入需要独立根事务','recording_buffer_invalid');
    if(!Array.isArray(commands)||!Object.isFrozen(commands)||!commands.length)throw new AppError(503,'正文批量命令无效','recording_buffer_invalid');
    const values=[];let bytes=2;
    for(const command of commands){let clipped=false;const value=sanitizeRecord(command,{maxBytes:this.maxBytes,onTruncate:()=>{clipped=true;}});bytes+=Buffer.byteLength(JSON.stringify(value))+(values.length?1:0);if(clipped||bytes>this.maxBytes)throw new AppError(503,'正文批量命令超过容量','recording_buffer_invalid');values.push(value);}
    let executed=false;
    await this.store.transaction(async()=>{
      if(!this.store.context.getStore()?.open)throw new AppError(503,'正文原子事务未建立','recording_buffer_invalid');
      for(const value of values){
        if(value.type==='start'){const turnState=value.turnState===undefined?'uncertain':value.turnState;validateTurnState(turnState);await this.#startInTransaction(value.requestId,turnState,this.clock());}
        else if(value.type==='append')await this.#appendInTransaction(value.requestId,this.#prepareEvents(value.events));
        else if(value.type==='finish')await this.finish(value.requestId,value.result);
        else if(value.type==='settle')await this.store.finish(value.requestId,value.result,value.at);
        else throw new AppError(503,'正文缓冲命令无效','recording_buffer_invalid');
      }
      executed=true;
    });
    if(!executed)throw new AppError(503,'正文批量事务未执行','recording_buffer_invalid');
    const receipt=Object.freeze({});atomicCommits.set(receipt,{commands,repository:this});return receipt;
  }
  async finish(requestId,{state='complete',reason=null}={}){
    if(!states.has(state)||reason!==null&&!['upstream_interrupted','capture_failed','queue_limit','size_limit','process_interrupted'].includes(reason))throw new AppError(400,'正文完成状态无效');
    await this.store.run(`UPDATE harbor.requests SET recording_state=CASE WHEN recording_reason IS NOT NULL AND $1='complete' THEN 'partial' ELSE $1 END,recording_reason=COALESCE($2,recording_reason) WHERE id=$3 AND recording_state='pending'`,state,reason,requestId);
  }
  async read(requestId,{after=0,limit=32}={}){
    if(!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)throw new AppError(400,'正文分页无效');
    return this.store.transaction(async()=>{
      const row=await this.store.get('SELECT r.recording_state,r.recording_reason,c.expires_at,c.turn_state,c.persisted_sequence FROM harbor.requests r LEFT JOIN harbor.recordings c ON c.request_id=r.id WHERE r.id=$1',requestId);
      if(!row)throw new AppError(404,'请求不存在');
      const expired=row.expires_at!==null&&row.expires_at<=this.clock();
      if(!row.expires_at||expired||row.recording_state==='expired')return {state:expired?'expired':row.recording_state,reason:row.recording_reason,events:[],next:after,hasMore:false};
      const sizes=await this.store.all('SELECT sequence,octet_length(content_cipher) bytes FROM harbor.conversation_events WHERE request_id=$1 AND sequence>$2 ORDER BY sequence LIMIT $3',requestId,after,limit+1);
      let bytes=0,count=0;for(const event of sizes){if(count>=limit||count&&bytes+event.bytes>this.maxPageBytes)break;bytes+=event.bytes;count++;}
      const end=count?sizes[count-1].sequence:after;
      const events=count?await this.store.all('SELECT sequence,source_key,kind,provenance,created,content_cipher FROM harbor.conversation_events WHERE request_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence',requestId,after,end):[];
      const page=events.map(event=>({sequence:event.sequence,kind:event.kind,provenance:event.provenance,created:event.created,content:this.open(event.content_cipher,requestId+':'+event.sequence+':'+event.source_key)}));
      return {state:row.recording_state,reason:row.recording_reason,turnState:row.turn_state,persistedSequence:row.persisted_sequence,expiresAt:row.expires_at,events:page,next:page.at(-1)?.sequence??after,hasMore:count<sizes.length};
    },{level:'REPEATABLE READ',readOnly:true});
  }
}
