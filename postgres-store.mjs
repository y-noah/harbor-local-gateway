import {canonicalStreamTiming} from './request-stream-timing-facts.mjs';
import {migrateRecordingRetention} from './recording-retention.mjs';
import pg from 'pg';
import {requestParameters,safeUpstreamRequestId} from './request-parameters.mjs';
import {AsyncLocalStorage} from 'node:async_hooks';
import {readFile} from 'node:fs/promises';
import {createHash,randomUUID} from 'node:crypto';
import {AppError,hash,usageWindow,modelAllowlist,checkAllowedModel} from './store.mjs';
import seed from './pricing-data.mjs';
import {canonicalChatFacts} from './chat-facts.mjs';
import {quoteReference,emptyReferenceQuote,publicUsage} from './pricing.mjs';

const integer=value=>{
  const n=Number(value);if(!Number.isSafeInteger(n))throw new AppError(503,'数据库整数字段超出安全范围','database_value_invalid');return n;
};
const migrationFiles=['001-postgres.sql','002-request-queries.sql','003-startup-recovery.sql','004-account-bindings.sql','005-quota-refresh.sql','006-background-leases.sql','007-routing-plans.sql','008-nightly-execution.sql','009-job-dependencies.sql','010-session-handoffs.sql','011-member-types.sql','012-member-config-batches.sql','013-member-pages.sql','014-device-access.sql','015-recording-jobs.sql','016-automatic-handoffs.sql','017-member-actions.sql','018-account-packages.sql','019-recording-export-history.sql','020-recording-storage-reports.sql','021-account-extra-usage.sql','022-extra-usage-batches.sql'];
const contextMigrationFiles=['023-recording-context.sql','024-recording-context-derived.sql'];
export const postgresContextSchemaVersions=Object.freeze({storage:23,index:24});
const isolation=new Set(['READ COMMITTED','REPEATABLE READ','SERIALIZABLE']);
const copyTls=value=>value&&typeof value==='object'?{...value,...(value.key!==undefined?{key:value.key}:{})}:value;
function databaseTransport(connectionString,connection,allowPlaintextLocalNetwork){
  let url,parameters;
  try{
    if(connection!==undefined&&(!connection||typeof connection!=='object'||Array.isArray(connection)))throw Error();
    const address=connectionString||connection?.connectionString;
    if(address){url=new URL(address);if(!['postgres:','postgresql:'].includes(url.protocol))throw Error();}
    // Resolve pg's complete precedence, including PGHOST/PGSSLMODE, before classifying transport.
    parameters=new pg.Client(address?{...connection,password:connection?.password,connectionString:address}:connection||{}).connectionParameters;
    if(typeof parameters.host!=='string'||!parameters.host)throw Error();
  }catch{throw new AppError(503,'PostgreSQL连接格式无效','database_configuration');}
  const host=parameters.host,local=host.startsWith('/')||['localhost','127.0.0.1','::1','[::1]'].includes(host);
  let ssl=copyTls(parameters.ssl);
  if(connection?.ssl&&typeof connection.ssl==='object')ssl={...(ssl&&typeof ssl==='object'?copyTls(ssl):{}),...copyTls(connection.ssl)};
  if(!local&&!allowPlaintextLocalNetwork){
    const mode=url?.searchParams.get('sslmode')||(connection?.ssl===undefined?process.env.PGSSLMODE:undefined);
    if(mode&&mode!=='verify-full')throw new AppError(503,'跨主机数据库须使用verify-full认证加密连接','database_configuration');
    if(connection?.ssl===false||connection?.ssl==='no-verify'||ssl?.rejectUnauthorized===false)throw new AppError(503,'跨主机数据库不能关闭证书校验','database_configuration');
    ssl=ssl&&typeof ssl==='object'?{...copyTls(ssl),rejectUnauthorized:true}:{rejectUnauthorized:true};
  }
  // Do not let a second URL parse discard explicit CA/cert/key. password is non-enumerable in pg.
  const normalized={...connection,...parameters,password:parameters.password,ssl};delete normalized.connectionString;return normalized;
}
export class PostgresStore{
  #connections=new WeakMap();
  constructor({connectionString,connection,poolSize=12,connectionTimeoutMs=3000,statementTimeoutMs=10000,allowPlaintextLocalNetwork=false,Client}={}){
    if(!connectionString&&!connection)throw new AppError(503,'请配置PostgreSQL连接','database_configuration');
    if(!Number.isInteger(poolSize)||poolSize<1||poolSize>64||!Number.isInteger(connectionTimeoutMs)||connectionTimeoutMs<100||connectionTimeoutMs>30000||!Number.isInteger(statementTimeoutMs)||statementTimeoutMs<100||statementTimeoutMs>600000)throw new AppError(503,'数据库资源配置无效','database_configuration');
    this.context=new AsyncLocalStorage();this.savepoint=0;this.poolErrors=0;
    if(typeof allowPlaintextLocalNetwork!=='boolean')throw new AppError(503,'数据库传输配置无效','database_configuration');
    if(Client!==undefined&&typeof Client!=='function')throw new AppError(503,'数据库连接实现无效','database_configuration');
    this.pool=new pg.Pool({...databaseTransport(connectionString,connection,allowPlaintextLocalNetwork),max:poolSize,connectionTimeoutMillis:connectionTimeoutMs,
      ...(Client?{Client}:{}),
      idleTimeoutMillis:10000,statement_timeout:statementTimeoutMs,idle_in_transaction_session_timeout:30000,
      types:{getTypeParser:(oid,format)=>oid===20&&format!=='binary'?integer:pg.types.getTypeParser(oid,format)}});
    // pg-pool removes its idle error listener before resolving a checkout.
    // Observe each physical client for its whole lifetime, including that gap.
    this.pool.on('connect',client=>{this.#connectionState(client);client.on('error',error=>{this.#connectionState(client,error);});});
    this.pool.on('error',(error,client)=>{if(client)this.#connectionState(client,error);else this.poolErrors++;});
  }
  #connectionState(client,error){
    let state=this.#connections.get(client);if(!state){state={error:null};this.#connections.set(client,state);}
    if(error&&!state.error){state.error=error;this.poolErrors++;}return state;
  }
  #assertConnection(scope){if(scope.connection?.error)throw scope.connection.error;}
  enqueue(scope,operation){
    if(!scope.open)return Promise.reject(new AppError(503,'数据库事务上下文已关闭','database_transaction_closed'));
    const result=scope.tail.then(()=>{this.#assertConnection(scope);return operation();});scope.tail=result.catch(()=>{});return result;
  }
  query(sql,args=[]){const scope=this.context.getStore();return scope?this.enqueue(scope,()=>scope.client.query(sql,args)):this.pool.query(sql,args);}
  async all(sql,...args){return (await this.query(sql,args)).rows;}
  async get(sql,...args){return (await this.query(sql,args)).rows[0];}
  async run(sql,...args){const result=await this.query(sql,args);return {changes:result.rowCount};}
  async transaction(operation,{level='READ COMMITTED',readOnly=false}={}){
    if(!isolation.has(level))throw new AppError(500,'数据库事务配置无效','database_configuration');
    const existing=this.context.getStore();
    if(existing){
      return this.enqueue(existing,async()=>{
        const client=existing.client,name='harbor_sp_'+(++this.savepoint),child={client,connection:existing.connection,tail:Promise.resolve(),open:true};
        await client.query('SAVEPOINT '+name);
        try{const value=await this.context.run(child,()=>operation(this));child.open=false;await child.tail;this.#assertConnection(child);await client.query('RELEASE SAVEPOINT '+name);return value;}
        catch(e){child.open=false;await child.tail;await client.query('ROLLBACK TO SAVEPOINT '+name).catch(()=>{});await client.query('RELEASE SAVEPOINT '+name).catch(()=>{});throw e;}
      });
    }
    const client=await this.pool.connect(),scope={client,connection:this.#connectionState(client),tail:Promise.resolve(),open:true};
    try{
      this.#assertConnection(scope);
      await client.query('BEGIN ISOLATION LEVEL '+level+(readOnly?' READ ONLY':''));
      const result=await this.context.run(scope,()=>operation(this));scope.open=false;await scope.tail;this.#assertConnection(scope);
      const committed=await client.query('COMMIT');if(committed.command!=='COMMIT')throw new AppError(503,'数据库事务未提交','database_transaction_failed');return result;
    }catch(e){scope.open=false;await scope.tail;await client.query('ROLLBACK').catch(()=>{});throw e;}
    finally{client.release(scope.connection.error??undefined);}
  }
  async migrate(options={}){
    if(!options||typeof options!=='object'||Array.isArray(options)||Object.keys(options).some(name=>!['recordingContext','recordingContextIndex','recordingRetentionUnlimited'].includes(name))||['recordingContext','recordingContextIndex','recordingRetentionUnlimited'].some(name=>options[name]!==undefined&&typeof options[name]!=='boolean')||options.recordingContext===false&&options.recordingContextIndex===true)throw new AppError(503,'数据库迁移选项无效','database_configuration');
    return this.transaction(async()=>{
      // The bridge recognizes only the exact known additive migrations. It never enables
      // context storage by merely starting a gateway or calling migrate().
      const lock=createHash('sha256').update('harbor-postgres-migrations').digest().readInt32BE();
      await this.query('SELECT pg_advisory_xact_lock($1)',[lock]);
      await this.query('CREATE SCHEMA IF NOT EXISTS harbor');
      await this.query('CREATE TABLE IF NOT EXISTS harbor.schema_migrations(version INTEGER PRIMARY KEY,checksum TEXT NOT NULL,applied BIGINT NOT NULL)');
      const existing=await this.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version');
      if(existing.length>migrationFiles.length+contextMigrationFiles.length||existing.some((row,index)=>row.version!==index+1))throw new AppError(503,'数据库结构版本不匹配，禁止自动修补未知迁移','database_migration_required');
      const target=Math.max(migrationFiles.length,existing.length,options.recordingContextIndex===true?postgresContextSchemaVersions.index:options.recordingContext===true?postgresContextSchemaVersions.storage:migrationFiles.length);
      const files=[...migrationFiles,...contextMigrationFiles].slice(0,target);let changed=false,version=0;
      for(const file of files){
        version++;const sql=await readFile(new URL('./migrations/'+file,import.meta.url),'utf8'),checksum=createHash('sha256').update(sql).digest('hex');
        const prior=existing[version-1];
        if(prior){if(prior.checksum!==checksum)throw new AppError(503,'数据库迁移校验值不匹配，禁止覆盖历史迁移','database_migration_conflict');continue;}
        await this.query(sql);await this.run('INSERT INTO harbor.schema_migrations VALUES($1,$2,$3)',version,checksum,Date.now());changed=true;
      }
      const retentionChanged=await migrateRecordingRetention(this,{apply:options.recordingRetentionUnlimited===true});
      return {version,changed:changed||retentionChanged};
    });
  }
  async verifySchema(){
    const rows=await this.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version');
    if(![migrationFiles.length,postgresContextSchemaVersions.storage,postgresContextSchemaVersions.index].includes(rows.length))throw new AppError(503,'数据库结构版本不匹配，请先执行对应迁移','database_migration_required');
    const files=[...migrationFiles,...contextMigrationFiles].slice(0,rows.length);
    for(const [index,file] of files.entries()){
      const sql=await readFile(new URL('./migrations/'+file,import.meta.url),'utf8');
      if(rows[index].version!==index+1||rows[index].checksum!==createHash('sha256').update(sql).digest('hex'))throw new AppError(503,'数据库迁移校验值不匹配，禁止启动','database_migration_conflict');
    }
    await migrateRecordingRetention(this);
    if(!await this.ready())throw new AppError(503,'数据库数据契约不兼容','database_contract_invalid');
    return true;
  }
  status(){return {total:this.pool.totalCount,idle:this.pool.idleCount,waiting:this.pool.waitingCount,errors:this.poolErrors};}
  async ready(){
    try{const row=await this.get('SELECT name FROM harbor.data_contract WHERE singleton=1');return row?.name==='postgres-conversation-v2';}catch{return false;}
  }
  async authenticate(secret,{at=Date.now(),lock=false,exclusive=false}={}){
    if(typeof secret!=='string'||!secret||secret.length>4096)throw new AppError(401,'Key 无效','invalid_api_key');
    if(typeof lock!=='boolean'||typeof exclusive!=='boolean'||(lock||exclusive)&&!this.context.getStore())throw new AppError(503,'受理鉴权锁必须位于请求事务内','database_transaction_required');
    // Settlement updates the member before the Key. Acquire the same order
    // explicitly; a joined row-lock query can otherwise retain a Key lock while
    // waiting for the member and deadlock with a concurrent settlement.
    if(lock||exclusive)await this.get(`SELECT m.id FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id
      WHERE k.digest=$1 AND k.removed_at IS NULL AND m.removed_at IS NULL FOR ${exclusive?'UPDATE':'SHARE'} OF m`,hash(secret));
    const row=await this.get(`SELECT m.*,k.id key_id,k.enabled key_enabled,k.expires,k.allowed_models,k.model_policy_version
      FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id
      WHERE k.digest=$1 AND k.removed_at IS NULL AND m.removed_at IS NULL`+(lock||exclusive?' FOR SHARE OF k,m':''),hash(secret));
    if(!row)throw new AppError(401,'Key 无效','invalid_api_key');
    if(!row.enabled||!row.key_enabled)throw new AppError(403,'Key 或员工已停用','key_disabled');
    if(row.expires!==null&&row.expires<=at)throw new AppError(403,'Key 已到期','key_expired');
    return row;
  }
  async initializePricing(){
    const value={rates:seed.rates,aliases:[]},book=JSON.stringify(value),revision=createHash('sha256').update(book).digest('hex');
    return this.transaction(async()=>{
      const saved=await this.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'");
      if(saved){const active=JSON.parse(saved.value).active;if(!active||!await this.pricingBook(active))throw new AppError(503,'已保存计价版本缺失，须核查恢复','pricing_unavailable');return active;}
      await this.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING',revision,Date.now(),book);
      await this.run("INSERT INTO harbor.gateway_settings VALUES('pricing.state',$1) ON CONFLICT(name) DO NOTHING",JSON.stringify({active:revision}));
      return JSON.parse((await this.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;
    });
  }
  async checkModel(member,model,tools){
    const key=await this.get('SELECT allowed_models FROM harbor.keys WHERE id=$1 AND member_id=$2 AND removed_at IS NULL',member.key_id,member.id);
    if(!key)throw new AppError(401,'Key无效','invalid_api_key');
    let allowed;try{allowed=key.allowed_models===null?null:modelAllowlist(JSON.parse(key.allowed_models));}catch{throw new AppError(403,'Key模型权限配置无效，请联系管理员','model_policy_invalid');}
    checkAllowedModel(allowed,model,tools);
  }
  async pricingBook(revision){const row=await this.get('SELECT book FROM harbor.pricing_versions WHERE id=$1',revision);return row?JSON.parse(row.book):null;}
  // Books are immutable versioned inputs; reuse only within this one operation.
  async #operationBook(revision,books){
    if(books?.has(revision))return books.get(revision);
    const book=await this.pricingBook(revision);if(book&&books)books.set(revision,book);return book;
  }
  async quote(row){return this.#operationQuote(row);}
  async #operationQuote(row,books){
    let revision=row.price_version;
    if(!revision)revision=JSON.parse((await this.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'"))?.value||'{}').active;
    const book=revision?await this.#operationBook(revision,books):null;
    return book?quoteReference(row,book,revision):emptyReferenceQuote('unpriced',row.billing_model||row.model,revision);
  }
  async observe(member,ip,agent,model,{at=Date.now(),channel='responses',device,parameters}={}){
    return this.transaction(async()=>{
      const currentMember=await this.get('SELECT id,usage_type_id,usage_type_version FROM harbor.members WHERE id=$1 FOR SHARE',member.id);
      const type=currentMember?.usage_type_id?await this.get('SELECT id,code,name FROM harbor.member_types WHERE id=$1 FOR SHARE',currentMember.usage_type_id):null;
      const owner=await this.get(`SELECT k.id FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id
        WHERE k.id=$1 AND m.id=$2 AND k.removed_at IS NULL AND m.removed_at IS NULL
        AND k.enabled=1 AND m.enabled=1 AND (k.expires IS NULL OR k.expires>$3) FOR SHARE OF k,m`,member.key_id,member.id,at);
      if(!owner)throw new AppError(403,'Key或员工已停用或到期','key_disabled');
      const revision=JSON.parse((await this.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'"))?.value||'{}').active;
      const books=new Map();
      if(!revision||!await this.#operationBook(revision,books))throw new AppError(503,'计价版本尚未准备好','pricing_unavailable');
      const request=randomUUID(),observedParameters=requestParameters(parameters);
      const row=await this.get(`INSERT INTO harbor.requests(id,member_id,key_id,started,received_at,status,business_status,ip,agent,model,price_version,tracking_only,predicted,source_channel,recording_state,phase,phase_at,phase_history,usage_type_id,usage_type_code,usage_type_name,usage_type_version,device_id,device_name,device_system,device_generation,device_auth_state,effort,service_tier)
        VALUES($1,$2,$3,$4,$4,'queued','queued',$5,$6,$7,$8,1,0,$9,'disabled','queued',$4,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21) RETURNING *`,request,member.id,member.key_id,at,ip,agent,model,revision,channel,JSON.stringify([{phase:'queued',at}]),type?.id??null,type?.code??null,type?.name??null,currentMember?.usage_type_version??null,device?.deviceId??null,device?.deviceName??null,device?.deviceSystem??null,device?.generation??null,device?.authState??'historical_unknown',observedParameters.effort,observedParameters.service_tier);
      await this.#projectLockedPrice(row,at,books);return request;
    });
  }
  async projectPrice(row,at=Date.now()){
    if(!this.context.getStore())return this.transaction(()=>this.projectPrice(row,at));
    // A repair using an older job payload must never overwrite a later settlement.
    row=await this.get('SELECT * FROM harbor.requests WHERE id=$1 FOR UPDATE',row.id);
    if(!row)throw new AppError(404,'请求不存在');
    return this.#projectLockedPrice(row,at);
  }
  // Callers own either the newly inserted row or its FOR UPDATE lock.
  async #projectLockedPrice(row,at,books){
    const quote=await this.#operationQuote(row,books),atoms=name=>quote[name+'Atoms']===null?null:quote[name+'Atoms'].toString();
    await this.run(`INSERT INTO harbor.request_prices(request_id,state,model,model_source,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT(request_id) DO UPDATE SET state=EXCLUDED.state,model=EXCLUDED.model,model_source=EXCLUDED.model_source,revision=EXCLUDED.revision,
      input_atoms=EXCLUDED.input_atoms,output_atoms=EXCLUDED.output_atoms,cached_atoms=EXCLUDED.cached_atoms,adjustment_atoms=EXCLUDED.adjustment_atoms,total_atoms=EXCLUDED.total_atoms,measured_at=EXCLUDED.measured_at`,
      row.id,quote.state,quote.model,quote.modelSource||null,quote.revision,atoms('input'),atoms('output'),atoms('cached'),atoms('adjustment'),atoms('total'),at);
    return publicUsage(quote);
  }
  async finish(requestId,result={},at=Date.now()){
    return this.transaction(async()=>{
      const row=await this.get('SELECT * FROM harbor.requests WHERE id=$1 FOR UPDATE',requestId);
      if(!row)throw new AppError(404,'请求不存在');
      if(!['queued','running'].includes(row.status)||row.ended!==null){
        // The HTTP finish callback arrives after billing. Its bounded summary
        // may be replayed, but must never repeat usage or change business facts.
        const timing=canonicalStreamTiming(result.streamTiming);let history;
        try{history=JSON.parse(row.phase_history);}catch{}
        if(timing&&Array.isArray(history)&&history.length&&row.ended!==null){
          const terminal=history.at(-1);
          if(terminal?.phase===row.status&&terminal.at===row.ended&&!terminal.stream_timing){
            terminal.stream_timing=timing;
            await this.run('UPDATE harbor.requests SET phase_history=$1 WHERE id=$2',JSON.stringify(history.slice(-32)),requestId);
          }
        }
        return {settled:false};
      }
      let status=result.status||'ok';if(!['ok','failed','unknown','interrupted','cancelled','rejected'].includes(status))throw new AppError(400,'请求结算状态无效');
      let {input=0,output=0,cached=0,usageKnown=true}=result;
      if(![input,output,cached].every(n=>Number.isSafeInteger(n)&&n>=0)||cached>input){input=0;output=0;cached=0;usageKnown=false;}
      if(row.source_channel==='chat'&&!usageKnown&&status!=='interrupted')status='unknown';
      const actualModel=typeof result.actualModel==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(result.actualModel)?result.actualModel:row.billing_model;
      const next={...row,ended:at,status,business_status:status,input,output,cached,usage_unknown:usageKnown?0:1,billing_model:actualModel};
      const books=new Map(),prior=await this.#operationQuote(next,books),active=JSON.parse((await this.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'"))?.value||'{}').active;
      // Match the existing missing-price backfill; valid dispatch prices stay immutable.
      if(prior.state==='unpriced'&&active&&active!==row.price_version&&row.manual_cost_atoms===null&&await this.#operationBook(row.price_version,books)){
        const candidate=await this.#operationQuote({...next,price_version:active},books);if(candidate.state==='priced')next.price_version=active;
      }
      let history;try{history=JSON.parse(row.phase_history);}catch{}if(!Array.isArray(history))history=[];
      const streamTiming=canonicalStreamTiming(result.streamTiming);history.push({phase:status,at,...(streamTiming?{stream_timing:streamTiming}:{})});
      const settledRow=await this.get(`UPDATE harbor.requests SET ended=$1,status=$2,business_status=$2,input=$3,output=$4,cached=$5,usage_unknown=$6,billing_model=$7,price_version=$8,
        reserved=CASE WHEN $6=0 THEN 0 ELSE reserved END,phase=$2,phase_at=$1,phase_history=$9,upstream_request_id=COALESCE($11,upstream_request_id) WHERE id=$10 RETURNING *`,at,status,input,output,cached,next.usage_unknown,actualModel,next.price_version,JSON.stringify(history.slice(-32)),requestId,safeUpstreamRequestId(result.upstreamRequestId));
      const total=(BigInt(input)+BigInt(output)).toString();
      await this.run('UPDATE harbor.members SET used=used+$1 WHERE id=$2',total,row.member_id);
      await this.run('UPDATE harbor.keys SET used=used+$1,unmetered=unmetered+$2 WHERE id=$3',total,row.tracking_only&&!usageKnown?1:0,row.key_id);
      if(row.account_id)await this.run('UPDATE harbor.accounts SET budget_used=budget_used+$1,since_check_tokens=since_check_tokens+$1 WHERE id=$2',total,row.account_id);
      if(row.session_id&&(status==='ok'||!usageKnown))await this.run('UPDATE harbor.sessions SET state=$1,updated=GREATEST(updated,$2) WHERE id=$3 AND account_id=$4 AND route_version=$5',status==='ok'?'active':'uncertain',at,row.session_id,row.account_id,row.route_version);
      if(row.source_channel==='chat'){
        const facts=canonicalChatFacts(result.chat),current=await this.get("SELECT * FROM harbor.sessions WHERE id=$1 AND account_id=$2 AND route_version=$3 AND protocol='chat' FOR UPDATE",row.session_id,row.account_id,row.route_version);
        const candidate=status==='ok'&&!next.usage_unknown?facts?.cursor:null;
        const cursor=current&&candidate&&candidate.messageCount>current.message_count&&(!current.upstream_id||current.upstream_id===facts.upstreamId)&&['input','output','cached'].every(name=>candidate.cumulative[name]===current['cum_'+name]+next[name])?candidate:null;
        if(cursor)await this.run("UPDATE harbor.sessions SET upstream_id=$1,state='active',message_count=$2,history_digest=$3,cum_input=$4,cum_output=$5,cum_cached=$6,updated=GREATEST(updated,$7) WHERE id=$8 AND account_id=$9 AND route_version=$10 AND protocol='chat'",facts.upstreamId,cursor.messageCount,cursor.historyDigest,cursor.cumulative.input,cursor.cumulative.output,cursor.cumulative.cached,at,row.session_id,row.account_id,row.route_version);
        else if(facts?.dispatched||status==='ok'||!usageKnown)await this.run("UPDATE harbor.sessions SET upstream_id=COALESCE(upstream_id,$1),state='uncertain',updated=GREATEST(updated,$2) WHERE id=$3 AND account_id=$4 AND route_version=$5 AND protocol='chat'",facts?.upstreamId??null,at,row.session_id,row.account_id,row.route_version);
        if(facts?.dispatched&&status!=='ok')await this.run("UPDATE harbor.accounts SET state='degraded' WHERE id=$1 AND route_epoch=$2 AND enabled=1 AND removed_at IS NULL",row.account_id,facts.accountEpoch);
      }
      return {settled:true,standard:await this.#projectLockedPrice(settledRow,at,books)};
    });
  }
  async referenceSummary(column,value,at=Date.now()){
    if(!['member_id','key_id','account_id','session_id'].includes(column))throw new AppError(400,'用量维度无效');
    const window=usageWindow(at),row=await this.get(`SELECT COUNT(*) requests,
      COUNT(*) FILTER(WHERE p.total_atoms IS NOT NULL) known,
      COUNT(*) FILTER(WHERE p.state='pending') pending,
      COUNT(*) FILTER(WHERE p.state='unpriced' OR p.request_id IS NULL) unpriced,
      COUNT(*) FILTER(WHERE p.total_atoms IS NULL AND p.state NOT IN ('pending','unpriced')) unmetered,
      COALESCE(SUM(p.input_atoms),0) input,COALESCE(SUM(p.output_atoms),0) output,COALESCE(SUM(p.cached_atoms),0) cached,
      COALESCE(SUM(p.adjustment_atoms),0) adjustment,COALESCE(SUM(p.total_atoms),0) total,
      ARRAY_AGG(DISTINCT p.revision) FILTER(WHERE p.total_atoms IS NOT NULL AND p.revision IS NOT NULL) revisions
      FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id WHERE r.${column}=$1 AND r.usage_at>=$2 AND r.usage_at<$3`,value,window.start,window.end);
    const absent=row.requests>0&&row.known===0,state=row.unmetered+row.unpriced?'partial':row.pending?'pending':'priced';
    const quote={state,model:null,revision:null,...Object.fromEntries(['input','output','cached','adjustment','total'].map(k=>[k+'Atoms',absent?null:BigInt(row[k])]))};
    return {...publicUsage(quote),requests:row.requests,known:row.known,unmetered:row.unmetered,unpriced:row.unpriced,pending:row.pending,expired:0,revisions:(row.revisions||[]).sort()};
  }
  status(){return {connections:this.pool.totalCount,idle:this.pool.idleCount,waiting:this.pool.waitingCount,pool_errors:this.poolErrors};}
  close(){return this.pool.end();}
}
