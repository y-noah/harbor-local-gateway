import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
import {accountCapacity} from './rates.mjs';
import {retainCodexQuotaDenials} from './codex-entitlements.mjs';
import {apiCredentialQuotaSubject} from './quota-subject.mjs';

export const quotaSubjectContract='quota-subject-coordination-v1';
const setting='schema.quota-subject-coordination',availability=new WeakMap();
const migration=new URL('./migrations/quota-subject-coordination.sql',import.meta.url);
const expectedConstraints=[
  ['quota_subject_accounts','quota_subject_accounts_account_id_fkey','FOREIGN KEY (account_id) REFERENCES harbor.accounts(id)'],
  ['quota_subject_accounts','quota_subject_accounts_pkey','PRIMARY KEY (account_id, identity_epoch)'],
  ['quota_subject_accounts','quota_subject_accounts_subject_ref_fkey','FOREIGN KEY (subject_ref) REFERENCES harbor.quota_subjects(ref)'],
  ['quota_subject_confirmations','quota_subject_confirmations_actor_kind_check',"CHECK ((actor_kind = ANY (ARRAY['super_admin'::text, 'department_admin'::text])))"],
  ['quota_subject_confirmations','quota_subject_confirmations_pkey','PRIMARY KEY (request_id)'],
  ['quota_subject_confirmations','quota_subject_confirmations_request_id_fkey','FOREIGN KEY (request_id) REFERENCES harbor.requests(id)'],
  ['quota_subject_confirmations','quota_subject_confirmations_statement_check',"CHECK ((statement = ANY (ARRAY['upstream_finished'::text, 'not_executed'::text])))"],
  ['quota_subject_confirmations','quota_subject_confirmations_subject_ref_fkey','FOREIGN KEY (subject_ref) REFERENCES harbor.quota_subjects(ref)'],
  ['quota_subjects','quota_subjects_check','CHECK ((((checked_from IS NULL) AND (checked IS NULL) AND (snapshot IS NULL)) OR ((checked_from IS NOT NULL) AND (checked IS NOT NULL) AND (checked_from >= 0) AND (checked >= checked_from) AND (snapshot IS NOT NULL))))'],
  ['quota_subjects','quota_subjects_pkey','PRIMARY KEY (ref)'],
  ['quota_subjects','quota_subjects_ref_check',"CHECK ((ref ~ '^[a-f0-9]{64}$'::text))"],
  ['quota_subjects','quota_subjects_scope_check',"CHECK ((scope = ANY (ARRAY['account_metered_bucket'::text, 'same_credential'::text])))"],
  ['requests','quota_subject_request_pair',"CHECK ((((quota_subject_ref IS NULL) AND (quota_subject_scope IS NULL)) OR ((quota_subject_ref IS NOT NULL) AND (quota_subject_scope IS NOT NULL) AND (quota_subject_scope = ANY (ARRAY['account_metered_bucket'::text, 'same_credential'::text])))))"],
  ['requests','requests_quota_subject_ref_fkey','FOREIGN KEY (quota_subject_ref) REFERENCES harbor.quota_subjects(ref)']
];
const fail=(code='quota_subject_contract_invalid',status=503)=>{throw new AppError(status,'供应方共享额度暂不能安全核对，请稍后重试',code);};
const source=async()=>{const sql=await readFile(migration,'utf8');return {sql,checksum:createHash('sha256').update(sql).digest('hex')};};
const lock=store=>store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-quota-subject-coordination-v1',0))");
const subjectRequests=`SELECT r.id,r.ended,r.usage_unknown,r.input,r.output FROM harbor.requests r WHERE r.quota_subject_ref=$1
  UNION ALL SELECT r.id,r.ended,r.usage_unknown,r.input,r.output FROM harbor.quota_subject_accounts qb JOIN harbor.requests r ON r.account_id=qb.account_id
  WHERE qb.subject_ref=$1 AND r.quota_subject_ref IS NULL AND r.quota_ordinary IS DISTINCT FROM FALSE AND r.quota_account_identity_epoch IS NOT NULL AND r.quota_account_identity_epoch=qb.identity_epoch
  UNION ALL SELECT r.id,r.ended,r.usage_unknown,r.input,r.output FROM harbor.quota_subject_accounts qb JOIN harbor.requests r ON r.account_id=qb.account_id
  WHERE qb.subject_ref=$1 AND r.quota_subject_ref IS NULL AND r.quota_ordinary IS DISTINCT FROM FALSE AND r.ended IS NULL AND r.status IN ('queued','running')
  AND NOT EXISTS(SELECT 1 FROM harbor.quota_subject_accounts known WHERE known.account_id=r.account_id AND known.identity_epoch=r.quota_account_identity_epoch)`;

export async function verifyQuotaSubjectAvailability(store){
  const marker=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting);
  const present=await store.get("SELECT to_regclass('harbor.quota_subject_confirmations') IS NOT NULL OR to_regclass('harbor.quota_subjects') IS NOT NULL OR to_regclass('harbor.quota_subject_accounts') IS NOT NULL OR EXISTS(SELECT 1 FROM pg_attribute WHERE ((attrelid='harbor.requests'::regclass AND attname IN ('quota_subject_ref','quota_subject_scope','quota_account_identity_epoch','quota_ordinary','quota_ordinary_denied')) OR (attrelid='harbor.accounts'::regclass AND attname='quota_identity_epoch')) AND NOT attisdropped) present");
  if(!marker){if(present.present)fail();availability.set(store,false);return false;}
  let value;try{value=JSON.parse(marker.value);}catch{fail();}
  if(value.contract!==quotaSubjectContract||value.checksum!==(await source()).checksum||value.version!==1||!present.present)fail();
  const columns=await store.all("SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod) type,a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND ((c.relname IN ('quota_subjects','quota_subject_accounts','quota_subject_confirmations')) OR (c.relname='requests' AND a.attname IN ('quota_subject_ref','quota_subject_scope','quota_account_identity_epoch','quota_ordinary','quota_ordinary_denied')) OR (c.relname='accounts' AND a.attname='quota_identity_epoch')) AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attname");
  const expected=[['accounts','quota_identity_epoch','bigint',true],['quota_subject_accounts','account_id','text',true],['quota_subject_accounts','identity_epoch','bigint',true],['quota_subject_accounts','subject_ref','text',true],['quota_subject_accounts','updated','bigint',true],['quota_subject_confirmations','actor_id','text',true],['quota_subject_confirmations','actor_kind','text',true],['quota_subject_confirmations','actor_version','bigint',true],['quota_subject_confirmations','confirmed_at','bigint',true],['quota_subject_confirmations','reason','text',true],['quota_subject_confirmations','request_id','text',true],['quota_subject_confirmations','statement','text',true],['quota_subject_confirmations','subject_ref','text',true],['quota_subjects','checked','bigint',false],['quota_subjects','checked_from','bigint',false],['quota_subjects','denial_at','bigint',false],['quota_subjects','ref','text',true],['quota_subjects','schema_version','text',false],['quota_subjects','scope','text',true],['quota_subjects','shared','boolean',true],['quota_subjects','snapshot','jsonb',false],['quota_subjects','source','text',true],['requests','quota_account_identity_epoch','bigint',false],['requests','quota_ordinary','boolean',false],['requests','quota_ordinary_denied','boolean',false],['requests','quota_subject_ref','text',false],['requests','quota_subject_scope','text',false]];
  if(JSON.stringify(columns.map(row=>[row.relname,row.attname,row.type,row.attnotnull]))!==JSON.stringify(expected))fail();
  const constraints=await store.all("SELECT c.relname,co.conname,pg_get_constraintdef(co.oid) def FROM pg_constraint co JOIN pg_class c ON c.oid=co.conrelid WHERE co.convalidated AND co.contype<>'n' AND (c.oid IN ('harbor.quota_subjects'::regclass,'harbor.quota_subject_accounts'::regclass,'harbor.quota_subject_confirmations'::regclass) OR (c.oid='harbor.requests'::regclass AND co.conname IN ('quota_subject_request_pair','requests_quota_subject_ref_fkey'))) ORDER BY c.relname,co.conname");
  if(JSON.stringify(constraints.map(row=>[row.relname,row.conname,row.def]))!==JSON.stringify(expectedConstraints))fail();
  const tables=await store.all("SELECT relname,relkind,relpersistence,relrowsecurity,(has_table_privilege(current_user,oid,'SELECT') AND has_table_privilege(current_user,oid,'INSERT') AND (relname='quota_subject_confirmations' OR has_table_privilege(current_user,oid,'UPDATE'))) permitted FROM pg_class WHERE oid IN ('harbor.quota_subjects'::regclass,'harbor.quota_subject_accounts'::regclass,'harbor.quota_subject_confirmations'::regclass)");
  if(tables.length!==3||tables.some(row=>row.relkind!=='r'||row.relpersistence!=='p'||row.relrowsecurity||!row.permitted))fail();
  const indexes=await store.all("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='harbor' AND indexname IN ('quota_subject_account_ref','quota_subject_request_state','quota_subject_unbound_account','quota_subject_request_denial') ORDER BY indexname");
  if(JSON.stringify(indexes)!==JSON.stringify([{indexname:'quota_subject_account_ref',indexdef:'CREATE INDEX quota_subject_account_ref ON harbor.quota_subject_accounts USING btree (subject_ref)'},{indexname:'quota_subject_request_denial',indexdef:'CREATE INDEX quota_subject_request_denial ON harbor.requests USING btree (account_id, quota_account_identity_epoch, ended) WHERE (quota_ordinary_denied IS TRUE)'},{indexname:'quota_subject_request_state',indexdef:'CREATE INDEX quota_subject_request_state ON harbor.requests USING btree (quota_subject_ref, ended, status) WHERE (quota_subject_ref IS NOT NULL)'},{indexname:'quota_subject_unbound_account',indexdef:'CREATE INDEX quota_subject_unbound_account ON harbor.requests USING btree (account_id, quota_account_identity_epoch, ended) WHERE ((quota_subject_ref IS NULL) AND (quota_ordinary IS DISTINCT FROM false) AND (quota_account_identity_epoch IS NOT NULL))'}]))fail();
  if((await store.get("SELECT COUNT(*)::int n FROM pg_index WHERE indexrelid IN ('harbor.quota_subject_account_ref'::regclass,'harbor.quota_subject_request_state'::regclass,'harbor.quota_subject_unbound_account'::regclass,'harbor.quota_subject_request_denial'::regclass) AND indisvalid AND indisready AND indislive")).n!==4)fail();
  availability.set(store,true);return true;
}

export async function migrateQuotaSubjects(store,{apply=false,runtimeRole}={}){
  if(typeof apply!=='boolean'||runtimeRole!==undefined&&(typeof runtimeRole!=='string'||!/^[a-z_][a-z0-9_]{0,62}$/.test(runtimeRole)))fail('quota_subject_migration_invalid');
  if(!apply){await verifyQuotaSubjectAvailability(store);return false;}
  return store.transaction(async()=>{
    await lock(store);if(await verifyQuotaSubjectAvailability(store))return false;
    const bundle=await source();let runtime=runtimeRole;
    if((await store.get("SELECT to_regclass('harbor_operator.activation') relation")).relation){
      const trusted=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton'),database=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');
      if(!trusted||trusted.database_oid!==database.oid||runtime!==undefined&&trusted.runtime!==undefined&&runtime!==trusted.runtime)fail('quota_subject_runtime_invalid');
      runtime=trusted.runtime??runtime;if(!runtime)fail('quota_subject_runtime_required');
    }
    await store.query(bundle.sql);
    if(runtime){if(!/^[a-z_][a-z0-9_]{0,62}$/.test(runtime))fail('quota_subject_runtime_invalid');await store.query('GRANT SELECT,INSERT,UPDATE ON harbor.quota_subjects,harbor.quota_subject_accounts TO "'+runtime+'"');await store.query('GRANT SELECT,INSERT ON harbor.quota_subject_confirmations TO "'+runtime+'"');}
    await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',setting,JSON.stringify({version:1,contract:quotaSubjectContract,checksum:bundle.checksum}));
    await verifyQuotaSubjectAvailability(store);return true;
  });
}

async function installed(store){if(!availability.has(store))await verifyQuotaSubjectAvailability(store);return availability.get(store);}
export const quotaSubjectsReady=installed;
export async function lockQuotaSubjectState(store){if(!store.context.getStore())fail('quota_subject_transaction_required');await lock(store);}
export async function quotaSubjectScopeHints(store,accountIds){
  const unknown=()=>({state:'unknown',ordinary_scope:'unknown',extra_scope:'unknown'}),hints=new Map(accountIds.map(id=>[id,unknown()]));
  if(!accountIds.length||!await installed(store))return hints;
  const rows=await store.all('SELECT b.account_id,s.scope FROM harbor.quota_subject_accounts b JOIN harbor.accounts a ON a.id=b.account_id AND a.quota_identity_epoch=b.identity_epoch JOIN harbor.quota_subjects s ON s.ref=b.subject_ref WHERE b.account_id=ANY($1::text[])',accountIds);
  for(const row of rows)if(hints.has(row.account_id)&&['account_metered_bucket','same_credential'].includes(row.scope))hints.set(row.account_id,{state:'known',ordinary_scope:row.scope,extra_scope:'unknown'});
  return hints;
}
export async function invalidateQuotaSubjectIdentity(store,accountId){
  if(!await installed(store))return;
  if(!store.context.getStore())fail('quota_subject_transaction_required');await lock(store);
  await store.run('UPDATE harbor.accounts SET quota_identity_epoch=quota_identity_epoch+1 WHERE id=$1',accountId);
}

// Persist with the real request terminal and the existing encrypted metering
// replay, not with the current account's mutable routing identity. A replay
// older than a covering official read cannot re-latch a recovered denial.
export async function recordQuotaSubjectDenial(store,request){
  if(!await installed(store))return;
  if(!store.context.getStore())fail('quota_subject_transaction_required');await lock(store);
  if(request.quota_ordinary!==true||request.quota_account_identity_epoch===null||request.ended===null||request.quota_subject_scope==='same_credential')return;
  // Keep the trusted terminal fact even before the first identity read. The
  // later binding may correlate this exact frozen generation, never history
  // guessed from mutable account data. This shares the real finish transaction.
  await store.run('UPDATE harbor.requests SET quota_ordinary_denied=true WHERE id=$1',request.id);
  if(!request.quota_subject_ref){
    const binding=await store.get('SELECT b.subject_ref,s.scope FROM harbor.quota_subject_accounts b JOIN harbor.quota_subjects s ON s.ref=b.subject_ref WHERE b.account_id=$1 AND b.identity_epoch=$2',request.account_id,request.quota_account_identity_epoch);
    if(binding)request={...request,quota_subject_ref:binding.subject_ref,quota_subject_scope:binding.scope};
  }
  if(request.quota_subject_scope!=='account_metered_bucket'||!request.quota_subject_ref)return;
  const subject=await store.get('SELECT * FROM harbor.quota_subjects WHERE ref=$1 FOR UPDATE',request.quota_subject_ref);if(!subject||subject.scope!=='account_metered_bucket')fail();
  await latchDenial(store,subject,request.ended);
}
async function latchDenial(store,subject,ended){
  if(ended===null||subject.checked_from!==null&&subject.checked_from>ended)return;
  let snapshot=subject.snapshot;
  if(snapshot){snapshot={...snapshot,ordinaryUsageAllowed:false};Object.assign(snapshot,retainCodexQuotaDenials(subject.snapshot,snapshot));}
  await store.run('UPDATE harbor.quota_subjects SET snapshot=$1,denial_at=GREATEST(denial_at,$3) WHERE ref=$2',snapshot,subject.ref,ended);
  subject.snapshot=snapshot;subject.denial_at=Math.max(subject.denial_at??0,ended);
}
const integer=value=>{const n=Number(value);if(!Number.isSafeInteger(n)||n<0)fail('quota_subject_accounting_unknown');return n;};
function ordinarySnapshot(snapshot){
  return {checked:snapshot.checked,windows:snapshot.windows,remaining:snapshot.remaining,ordinaryUsageAllowed:snapshot.ordinaryUsageAllowed,limitReached:snapshot.limitReachedType==='rate_limit_reached',limitReachedType:snapshot.limitReachedType==='rate_limit_reached'?'rate_limit_reached':null};
}

export function createPostgresQuotaSubjects(store,{identity,clock=Date.now}={}){
  async function transaction(){if(!store.context.getStore())fail('quota_subject_transaction_required');await lock(store);}
  async function bind(account,facts){
    if(facts?.state!=='known')return null;
    if(!['account_metered_bucket','same_credential'].includes(facts.ordinary_scope)||!/^[a-f0-9]{64}$/.test(facts.ref))fail();
    const prior=await store.get('SELECT subject_ref FROM harbor.quota_subject_accounts WHERE account_id=$1 AND identity_epoch=$2',account.id,account.quota_identity_epoch);
    if(prior&&prior.subject_ref!==facts.ref){
      const changed=await store.get('UPDATE harbor.accounts SET quota_identity_epoch=quota_identity_epoch+1 WHERE id=$1 AND quota_identity_epoch=$2 RETURNING quota_identity_epoch',account.id,account.quota_identity_epoch);if(!changed)fail('quota_subject_identity_changed',409);
      account.quota_identity_epoch=changed.quota_identity_epoch;
    }
    await store.run('INSERT INTO harbor.quota_subjects(ref,scope,source,schema_version) VALUES($1,$2,$3,$4) ON CONFLICT(ref) DO NOTHING',facts.ref,facts.ordinary_scope,facts.source,facts.schema_version??null);
    const subject=await store.get('SELECT * FROM harbor.quota_subjects WHERE ref=$1 FOR UPDATE',facts.ref);if(subject.scope!==facts.ordinary_scope)fail();
    await store.run('INSERT INTO harbor.quota_subject_accounts(account_id,identity_epoch,subject_ref,updated) VALUES($1,$2,$3,$4) ON CONFLICT(account_id,identity_epoch) DO UPDATE SET subject_ref=EXCLUDED.subject_ref,updated=EXCLUDED.updated',account.id,account.quota_identity_epoch,facts.ref,clock());
    if(subject.scope==='account_metered_bucket'){
      const denial=await store.get('SELECT MAX(ended) ended FROM harbor.requests WHERE account_id=$1 AND quota_account_identity_epoch=$2 AND quota_ordinary=true AND quota_ordinary_denied IS TRUE',account.id,account.quota_identity_epoch);
      await latchDenial(store,subject,denial.ended);
    }
    await store.run(`UPDATE harbor.quota_subjects s SET shared=true WHERE ref=$1 AND NOT shared AND (
      SELECT COUNT(DISTINCT account_id) FROM (
        SELECT b.account_id FROM harbor.quota_subject_accounts b WHERE b.subject_ref=$1
        UNION ALL SELECT r.account_id FROM harbor.requests r WHERE r.quota_subject_ref=$1
      ) owners
    )>1`,facts.ref);return subject;
  }
  async function refresh(account,facts,snapshot,{checkedFrom}={}){
    if(!await installed(store)||facts?.state!=='known')return {coordinated:false};
    await transaction();if(!Number.isSafeInteger(checkedFrom)||checkedFrom<0||!Number.isSafeInteger(snapshot?.checked)||snapshot.checked<checkedFrom)fail('quota_subject_snapshot_invalid');
    const subject=await bind(account,facts);
    if(subject.checked!==null&&subject.checked>checkedFrom)return {coordinated:true,stale:true};
    const next=ordinarySnapshot(snapshot);
    if(subject.denial_at!==null&&checkedFrom<=subject.denial_at)next.ordinaryUsageAllowed=false;
    Object.assign(next,retainCodexQuotaDenials(subject.snapshot,next));
    await store.run('UPDATE harbor.quota_subjects SET snapshot=$1,checked_from=$2,checked=$3 WHERE ref=$4',next,checkedFrom,snapshot.checked,facts.ref);return {coordinated:true};
  }
  async function binding(account){
    if(account.auth_mode==='openai_api'){
      const facts=apiCredentialQuotaSubject(identity,account.api_secret_digest);
      if(facts.state==='known'){
        // Identical stored digests establish only identical credentials. Save
        // that internal identity for all peers before deciding admission, so a
        // rejected peer cannot make an already-known duplicate disappear.
        const peers=await store.all("SELECT id,quota_identity_epoch FROM harbor.accounts WHERE auth_mode='openai_api' AND kind='codex' AND api_secret_digest=$1 AND removed_at IS NULL ORDER BY id",account.api_secret_digest);
        for(const peer of peers)await bind(peer.id===account.id?account:peer,facts);
      }
    }
    return store.get('SELECT b.subject_ref,s.* FROM harbor.quota_subject_accounts b JOIN harbor.quota_subjects s ON s.ref=b.subject_ref WHERE b.account_id=$1 AND b.identity_epoch=$2 FOR UPDATE OF s',account.id,account.quota_identity_epoch);
  }
  async function check(request,account,subject){
    if(!subject?.shared)return;
    // Real request terminal facts are the only permit state machine. Unknown
    // execution remains held until a trusted later snapshot covers its end.
    const pending=await store.get(`SELECT 1 FROM (${subjectRequests}) r WHERE r.id<>$2 AND (r.ended IS NULL OR (r.usage_unknown<>0 AND ($3::bigint IS NULL OR r.ended>=$3) AND NOT EXISTS(SELECT 1 FROM harbor.quota_subject_confirmations c WHERE c.request_id=r.id AND c.subject_ref=$1))) LIMIT 1`,subject.ref,request.id,subject.checked_from);
    if(pending)fail('shared_supplier_quota_busy',429);
    if(subject.scope==='account_metered_bucket'){
      if(!subject.snapshot)fail('shared_supplier_quota_unknown',429);
      const used=await store.get(`SELECT COALESCE(SUM(r.input+r.output),0)::text tokens FROM (${subjectRequests}) r WHERE r.id<>$2 AND r.ended>=$3 AND r.usage_unknown=0`,subject.ref,request.id,subject.checked_from);
      const calibration=await store.get('SELECT MIN(a.tokens_per_percent) n FROM harbor.quota_subject_accounts b JOIN harbor.accounts a ON a.id=b.account_id AND a.quota_identity_epoch=b.identity_epoch WHERE b.subject_ref=$1 AND a.tokens_per_percent>0',subject.ref);
      const capacity=accountCapacity({...account,quota_json:JSON.stringify(subject.snapshot),token_budget:null,budget_used:0,since_check_tokens:integer(used.tokens),tokens_per_percent:calibration.n??account.tokens_per_percent},0,0,clock());
      if(!capacity.eligible)fail('shared_supplier_quota_unavailable',429);
    }
  }
  async function admit(requestId,account,{mode,replaceConfirmedUnused=false}={}){
    if(!await installed(store))return {coordinated:false};
    await transaction();const request=await store.get('SELECT * FROM harbor.requests WHERE id=$1',requestId),current=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',account.id);
    if(!request||!current||request.account_id!==current.id||current.route_epoch!==account.route_epoch||request.ended!==null||request.department_id!==current.department_id)fail('quota_subject_request_binding_conflict',409);
    if(!replaceConfirmedUnused&&request.quota_account_identity_epoch!==null&&request.quota_account_identity_epoch!==current.quota_identity_epoch)fail('quota_subject_identity_changed',409);
    if(mode==='manual_extra'){
      if(request.quota_subject_ref&&request.status!=='queued'&&!replaceConfirmedUnused)fail('quota_subject_request_binding_conflict',409);
      await store.run('UPDATE harbor.requests SET quota_subject_ref=NULL,quota_subject_scope=NULL,quota_account_identity_epoch=$2,quota_ordinary=false WHERE id=$1',requestId,current.quota_identity_epoch);
      return {coordinated:false,reason:'extra_scope_unknown'};
    }
    const subject=await binding(current);
    if(!replaceConfirmedUnused&&request.quota_account_identity_epoch!==null&&request.quota_account_identity_epoch!==current.quota_identity_epoch)fail('quota_subject_identity_changed',409);
    await store.run('UPDATE harbor.requests SET quota_account_identity_epoch=$1,quota_ordinary=true WHERE id=$2',current.quota_identity_epoch,requestId);
    if(request.quota_subject_ref&&request.quota_subject_ref!==subject?.ref&&!replaceConfirmedUnused)fail('quota_subject_request_binding_conflict',409);
    if(subject){await check(request,current,subject);await store.run('UPDATE harbor.requests SET quota_subject_ref=$1,quota_subject_scope=$2 WHERE id=$3',subject.ref,subject.scope,requestId);}
    else if(replaceConfirmedUnused)await store.run('UPDATE harbor.requests SET quota_subject_ref=NULL,quota_subject_scope=NULL WHERE id=$1',requestId);
    return {coordinated:!!subject};
  }
  async function beforeDispatch(requestId,{mode}={}){
    if(!await installed(store))return {coordinated:false};
    await transaction();const request=await store.get('SELECT * FROM harbor.requests WHERE id=$1',requestId);if(!request||request.ended!==null)fail('quota_subject_request_binding_conflict',409);
    const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',request.account_id);if(!account)fail('quota_subject_request_binding_conflict',409);
    return admit(requestId,account,{mode});
  }
  return {refresh,admit,beforeDispatch};
}
