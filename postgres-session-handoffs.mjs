import {AppError,id,nextDailyReset,usageWindow,participatesNightly} from './store.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';

const safeReasons=new Set(['session_handoff_expired','session_route_conflict','session_disabled','session_route_unsupported','session_context_unsupported','invalid_route_account','session_account_changed','session_permissions_changed','session_busy','service_stopping','session_model_unsupported']);
const failure=(code,message,status=409)=>{throw new AppError(status,message,code);};
const fields='id,session_id,source_account_id,target_account_id,expected_route_version,origin,status,requested,expires,finished,reason,applied_route_version,pin_expires';

// Only the gateway owns request tickets. Durable rows are decisions, never a
// claim that a binding has changed. A worker cannot apply them independently.
export function createPostgresSessionHandoffs(store,{gate,readStore=store,queryQueue,clock=Date.now,isStopping=()=>false,validateTarget,maxPending=64,pendingMs=900000}={}){
  if(!gate||['register','stop'].some(k=>typeof gate[k]!=='function')||typeof validateTarget!=='function'||!Number.isSafeInteger(maxPending)||maxPending<1||maxPending>64||!Number.isSafeInteger(pendingMs)||pendingMs<30000||pendingMs>86400000)throw new AppError(503,'会话交接服务配置无效','postgres_configuration');
  const active=new Map(),operations=new Set(),checks=createPostgresQueryQueue({maxConcurrent:2,maxQueued:64,queueTimeoutMs:15000});let stopping=false,lastError=null,automatic=null;
  const alive=signal=>{if(stopping||isStopping()||signal?.aborted)failure('service_stopping','会话交接已停止',503);};
  const audit=(action,detail)=>store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),action,JSON.stringify(detail));
  const publicRow=row=>row?{...row,phase:row.status==='waiting'?(row.origin==='automatic'?(['checking','applying'].includes(automatic?.phase(row.id))?automatic.phase(row.id):'waiting'):active.has(row.id)?active.get(row.id).handle.status().state==='applying'?active.get(row.id).stage==='checking'?'checking':'applying':'waiting':lastError?'recovery_required':'waiting'):row.status}:null;
  async function latest(sessionId){
    const at=clock(),row=await readStore.get(`SELECT ${fields.split(',').map(name=>'h.'+name).join(',')},
      s.account_id binding_account,s.route_version binding_version,s.protocol binding_protocol,s.enabled binding_enabled,
      s.nightly_enabled binding_nightly,s.nightly_expires binding_expires,a.name binding_name,a.auth_mode binding_auth,
      source.name source_account_name,target.name target_account_name
      FROM harbor.sessions s LEFT JOIN harbor.accounts a ON a.id=s.account_id
      LEFT JOIN LATERAL(SELECT ${fields} FROM harbor.session_handoffs WHERE session_id=s.id AND requested>=$2 ORDER BY (status='waiting') DESC,sequence DESC LIMIT 1) h ON TRUE
      LEFT JOIN harbor.accounts source ON source.id=h.source_account_id LEFT JOIN harbor.accounts target ON target.id=h.target_account_id
      WHERE s.id=$1`,sessionId,usageWindow(at).start);
    if(!row)throw new AppError(404,'会话不存在');
    const {binding_account,binding_version,binding_protocol,binding_enabled,binding_nightly,binding_expires,binding_name,binding_auth,...detail}=row;
    const binding={account_id:binding_account,account_name:binding_name,route_version:binding_version,protocol:binding_protocol,enabled:binding_enabled,nightly_enabled:Number(participatesNightly({nightly_enabled:binding_nightly,nightly_expires:binding_expires},at)),nightly_expires:binding_expires,auth_mode:binding_auth};
    // Current binding and latest result share one SQL snapshot. A historical
    // completed target must never masquerade as a later forced binding.
    return {...(detail.id===null?{id:null,session_id:sessionId,status:'none',phase:'none'}:publicRow(detail)),binding};
  }
  async function inspect(sessionId,{lock=false}={}){
    const identity=await store.get('SELECT member_id FROM harbor.sessions WHERE id=$1',sessionId);if(!identity)throw new AppError(404,'会话不存在');
    const member=await store.get('SELECT id,enabled,removed_at FROM harbor.members WHERE id=$1'+(lock?' FOR UPDATE':''),identity.member_id);
    if(lock)await lockPostgresRouting(store);
    const session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2'+(lock?' FOR UPDATE':''),sessionId,identity.member_id);
    if(!session||!member?.enabled||member.removed_at!==null||!session.enabled)failure('session_disabled','会话或员工不可访问');
    if(session.protocol!=='responses')failure('session_route_unsupported','此通道不支持请求间平滑转移');
    return {member,session};
  }
  async function sourceContext(session,{allowPending=false}={}){
    const last=await store.get('SELECT key_id,model,handoff_context,handoff_input,status,started,query_sequence FROM harbor.requests WHERE session_id=$1 AND route_version=$2 ORDER BY query_sequence DESC LIMIT 1',session.id,session.route_version);
    const pendingInput=allowPending&&['queued','running'].includes(last?.status)&&last?.handoff_input==='replayable';
    if(last&&last.handoff_context!=='replayable'&&!pendingInput)failure('session_context_unsupported','当前上下文尚未验证可跨账号续接，请由客户端提供可重放的完整上下文');
    if(last&&await store.get(`SELECT id FROM harbor.requests WHERE session_id=$1 AND route_version=$2 AND query_sequence<$3
      AND (ended IS NULL OR ended>=$4) AND handoff_context<>'replayable'
      AND NOT($5 AND status IN ('queued','running') AND handoff_input='replayable') LIMIT 1`,session.id,session.route_version,last.query_sequence,last.started,allowPending))failure('session_context_unsupported','并行旧请求存在尚未验证可跨账号续接的上下文');
    if(last){
      const key=await store.get('SELECT id,enabled,expires FROM harbor.keys WHERE id=$1 AND member_id=$2 AND removed_at IS NULL',last.key_id,session.member_id);
      if(!key?.enabled||key.expires!==null&&key.expires<=clock())failure('session_permissions_changed','最近请求的访问权限已变化，请重新核对');
      try{await store.checkModel({id:session.member_id,key_id:key.id},last.model);}catch{failure('session_permissions_changed','最近请求的模型权限已变化，请重新核对');}
    }
    return last;
  }
  async function targetAccount(accountId,{lock=false}={}){
    const target=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND kind='codex' AND enabled=1 AND removed_at IS NULL AND state='ready' AND route_block IS NULL"+(lock?' FOR SHARE':''),accountId);
    if(!target)failure('invalid_route_account','目标账号须已启用、检查通过并可使用Responses',400);return target;
  }
  async function checkedTarget(target,model,signal){
    const checkSignal=AbortSignal.any([signal,AbortSignal.timeout(15000)].filter(Boolean));
    return checks.run(async()=>{alive(checkSignal);const result=await validateTarget(target,model,checkSignal);alive(checkSignal);return result;});
  }
  async function restoreAutomatic(row,status,reason){
    if(!row.replaces_handoff_id)return;
    let restored=false;
    await store.transaction(async()=>{
      const original=await store.get('SELECT automatic_group_id FROM harbor.session_handoffs WHERE id=$1',row.replaces_handoff_id);if(!original?.automatic_group_id)return;
      const group=await store.get('SELECT status,expires FROM harbor.automatic_handoff_groups WHERE id=$1 FOR UPDATE',original.automatic_group_id);
      const current=await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1 FOR UPDATE',row.id);
      if(!current||current.status==='completed')return;
      if(current.status==='waiting'){
        await store.run('UPDATE harbor.session_handoffs SET status=$1,reason=$2,finished=$3 WHERE id=$4',status,reason,clock(),row.id);
        await audit('session.handoff.'+status,{session:row.session_id,handoff:row.id,reason});
      }
      if(group?.status==='waiting'&&group.expires>clock()&&['waiting','failed','cancelled'].includes(current.status))restored=!!(await store.run("UPDATE harbor.session_handoffs SET status='waiting',reason=NULL,finished=NULL WHERE id=$1 AND status='superseded'",row.replaces_handoff_id)).changes;
    });
    if(restored)automatic?.restored();
  }
  async function apply(decisionId,signal){
    const currentStage=active.get(decisionId);if(currentStage)currentStage.stage='checking';
    alive(signal);
    const decision=await store.get('SELECT * FROM harbor.session_handoffs WHERE id=$1',decisionId);
    if(!decision||decision.status!=='waiting')failure('session_route_conflict','转移决定已变化');
    const before=await inspect(decision.session_id),last=await sourceContext(before.session),target=await targetAccount(decision.target_account_id);
    // Directory/credential checks are bounded and run outside every DB lock.
    if(!await checkedTarget(target,last?.model,signal))failure('session_model_unsupported','目标账号不支持当前会话模型');
    // No cancellation can interleave this synchronous phase transition with
    // entering the transaction. Once started, report its actual final result.
    alive(signal);if(currentStage)currentStage.stage='applying';
    await store.transaction(async()=>{
      alive(signal);const {session}=await inspect(decision.session_id,{lock:true});alive(signal);
      const row=await store.get('SELECT * FROM harbor.session_handoffs WHERE id=$1 FOR UPDATE',decisionId);
      if(row?.status!=='waiting'||session.account_id!==row.source_account_id||session.route_version!==row.expected_route_version||session.nightly_enabled!==row.source_nightly_enabled||session.nightly_expires!==row.source_nightly_expires)failure('session_route_conflict','会话绑定或人工配置已变化');
      if(row.expires<=clock()||row.pin_expires!==null&&row.pin_expires<=clock())failure('session_handoff_expired','本次转移已过期');
      const source=await store.get('SELECT route_epoch FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id),currentTarget=await targetAccount(row.target_account_id,{lock:true});
      if(source?.route_epoch!==row.source_epoch||currentTarget.route_epoch!==row.target_epoch)failure('session_account_changed','源或目标账号状态已变化');
      if(await store.get("SELECT id FROM harbor.requests WHERE session_id=$1 AND status IN ('queued','running') LIMIT 1",session.id))failure('session_busy','旧请求结算尚未完成，本次交接取消');
      const current=await sourceContext(session);if(current?.key_id!==last?.key_id||current?.model!==last?.model)failure('session_route_conflict','模型或访问权限已变化，请重新规划');
      const version=session.route_version+1,at=clock();
      await store.run('UPDATE harbor.sessions SET account_id=$1,upstream_thread=$2,route_version=$3,nightly_enabled=0,nightly_expires=$4,handoff_requires_replay=1 WHERE id=$5',row.target_account_id,id(),version,row.pin_expires,session.id);
      await store.run("UPDATE harbor.session_handoffs SET status='completed',finished=$1,applied_route_version=$2 WHERE id=$3",at,version,row.id);
      await audit('session.route',{sessionId:session.id,from:row.source_account_id,to:row.target_account_id,version,reason:'manual',handoff:row.id});
      await audit('session.handoff.completed',{session:session.id,handoff:row.id,version});alive(signal);
    });
  }
  function watch(row,handle,registered=Promise.resolve()){
    const state={row,handle,errorCode:null,stage:'waiting'};active.set(row.id,state);
    const task=handle.done.then(async result=>{
      // Cancellation may happen after the transaction callback returns but
      // before COMMIT makes its row visible to another connection. Finalize
      // only after the registration has definitively left that window.
      await registered;
      if(result.state==='completed'&&row.replaces_handoff_id)automatic?.restored();
      if(result.state==='completed'||result.state==='stopped')return;
      const status=result.state==='superseded'?'superseded':result.state==='failed'&&state.errorCode==='session_handoff_failed'?'failed':'cancelled',reason=result.state==='failed'?state.errorCode||'session_handoff_failed':result.state;
      if(['cancelled','failed'].includes(result.state))await restoreAutomatic(row,status,reason);
      await store.transaction(async()=>{const changed=await store.run("UPDATE harbor.session_handoffs SET status=$1,reason=$2,finished=$3 WHERE id=$4 AND status='waiting'",status,reason,clock(),row.id);if(changed.changes)await audit('session.handoff.'+status,{session:row.session_id,handoff:row.id,reason});});
    }).finally(()=>{if(active.get(row.id)===state)active.delete(row.id);operations.delete(task);});
    // A failed finalizer leaves the durable waiting row for recovery. Do not
    // retry binding or hide the write failure by marking it complete in memory.
    state.task=task;operations.add(task);task.catch(()=>{lastError='session_handoff_persistence';});
    return {state,task,activate:()=>handle.activate(async signal=>{try{await apply(row.id,signal);}catch(error){state.errorCode=safeReasons.has(error?.code)?error.code:'session_handoff_failed';throw error;}})};
  }
  async function request(sessionId,payload){
    alive();const validId=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,80}$/.test(value);
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(k=>!['account_id','expected_route_version','replace_handoff_id'].includes(k))||!validId(payload.account_id)||!Number.isSafeInteger(payload.expected_route_version)||payload.expected_route_version<0||payload.replace_handoff_id!==undefined&&!validId(payload.replace_handoff_id))throw new AppError(400,'转移参数无效');
    const before=await inspect(sessionId);if(before.session.route_version!==payload.expected_route_version)failure('session_route_conflict','绑定版本已变化');
    if(before.session.account_id===payload.account_id)throw new AppError(400,'请选择不同的目标账号');
    if(active.size>=maxPending&&!payload.replace_handoff_id)throw new AppError(429,'待转移会话已满','session_handoff_busy');
    const pendingDecision=await store.get("SELECT id,origin FROM harbor.session_handoffs WHERE session_id=$1 AND status='waiting'",sessionId),candidate=automatic?.replacement(sessionId),autoReplacement=pendingDecision?.origin==='automatic'&&candidate?.id===pendingDecision.id?candidate:null,replacementId=payload.replace_handoff_id??autoReplacement?.id;
    if(replacementId&&active.get(replacementId)?.row.session_id!==sessionId&&autoReplacement?.id!==replacementId)failure('session_route_conflict','待转移决定已变化');
    const handoffId=id(),row={id:handoffId,session_id:sessionId,...(autoReplacement?{replaces_handoff_id:autoReplacement.id}:{})};let observed;
    const handle=gate.register({memberId:before.member.id,sessionId,decisionId:handoffId,replaceDecisionId:replacementId,retainPrevious:!!autoReplacement,beforeRestore:async state=>restoreAutomatic(row,state==='failed'?'failed':'cancelled',observed?.state.errorCode||state)});
    let finishRegistration;const registered=new Promise(resolve=>{finishRegistration=resolve;});
    observed=watch(row,handle,registered);
    try{
      await store.transaction(async()=>{
        alive();const {session}=await inspect(sessionId,{lock:true});alive();
        if(session.route_version!==payload.expected_route_version||session.account_id===payload.account_id)failure('session_route_conflict','会话绑定已变化');
        await sourceContext(session,{allowPending:true});const target=await targetAccount(payload.account_id,{lock:true}),source=await store.get('SELECT route_epoch FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id);
        const pending=await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id=$1 AND status='waiting' FOR UPDATE",sessionId);
        if(pending&&pending.id!==replacementId)failure('session_route_conflict','待转移决定已变化');
        if(replacementId){
          const previous=await store.get('SELECT id,status FROM harbor.session_handoffs WHERE id=$1 AND session_id=$2 FOR UPDATE',replacementId,sessionId);
          if(!previous||!['waiting','superseded'].includes(previous.status))failure('session_route_conflict','待转移决定已变化');
          await store.run("UPDATE harbor.session_handoffs SET status='superseded',finished=$1,reason='replaced' WHERE id=$2",clock(),previous.id);
          await audit('session.handoff.superseded',{session:sessionId,handoff:previous.id,replacement:row.id});
        }
        const at=clock();Object.assign(row,{source_account_id:session.account_id,target_account_id:target.id,expected_route_version:session.route_version,source_epoch:source.route_epoch,target_epoch:target.route_epoch,source_nightly_enabled:session.nightly_enabled,source_nightly_expires:session.nightly_expires,pin_expires:target.auth_mode==='openai_api'?null:nextDailyReset(at),origin:'manual',status:'waiting',requested:at,expires:at+pendingMs});
        await store.run(`INSERT INTO harbor.session_handoffs(id,session_id,member_id,source_account_id,target_account_id,expected_route_version,source_epoch,target_epoch,source_nightly_enabled,source_nightly_expires,pin_expires,origin,status,requested,expires)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'manual','waiting',$12,$13)`,row.id,sessionId,session.member_id,row.source_account_id,row.target_account_id,row.expected_route_version,row.source_epoch,row.target_epoch,row.source_nightly_enabled,row.source_nightly_expires,row.pin_expires,at,row.expires);
        if(row.replaces_handoff_id)await store.run('UPDATE harbor.session_handoffs SET replaces_handoff_id=$1 WHERE id=$2',row.replaces_handoff_id,row.id);
        await audit('session.handoff.requested',{session:sessionId,handoff:row.id,from:row.source_account_id,to:row.target_account_id,version:session.route_version});alive();
        if(handle.status().state!=='registering')failure('session_route_conflict','交接登记已失效');
      });
      finishRegistration();observed.activate();return {handoff_id:handoffId,status:'waiting'};
    }catch(error){handle.cancel();finishRegistration();await observed.task.catch(()=>{});throw error;}
  }
  async function cancel(sessionId,handoffId){
    alive();const entry=active.get(handoffId);
    if(!entry&&automatic?.replacement(sessionId)?.id===handoffId){await automatic.cancel(sessionId,handoffId);return latest(sessionId);}
    if(!entry||entry.row.session_id!==sessionId)failure('session_route_conflict','待转移决定已结束或变化');
    const phase=entry.handle.status().state;
    if(!['registering','draining'].includes(phase)&&!(phase==='applying'&&entry.stage==='checking'))failure('session_route_conflict','已开始提交换绑或正在替换，请完成后再核对结果');
    entry.handle.cancel();await entry.task;return latest(sessionId);
  }
  async function recover(){
    alive();const rows=await store.all("SELECT * FROM harbor.session_handoffs WHERE status='waiting' AND origin='manual' ORDER BY requested,id LIMIT $1",maxPending+1);
    if(rows.length>maxPending)throw new AppError(503,'待转移决定超过恢复容量，请先核查配置','session_handoff_busy');
    for(const row of rows){
      alive();if(row.origin==='automatic')continue;
      const handle=gate.register({memberId:row.member_id,sessionId:row.session_id,decisionId:row.id}),observed=watch(row,handle);observed.activate();
    }
  }
  async function handle(path,method,payload){
    const match=path.match(/^\/api\/admin\/sessions\/([a-zA-Z0-9-]{1,80})\/handoffs(?:\/([a-zA-Z0-9-]{1,80}))?$/);if(!match)return null;
    if(method==='POST'&&!match[2])return {status:202,value:await request(match[1],payload)};
    if(method==='GET'&&!match[2])return {status:200,value:await (queryQueue?queryQueue.run(()=>latest(match[1])):latest(match[1]))};
    if(method==='DELETE'&&match[2])return {status:200,value:await cancel(match[1],match[2])};
    throw new AppError(405,'不支持的会话交接操作');
  }
  function stop(){stopping=true;gate.stop();}
  async function close(){stop();await checks.close();await Promise.allSettled([...operations]);}
  return {handle,recover,stop,close,reconcileAutomatic:()=>automatic?.reconcile(),contexts:{inspect,sourceContext,targetAccount,validateTarget:checkedTarget},attachAutomatic:value=>{if(automatic&&automatic!==value&&!automatic.status().stopping)failure('postgres_configuration','自动交接已接入',503);automatic=value;},status:()=>({pending:active.size,checks:checks.status(),stopping,lastError})};
}
