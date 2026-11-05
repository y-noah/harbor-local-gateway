import {isDeepStrictEqual as equal} from 'node:util';
import {AppError,id,participatesNightly} from './store.mjs';
import {registerRequestHandoffGroup} from './request-handoff-group.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {transferWindow} from './transfer-schedule.mjs';

const failure=(code,message,status=409)=>{throw new AppError(status,message,code);};
const terminalReason=error=>new Set(['session_context_unsupported','session_permissions_changed','session_model_unsupported','session_account_changed','session_handoff_expired','nightly_plan_expired','nightly_plan_changed','manual_weights_active','session_disabled','invalid_route_account','service_stopping']).has(error?.code)?error.code:'automatic_handoff_failed';

// The active gateway owns these barriers. Durable rows record an intended
// atomic group; a Worker may plan but cannot make or recover live bindings.
export function createPostgresAutomaticHandoffs(store,{gate,contexts,clock=Date.now,isStopping=()=>false,targetAccount=contexts?.targetAccount,maxSessions=20000,pendingMs=900000,onSettled=async()=>{}}={}){
  if(typeof targetAccount!=='function'||typeof onSettled!=='function'||!gate||typeof gate.register!=='function'||!contexts||['inspect','sourceContext','targetAccount','validateTarget'].some(key=>typeof contexts[key]!=='function')||!Number.isInteger(maxSessions)||maxSessions<1||maxSessions>100000||!Number.isSafeInteger(pendingMs)||pendingMs<30000||pendingMs>86400000)failure('automatic_handoff_configuration','自动交接依赖无效',503);
  const active=new Map(),bySession=new Map(),operations=new Set();let stopping=false,lastError=null,recoveryNeeded=false;
  const alive=signal=>{if(stopping||isStopping()||signal?.aborted)throw signal?.reason instanceof AppError?signal.reason:new AppError(503,'自动交接已停止','service_stopping');};
  async function policy(){
    const rows=await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('balancing','transfer_schedule','request_handoff_policy','routing_weights','account_package_policy')"),values=Object.fromEntries(rows.map(row=>[row.name,JSON.parse(row.value)]));return {weights:values.routing_weights??null,packagePolicy:values.account_package_policy??null,enabled:values.balancing?.nightly===true,requestBoundary:values.request_handoff_policy?.mode==='request-boundary',window:transferWindow(clock(),values.transfer_schedule)};
  }
  async function valid(plan){const current=await policy();alive();if(current.weights?.mode==='manual'&&current.weights.expires>clock())failure('manual_weights_active','临时人工比例生效，自动整组迁移已停止');if(!equal(current.weights,plan.header.configuration.routing_weights??null)||!equal(current.packagePolicy,plan.header.configuration.account_package_policy??null))failure('nightly_plan_changed','自动权重或套餐策略版本已变化');if(!current.requestBoundary||!current.enabled||!current.window.due||current.window.key!==plan.target||plan.expires<=clock())failure('nightly_plan_expired','自动交接所属时段已结束或停用');}
  async function persist(spec,rows,groupId){
    const {plan,entry}=spec,at=clock(),expires=Math.min(plan.expires,at+pendingMs);
    if(expires<=at)failure('session_handoff_expired','自动交接已过期');
    await store.transaction(async()=>{
      alive();await valid(plan);await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1 FOR UPDATE',plan.target);const owners=[...new Set(rows.map(row=>row.member_id))].sort();
      const members=await store.all('SELECT id,enabled,removed_at FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id FOR SHARE',owners);
      if(members.length!==owners.length||members.some(member=>!member.enabled||member.removed_at!==null))failure('session_disabled','会话员工已停用');
      await lockPostgresRouting(store);
      const previous=await store.get('SELECT * FROM harbor.automatic_handoff_groups WHERE plan_id=$1 AND group_ordinal=$2 FOR UPDATE',plan.id,entry.ordinal);
      if(previous){if(previous.id!==groupId||previous.status!=='waiting')failure('nightly_plan_changed','自动组决定已结束或变化');await onSettled(plan.target);return;}
      const sessions=await store.all('SELECT * FROM harbor.sessions WHERE id=ANY($1::text[]) ORDER BY id FOR UPDATE',rows.map(row=>row.session_id)),current=new Map(sessions.map(session=>[session.id,session]));
      await store.run("INSERT INTO harbor.automatic_handoff_groups(id,plan_id,slot,group_ordinal,requested,expires,status) VALUES($1,$2,$3,$4,$5,$6,'waiting')",groupId,plan.id,plan.target,entry.ordinal,at,expires);
      for(const row of rows){
        const session=current.get(row.session_id);if(!session||!session.enabled||session.member_id!==row.member_id||session.protocol!=='responses'||session.account_id!==row.source_account_id||session.route_version!==row.expected_route_version||!participatesNightly(session,at))failure('nightly_plan_changed','原绑定或人工配置已变化');
        const latest=await contexts.sourceContext(session,{allowPending:true});const target=await targetAccount(row.target_account_id,{lock:true,model:latest?.model}),source=await store.get('SELECT route_epoch FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id);
        Object.assign(row,{source_epoch:source.route_epoch,target_epoch:target.route_epoch,source_nightly_enabled:session.nightly_enabled,source_nightly_expires:session.nightly_expires,requested:at,expires});
        await store.run(`INSERT INTO harbor.session_handoffs(id,session_id,member_id,source_account_id,target_account_id,expected_route_version,source_epoch,target_epoch,source_nightly_enabled,source_nightly_expires,pin_expires,origin,status,requested,expires,automatic_group_id)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL,'automatic','waiting',$11,$12,$13)`,row.id,row.session_id,row.member_id,row.source_account_id,row.target_account_id,row.expected_route_version,row.source_epoch,row.target_epoch,row.source_nightly_enabled,row.source_nightly_expires,at,expires,groupId);
        await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),at,'session.handoff.requested',JSON.stringify({session:row.session_id,handoff:row.id,from:row.source_account_id,to:row.target_account_id,origin:'automatic',group:groupId}));
      }
      await onSettled(plan.target);alive();
    });
  }
  async function validate(state,signal){
    alive(signal);await valid(state.spec.plan);const seen=new Set();
    for(const row of state.rows){
      alive(signal);const {session}=await contexts.inspect(row.session_id),latest=await contexts.sourceContext(session),target=await targetAccount(row.target_account_id,{model:latest?.model});
      if(session.account_id!==row.source_account_id||session.route_version!==row.expected_route_version||target.route_epoch!==row.target_epoch||session.nightly_enabled!==row.source_nightly_enabled||session.nightly_expires!==row.source_nightly_expires)failure('nightly_plan_changed','等待期间绑定、目标或人工配置已变化');
      const key=JSON.stringify([target.id,target.route_epoch,latest?.model]);if(!seen.has(key)){seen.add(key);if(!await contexts.validateTarget(target,latest?.model,signal))failure('session_model_unsupported','目标不支持当前会话模型');}
    }
  }
  async function mark(state,status,reason){
    await store.transaction(async()=>{
      const slot=state.spec?.plan.target??state.slot;
      if(slot)await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1 FOR UPDATE',slot);
      const changed=await store.run("UPDATE harbor.automatic_handoff_groups SET status=$1,reason=$2,finished=$3 WHERE id=$4 AND status='waiting'",status,reason,clock(),state.id);if(!changed.changes)return false;
      const rows=await store.all("UPDATE harbor.session_handoffs SET status=$1,reason=$2,finished=$3 WHERE automatic_group_id=$4 AND status='waiting' RETURNING id,session_id",status,reason,clock(),state.id);
      for(const row of rows)await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'session.handoff.'+status,JSON.stringify({session:row.session_id,handoff:row.id,reason,origin:'automatic',group:state.id}));
      await onSettled(slot);
    });
  }
  async function start(spec,execute){
    alive();const {plan,entry,moves,sessions}=spec;if(!plan||!entry||!Array.isArray(moves)||!moves.length||moves.length>maxSessions||typeof execute!=='function')failure('automatic_handoff_configuration','自动组输入无效',503);
    const previous=await store.get('SELECT * FROM harbor.automatic_handoff_groups WHERE plan_id=$1 AND group_ordinal=$2',plan.id,entry.ordinal);
    if(previous&&previous.status!=='waiting')failure('nightly_plan_changed','自动组已结束，请核对实际结果');
    const existing=previous?await store.all('SELECT * FROM harbor.session_handoffs WHERE automatic_group_id=$1 ORDER BY id',previous.id):null,sessionMap=new Map(sessions.map(session=>[session.id,session]));
    const rows=existing||moves.map(move=>({id:id(),session_id:move.session_id,member_id:sessionMap.get(move.session_id)?.member_id,source_account_id:move.from,target_account_id:move.to,expected_route_version:move.version}));
    if(rows.length!==moves.length||rows.some(row=>!row.member_id||row.status&&row.status!=='waiting'))failure('nightly_plan_changed','自动组成员不完整');
    if(active.has(previous?.id))return {id:previous.id,done:active.get(previous.id).task};
    if(bySession.size+rows.length>maxSessions)failure('session_handoff_busy','自动待转移容量已满',429);
    const groupId=previous?.id??id(),intent=registerRequestHandoffGroup(gate,rows.map(row=>({memberId:row.member_id,sessionId:row.session_id,decisionId:row.id}))),state={id:groupId,intent,spec,rows,stage:'registering',error:null};
    let finishRegistration;const registered=new Promise(resolve=>{finishRegistration=resolve;});
    active.set(groupId,state);for(const row of rows)bySession.set(row.session_id,state);
    const task=(async()=>{
      await registered;const result=await intent.done;
      if(result.state!=='completed'&&result.state!=='stopped')await mark(state,result.state==='superseded'?'superseded':state.error?'failed':'cancelled',state.error||result.state);
      if(result.state!=='completed')throw state.errorObject||new AppError(409,'自动整组交接未完成',state.error||result.state);
      return result.value;
    })().finally(()=>{clearTimeout(state.deadline);if(active.get(groupId)===state)active.delete(groupId);for(const row of rows)if(bySession.get(row.session_id)===state)bySession.delete(row.session_id);operations.delete(task);});
    state.task=task;operations.add(task);task.catch(()=>{lastError='automatic_handoff_incomplete';});
    try{
      await persist(spec,rows,groupId);const expires=previous?.expires??rows[0].expires;
      state.deadline=setTimeout(()=>intent.cancel('expired'),Math.max(1,expires-clock()));state.deadline.unref?.();state.stage='waiting';finishRegistration();
      intent.activate(async(signal,beginCommit)=>{
        try{
          state.stage='checking';await validate(state,signal);await beginCommit();alive(signal);state.stage='applying';
          return await store.transaction(async()=>{
            await valid(plan);
            const value=await execute(signal,async()=>{
              // Match manual registration: members -> routing -> sessions ->
              // decision rows. Never lock decisions then wait for a member.
              const group=await store.get('SELECT status,expires FROM harbor.automatic_handoff_groups WHERE id=$1 FOR UPDATE',groupId);if(group?.status!=='waiting'||group.expires<=clock())failure('session_handoff_expired','自动组决定已过期');
              const current=await store.all('SELECT id,status FROM harbor.session_handoffs WHERE automatic_group_id=$1 ORDER BY id FOR UPDATE',groupId);if(current.length!==rows.length||current.some(row=>row.status!=='waiting'))failure('nightly_plan_changed','自动组已被人工决定替代');
              for(const row of rows){const checked=await contexts.inspect(row.session_id),source=await store.get('SELECT route_epoch FROM harbor.accounts WHERE id=$1',row.source_account_id),latest=await contexts.sourceContext(checked.session),target=await targetAccount(row.target_account_id,{model:latest?.model});
                if(source?.route_epoch!==row.source_epoch||target.route_epoch!==row.target_epoch||checked.session.account_id!==row.source_account_id||checked.session.route_version!==row.expected_route_version)failure('nightly_plan_changed','提交前账号、模型或绑定已变化');}
            });
            const at=clock();for(const row of rows){const checked=await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',row.session_id);if(checked?.account_id!==row.target_account_id||checked.route_version!==row.expected_route_version+1)failure('nightly_plan_changed','整组实际结果不一致');
              await store.run("UPDATE harbor.session_handoffs SET status='completed',finished=$1,applied_route_version=$2 WHERE id=$3 AND status='waiting'",at,row.expected_route_version+1,row.id);
              await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),at,'session.handoff.completed',JSON.stringify({session:row.session_id,handoff:row.id,origin:'automatic',group:groupId}));
            }
            await store.run("UPDATE harbor.automatic_handoff_groups SET status='completed',finished=$1 WHERE id=$2",at,groupId);await onSettled(plan.target);alive(signal);return value;
          });
        }catch(error){if(signal.aborted)throw signal.reason;state.error=terminalReason(error);state.errorObject=error;throw error;}
      },{deferCommit:true});
    }catch(error){state.error=terminalReason(error);state.errorObject=error;intent.cancel();finishRegistration();await task.catch(()=>{});throw error;}
    return {id:groupId,done:task};
  }
  async function reconcile(){
    alive();const current=await policy();
    for(const state of [...active.values()])if(!current.requestBoundary||!current.enabled||!current.window.due||state.spec.plan.target!==current.window.key||state.spec.plan.expires<=clock()||!equal(current.weights,state.spec.plan.header.configuration.routing_weights??null)||!equal(current.packagePolicy,state.spec.plan.header.configuration.account_package_policy??null)){if(current.weights?.mode==='manual'&&current.weights.expires>clock()){state.error='manual_weights_active';state.errorObject=new AppError(409,'临时人工比例生效，自动整组迁移已停止','manual_weights_active');}state.intent.cancel();await state.task.catch(()=>{});}
    // If writing a terminal state failed until the plan exhausted its retry
    // budget, no in-memory barrier remains to run that finalizer again. Reap
    // only obsolete/inactive durable groups; never touch a manual takeover.
    if(current.requestBoundary){const orphaned=await store.all(`SELECT g.id,g.slot,a.status application_status FROM harbor.automatic_handoff_groups g
      LEFT JOIN harbor.nightly_plan_applications a ON a.plan_id=g.plan_id
      WHERE g.status='waiting' AND ($1 OR g.slot<>$2 OR g.expires<=$3 OR a.status IN ('failed','interrupted','completed'))
      ORDER BY g.requested,g.id LIMIT 256`,!current.enabled||!current.window.due,current.window.key,clock());
      for(const row of orphaned)if(!active.has(row.id))await mark(row,row.application_status==='failed'?'failed':'cancelled',row.application_status==='failed'?'application_attempts_exhausted':'nightly_plan_expired');
    }
  }
  async function cancel(sessionId,decisionId){const state=bySession.get(sessionId);if(!state||!state.rows.some(row=>row.id===decisionId))failure('session_route_conflict','自动交接决定已变化');if(state.stage==='applying')failure('session_route_conflict','整组已开始提交，请核对实际结果');state.intent.cancel();await state.task.catch(()=>{});}
  const replacement=sessionId=>{const state=bySession.get(sessionId),row=state?.rows.find(row=>row.session_id===sessionId);return row&&state.stage!=='applying'?{id:row.id,group:state.id}:null;};
  async function close(){stopping=true;for(const state of active.values())state.intent.stop();await Promise.allSettled([...operations]);}
  return {start,run:async(spec,execute)=>(await start(spec,execute)).done,reconcile,cancel,replacement,close,has:groupId=>active.has(groupId),restored:()=>{recoveryNeeded=true;},takeRecovery:()=>{const needed=recoveryNeeded;recoveryNeeded=false;return needed;},discard:group=>mark(group,'cancelled','nightly_plan_expired'),phase:decisionId=>{for(const state of active.values())if(state.rows.some(row=>row.id===decisionId))return state.stage;return null;},status:()=>({pendingGroups:active.size,pendingSessions:bySession.size,stopping,lastError})};
}
