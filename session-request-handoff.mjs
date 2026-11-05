import {createStreamingStageObserver} from './streaming-stage-observer.mjs';
import {AppError} from './store.mjs';

// Single-active-gateway request boundary. Tickets begin before body upload and
// end after the request has really finished. Database ownership, routing and
// context checks remain the caller's responsibility, in a short transaction.
export function createSessionRequestHandoff({maxTickets=120,maxPending=120,maxWaiting=120,maxWaitingPerMember=4,maxWaitingPerSession=4,waitMs=30000,pendingMs=900000,stageObserver}={}){
  const bounded=(value,min,max)=>Number.isSafeInteger(value)&&value>=min&&value<=max;
  if(![maxTickets,maxWaiting,maxWaitingPerMember,maxWaitingPerSession].every(n=>bounded(n,1,10000))||!bounded(maxPending,1,200000)||!bounded(waitMs,1,120000)||!bounded(pendingMs,waitMs,86400000))throw new AppError(503,'请求间交接配置无效','session_handoff_configuration');
  const stages=createStreamingStageObserver(stageObserver);
  const tickets=new Set(),pending=new Map(),waitingMembers=new Map();let waiting=0,stopped=false;
  const key=(member,session)=>JSON.stringify([member,session]);
  const identity=value=>typeof value==='string'&&value.length>0&&value.length<=200;
  const cancelled=signal=>signal?.reason instanceof AppError?signal.reason:new AppError(499,'请求已取消','request_cancelled');
  function finish(entry,state){
    if(entry.finished)return;
    entry.finished=true;entry.result=state;clearTimeout(entry.deadline);
    if(state==='superseded'&&entry.previous){finish(entry.previous,'superseded');entry.previous=null;}
    if(pending.get(entry.key)===entry)pending.delete(entry.key);
    // Release only after apply has settled: an abort signal alone does not
    // prove that a database transaction has rolled back.
    for(const waiter of [...entry.waiters])waiter.resume();
    entry.resolve({state});
  }
  function restore(entry,state){
    if(entry.restoration)return;
    entry.state='restoring';
    entry.restoration=Promise.resolve().then(()=>entry.beforeRestore?.(state)).then(()=>{
      const previous=entry.previous;if(!previous||previous.finished||previous.controller.signal.aborted){finish(entry,entry.cancelled||state);return;}
      previous.old=entry.old;previous.state=entry.previousState;
      for(const waiter of entry.waiters){waiter.entry=previous;previous.waiters.add(waiter);}entry.waiters.clear();entry.previous=null;pending.set(entry.key,previous);
      finish(entry,state);previous.onResume?.();pump(previous);
    },()=>{const previous=entry.previous;entry.previous=null;if(previous&&!previous.finished)cancel(previous,'cancelled');finish(entry,'failed');});
  }
  function cancel(entry,reason='cancelled'){
    if(entry.finished)return false;
    if(entry.state==='suspended'){
      const replacement=pending.get(entry.key);
      if(replacement?.previous===entry){
        replacement.previous=null;if(!entry.detachReplacement||!replacement.retainPrevious)cancel(replacement,reason);
      }
    }
    entry.cancelled=reason;entry.controller.abort(new AppError(409,'本次请求间交接已取消','session_handoff_cancelled'));
    const previous=entry.previous;
    if(reason==='cancelled'&&entry.retainPrevious&&previous&&!previous.finished&&!previous.controller.signal.aborted){if(entry.state!=='applying')restore(entry,reason);return true;}
    if(reason==='cancelled'&&entry.state==='registering'&&previous&&!previous.finished&&!previous.controller.signal.aborted){
      // A rejected replacement never revokes the already accepted decision.
      // Its old cohort may have drained during validation; keep that progress,
      // all original waiter deadlines, and the original apply callback.
      previous.old=entry.old;previous.state=entry.previousState;
      for(const waiter of entry.waiters){waiter.entry=previous;previous.waiters.add(waiter);}
      entry.waiters.clear();entry.previous=null;pending.set(entry.key,previous);
      finish(entry,reason);previous.onResume?.();pump(previous);return true;
    }
    // Timeouts, exhausted waiting capacity and shutdown cancel the whole
    // reservation instead of restoring an older decision behind its deadline.
    if(previous&&!previous.finished){entry.previous=null;cancel(previous,reason);}
    if(entry.state!=='applying')finish(entry,reason);
    return true;
  }
  function pump(entry){
    if(entry.finished||entry.state!=='draining'||entry.old.size)return;
    entry.state='applying';
    // Do not invoke any database code from a ticket-release stack. New requests
    // see the barrier until the returned promise confirms commit or rollback.
    Promise.resolve().then(async()=>{
      if(entry.controller.signal.aborted)return false;
      await entry.apply(entry.controller.signal);return true;
    }).then(applied=>{
      if(applied){if(entry.previous){finish(entry.previous,'superseded');entry.previous=null;}finish(entry,'completed');}
      else if(entry.retainPrevious&&entry.previous&&!entry.previous.finished&&entry.cancelled==='cancelled')restore(entry,'cancelled');
      else finish(entry,entry.cancelled||'cancelled');
    },()=>{if(entry.retainPrevious&&entry.previous&&!entry.previous.finished&&(!entry.cancelled||entry.cancelled==='cancelled'))restore(entry,entry.cancelled||'failed');else finish(entry,entry.cancelled||'failed');});
  }
  function releaseOld(ticket){
    for(const entry of pending.values())if(entry.old.has(ticket)&&(!ticket.open||ticket.session!==null&&ticket.session!==entry.session)){
      entry.old.delete(ticket);pump(entry);
    }
  }
  function arrive(member,signal){
    if(!identity(member))throw new AppError(503,'请求间交接身份无效','session_handoff_configuration');
    if(stopped)throw new AppError(503,'请求间交接正在关闭','service_stopping');
    if(signal?.aborted)throw cancelled(signal);
    if(tickets.size>=maxTickets)throw new AppError(429,'请求间交接容量已满','session_handoff_busy');
    const stage=stages.capture(),ticket={member,session:null,open:true,waiter:null};tickets.add(ticket);
    const close=()=>{
      if(!ticket.open)return;
      ticket.open=false;tickets.delete(ticket);signal?.removeEventListener('abort',abort);
      ticket.waiter?.reject(cancelled(signal));releaseOld(ticket);
    };
    // Aborting a transport is not proof its upload/model/settlement has ended.
    // The request owner closes its ticket in finally, after actual cleanup.
    const abort=()=>ticket.waiter?.reject(cancelled(signal));signal?.addEventListener('abort',abort,{once:true});
    async function bind(session){
      if(!identity(session))throw new AppError(503,'请求间交接会话无效','session_handoff_configuration');
      if(!ticket.open||signal?.aborted)throw cancelled(signal);
      if(ticket.session!==null)throw new AppError(503,'请求间交接不能重复绑定','session_handoff_configuration');
      ticket.session=session;releaseOld(ticket);
      const entry=pending.get(key(member,session));
      if(!entry||entry.old.has(ticket))return;
      if(waiting>=maxWaiting||(waitingMembers.get(member)||0)>=maxWaitingPerMember||entry.waiters.size>=maxWaitingPerSession){
        cancel(entry,'wait_capacity');
        // If applying, the transaction still owns the barrier. Reject this
        // excess request rather than let it race the route commit.
        if(!entry.finished)throw new AppError(503,'会话切换暂未结束，请稍后重试','session_handoff_busy');
        return;
      }
      const startNs=process.hrtime.bigint().toString();
      return new Promise((resolve,reject)=>{
        let open=true,timer;
        const clean=()=>{
          if(!open)return false;open=false;clearTimeout(timer);waiter.entry.waiters.delete(waiter);ticket.waiter=null;waiting--;
          const count=waitingMembers.get(member)-1;if(count)waitingMembers.set(member,count);else waitingMembers.delete(member);return true;
        };
        const observed=outcome=>stage.emit('wait',{queue:'handoff',queued:true,outcome,startNs,endNs:process.hrtime.bigint().toString()});
        const waiter={entry,resume:()=>{if(clean()){if(ticket.open&&!signal?.aborted){observed('granted');resolve();}else{observed('rejected');reject(cancelled(signal));}}},reject:error=>{if(clean()){observed('rejected');reject(error);}}};
        ticket.waiter=waiter;entry.waiters.add(waiter);waiting++;waitingMembers.set(member,(waitingMembers.get(member)||0)+1);
        timer=setTimeout(()=>{
          cancel(waiter.entry,'wait_timeout');
          // While a cancelled apply is rolling back, this request must leave
          // with an error rather than retain an unbounded waiter.
          if(!waiter.entry.finished)waiter.reject(new AppError(503,'会话切换等待超时，请稍后重试','session_handoff_timeout'));
        },waitMs);timer.unref?.();
      });
    }
    return {bind,close};
  }
  function register({memberId,sessionId,decisionId,replaceDecisionId,canReplaceApplying,onSuspend,onResume,retainPrevious=false,beforeRestore,detachReplacement=false}={}){
    if(![memberId,sessionId,decisionId].every(identity))throw new AppError(503,'请求间交接决定无效','session_handoff_configuration');
    if(stopped)throw new AppError(503,'请求间交接正在关闭','service_stopping');
    const entryKey=key(memberId,sessionId),previous=pending.get(entryKey);
    if([canReplaceApplying,onSuspend,onResume,beforeRestore].some(fn=>fn!==undefined&&typeof fn!=='function')||typeof retainPrevious!=='boolean'||typeof detachReplacement!=='boolean')throw new AppError(503,'请求间交接生命周期无效','session_handoff_configuration');
    const replaceable=previous&&(previous.state==='draining'||previous.state==='applying'&&previous.canReplaceApplying?.()===true);
    if(previous&&(replaceDecisionId!==previous.id||!replaceable)||replaceDecisionId!==undefined&&(!identity(replaceDecisionId)||!previous))throw new AppError(409,'此会话交接决定已变化，请先核对','session_handoff_conflict');
    if(!previous&&pending.size>=maxPending)throw new AppError(429,'待交接会话已满','session_handoff_busy');
    let resolve;const done=new Promise(r=>{resolve=r;});
    const entry={key:entryKey,member:memberId,session:sessionId,id:decisionId,state:'registering',previous,previousState:previous?.state,canReplaceApplying,onSuspend,onResume,retainPrevious,beforeRestore,detachReplacement,old:previous?new Set(previous.old):new Set([...tickets].filter(ticket=>ticket.member===memberId&&(ticket.session===null||ticket.session===sessionId))),waiters:new Set(),controller:new AbortController(),resolve,finished:false,cancelled:null};
    if(previous){
      // Preserve the original old cohort. Treating already waiting requests as
      // old would deadlock, or let the next schedule postpone handoff forever.
      for(const waiter of previous.waiters){waiter.entry=entry;entry.waiters.add(waiter);}
      previous.waiters.clear();previous.state='suspended';previous.onSuspend?.();
    }
    pending.set(entryKey,entry);entry.deadline=setTimeout(()=>cancel(entry,'expired'),pendingMs);entry.deadline.unref?.();
    return {
      // Call only once the durable decision has committed. No callback runs
      // during registration or while any old upload/request remains alive.
      activate(apply){if(typeof apply!=='function'||entry.finished||entry.state!=='registering')throw new AppError(409,'请求间交接登记已变化','session_handoff_conflict');entry.apply=apply;if(entry.previous&&!entry.retainPrevious){finish(entry.previous,'superseded');entry.previous=null;}entry.state='draining';pump(entry);},
      cancel:(reason='cancelled')=>cancel(entry,['cancelled','stopped','expired'].includes(reason)?reason:'cancelled'),done,signal:entry.controller.signal,
      status:()=>({state:entry.finished?entry.result:entry.state,oldRequests:entry.old.size,waiting:entry.waiters.size})
    };
  }
  function stop(){stopped=true;for(const entry of [...pending.values()])cancel(entry,'stopped');}
  return {arrive,register,stop,stageObserverStatus:stages.status,status:()=>({tickets:tickets.size,pending:pending.size,waiting,stopped})};
}
