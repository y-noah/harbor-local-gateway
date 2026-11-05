import {createStreamingStageObserver} from './streaming-stage-observer.mjs';
import {AppError} from './store.mjs';

// Process-local handoff for the single active gateway. Readers cover upload
// through committed admission; an exclusive migration must be released only
// after its database transaction has committed or rolled back.
export function createRoutingPreparation({maxWaiting=120,stageObserver}={}){
  if(!Number.isSafeInteger(maxWaiting)||maxWaiting<1)throw new AppError(503,'路由准备资源配置无效','routing_preparation_configuration');
  const stages=createStreamingStageObserver(stageObserver);
  const readers=new Map(),blocked=new Set(),waiters=new Map();let waiting=0;
  const cancelled=signal=>signal.reason instanceof AppError?signal.reason:new AppError(499,'请求已取消','request_cancelled');
  function grant(member){
    readers.set(member,(readers.get(member)||0)+1);let released=false;
    return ()=>{if(released)return;released=true;const count=readers.get(member)-1;if(count)readers.set(member,count);else readers.delete(member);};
  }
  function enter(member,signal){
    if(typeof member!=='string'||!member)throw new AppError(503,'路由准备身份无效','routing_preparation_configuration');
    if(signal?.aborted)return Promise.reject(cancelled(signal));
    if(!blocked.has(member))return Promise.resolve(grant(member));
    if(waiting>=maxWaiting)return Promise.reject(new AppError(429,'等待会话切换的请求已满，请稍后重试','routing_preparation_busy'));
    const stage=stages.capture(),startNs=process.hrtime.bigint().toString();
    return new Promise((resolve,reject)=>{
      let pending=true;
      const finish=()=>{if(!pending)return false;pending=false;waiting--;const group=waiters.get(member);group.delete(entry);if(!group.size)waiters.delete(member);signal?.removeEventListener('abort',abort);return true;};
      const observed=outcome=>stage.emit('wait',{queue:'preparation',queued:true,outcome,startNs,endNs:process.hrtime.bigint().toString()});
      const abort=()=>{if(finish()){observed('rejected');reject(cancelled(signal));}};
      const entry=()=>{if(finish()){if(signal?.aborted){observed('rejected');reject(cancelled(signal));}else{observed('granted');resolve(grant(member));}}};
      if(!waiters.has(member))waiters.set(member,new Set());waiters.get(member).add(entry);waiting++;signal?.addEventListener('abort',abort,{once:true});
    });
  }
  function reserve(members){
    if(!Array.isArray(members)||members.some(member=>typeof member!=='string'||!member))throw new AppError(503,'迁移准备身份无效','routing_preparation_configuration');
    const ids=[...new Set(members)];if(ids.some(member=>readers.has(member)||blocked.has(member)))return null;
    for(const member of ids)blocked.add(member);let released=false;
    return ()=>{
      if(released)return;released=true;for(const member of ids)blocked.delete(member);
      // Grant waiting readers synchronously, before another migration can
      // reserve the same member during the promise continuation gap.
      for(const member of ids)for(const resume of [...(waiters.get(member)||[])])resume();
    };
  }
  return {enter,reserve,stageObserverStatus:stages.status,isPreparing:member=>readers.has(member),status:()=>({readers:[...readers.values()].reduce((n,v)=>n+v,0),waiting,blockedMembers:blocked.size})};
}
