import {AppError} from './store.mjs';

// A plan group must hold every session barrier through one actual commit or
// rollback. Reaching the boundary of only one session cannot release a subset.
export function registerRequestHandoffGroup(gate,members){
  if(!gate||typeof gate.register!=='function'||!Array.isArray(members)||!members.length||members.length>100000)throw new AppError(503,'整组请求交接配置无效','session_handoff_configuration');
  const keys=new Set(),ids=new Set();
  for(const member of members){
    if(!member||typeof member!=='object'||Array.isArray(member)||Object.keys(member).some(key=>!['memberId','sessionId','decisionId'].includes(key))||!['memberId','sessionId','decisionId'].every(key=>typeof member[key]==='string'&&member[key].length>0&&member[key].length<=200))throw new AppError(503,'整组请求交接身份无效','session_handoff_configuration');
    const key=JSON.stringify([member.memberId,member.sessionId]);if(keys.has(key)||ids.has(member.decisionId))throw new AppError(503,'整组请求交接不能重复登记','session_handoff_configuration');keys.add(key);ids.add(member.decisionId);
  }
  const handles=[],controller=new AbortController(),resuming=new Set();let activated=false,ready=0,paused=0,started=false,committing=false,settled=false,value,error,resolveCommit,rejectCommit,applyCallback,deferCommit=false;
  const commit=new Promise((resolve,reject)=>{resolveCommit=resolve;rejectCommit=reject;});commit.catch(()=>{});
  function start(){
    if(!activated||started||settled||paused||ready!==handles.length||controller.signal.aborted)return;
    started=true;committing=!deferCommit;Promise.resolve().then(()=>{if(controller.signal.aborted)throw controller.signal.reason;return applyCallback(controller.signal,beginCommit);}).then(result=>settle(false,result),failure=>{settle(true,failure);abort();});
  }
  async function beginCommit(){
    while(paused&&!controller.signal.aborted)await new Promise((resolve,reject)=>{
      const resume=()=>{clean();resolve();},cancel=()=>{clean();reject(controller.signal.reason);},clean=()=>{resuming.delete(resume);controller.signal.removeEventListener('abort',cancel);};
      resuming.add(resume);controller.signal.addEventListener('abort',cancel,{once:true});
    });
    if(controller.signal.aborted)throw controller.signal.reason;committing=true;
  }
  function settle(failed,result){if(settled)return;settled=true;if(failed){error=result;rejectCommit(result);}else{value=result;resolveCommit(result);}}
  function abort(reason='cancelled'){
    // The aborted signal is permanent, unlike a synchronous reentrancy guard:
    // every later member.done notification must also avoid another group scan.
    if(controller.signal.aborted)return;
    const terminal=['stopped','expired'].includes(reason)?reason:'cancelled';
    controller.abort(new AppError(terminal==='stopped'?503:409,'整组请求交接已取消',terminal==='stopped'?'service_stopping':'session_handoff_cancelled'));
    for(const handle of handles)if(!handle.signal.aborted)handle.cancel(terminal);
    // A real apply owns its barriers until the actual callback settles. An
    // aborted signal alone is never evidence that the transaction has ended.
    if(!started)settle(true,controller.signal.reason);
  }
  try{
    for(const member of members){const handle=gate.register({...member,detachReplacement:true,canReplaceApplying:()=>!committing&&!settled,onSuspend:()=>{paused++;},onResume:()=>{paused--;if(!paused)for(const resume of [...resuming])resume();start();}});if(!handle.signal||typeof handle.signal.addEventListener!=='function'){handle.cancel?.();throw new AppError(503,'整组请求交接生命周期未接通','session_handoff_configuration');}handles.push(handle);handle.signal.addEventListener('abort',abort);handle.done.then(result=>{if(result.state!=='completed')abort();});}
  }catch(failure){abort();for(const handle of handles)handle.signal.removeEventListener('abort',abort);throw failure;}
  const done=Promise.all(handles.map(handle=>handle.done)).then(results=>{
    for(const handle of handles)handle.signal.removeEventListener('abort',abort);
    const completed=results.every(result=>result.state==='completed');
    const terminal=results.find(result=>!['completed','cancelled'].includes(result.state))?.state||'cancelled';
    return {state:completed?'completed':error&&error!==controller.signal.reason&&error?.code!=='session_handoff_cancelled'?'failed':terminal,...(completed?{value}:{error:error||controller.signal.reason})};
  });
  return {
    activate(apply,options={}){
      if(activated||settled||typeof apply!=='function'||!options||typeof options!=='object'||Array.isArray(options)||Object.keys(options).some(key=>key!=='deferCommit')||options.deferCommit!==undefined&&typeof options.deferCommit!=='boolean')throw new AppError(409,'整组请求交接登记已变化','session_handoff_conflict');activated=true;applyCallback=apply;deferCommit=options.deferCommit??false;
      try{for(const handle of handles)handle.activate(async()=>{
        ready++;start();
        return commit;
      });}catch(failure){error=failure;abort();throw failure;}
    },
    cancel:abort,stop:()=>abort('stopped'),done,
    status:()=>({members:handles.length,ready,applying:started&&!settled,settled,cancelled:controller.signal.aborted}),
    signal:controller.signal
  };
}
