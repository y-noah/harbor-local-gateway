import {recordingCaptureDecision,recordingBufferWrite,recordingFinishReason} from './postgres-recording-storage.mjs';
import {writeOutput} from './output-budget.mjs';
import {AppError} from './store.mjs';
import {readJsonBody} from './request-body.mjs';
import {createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {createPostgresChatState} from './postgres-chat-state.mjs';
import {chatSettlementFacts,validateLegacyChat} from './chat-facts.mjs';
import {ConversationCapture,currentInput} from './conversation-capture.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {observeRequestContext,incompleteRequestContext} from './recording-context-capture.mjs';

const fail=(status,message,code)=>new AppError(status,message,code);
export function createPostgresChatService({store,buffer,meteringRecovery,checkLimits,runChat,execution,cliProcesses,outputBudget,refreshQuotas=async()=>{},activity,deviceAccess,recordingEnabled=true,captureContext,recordingPolicy=()=>true,clock=Date.now,extraRuntime=createPostgresExtraRuntime(store,{clock}),readBody=readJsonBody,reserveTokens=32000,maxRequests=16,maxQueued=5,waitMs=120000,timeoutMs=120000,maxConcurrent=120,maxPerMember=8}={}){
  if(typeof runChat!=='function'||typeof refreshQuotas!=='function'||!activity||![maxRequests,maxQueued,waitMs,timeoutMs].every(Number.isSafeInteger)||maxRequests<1||maxRequests>64||maxQueued<1||maxQueued>64||waitMs<100||waitMs>3600000||timeoutMs<100||timeoutMs>86400000)throw fail(503,'文本执行资源配置无效','chat_configuration');
  const state=createPostgresChatState(store,{checkLimits,clock,reserveTokens,extraRuntime}),queue=[],owners=new Set();let pumping,again=false;
  const releaseAccount=id=>{const count=activity.accounts.get(id)-1;if(count)activity.accounts.set(id,count);else activity.accounts.delete(id);void wake();};
  function remove(job){const index=queue.indexOf(job);if(index!==-1)queue.splice(index,1);clearTimeout(job.timer);job.signal.removeEventListener('abort',job.abort);}
  function reject(job,error){job.failure=error;if(job.selecting)return;remove(job);job.reject(error);}
  function wake(){
    if(pumping){again=true;return pumping;}
    pumping=(async()=>{do{again=false;
      for(const job of [...queue]){
        if(job.failure||job.signal.aborted){reject(job,job.failure||fail(499,'排队期间已取消','request_cancelled'));continue;}
        if(activity.isStopping()){reject(job,fail(503,'服务排空，取消尚未派发的请求','service_stopping'));continue;}
        let release,processLease;job.selecting=true;
        try{
          const dispatched=await state.dispatch(job.secret,job.id,{followDisabled:true,deferAuthorization:true,isCancelled:()=>!!job.failure||job.signal.aborted||activity.isStopping(),authorizeDevice:person=>deviceAccess?.authorize(person,job.request,{phase:'dispatch'}),acquireAccount:account=>{
            if(activity.accounts.has(account.id)||cliProcesses&&!cliProcesses.available(account.home,{kind:'model'}))return false;const permit=execution?.acquire({memberId:job.memberId,accountId:account.id,cli:true,requireIdle:true});if(execution&&!permit)return false;try{processLease=cliProcesses?.reserve(account.home,{kind:'model',signal:job.signal});}catch(error){permit?.();throw error;}activity.accounts.set(account.id,1);let released=false;release=()=>{if(!released){released=true;releaseAccount(account.id);permit?.();processLease?.release();}};return true;
          }});
          if(job.failure||job.signal.aborted||activity.isStopping())throw job.failure||fail(503,'文本请求已取消或服务停止','service_stopping');
          job.selecting=false;remove(job);job.resolve({...dispatched,release,processLease});release=null;
        }catch(error){release?.();job.selecting=false;if(error.code==='chat_account_busy'&&!job.failure&&!job.signal.aborted&&!activity.isStopping())continue;reject(job,job.failure||error);}
      }
    }while(again);})().finally(()=>{pumping=null;if(again){again=false;void wake();}});return pumping;
  }
  function enqueue(secret,id,signal,memberId,request){
    if(queue.length>=maxQueued)throw fail(429,'文本请求等待队列已满','chat_queue_full');
    return new Promise((resolve,rejectPromise)=>{const job={secret,id,signal,memberId,request,resolve,reject:rejectPromise,selecting:false};job.abort=()=>reject(job,fail(499,'排队期间已取消','request_cancelled'));job.timer=setTimeout(()=>reject(job,fail(503,'文本请求排队超时','chat_queue_timeout')),waitMs);job.timer.unref();queue.push(job);signal.addEventListener('abort',job.abort,{once:true});if(signal.aborted)job.abort();else void wake();});
  }
  async function handle(req,res){
    const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});
    await deviceAccess?.authorize(member,req);
    if(activity.isStopping())throw fail(503,'服务正在排空','service_stopping');
    if(owners.has(member.id))throw fail(429,'该员工已有文本请求在处理','member_busy');
    if(owners.size>=maxRequests||queue.length>=maxQueued||activity.active.size>=maxConcurrent||(activity.members.get(member.id)||0)>=maxPerMember)throw fail(429,'文本请求容量已满，请稍后重试','chat_queue_full');
    const controller=new AbortController();owners.add(member.id);activity.active.add(controller);activity.members.set(member.id,(activity.members.get(member.id)||0)+1);
    const closed=()=>{if(!res.writableEnded)controller.abort();};res.on('close',closed);
    const captureDecision=await recordingCaptureDecision(recordingEnabled,recordingPolicy),captureEnabled=captureDecision.enabled;
    let output,admission,extraBoundary,releasePreparation,releaseSlot,dispatched=false,settlementAttempted=false,capture,requestContext,recordStarted=false,recordFailed=false,recordFailureReason=null,flushTimer,batch=[],bytes=0;
    const record=operation=>{try{return operation();}catch{recordFailed=true;recordFailureReason??='capture_failed';capture=undefined;return undefined;}};
    const writeRecording=(method,...args)=>record(()=>{const result=recordingBufferWrite(buffer,method,...args);if(!result.accepted){recordFailed=true;recordFailureReason??=result.reason;}return result.accepted;});
    const flush=()=>{if(!batch.length)return;const events=batch;batch=[];bytes=0;if(recordStarted&&!writeRecording('append',admission.requestId,events))recordFailed=true;};
    const alive=()=>{if(controller.signal.aborted||res.destroyed)throw controller.signal.reason instanceof AppError?controller.signal.reason:fail(499,'客户端已断开','request_cancelled');};
    const settle=async result=>{
      settlementAttempted=true;const facts={...result,chat:chatSettlementFacts(admission,result,{dispatched})},at=clock();
      try{const saved=await store.finish(admission.requestId,facts,at);await extraBoundary?.settle();return {persisted:true,standard:saved.standard??null};}catch{if(!meteringRecovery.settle(admission.requestId,facts,at))throw fail(503,'关键计量暂存已满，需人工核对','metering_unavailable');const saved=await meteringRecovery.flush();if(saved.queued)throw fail(503,'关键计量尚未持久化','metering_unavailable');await extraBoundary?.settle();return {persisted:false,standard:null};}
    };
    try{
      output=outputBudget?.open('member:'+member.id);
      releasePreparation=await activity.preparation.enter(member.id,controller.signal);alive();
      activity.preparing.set(member.id,(activity.preparing.get(member.id)||0)+1);let original,received;
      try{const body=await readBody(req,{owner:'member:'+member.id,signal:controller.signal});if(captureContext)received=body;original=validateLegacyChat(body);}finally{const n=activity.preparing.get(member.id)-1;if(n)activity.preparing.set(member.id,n);else activity.preparing.delete(member.id);}
      await refreshQuotas().catch(()=>{});alive();if(activity.isStopping())throw fail(503,'服务正在排空','service_stopping');
      admission=await state.admit(secret,original,{headers:req.headers,ip:req.socket.remoteAddress||'unknown',agent:String(req.headers['user-agent']||'').slice(0,200),recordingEnabled:captureEnabled,authorizeDevice:person=>deviceAccess?.authorize(person,req,{phase:'admission'})});extraBoundary=extraRuntime?.boundary({requestId:admission.requestId,protocol:'chat',admittedExtra:admission.extraAdmission.extraReserved});releasePreparation();releasePreparation=null;
      if(!captureEnabled)await store.run('UPDATE harbor.requests SET recording_reason=$1 WHERE id=$2 AND recording_state=\'disabled\'',captureDecision.reason,admission.requestId).catch(()=>{});
      if(captureEnabled){const input=record(()=>currentInput({input:admission.delta}));recordStarted=!!input&&!!writeRecording('start',admission.requestId,input.state);if(!recordStarted)recordFailed=true;
        if(recordStarted)requestContext=observeRequestContext(captureContext,admission.requestId,received);
        if(recordStarted){capture=record(()=>new ConversationCapture({emit:event=>{const size=Buffer.byteLength(JSON.stringify(event));if(batch.length>=128||bytes+size>65536)flush();batch.push(event);bytes+=size;return !recordFailed;}}));record(()=>capture?.input({input:admission.delta},input));flush();flushTimer=setInterval(flush,250);flushTimer.unref();}}
      received=undefined;
      const ready=await enqueue(secret,admission.requestId,controller.signal,member.id,req);releaseSlot=ready.release;admission={...admission,account:ready.account,session:ready.session};alive();
      if(ready.contextReset)record(()=>capture?.send('boundary',{reason:'context_reset'},'gateway'));
      extraBoundary=state.dispatchBoundary(secret,admission,{isCancelled:()=>controller.signal.aborted||res.destroyed||activity.isStopping(),authorizeDevice:person=>deviceAccess?.authorize(person,req,{phase:'dispatch'})});const result=await extraBoundary.run(async()=>{alive();dispatched=true;return runChat({account:admission.account,processLease:ready.processLease,output,session:admission.session,messages:admission.delta,model:original.model,signal:controller.signal,timeoutMs,onText:text=>record(()=>capture?.delta('chat-output','assistant',{},text))});});
      const facts=chatSettlementFacts(admission,result);if(result.usageKnown!==true||!['input','output','cached'].every(name=>Number.isSafeInteger(result[name])&&result[name]>=0)||result.cached>result.input||!facts.cursor||admission.session.upstream_id&&facts.upstreamId!==admission.session.upstream_id)throw Object.assign(fail(502,'CLI未返回可核对的会话和用量，请新建会话','chat_result_invalid'),result,{status:502});
      record(()=>capture?.complete('chat-output','assistant',{},result.text));const recording=record(()=>capture?.finish());flush();if(recordStarted&&!writeRecording('finish',admission.requestId,{state:recordFailed||recording?.failed||incompleteRequestContext(requestContext)?'partial':recording?.truncated?'truncated':'complete',...recordingFinishReason(recordFailed||recording?.failed?recordFailureReason??'capture_failed':incompleteRequestContext(requestContext)?requestContext.reason??'capture_failed':recording?.truncated?'size_limit':null)}))recordFailed=true;
      const settled=await settle(result);await deviceAccess?.markVerified(admission.requestId).catch(()=>{});if(res.destroyed)return true;
      const usage={prompt_tokens:result.input,completion_tokens:result.output,total_tokens:result.input+result.output,prompt_tokens_details:{cached_tokens:result.cached}},standard=settled.standard;
      const common={id:'chatcmpl-'+admission.requestId,created:Math.floor(clock()/1000),model:original.model,session_id:admission.session.id,harbor_standard_usage:standard,...(ready.contextReset?{context_reset:true}:{})};res.setHeader('X-Harbor-Session',admission.session.id);res.setHeader('X-Harbor-Request',admission.requestId);if(!settled.persisted)res.setHeader('X-Harbor-Usage','pending');
      if(original.stream){
        await writeOutput(res,{...common,object:'chat.completion.chunk',choices:[{index:0,delta:{role:'assistant',content:result.text},finish_reason:null}]},{output,signal:controller.signal,prefix:'data: ',suffix:'\n\n',headers:{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no'}});
        await writeOutput(res,{...common,object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:'stop'}],usage},{output,signal:controller.signal,prefix:'data: ',suffix:'\n\ndata: [DONE]\n\n'});res.end();
      }else{await writeOutput(res,{...common,object:'chat.completion',choices:[{index:0,message:{role:'assistant',content:result.text},finish_reason:'stop'}],usage},{output,signal:controller.signal,headers:{'Content-Type':'application/json; charset=utf-8'}});res.end();}return true;
    }catch(error){
      dispatched||=extraBoundary?.status().dispatched===true;
      if(admission){record(()=>capture?.finish());flush();if(recordStarted&&!writeRecording('finish',admission.requestId,{state:dispatched||recordFailed||incompleteRequestContext(requestContext)?'partial':'complete',...recordingFinishReason(recordFailed?recordFailureReason??'capture_failed':incompleteRequestContext(requestContext)?requestContext.reason??'capture_failed':dispatched?(controller.signal.reason?.code==='service_shutdown'?'process_interrupted':'upstream_interrupted'):null)}))recordFailed=true;
        if(!settlementAttempted)await settle({...error,status:controller.signal.reason?.code==='service_shutdown'?'interrupted':dispatched?(error.usageKnown===true?'failed':'unknown'):controller.signal.aborted?'cancelled':'rejected',usageKnown:dispatched?error.usageKnown===true:true}).catch(()=>{});}
      if(error instanceof AppError)throw error;throw fail(502,sanitizeRecord(error.message||'CLI请求失败',{maxBytes:2048}),'upstream_error');
    }finally{
      releasePreparation?.();await extraBoundary?.releaseUnused();if(flushTimer)clearInterval(flushTimer);if(admission&&captureEnabled&&(!recordStarted||recordFailed))await store.run("UPDATE harbor.requests SET recording_state='partial',recording_reason=$2 WHERE id=$1 AND recording_state IN ('pending','complete')",admission.requestId,recordFailureReason??'capture_failed').catch(()=>{});
      releaseSlot?.();output?.close();res.off('close',closed);owners.delete(member.id);activity.active.delete(controller);const n=activity.members.get(member.id)-1;if(n)activity.members.set(member.id,n);else activity.members.delete(member.id);
    }
  }
  return {handle,wake,drain:()=>{for(const job of [...queue])reject(job,fail(503,'服务排空，取消尚未派发的请求','service_stopping'));},status:()=>({requests:owners.size,queued:queue.length})};
}
