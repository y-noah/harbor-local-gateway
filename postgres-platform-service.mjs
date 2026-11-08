import {createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {recordingCaptureDecision,recordingBufferWrite,recordingFinishReason} from './postgres-recording-storage.mjs';
import {writeOutput} from './output-budget.mjs';
import {AppError,id,storedModelAllowlist} from './store.mjs';
import {readJsonBody} from './request-body.mjs';
import {platformModel,validatePlatformChat} from './platforms.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {ConversationCapture,currentInput} from './conversation-capture.mjs';

const fail=(status,message,code)=>new AppError(status,message,code);
const json=(res,status,value)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(JSON.stringify(value));};
export function createPostgresPlatformService({store,queryStore=store,queryQueue,buffer,meteringRecovery,checkLimits,transport,credential,activity,execution,deviceAccess,outputBudget,recordingEnabled=true,recordingPolicy=()=>true,clock=Date.now,extraRuntime=createPostgresExtraRuntime(store,{clock}),readBody=readJsonBody,maxConcurrent=120,maxPerMember=8}={}){
  const supports=(row,provider,model)=>{try{return row?.auth_mode==='platform'&&row.kind===provider&&row.enabled===1&&row.removed_at===null&&row.state==='ready'&&Array.isArray(JSON.parse(row.platform_models))&&JSON.parse(row.platform_models).includes(model);}catch{return false;}};
  async function candidateView(provider,model,requestModel,requestId){
    const rows=await store.all("SELECT * FROM harbor.accounts WHERE auth_mode='platform' AND kind=$1 AND removed_at IS NULL ORDER BY id",provider),accounts=rows.filter(row=>supports(row,provider,model)),classifications=await extraRuntime.classifyAccounts(accounts,{model:requestModel,protocol:'platform'});
    return {rows,accounts,classifications,normal:accounts.filter(row=>['package','package_compatibility'].includes(classifications.get(row.id).mode))};
  }
  async function selectAccount(provider,model,requestModel){const view=await candidateView(provider,model,requestModel);return [...view.normal].sort((a,b)=>a.last_used-b.last_used||a.id.localeCompare(b.id))[0]??null;}
  async function handle(req,res){
    const path=new URL(req.url,'http://localhost').pathname,secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});
    await deviceAccess?.authorize(member,req);
    if(activity.isStopping())throw fail(503,'服务正在排空','service_stopping');
    if(path==='/v1/platform/models'&&req.method==='GET'){
      const data=await queryQueue.run(()=>queryStore.transaction(async()=>{
        const current=await queryStore.authenticate(secret,{at:clock()}),key=await queryStore.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',current.key_id),allowed=storedModelAllowlist(key.allowed_models),rows=await queryStore.all("SELECT kind,platform_models FROM harbor.accounts WHERE auth_mode='platform' AND enabled=1 AND removed_at IS NULL AND state='ready' ORDER BY kind,id");
        const models=new Set();for(const row of rows){let catalog;try{catalog=JSON.parse(row.platform_models);if(!Array.isArray(catalog))throw Error();}catch{throw fail(503,'平台模型目录不可用，请重新检查账号','platform_configuration');}for(const model of catalog){const value=row.kind+'/'+model;platformModel(value);if(allowed===null||allowed.includes(value))models.add(value);}}
        return [...models].map(id=>({id,object:'model',owned_by:id.split('/')[0]}));
      },{readOnly:true,level:'REPEATABLE READ'}));json(res,200,{object:'list',data});return true;
    }
    if(path!=='/v1/platform/chat/completions'||req.method!=='POST')throw fail(405,'不支持的操作','method_not_allowed');
    if(activity.active.size>=maxConcurrent||(activity.members.get(member.id)||0)>=maxPerMember)throw fail(429,'请求并发已满，请稍后重试','gateway_busy');
    const controller=new AbortController();activity.active.add(controller);activity.members.set(member.id,(activity.members.get(member.id)||0)+1);
    const closed=()=>{if(!res.writableEnded)controller.abort();};res.on('close',closed);
    const captureDecision=await recordingCaptureDecision(recordingEnabled,recordingPolicy),captureEnabled=captureDecision.enabled;
    let output,releasePreparation,releaseExecution,route,extraBoundary,dispatched=false,settlementAttempted=false,capture,recordStarted=false,recordFailed=false,recordFailureReason=null,flushTimer,batch=[],bytes=0;
    const record=operation=>{try{return operation();}catch{recordFailed=true;recordFailureReason??='capture_failed';capture=undefined;return undefined;}};
    const writeRecording=(method,...args)=>record(()=>{const result=recordingBufferWrite(buffer,method,...args);if(!result.accepted){recordFailed=true;recordFailureReason??=result.reason;}return result.accepted;});
    const flush=()=>{if(!batch.length)return;const events=batch;batch=[];bytes=0;if(recordStarted&&!writeRecording('append',route.requestId,events))recordFailed=true;};
    const alive=()=>{if(controller.signal.aborted||res.destroyed)throw controller.signal.reason instanceof AppError?controller.signal.reason:fail(499,'客户端已断开','request_cancelled');if(activity.isStopping()&&!dispatched)throw fail(503,'服务正在排空','service_stopping');};
    const settle=async result=>{settlementAttempted=true;const at=clock();try{const saved=await store.finish(route.requestId,result,at);await extraBoundary?.settle();return {persisted:true,standard:saved.standard??null};}catch{if(!meteringRecovery.settle(route.requestId,result,at))throw fail(503,'关键计量暂存已满，需人工核对','metering_unavailable');if((await meteringRecovery.flush()).queued)throw fail(503,'关键计量尚未持久化','metering_unavailable');await extraBoundary?.settle();return {persisted:false,standard:null};}};
    try{
      output=outputBudget?.open('member:'+member.id);
      releasePreparation=await activity.preparation.enter(member.id,controller.signal);alive();let payload;
      activity.preparing.set(member.id,(activity.preparing.get(member.id)||0)+1);
      try{payload=validatePlatformChat(await readBody(req,{owner:'member:'+member.id,signal:controller.signal}));}finally{const n=activity.preparing.get(member.id)-1;if(n)activity.preparing.set(member.id,n);else activity.preparing.delete(member.id);}
      alive();const {provider,model}=platformModel(payload.model);
      route=await store.transaction(async()=>{
        const current=await store.authenticate(secret,{at:clock(),lock:true}),device=await deviceAccess?.authorize(current,req,{phase:'admission'});await store.checkModel(current,payload.model);await checkLimits(current,payload,{at:clock()});await lockPostgresRouting(store);alive();
        let session=payload.session_id?await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='platform' FOR UPDATE",payload.session_id,current.id):null;
        if(payload.session_id&&!session)throw fail(404,'平台会话不存在或不属于该员工','session_not_found');if(session&&!session.enabled)throw fail(403,'会话已禁止访问','session_disabled');
        const account=session?await store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id):await selectAccount(provider,model,payload.model);
        if(session&&account?.enabled&&account.removed_at===null&&account.state==='ready'&&!supports(account,provider,model))throw fail(409,'会话绑定的平台或模型不匹配，请新建会话','platform_session_model_conflict');
        if(!supports(account,provider,model))throw fail(503,'没有已连接且支持此模型的平台账号','platform_account_unavailable');
        if(!session){session={id:id(),account_id:account.id,route_version:0};await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,nightly_enabled,created,updated) VALUES($1,$2,$3,'platform',0,$4,$4)",session.id,current.id,account.id,clock());}
        const requestId=await store.observe(current,req.socket.remoteAddress||'unknown',String(req.headers['user-agent']||'').slice(0,200),payload.model,{at:clock(),channel:'platform',device});
        await store.run('UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3,recording_state=$4,recording_reason=$6 WHERE id=$5',account.id,session.id,session.route_version,captureEnabled?'pending':'disabled',requestId,captureDecision.reason);const extraAdmission=await extraRuntime.admit({requestId,account,model:payload.model,protocol:'platform'});return {requestId,account,session,extraAdmission};
      });extraBoundary=extraRuntime.boundary({requestId:route.requestId,protocol:'platform',admittedExtra:route.extraAdmission.extraReserved,packagePreference:async request=>{const view=await candidateView(provider,model,request.model,request.id);return {mode:view.classifications.get(request.account_id)?.mode,manual:false,manualPositive:false,accountIds:view.rows.map(row=>row.id),packageAlternatives:view.normal.filter(row=>row.id!==request.account_id).map(row=>row.id)};},prepare:async request=>{alive();const current=await store.authenticate(secret,{at:clock(),lock:true});if(current.id!==request.member_id||current.key_id!==request.key_id)throw fail(403,'请求访问身份已变化','request_identity_changed');await deviceAccess?.authorize(current,req,{phase:'dispatch'});await store.checkModel(current,payload.model);await checkLimits(current,payload,{at:clock(),requestId:route.requestId});await lockPostgresRouting(store);return alive;},validate:async(owner,request)=>{const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',owner.id),session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE',route.session.id);if(account.id!==route.account.id||!supports(account,provider,model)||account.route_epoch!==route.account.route_epoch||!session||session.member_id!==request.member_id||!session.enabled||session.protocol!=='platform'||session.account_id!==account.id||session.route_version!==route.session.route_version||request.route_version!==session.route_version)throw fail(409,'平台账号或会话配置已变化，请重试','platform_configuration_changed');alive();releaseExecution=execution?.acquire({memberId:request.member_id,accountId:account.id,cli:provider==='qoder'});if(execution&&!releaseExecution)throw fail(503,'账号或模型CLI执行容量已满，请稍后重试','inference_capacity_busy');const phase=provider==='qoder'?'cli':'upstream',at=clock();await store.run('UPDATE harbor.requests SET phase=$1,phase_at=$2,phase_history=$3 WHERE id=$4',phase,at,JSON.stringify([{phase:'queued',at:request.started},{phase,at}]),route.requestId);await store.run('UPDATE harbor.accounts SET last_used=$1 WHERE id=$2',at,account.id);}});activity.accounts.set(route.account.id,(activity.accounts.get(route.account.id)||0)+1);releasePreparation();releasePreparation=null;
      if(captureEnabled){const input=record(()=>currentInput({input:payload.messages}));recordStarted=!!input&&!!writeRecording('start',route.requestId,input.state);if(!recordStarted)recordFailed=true;
        if(recordStarted){capture=record(()=>new ConversationCapture({emit:event=>{const size=Buffer.byteLength(JSON.stringify(event));if(batch.length>=128||bytes+size>65536)flush();batch.push(event);bytes+=size;return !recordFailed;}}));record(()=>capture?.input({input:payload.messages},input));flush();flushTimer=setInterval(flush,250);flushTimer.unref();}}
      const value=await credential(route.account,controller.signal);alive();
      const result=await extraBoundary.run(async()=>{alive();dispatched=true;return transport.run(route.account,value,payload,controller.signal,{output,onText:text=>record(()=>capture?.delta('platform-output','assistant',{},text))});});
      if(typeof result?.text!=='string')throw fail(502,'平台未返回有效文本；用量需核对','platform_protocol');
      record(()=>capture?.complete('platform-output','assistant',{},result.text));const recording=record(()=>capture?.finish());flush();if(recordStarted&&!writeRecording('finish',route.requestId,{state:recordFailed||recording?.failed?'partial':recording?.truncated?'truncated':'complete',...recordingFinishReason(recordFailed||recording?.failed?recordFailureReason??'capture_failed':recording?.truncated?'size_limit':null)}))recordFailed=true;
      const known=result.usageKnown===true&&['input','output','cached'].every(name=>Number.isSafeInteger(result[name])&&result[name]>=0)&&result.cached<=result.input;
      const settled=await settle({...result,usageKnown:known});await deviceAccess?.markVerified(route.requestId).catch(()=>{});if(res.destroyed)return true;
      const usage=known?{prompt_tokens:result.input,completion_tokens:result.output,total_tokens:result.input+result.output,prompt_tokens_details:{cached_tokens:result.cached}}:null;
      const common={id:'chatcmpl-'+route.requestId,created:Math.floor(clock()/1000),model:payload.model,session_id:route.session.id,harbor_standard_usage:settled.standard};res.setHeader('X-Harbor-Session',route.session.id);res.setHeader('X-Harbor-Request',route.requestId);if(!settled.persisted)res.setHeader('X-Harbor-Usage','pending');
      if(payload.stream){
        await writeOutput(res,{...common,object:'chat.completion.chunk',choices:[{index:0,delta:{role:'assistant',content:result.text},finish_reason:null}]},{output,signal:controller.signal,prefix:'data: ',suffix:'\n\n',headers:{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no'}});
        await writeOutput(res,{...common,object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:result.finishReason||'stop'}],usage},{output,signal:controller.signal,prefix:'data: ',suffix:'\n\ndata: [DONE]\n\n'});res.end();
      }else{await writeOutput(res,{...common,object:'chat.completion',choices:[{index:0,message:{role:'assistant',content:result.text},finish_reason:result.finishReason||'stop'}],usage},{output,signal:controller.signal,headers:{'Content-Type':'application/json; charset=utf-8'}});res.end();}return true;
    }catch(error){
      const boundaryStatus=extraBoundary?.status();dispatched||=boundaryStatus?.dispatched===true;if(error.notDispatched===true&&!boundaryStatus?.dispatched&&!boundaryStatus?.uncertain)dispatched=false;
      if(route){record(()=>capture?.finish());flush();if(recordStarted&&!writeRecording('finish',route.requestId,{state:dispatched||recordFailed?'partial':'complete',...recordingFinishReason(recordFailed?recordFailureReason??'capture_failed':dispatched?'upstream_interrupted':null)}))recordFailed=true;
        if(!settlementAttempted){const measured=error.usageKnown===true&&['input','output','cached'].every(name=>Number.isSafeInteger(error[name])&&error[name]>=0)&&error.cached<=error.input;await settle({input:measured?error.input:0,output:measured?error.output:0,cached:measured?error.cached:0,actualModel:measured?error.actualModel:undefined,status:controller.signal.reason?.code==='service_shutdown'?'interrupted':dispatched?(measured?(controller.signal.aborted?'cancelled':'failed'):'unknown'):controller.signal.aborted?'cancelled':'rejected',usageKnown:!dispatched||measured}).catch(()=>{});}}
      if(error instanceof AppError)throw error;throw fail(502,'平台请求失败；用量需核对','platform_upstream_error');
    }finally{
      releasePreparation?.();await extraBoundary?.releaseUnused();if(flushTimer)clearInterval(flushTimer);if(route&&captureEnabled&&(!recordStarted||recordFailed))await store.run("UPDATE harbor.requests SET recording_state='partial',recording_reason=$2 WHERE id=$1 AND recording_state IN ('pending','complete')",route.requestId,recordFailureReason??'capture_failed').catch(()=>{});
      if(route){const n=activity.accounts.get(route.account.id)-1;if(n)activity.accounts.set(route.account.id,n);else activity.accounts.delete(route.account.id);activity.wake();}releaseExecution?.();output?.close();res.off('close',closed);activity.active.delete(controller);const n=activity.members.get(member.id)-1;if(n)activity.members.set(member.id,n);else activity.members.delete(member.id);
    }
  }
  return {handle};
}
