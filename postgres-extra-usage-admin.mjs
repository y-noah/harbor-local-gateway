import {AppError,id} from './store.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';

const fail=(status,code,message='额外额度管理操作未完成，请核对状态后重试')=>{throw new AppError(status,message,code);};
const accountId=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$/.test(value);
const actorValid=value=>value&&Object.keys(value).length===1&&typeof value.session_ref==='string'&&/^[a-f0-9]{64}$/.test(value.session_ref);
const extraPage=params=>{if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1)||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))fail(400,'extra_preview_page_invalid');return {after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)};};
const json=(res,status,value)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(value));};

// The security callback is bound to the gateway's controlled Host/Origin rules.
// It is mandatory even when this handler is mounted behind the same guard.
export function createPostgresExtraUsageAdmin({service,reader=service,policyPreviews=null,batches=null,refreshSubscriptionQuota=null,adminAuthenticated,adminSession,acceptsAdminRequest,requestBodies,clock=Date.now,reads={},refreshes={},refreshSupported=false,refreshTimeoutMs=refreshSubscriptionQuota?30000:15000,maxJobs=128,jobTtlMs=3600000,maxBodyBytes=65536}={}){
 if(!service||['saveAccount','recordManual','recordResetCardUsage','refresh'].some(key=>typeof service[key]!=='function')||!reader||['readPolicy','readAccount'].some(key=>typeof reader[key]!=='function')||policyPreviews!==null&&typeof policyPreviews?.apply!=='function'||[adminAuthenticated,adminSession,acceptsAdminRequest,clock].some(fn=>typeof fn!=='function')||!requestBodies||['read','release'].some(key=>typeof requestBodies[key]!=='function')||typeof refreshSupported!=='boolean'||!Number.isSafeInteger(refreshTimeoutMs)||refreshTimeoutMs<100||refreshTimeoutMs>3600000||!Number.isInteger(maxJobs)||maxJobs<1||maxJobs>10000||!Number.isSafeInteger(jobTtlMs)||jobTtlMs<60000||jobTtlMs>86400000||!Number.isSafeInteger(maxBodyBytes)||maxBodyBytes<256||maxBodyBytes>1048576)fail(503,'extra_admin_configuration');
 if(batches!==null&&['coverage','records','list','detail','previewPolicy','previewAccounts','submit','retryAccounts'].some(key=>typeof batches[key]!=='function'))fail(503,'extra_batch_configuration');
 if(refreshSubscriptionQuota!==null&&typeof refreshSubscriptionQuota!=='function')fail(503,'extra_refresh_configuration');
 const readQueue=createPostgresQueryQueue(reads),refreshQueue=createPostgresQueryQueue(refreshes),jobs=new Map(),pending=new Map(),tasks=new Set(),controllers=new Set(),requests=new Set();let stopping=false,closePromise;
 const refreshLimits={maxConcurrent:refreshes.maxConcurrent??2,maxQueued:refreshes.maxQueued??8};
 const running=()=>{if(stopping)fail(503,'service_stopping','额外额度管理正在关闭');};
 const purge=()=>{const now=clock();for(const [key,job] of jobs)if(job.finished_at!==null&&job.finished_at+jobTtlMs<=now)jobs.delete(key);};
 const publicJob=job=>Object.freeze({id:job.id,account_id:job.account_id,state:job.state,created_at:job.created_at,finished_at:job.finished_at,verification:job.verification,reason:job.reason,components:Object.freeze(Object.fromEntries(Object.entries(job.components).map(([key,value])=>[key,Object.freeze({...value})]))),reset_card_verification:'pending'});
 // Quota recovery can change route_epoch, so read the extra entitlement after it.
 // A composite job reserves two existing 15-second reader windows by default.
 async function refreshComponent(job,name,signal){
  const component=job.components[name];if(name==='subscription'&&!refreshSubscriptionQuota)return;component.state='running';
  try{const result=await(name==='extra'?service.refresh(job.account_id,{signal}):refreshSubscriptionQuota(job.account_id,{signal}));
   // A confirmed commit remains a fact even if cancellation arrived at COMMIT.
   if(result.refreshed===true){component.state=name==='subscription'?'refreshed':result.state==='verified'?'verified':result.state==='rejected'?'rejected':'pending_verification';component.verification=name==='subscription'?'snapshot_refreshed':result.state==='verified'?'verified':'pending';}
   else if(signal.aborted||result.cancelled===true||result.reason==='extra_refresh_cancelled'){component.state='cancelled';component.reason=name==='extra'?'extra_refresh_cancelled':'subscription_quota_cancelled';}
   else if(result.unsupported===true||name==='extra'&&!refreshSupported){component.state='unsupported';component.reason=name==='extra'?'official_entitlement_not_supported':'subscription_quota_not_supported';}
   else if(result.skipped===true){component.state='skipped';component.reason='account_not_refreshable';}
   else{component.state='failed';component.reason=name==='extra'?'extra_refresh_failed':'subscription_quota_refresh_failed';}
  }catch{component.state=signal.aborted?'cancelled':'failed';component.reason=signal.aborted?(name==='extra'?'extra_refresh_cancelled':'subscription_quota_cancelled'):(name==='extra'?'extra_refresh_failed':'subscription_quota_refresh_failed');}
 }
 function enqueueRefresh(owner){
  running();purge();if(pending.has(owner))return publicJob(jobs.get(pending.get(owner)));
  const pressure=refreshQueue.status();if(pressure.active>=refreshLimits.maxConcurrent&&pressure.queued>=refreshLimits.maxQueued)fail(503,'extra_refresh_busy','额度刷新繁忙，请稍后重试');
  if(jobs.size>=maxJobs){
   // finished_at is assigned only after the actual reader promise settles.
   // Live work is never evicted, including adapters still finishing after abort.
   let oldest=null;for(const job of jobs.values())if(job.finished_at!==null&&!['queued','running'].includes(job.state)&&(!oldest||job.finished_at<oldest.finished_at))oldest=job;
   if(!oldest)fail(503,'extra_refresh_busy','额度刷新繁忙，请稍后重试');jobs.delete(oldest.id);
  }
  const job={id:id(),account_id:owner,state:'queued',created_at:clock(),finished_at:null,verification:'pending',reason:null,components:{extra:{state:'queued',verification:'pending',reason:null},subscription:{state:refreshSubscriptionQuota?'queued':'unsupported',verification:'pending',reason:refreshSubscriptionQuota?null:'subscription_quota_not_supported'}}};jobs.set(job.id,job);pending.set(owner,job.id);
  const task=refreshQueue.run(async()=>{job.state='running';const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),refreshTimeoutMs);timer.unref();controllers.add(controller);try{await refreshComponent(job,'subscription',controller.signal);await refreshComponent(job,'extra',controller.signal);const states=Object.values(job.components).map(value=>value.state);job.verification=job.components.extra.verification;if(!refreshSubscriptionQuota){job.state=job.components.extra.state;job.reason=job.components.extra.reason;}else{job.state=states.every(state=>state==='unsupported')?'unsupported':states.every(state=>state==='cancelled')?'cancelled':states.some(state=>['verified','refreshed','pending_verification','rejected'].includes(state))?'completed':states.some(state=>state==='failed')?'failed':'completed';job.reason=states.some(state=>['failed','cancelled','unsupported'].includes(state))?'refresh_components_incomplete':null;}}finally{clearTimeout(timer);controllers.delete(controller);}}).catch(()=>{job.state='failed';job.reason='extra_refresh_not_started';for(const component of Object.values(job.components))if(component.state==='queued'){component.state='not_started';component.reason='refresh_not_started';}}).finally(()=>{job.finished_at=clock();pending.delete(owner);tasks.delete(task);});tasks.add(task);return publicJob(job);
 }
 async function perform(req,res){
  const url=new URL(req.url,'http://localhost'),global=url.pathname==='/api/admin/extra-usage/policy',extra=/^\/api\/admin\/extra-usage\/(coverage|policy\/preview|batches(?:\/preview|\/[a-f0-9-]{36}(?:\/(?:execute|retry))?)?)$/.exec(url.pathname),match=/^\/api\/admin\/accounts\/([^/]+)\/extra-usage(?:\/(manual|reset-card-used|refresh|records)(?:\/([^/]+))?)?$/.exec(url.pathname);
  if(!global&&!match&&!extra)return false;
  if(!await adminAuthenticated(req))fail(401,'admin_required','请先输入管理密码登录');
  if(!await acceptsAdminRequest(req))fail(403,'admin_origin_forbidden','不允许此Host或跨站请求');
  running();if(!extra&&match?.[2]!=='records'&&[...url.searchParams.keys()].length)fail(400,'extra_admin_query_invalid');
  if(extra){if(!batches)fail(503,'extra_batch_unavailable');const route=extra[1],detail=/^batches\/([a-f0-9-]{36})(?:\/(execute|retry))?$/.exec(route);if(req.method==='GET'){const value=await readQueue.run(()=>route==='coverage'?batches.coverage(url.searchParams):route==='batches'?batches.list(url.searchParams):detail&&!detail[2]?batches.detail(detail[1],extraPage(url.searchParams)):fail(405,'method_not_allowed'));json(res,200,value);return true;}if(req.method!=='POST'||[...url.searchParams.keys()].length)fail(405,'method_not_allowed');const actor=await adminSession(req);if(!actorValid(actor))fail(403,'extra_admin_context_required');try{const payload=await requestBodies.read(req,{maxBytes:maxBodyBytes,owner:'extra-admin:'+actor.session_ref});running();let value,status=201;if(route==='policy/preview')value=await batches.previewPolicy(payload,actor);else if(route==='batches/preview')value=await batches.previewAccounts(payload,actor);else if(detail&&detail[2]){if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).length)fail(400,'extra_preview_confirm_invalid');value=await(detail[2]==='execute'?batches.submit(detail[1],actor):batches.retryAccounts(detail[1],actor));status=detail[2]==='execute'?202:201;}else fail(405,'method_not_allowed');json(res,status,value);return true;}finally{requestBodies.release(req);}}
  let owner;if(match){try{owner=decodeURIComponent(match[1]);}catch{fail(400,'extra_account_id_invalid');}if(!accountId(owner))fail(400,'extra_account_id_invalid');}
  const method=req.method,action=match?.[2],jobId=match?.[3];
  if(method==='GET'){
   if(global){json(res,200,await readQueue.run(()=>reader.readPolicy()));return true;}
   if(action==='records'&&!jobId){if(!batches)fail(503,'extra_batch_unavailable');json(res,200,await readQueue.run(()=>batches.records(owner,url.searchParams)));return true;}
   if(!action){json(res,200,await readQueue.run(()=>reader.readAccount(owner)));return true;}
   if(action==='refresh'&&jobId){purge();const job=jobs.get(jobId);if(!job||job.account_id!==owner)fail(404,'extra_refresh_not_found','刷新任务记录已过期或被回收，请重新读取账号权益结果；不要自动重新提交刷新');json(res,200,publicJob(job));return true;}
   fail(405,'method_not_allowed');
  }
  if(action==='records'||jobId||global&&method!=='PATCH'||!global&&(!action&&method!=='PATCH'||action&&method!=='POST'))fail(405,'method_not_allowed');
  const actor=await adminSession(req);if(!actorValid(actor))fail(403,'extra_admin_context_required');
  let payload;try{payload=await requestBodies.read(req,{maxBytes:maxBodyBytes,owner:'extra-admin:'+actor.session_ref});running();
   if(global){if(!policyPreviews)fail(503,'extra_policy_preview_required','须先完成固定目标预览后才能修改总开关');json(res,200,await policyPreviews.apply(payload,actor));return true;}
   if(!action){json(res,200,await service.saveAccount(owner,payload,actor));return true;}
   if(action==='manual'){json(res,200,await service.recordManual(owner,payload,actor));return true;}
   if(action==='reset-card-used'){const result=await service.recordResetCardUsage(owner,payload,actor);let refresh;try{refresh=enqueueRefresh(owner);}catch{refresh={state:'not_started',verification:'pending',reason:'extra_refresh_busy'};}json(res,200,{...result,refresh});return true;}
   if(action==='refresh'){if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).length)fail(400,'extra_refresh_payload_invalid');await readQueue.run(()=>reader.readAccount(owner));json(res,202,enqueueRefresh(owner));return true;}
   fail(405,'method_not_allowed');
  }finally{requestBodies.release(req);}
 }
 const handle=(req,res)=>{const operation=perform(req,res);requests.add(operation);void operation.then(()=>requests.delete(operation),()=>requests.delete(operation));return operation;};
 const close=()=>closePromise??=(async()=>{stopping=true;for(const controller of controllers)controller.abort();await Promise.all([readQueue.close(),refreshQueue.close()]);await Promise.allSettled([...requests,...tasks]);})();
 return Object.freeze({handle,close,status:()=>({reads:readQueue.status(),refresh:refreshQueue.status(),jobs:jobs.size,pending:pending.size,requests:requests.size,stopping})});
}
