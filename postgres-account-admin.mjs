import {createPostgresAudit} from './postgres-audit.mjs';
import {createAccountDepartments,accountAuthorizationError} from './postgres-account-departments.mjs';
import {AsyncLocalStorage} from 'node:async_hooks';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {createPostgresSubscriptionPrices,subscriptionMonthlyUsd} from './postgres-subscription-price.mjs';
import {mkdir,rmdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {AppError,id,hash,encryptStoredKey} from './store.mjs';
import {name,bool,integer,apiKeyValue,apiWeightValue} from './admin-input.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {createPostgresAccountQuotas} from './postgres-account-quotas.mjs';
import {createPostgresAccountLogin} from './postgres-account-login.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';
import {createPostgresPlatformAdmin} from './postgres-platform-admin.mjs';

export function createPostgresAccountAdmin(store,{runtime,encryptionKey,router,checkApi,checkSubscription,clock=Date.now,checks={},startLogin,reserveLogin,local=true,readQuota,platformTransport}={}){
  if(typeof runtime!=='string'||!runtime||!router||typeof router.disableAccount!=='function'||typeof router.block!=='function'||typeof checkApi!=='function'||typeof checkSubscription!=='function')throw new AppError(503,'账号管理依赖尚未配置完整','account_configuration');
  const departments=createAdminDepartments(store),access=createAccountDepartments(store,{clock});
  const queue=createPostgresQueryQueue(checks),readContext=new AsyncLocalStorage(),readQueue={...queue,run:(operation,options)=>readContext.getStore()?operation():queue.run(()=>readContext.run(true,operation),options)},pending=new Map(),controllers=new Set();let stopping=false;
  const packages=createPostgresAccountPackages(store,{clock,queryQueue:readQueue,onChange:(_reason,options)=>router.syncWeights('package',clock(),options)});
  const subscriptionPrices=createPostgresSubscriptionPrices(store,{clock});
  const quotas=readQuota?createPostgresAccountQuotas(store,{router,packages,queue,readQuota,clock,parallelism:checks.maxConcurrent??2}):null;
  const login=startLogin?createPostgresAccountLogin(store,{startLogin,reserveLogin,clock,local,onReady:(account,options)=>quotas?.refresh(account,{force:true,...options}),checkSubscription:(account,options)=>queue.run(()=>checkSubscription(account,options))}):null;
  const running=()=>{if(stopping)throw new AppError(503,'账号管理正在关闭','service_stopping');};
  const auditWriter=createPostgresAudit(store,{clock}),audit=(action,detail,scope,accountId=typeof detail==='string'?detail:detail.account)=>auditWriter.write(scope,{action,detail,subject:{kind:'account',id:accountId}});
  const account=async(accountId,lock=false,scope=null)=>{await departments.current(scope,{lock});const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL'+(scope?.view==='department'?' AND department_id=$2':'')+(lock?' FOR UPDATE':''),accountId,...(scope?.view==='department'?[scope.department_id]:[]));if(!row)throw new AppError(404,'账号不存在或已移除','department_object_not_found');departments.same(scope,row);if(lock)running();return row;};
  const weight=async(accountId,value,scope=null)=>apiWeightValue(value,(await store.get("SELECT COALESCE(SUM(api_weight),0) n FROM harbor.accounts WHERE auth_mode='openai_api' AND removed_at IS NULL AND id<>$1"+(scope?' AND department_id=$2':''),accountId,...(scope?[scope.department_id]:[]))).n);
  function cipher(secret,accountId){if(!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32)throw new AppError(503,'上游凭据加密尚未配置','upstream_configuration');return encryptStoredKey(encryptionKey,secret,'upstream:'+accountId);}
  const mutate=(fn,{scope=null}={})=>store.transaction(async()=>{if(scope)await departments.current(scope,{lock:true,write:true});await lockPostgresRouting(store);if(stopping)throw new AppError(503,'账号管理正在关闭','service_stopping');return fn();});
  const platform=platformTransport?createPostgresPlatformAdmin(store,{runtime,encryptionKey,transport:platformTransport,queue,mutate,cipher,clock,packages}):null;
  async function check(current,options={}){
    await access.account(current.id,options,{write:true});
    if(current.state==='login')throw new AppError(409,'账号正在登录，请先完成或取消登录');
    const key=current.id+':'+current.route_epoch+':'+access.identity(options);if(pending.has(key))return pending.get(key);
    const task=queue.run(async()=>{
      if(stopping)throw new AppError(503,'账号管理正在关闭','service_stopping');
      const controller=new AbortController();controllers.add(controller);const signal=AbortSignal.any([controller.signal,AbortSignal.timeout(15000)]);
      try{
        if(current.auth_mode==='platform')throw new AppError(400,'请使用该平台专用的登录或检查入口');
        await access.account(current.id,options,{write:true});
        const api=current.auth_mode==='openai_api';
        const result=api?await checkApi(current,{signal}):await checkSubscription(current,{signal});
        if(api&&(!Array.isArray(result)||!result.length||result.some(model=>typeof model.slug!=='string')))throw new AppError(502,'官方API模型检查返回格式无效','upstream_protocol');
        return await mutate(async()=>{
          const {row:latest}=await access.account(current.id,options,{write:true,lock:true});if(latest.route_epoch!==current.route_epoch)throw new AppError(409,'账号配置已变化，请重新检查','account_configuration_changed');
          const ready=api||result===true;
          await store.run('UPDATE harbor.accounts SET state=$1,last_error=$2'+(api?',route_block=NULL,route_epoch=route_epoch+1':'')+' WHERE id=$3',ready?'ready':'offline',ready?null:'未检测到订阅登录',current.id);
          await audit(api?'account.api.check':'account.check',current.id,options.scope);
          return api?{ready:true,models:result.map(model=>model.slug),balance:{available:false,reason:'official_api_not_provided'}}:{ready};
        });
      }catch(error){
        running();if(error.code==='upstream_buffer_busy'||accountAuthorizationError(error))throw error;
        if(current.auth_mode==='openai_api')await mutate(async()=>{
          const {row:latest}=await access.account(current.id,options,{write:true,lock:true});
          running();if(!latest||latest.route_epoch!==current.route_epoch)return;
          await store.run('UPDATE harbor.accounts SET state=$1,last_error=$2 WHERE id=$3',['ready','degraded'].includes(current.state)?'degraded':'offline','官方API Key检查未通过，请核对权限和配置',current.id);
          await router.block(latest,error.routeFailure,{scope:options.scope});
        });
        throw error;
      }finally{controllers.delete(controller);}
    }).finally(()=>pending.delete(key));pending.set(key,task);return task;
  }
  async function handle(path,method,payload={},options={}){
    if(!/^\/api\/admin\/(?:accounts|account-packages|platform|quotas)(?:\/|$)/.test(path))return null;
    if(method==='GET')return readQueue.run(()=>dispatch(path,method,payload,options),{signal:options.signal});
    return dispatch(path,method,payload,options);
  }
  async function dispatch(path,method,payload,options){
    if(stopping)throw new AppError(503,'账号管理正在关闭','service_stopping');
    if(!/^\/api\/admin\/(?:accounts|account-packages|platform|quotas)(?:\/|$)/.test(path))return null;
    const trusted=await departments.resolve(options.scope,{write:method!=='GET',payload});await access.resolve(options,{write:method!=='GET'});options={...options,scope:trusted??undefined};
    if(trusted&&/^\/api\/admin\/platform(?:\/|$)/.test(path))departments.unsupported();
    const subscription=path.match(/^\/api\/admin\/accounts\/([a-zA-Z0-9-]+)\/subscription$/);
    if(subscription&&method==='GET')return {status:200,value:await readQueue.run(()=>subscriptionPrices.read(subscription[1],{scope:trusted??undefined}),{signal:options.signal})};
    if(subscription&&method==='PATCH')return {status:200,value:await subscriptionPrices.save(subscription[1],payload,{scope:trusted??undefined})};
    if(/^\/api\/admin\/(account-packages|accounts\/[^/]+\/package)(?:\/|$)/.test(path)){if(path==='/api/admin/account-packages/coverage'&&method==='GET')return {status:200,value:await router.readView({explanation:true,packagePreview:true,scope:trusted??undefined})};const result=await packages.handle(path,method,payload,{...options,scope:trusted??undefined});if(result)return result;}
    if(path.startsWith('/api/admin/platform/')&&platform)return platform.handle(path,method,payload);
    if(quotas&&path==='/api/admin/quotas/refresh'&&method==='POST')return {status:200,value:await quotas.refreshAll({force:true,...options})};
    if(path==='/api/admin/accounts'&&method==='POST'){
      const kind=payload.kind||'codex',mode=payload.auth_mode||'subscription',accountId=id(),accountName=name(payload.name);
      if(!['codex','claude'].includes(kind)||!['subscription','openai_api'].includes(mode)||mode==='openai_api'&&kind!=='codex')throw new AppError(400,'不支持的账号类型');
      if(payload.subscription_monthly_usd!==undefined){if(kind!=='codex'||mode!=='subscription')throw new AppError(400,'仅Codex官方订阅可设置订阅月费');subscriptionMonthlyUsd(payload.subscription_monthly_usd);}
      const secret=mode==='openai_api'?apiKeyValue(payload.api_key):null,encrypted=secret?cipher(secret,accountId):null,home=join(resolve(runtime),'accounts',accountId);let created=false;
      try{
        await mkdir(join(resolve(runtime),'accounts'),{recursive:true,mode:0o700});await mkdir(home,{mode:0o700});created=true;
        await mutate(async()=>{
          await departments.current(trusted,{lock:true});const share=mode==='openai_api'?await weight(accountId,payload.api_weight??0,trusted):0;
          await store.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,api_weight,state,api_secret_cipher,api_secret_digest"+(trusted?',department_id':'')+") VALUES($1,$2,$3,$4,$5,$6,'offline',$7,$8"+(trusted?',$9':'')+")",accountId,accountName,home,kind,mode,share,encrypted,secret?hash(secret):null,...(trusted?[trusted.department_id]:[]));if(mode!=='openai_api')await packages.attach(accountId,payload.package??{template_id:null},{scope:trusted??undefined});else if(payload.package!==undefined)throw new AppError(400,'API账号使用人工预算，不适用订阅套餐');await audit('account.create',accountId,trusted);
          if(payload.subscription_monthly_usd!==undefined)await subscriptionPrices.save(accountId,{monthly_usd:payload.subscription_monthly_usd,expected_version:0},{scope:trusted??undefined});
        },{scope:trusted});return {status:201,value:{id:accountId}};
      }catch(error){if(created)await rmdir(home).catch(()=>{});throw error;}
    }
    const match=path.match(/^\/api\/admin\/accounts\/([a-zA-Z0-9-]+)(?:\/(check|quota|login|login\/cancel))?$/);if(!match)return null;
    if(trusted)await account(match[1],false,trusted);
    if(method==='GET'&&!match[2]){const row=await account(match[1],false,trusted);const {id:accountId,name,kind,auth_mode,enabled,state,created,api_weight,token_budget}=row;return {status:200,value:{id:accountId,name,kind,auth_mode,enabled,state,created,api_weight,token_budget,...(trusted?{department_id:row.department_id}:{})}};}
    if(login&&match[2]?.startsWith('login')){if(trusted&&(await account(match[1],false,trusted)).kind!=='codex')departments.unsupported();return login.handle(match[1],match[2],method,options);}
    if(quotas&&method==='POST'&&match[2]==='quota')return {status:200,value:{ok:true,...await quotas.refresh(await account(match[1],false,trusted),{force:true,...options})}};
    if(method==='POST'&&match[2]==='check')return {status:200,value:await check(await account(match[1],false,trusted),options)};
    if(match[2])return null;
    if(method==='PATCH'){const value=await mutate(async()=>{
      const current=await account(match[1],true,trusted),api=current.auth_mode==='openai_api';
      if((payload.api_key!==undefined||payload.api_weight!==undefined)&&!api)throw new AppError(400,'订阅账号不能写入API Key或人工API占比');
      const secret=payload.api_key===undefined?null:apiKeyValue(payload.api_key),encrypted=secret?cipher(secret,current.id):null;
      const share=payload.api_weight===undefined?current.api_weight:await weight(current.id,payload.api_weight,trusted),enabled=payload.enabled===undefined?current.enabled:bool(payload.enabled);
      const budget=payload.token_budget===undefined?current.token_budget:integer(payload.token_budget,'本地预算',{nullable:true}),perPercent=payload.tokens_per_percent===undefined?current.tokens_per_percent:integer(payload.tokens_per_percent,'容量换算',{min:100,max:1e7});
      const migration=payload.enabled===false?await router.disableAccount(current.id,{scope:trusted??undefined}):{moved:0,resetTextSessions:0};
      if(payload.enabled===false&&current.state==='login')await store.run("UPDATE harbor.accounts SET state='offline',last_error='登录已取消' WHERE id=$1",current.id);
      await store.run('UPDATE harbor.accounts SET enabled=$1,token_budget=$2,tokens_per_percent=$3,api_weight=$4 WHERE id=$5',enabled,budget,perPercent,share,current.id);
      if(secret){await store.run("UPDATE harbor.accounts SET api_secret_cipher=$1,api_secret_digest=$2,route_epoch=route_epoch+1,state='offline',route_block=NULL,last_error=NULL WHERE id=$3",encrypted,hash(secret),current.id);await audit('account.api.key',current.id,trusted);}
      if(api&&share!==current.api_weight)await audit('account.api.weight',{account:current.id,from:current.api_weight,to:share},trusted);await audit('account.update',current.id,trusted);return {ok:true,...migration};
    },{scope:trusted});if(payload.enabled===false){login?.cancel(match[1]);platform?.cancel(match[1]);}return {status:200,value};}
    if(method==='DELETE'){const value=await mutate(async()=>{
      const current=await account(match[1],true,trusted);
      if(await store.get("SELECT id FROM harbor.requests WHERE account_id=$1 AND status IN ('queued','running') LIMIT 1",current.id))throw new AppError(409,'账号有请求正在处理或排队，请完成后再移除');
      await store.run("UPDATE harbor.accounts SET enabled=0,state='removed',api_secret_cipher=NULL,api_secret_digest=NULL,removed_at=$1,route_epoch=route_epoch+1 WHERE id=$2",clock(),current.id);
      await store.run("UPDATE harbor.sessions SET state='uncertain',route_version=route_version+1 WHERE account_id=$1",current.id);await audit('account.remove',current.id,trusted);return {ok:true};
    },{scope:trusted});login?.cancel(match[1]);platform?.cancel(match[1]);return {status:200,value};}
    return null;
  }
  const stop=()=>{stopping=true;login?.stop();platform?.stop();quotas?.stop();for(const controller of controllers)controller.abort();};
  const refreshQuota=async(accountId,{signal,scope,revalidateScope}={})=>{running();signal?.throwIfAborted();if(!quotas)return {unsupported:true,reason:'subscription_quota_not_supported'};const identity={scope:scope??await quotas.quotaScope(accountId),revalidateScope};const current=await quotas.load(accountId,identity);signal?.throwIfAborted();if(current.auth_mode!=='subscription'||current.kind!=='codex')return {unsupported:true,reason:'subscription_quota_not_supported'};return quotas.refresh(current,{force:true,signal,...identity});};
  return {handle,packages,stop,recover:()=>login?.recover(),platformCredential:(row,signal)=>platform?.authenticated(row,signal),refreshQuota,refreshQuotas:options=>quotas?.refreshAll(options),status:()=>({...queue.status(),pending:pending.size,stopping,login:login?.status(),quotas:quotas?.status(),platform:platform?.status()}),close:async()=>{stop();await queue.close();await login?.close();await platform?.close();await quotas?.close();await Promise.allSettled([...pending.values()]);}};
}
