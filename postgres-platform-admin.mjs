import {mkdir,rmdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {AppError,id,hash,revealStoredKey} from './store.mjs';
import {name} from './admin-input.mjs';
import {platformProviders,platformConfiguration} from './platforms.mjs';

// Uses the account administrator's bounded check queue and short mutation lock.
// Upstream authorization and refresh never hold a database transaction open.
export function createPostgresPlatformAdmin(store,{runtime,encryptionKey,transport,queue,mutate,cipher,clock=Date.now,packages}={}){
  const logins=new Map(),refreshes=new Map(),checks=new Map(),controllers=new Set(),tasks=new Set();let stopping=false;
  const fail=(status,message,code='platform_configuration_changed')=>new AppError(status,message,code);
  const running=()=>{if(stopping)throw fail(503,'平台账号管理正在关闭','service_stopping');};
  const audit=(action,account)=>store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),action,account);
  const account=async(accountId,lock=false)=>{const row=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND auth_mode='platform' AND removed_at IS NULL"+(lock?' FOR UPDATE':''),accountId);if(!row)throw fail(404,'平台账号不存在');running();return row;};
  const valid=async(row,lock=false)=>{const current=await account(row.id,lock);if(!current.enabled||current.route_epoch!==row.route_epoch)throw fail(409,'平台账号配置已变化，请重试');return current;};
  const token=value=>{if(typeof value!=='string'||!value.trim()||value.length>4096||/\s/.test(value.trim()))throw fail(400,'请填写有效的官方Key或访问令牌','invalid_platform_credential');return {token:value.trim()};};
  const credential=row=>{try{const raw=revealStoredKey(encryptionKey,{id:'upstream:'+row.id,secret_cipher:row.api_secret_cipher,digest:row.api_secret_digest}),value=JSON.parse(raw);if(!value||typeof value!=='object'||Array.isArray(value))throw Error();return value;}catch{throw fail(401,'平台凭据不可用，请重新接入','platform_authentication');}};
  const save=async(row,value)=>{const secret=JSON.stringify(value);await store.run('UPDATE harbor.accounts SET api_secret_cipher=$1,api_secret_digest=$2 WHERE id=$3',cipher(secret,row.id),hash(secret),row.id);};
  const modelList=value=>{if(!Array.isArray(value)||!value.length||value.length>4096||value.some(model=>typeof model!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(model)))throw fail(502,'平台模型检查返回格式无效','platform_protocol');return [...new Set(value)];};
  function tracked(task){tasks.add(task);void task.finally(()=>tasks.delete(task)).catch(()=>{});return task;}
  function cancel(accountId){const entry=logins.get(accountId);if(!entry)return;logins.delete(accountId);entry.controller.abort();entry.reject(fail(409,'平台登录已取消，请重新登录'));}
  async function authenticated(row,signal){
    running();if(signal?.aborted)throw fail(499,'平台请求已取消','request_cancelled');const value=credential(row);
    if(row.kind!=='workbuddy'||value.expiresAt>clock()+60000)return value;
    const key=row.id+':'+row.route_epoch;
    if(!refreshes.has(key)){
      const controller=new AbortController();controllers.add(controller);
      const task=tracked(queue.run(async()=>{running();const next=await transport.refresh(value,AbortSignal.any([controller.signal,AbortSignal.timeout(15000)]));
        return mutate(async()=>{await valid(row,true);if(controller.signal.aborted)throw fail(503,'平台刷新已取消');await save(row,next);return next;});
      }).finally(()=>{controllers.delete(controller);refreshes.delete(key);}));refreshes.set(key,task);
    }
    const next=await refreshes.get(key);if(signal?.aborted)throw fail(499,'平台请求已取消','request_cancelled');return next;
  }
  function check(row){
    if(!row.enabled||row.state==='login')throw fail(409,'请先启用账号并完成登录');const key=row.id+':'+row.route_epoch;if(checks.has(key))return checks.get(key);
    // Refresh may itself use the queue. Resolve it before acquiring a check slot.
    const task=tracked((async()=>{const value=await authenticated(row);return queue.run(async()=>{
      running();const controller=new AbortController();controllers.add(controller);
      try{const models=modelList(await transport.models(row,value,AbortSignal.any([controller.signal,AbortSignal.timeout(15000)])));return await mutate(async()=>{await valid(row,true);if(controller.signal.aborted)throw fail(503,'平台检查已取消');await store.run("UPDATE harbor.accounts SET state='ready',platform_models=$1,last_error=NULL WHERE id=$2",JSON.stringify(models),row.id);await audit('account.platform.check',row.id);return {ready:true,models:models.map(model=>row.kind+'/'+model)};});}
      finally{controllers.delete(controller);}
    });})().catch(async error=>{if(!stopping&&!['query_queue_full','query_timeout','service_stopping','cli_capacity_busy','upstream_buffer_busy'].includes(error.code))await mutate(async()=>{const current=await account(row.id,true);if(current.route_epoch!==row.route_epoch||!current.enabled)return;await store.run("UPDATE harbor.accounts SET state='offline',last_error='平台检查未通过，请核对认证、入口与CLI版本' WHERE id=$1",row.id);}).catch(()=>{});throw error;}).finally(()=>checks.delete(key)));checks.set(key,task);return task;
  }
  function launch(entry){
    const {controller}=entry;const timer=setTimeout(()=>controller.abort(),600000);timer.unref();
    tracked((async()=>{try{
      const value=await transport.login({signal:controller.signal,onChallenge:challenge=>{if(!stopping&&logins.get(entry.account.id)===entry&&!controller.signal.aborted){entry.challenge=challenge;entry.resolve(challenge);}}});
      if(stopping||logins.get(entry.account.id)!==entry||controller.signal.aborted)return;
      const models=await queue.run(async()=>{running();return modelList(await transport.models(entry.account,value,AbortSignal.any([controller.signal,AbortSignal.timeout(15000)])));});
      await mutate(async()=>{const current=await valid(entry.account,true);if(logins.get(current.id)!==entry||controller.signal.aborted)throw fail(409,'平台登录已取消');await save(current,value);await store.run("UPDATE harbor.accounts SET state='ready',platform_models=$1,last_error=NULL WHERE id=$2",JSON.stringify(models),current.id);await audit('account.login.success',current.id);});
    }catch{if(!stopping&&logins.get(entry.account.id)===entry)await mutate(async()=>{const current=await valid(entry.account,true);await store.run("UPDATE harbor.accounts SET state='offline',last_error='平台登录未完成，请重新授权或检查模型权限' WHERE id=$1",current.id);await audit('account.login.failed',current.id);}).catch(()=>{});}
    finally{clearTimeout(timer);controller.abort();entry.reject(fail(409,'平台登录流程已结束，请重新登录'));if(logins.get(entry.account.id)===entry)logins.delete(entry.account.id);}})());
  }
  async function handle(path,method,payload={}){
    running();if(path==='/api/admin/platform/providers'&&method==='GET')return {status:200,value:{providers:Object.entries(platformProviders).map(([provider,{label,authentication,regions}])=>({id:provider,label,authentication,regions:regions?Object.keys(regions):[]}))}};
    if(path==='/api/admin/platform/accounts'&&method==='POST'){
      const configuration=platformConfiguration(payload.provider,payload.region),provider=platformProviders[payload.provider],accountId=id(),accountName=name(payload.name),value=provider.authentication==='browser'?null:token(payload.credential),home=join(resolve(runtime),'accounts',accountId);let created=false;
      try{await mkdir(join(resolve(runtime),'accounts'),{recursive:true,mode:0o700});await mkdir(home,{mode:0o700});created=true;
        await mutate(async()=>{await store.run("INSERT INTO harbor.accounts(id,name,kind,home,auth_mode,state,platform_config) VALUES($1,$2,$3,$4,'platform','offline',$5)",accountId,accountName,payload.provider,home,JSON.stringify(configuration));if(value)await save({id:accountId},value);await packages?.attach(accountId,payload.package??{template_id:null});await audit('account.create',accountId);});return {status:201,value:{id:accountId}};
      }catch(error){if(created)await rmdir(home).catch(()=>{});throw error;}
    }
    const match=path.match(/^\/api\/admin\/platform\/accounts\/([a-zA-Z0-9-]+)(?:\/(check|login|login\/cancel))?$/);if(!match)return null;
    const row=await account(match[1]),action=match[2];
    if(method==='PATCH'&&!action){
      if(Object.keys(payload).some(key=>!['credential','expected_epoch'].includes(key))||!Number.isSafeInteger(payload.expected_epoch))throw fail(400,'平台凭据配置字段无效');if(row.kind==='workbuddy')throw fail(400,'WorkBuddy请使用官方网页登录');const value=token(payload.credential);
      await mutate(async()=>{const current=await account(row.id,true);if(payload.expected_epoch!==current.route_epoch)throw fail(409,'平台配置已变化，请重新打开设置');await save(current,value);await store.run("UPDATE harbor.accounts SET route_epoch=route_epoch+1,state='offline',platform_models='[]',last_error=NULL WHERE id=$1",row.id);await audit('account.platform.credential',row.id);});cancel(row.id);return {status:200,value:{ok:true}};
    }
    if(action==='check'&&method==='POST')return {status:200,value:await check(row)};
    if(action==='login'&&method==='GET'){const entry=logins.get(row.id);return {status:200,value:{state:row.state,message:row.last_error||'',...(entry?.challenge?{...entry.challenge,expiresAt:entry.expiresAt}:{})}};}
    if(action==='login/cancel'&&method==='POST'){await mutate(async()=>{const current=await account(row.id,true);if(current.kind!=='workbuddy')throw fail(400,'此平台请填写官方Key或访问令牌');if(current.state==='login'){await store.run("UPDATE harbor.accounts SET state='offline',route_epoch=route_epoch+1,last_error='登录已取消' WHERE id=$1",row.id);await audit('account.login.cancel',row.id);}});cancel(row.id);return {status:200,value:{ok:true}};}
    if(action!=='login'||method!=='POST')return null;
    let created,entry;try{entry=await mutate(async()=>{
      const current=await account(row.id,true);if(current.kind!=='workbuddy')throw fail(400,'此平台请填写官方Key或访问令牌');if(!current.enabled)throw fail(409,'请先启用账号再登录');if(logins.has(row.id))return logins.get(row.id);if(logins.size)throw fail(409,'另一个平台账号正在登录');
      if(await store.get("SELECT id FROM harbor.requests WHERE account_id=$1 AND status IN ('queued','running') LIMIT 1",row.id))throw fail(409,'账号正在使用，请稍后登录');
      await store.run("UPDATE harbor.accounts SET state='login',route_epoch=route_epoch+1,platform_models='[]',last_error=NULL WHERE id=$1",row.id);await audit('account.login',row.id);
      let resolveChallenge,reject;const challenge=new Promise((yes,no)=>{resolveChallenge=yes;reject=no;});challenge.catch(()=>{});created={account:{...current,route_epoch:current.route_epoch+1},controller:new AbortController(),expiresAt:clock()+600000,authorization:challenge,resolve:resolveChallenge,reject};logins.set(row.id,created);return created;
    });}catch(error){if(created)cancel(row.id);throw error;}
    if(created)launch(entry);const challenge=await entry.authorization;if(stopping||logins.get(row.id)!==entry)throw fail(409,'平台登录流程已结束，请重新登录');return {status:200,value:{...challenge,expiresAt:entry.expiresAt}};
  }
  const stop=()=>{stopping=true;for(const accountId of [...logins.keys()])cancel(accountId);for(const controller of controllers)controller.abort();};
  return {handle,authenticated,cancel,stop,status:()=>({logins:logins.size,checks:checks.size,refreshes:refreshes.size,tasks:tasks.size}),close:async()=>{stop();await Promise.allSettled([...tasks]);}};
}
