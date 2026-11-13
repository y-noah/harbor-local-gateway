import {createPostgresAudit} from './postgres-audit.mjs';
import {createDepartmentScopeIssuer} from './department-access.mjs';
import {createAccountDepartments} from './postgres-account-departments.mjs';
import {AppError} from './store.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';

// One official login at a time. No transaction is held while the CLI or the
// authenticated browser waits for the user. Epochs reject all late results.
export function createPostgresAccountLogin(store,{startLogin,reserveLogin,checkSubscription,clock=Date.now,local=true,onReady}={}){
  const access=createAccountDepartments(store,{clock});
  const pending=new Map(),tasks=new Set();let stopping=false;
  const running=()=>{if(stopping)throw new AppError(503,'账号登录正在关闭','service_stopping');};
  const auditWriter=createPostgresAudit(store,{clock}),issuer=createDepartmentScopeIssuer(),audit=(action,account,scope)=>auditWriter.write(scope,{action,detail:account,subject:{kind:'account',id:account}});
  const mutate=fn=>store.transaction(async()=>{await lockPostgresRouting(store);if(stopping)throw new AppError(503,'账号登录正在关闭','service_stopping');return fn();});
  const load=async(accountId,options={})=>{const {row}=await access.account(accountId,options,{write:true,lock:true});if(!row)throw new AppError(404,'账号不存在或已移除');running();return row;};
  async function validate(entry,{lock=false}={}){
    try{const {row}=await access.account(entry.account.id,entry.options,{write:true,lock});if(!row.enabled||row.state!=='login'||row.route_epoch!==entry.account.route_epoch)throw new AppError(409,'账号配置已变化','account_configuration_changed');return row;}
    catch(error){if([401,403].includes(error.status)){entry.authorizationLost=true;entry.controller.abort();entry.flow?.cancel();}throw error;}
  }
  async function invalidate(entry){
    if(!entry.authorizationLost)return;
    await store.transaction(async()=>{await lockPostgresRouting(store);await store.run("UPDATE harbor.accounts SET state='offline',last_error='管理授权已失效，请重新登录',route_epoch=route_epoch+1 WHERE id=$1 AND state='login' AND route_epoch=$2"+(entry.account.department_id?' AND department_id=$3':''),entry.account.id,entry.account.route_epoch,...(entry.account.department_id?[entry.account.department_id]:[]));});
  }
  function cancel(accountId){const entry=pending.get(accountId);if(!entry)return;pending.delete(accountId);entry.controller.abort();entry.flow?.cancel();entry.lease?.release();entry.reject(new AppError(409,'账号登录已取消，请重新登录','account_configuration_changed'));}
  function launch(entry){
    if(stopping||pending.get(entry.account.id)!==entry){entry.lease?.release();entry.reject(new AppError(409,'账号登录已取消'));return;}
    let flow;try{flow=startLogin(entry.account,{lease:entry.lease,signal:entry.controller.signal});if(!flow||!flow.challenge||!flow.completed||typeof flow.cancel!=='function')throw Error('invalid login flow');entry.flow=flow;}catch{flow={challenge:Promise.reject(Error('login start failed')),completed:Promise.resolve({success:false}),cancel:()=>{}};entry.flow=flow;}
    // Attach both handlers before awaiting either promise, including CLI failure
    // before an authorization challenge has been issued.
    const challenge=Promise.resolve(flow.challenge).then(async value=>{
      await validate(entry);
      if(stopping||pending.get(entry.account.id)!==entry)throw new AppError(409,'账号登录已取消');
      entry.challenge=value;entry.resolve(value);return value;
    },()=>{entry.reject(new AppError(502,'无法获取官方登录信息，请检查CLI和网络','login_failed'));return null;});
    challenge.catch(error=>{entry.reject(error);entry.controller.abort();flow.cancel();});
    const timeout=AbortSignal.timeout(600000),abort=()=>{entry.controller.abort();flow.cancel();};timeout.addEventListener('abort',abort,{once:true});
    const completed=Promise.race([Promise.resolve(flow.completed).catch(()=>({success:false})),new Promise(resolve=>entry.controller.signal.addEventListener('abort',()=>resolve({success:false}),{once:true}))]);
    const task=(async()=>{
      let verified=null;
      try{
        const result=await completed;let ready=false;
        if(result?.success&&!stopping&&pending.get(entry.account.id)===entry)try{await validate(entry);ready=await checkSubscription(entry.account,{signal:entry.controller.signal})===true;}catch{}
        if(stopping||pending.get(entry.account.id)!==entry)return;
        await mutate(async()=>{
          const current=await validate(entry,{lock:true});if(current.route_epoch!==entry.account.route_epoch||current.state!=='login')return;
          await store.run("UPDATE harbor.accounts SET state=$1,last_error=$2 WHERE id=$3",ready?'ready':'offline',ready?null:'官方登录未完成，请检查授权后重试',current.id);
          await audit(ready?'account.login.success':'account.login.failed',current.id,entry.options.scope);if(ready)verified={...current,state:'ready'};
        });
      }catch{/* Failed persistence leaves login state visible and recoverable. */}
      finally{
        await invalidate(entry).catch(()=>{});timeout.removeEventListener('abort',abort);entry.controller.abort();flow.cancel();entry.lease?.release();
        entry.reject(new AppError(409,'登录流程已结束，请重新登录','login_finished'));
        if(pending.get(entry.account.id)===entry)pending.delete(entry.account.id);
      }
      if(verified&&!stopping&&onReady)try{await onReady(verified,entry.options);}catch{}
    })();tasks.add(task);void task.finally(()=>tasks.delete(task)).catch(()=>{});
  }
  async function handle(accountId,action,method,options={}){
    await access.resolve(options,{write:method!=='GET'});
    if(method==='GET'&&action==='login'){
      const {row:current}=await access.account(accountId,options);if(!current)throw new AppError(404,'账号不存在或已移除');
      if(current.auth_mode!=='subscription')throw new AppError(400,'此账号不使用订阅登录');
      const entry=pending.get(accountId);if(entry)await validate(entry);return {status:200,value:{state:current.state,message:current.last_error?sanitizeRecord(current.last_error,{maxBytes:2048}):'',...(entry?.challenge?{...entry.challenge,expiresAt:entry.expiresAt}:{})}};
    }
    if(method==='POST'&&action==='login/cancel'){
      await mutate(async()=>{const current=await load(accountId,options);if(current.auth_mode!=='subscription')throw new AppError(400,'此账号不使用订阅登录');if(current.state==='login'){
        await store.run("UPDATE harbor.accounts SET state='offline',last_error='登录已取消',route_epoch=route_epoch+1 WHERE id=$1",accountId);await audit('account.login.cancel',accountId,options.scope);
      }});cancel(accountId);return {status:200,value:{ok:true}};
    }
    if(method!=='POST'||action!=='login')return null;
    let created,entry,reserved;
    try{
      entry=await mutate(async()=>{
        const current=await load(accountId,options);
        if(current.auth_mode!=='subscription')throw new AppError(400,'此账号不使用订阅登录');
        if(!current.enabled)throw new AppError(409,'请先启用账号再登录');
        if(current.kind==='claude'&&!local)throw new AppError(400,'服务器暂不支持Claude浏览器登录，请使用本机版完成授权');
        if(pending.has(accountId)){const existing=pending.get(accountId);if(existing.options.scope?.credential_id!==options.scope?.credential_id)throw new AppError(409,'另一个管理身份已发起该账号登录');return existing;}
        if(pending.size)throw new AppError(409,'另一个账号正在登录，请先完成该流程');
        if(await store.get("SELECT id FROM harbor.requests WHERE account_id=$1 AND status IN ('queued','running') LIMIT 1",accountId))throw new AppError(409,'账号正在使用，请稍后登录');
        running();reserved=reserveLogin?.(current);await store.run("UPDATE harbor.accounts SET state='login',route_epoch=route_epoch+1,last_error=NULL,quota_json=NULL,quota_calibration=NULL,quota_error=NULL,since_check_tokens=0 WHERE id=$1",accountId);
        await store.run("UPDATE harbor.sessions SET state='uncertain',route_version=route_version+1 WHERE account_id=$1",accountId);await audit('account.login',accountId,options.scope);
        let resolve,reject;const authorization=new Promise((yes,no)=>{resolve=yes;reject=no;});authorization.catch(()=>{});
        created={options:{...options},lease:reserved,account:{...current,route_epoch:current.route_epoch+1},expiresAt:clock()+600000,controller:new AbortController(),authorization,resolve,reject};pending.set(accountId,created);return created;
      });
    }catch(error){reserved?.release();if(created)cancel(accountId);throw error;}
    if(created){try{await validate(entry);launch(entry);}catch(error){await invalidate(entry).catch(()=>{});cancel(accountId);throw error;}}
    const challenge=await entry.authorization;await validate(entry);await access.resolve(options,{write:true});
    if(stopping||pending.get(accountId)!==entry)throw new AppError(409,'登录流程已结束，请重新登录');
    return {status:200,value:{ok:true,...challenge,expiresAt:entry.expiresAt}};
  }
  const stop=()=>{stopping=true;for(const accountId of [...pending.keys()])cancel(accountId);};
  return {handle,cancel,stop,status:()=>({pending:pending.size,tasks:tasks.size}),close:async()=>{stop();await Promise.allSettled([...tasks]);},recover:()=>mutate(async()=>{
    const isolated=await access.installed(),accounts=await store.all("SELECT * FROM harbor.accounts WHERE state='login' AND removed_at IS NULL FOR UPDATE");
    for(const row of accounts)await audit('account.login.failed',row.id,isolated?issuer.system({purpose:'account.login.recover',departmentId:row.department_id}):undefined);
    await store.run("UPDATE harbor.accounts SET state='offline',last_error='服务重启中断登录，请重新登录',route_epoch=route_epoch+1 WHERE state='login' AND removed_at IS NULL");
  })};
}
