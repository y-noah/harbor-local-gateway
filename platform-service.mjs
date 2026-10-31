import {AppError,id,now} from './store.mjs';
import {createPlatformTransport,platformProviders,platformConfiguration,platformModel,validatePlatformChat} from './platforms.mjs';
import {join} from 'node:path';
import {mkdirSync} from 'node:fs';

export function createPlatformService({store,runtime,body,json,employee,transport=createPlatformTransport(),busy,responseMembers,preparing,controllers,tasks,isStopping,checkLimits,clock=Date.now}){
  const logins=new Map(),refreshes=new Map();
  const account=id=>{const row=store.get("SELECT * FROM accounts WHERE id=? AND auth_mode='platform' AND removed_at IS NULL",id);if(!row)throw new AppError(404,'平台账号不存在');return row;};
  const valid=(previous)=>{const current=account(previous.id);if(current.route_epoch!==previous.route_epoch||!current.enabled||isStopping())throw new AppError(409,'平台账号配置已变化，请重试');return current;};
  function secret(value){if(typeof value!=='string'||!value.trim()||value.length>4096||/\s/.test(value.trim()))throw new AppError(400,'请填写有效的官方Key或访问令牌');return {token:value.trim()};}
  function credential(row){const value=store.upstreamKey(row);try{const parsed=JSON.parse(value);if(!parsed||typeof parsed!=='object')throw Error();return parsed;}catch{throw new AppError(401,'平台凭据不可用，请重新接入');}}
  function saveCredential(row,value){store.saveUpstreamKey(row.id,JSON.stringify(value));}
  async function authenticated(row,signal){
    const value=credential(row);if(row.kind!=='workbuddy'||value.expiresAt>clock()+60000)return value;
    const key=row.id+':'+row.route_epoch;
    if(!refreshes.has(key)){
      const controller=new AbortController();controllers.add(controller);
      const task=transport.refresh(value,controller.signal).then(next=>{valid(row);saveCredential(row,next);return next;}).finally(()=>{controllers.delete(controller);refreshes.delete(key);});
      refreshes.set(key,task);
    }
    const next=await refreshes.get(key);if(signal?.aborted)throw new AppError(499,'请求已取消');return next;
  }
  function cancel(accountId){const pending=logins.get(accountId);if(pending){logins.delete(accountId);pending.controller.abort();}}
  async function admin(req,res,url){
    const path=url.pathname,method=req.method;
    if(path==='/api/admin/platform/providers'&&method==='GET'){json(res,200,{providers:Object.entries(platformProviders).map(([id,{label,authentication,regions}])=>({id,label,authentication,regions:regions?Object.keys(regions):[]}))});return true;}
    if(path==='/api/admin/platform/accounts'&&method==='POST'){
      const b=await body(req),configuration=platformConfiguration(b.provider,b.region),provider=platformProviders[b.provider];
      if(typeof b.name!=='string'||!b.name.trim()||b.name.trim().length>60)throw new AppError(400,'名称需为1至60字');
      const value=provider.authentication==='browser'?null:secret(b.credential),accountId=id(),home=join(runtime,'accounts',accountId);mkdirSync(home,{recursive:true});
      store.db.exec('BEGIN IMMEDIATE');try{
        store.run("INSERT INTO accounts(id,name,kind,home,auth_mode,state,platform_config) VALUES(?,?,?,?,'platform','offline',?)",accountId,b.name.trim(),b.provider,home,JSON.stringify(configuration));
        if(value)saveCredential({id:accountId},value);store.audit('account.create',accountId);store.db.exec('COMMIT');
      }catch(error){store.db.exec('ROLLBACK');throw error;}
      json(res,201,{id:accountId});return true;
    }
    const match=path.match(/^\/api\/admin\/platform\/accounts\/([a-zA-Z0-9-]+)(?:\/(check|login|login\/cancel))?$/);if(!match)return false;
    let row=account(match[1]);const action=match[2];
    if(method==='PATCH'&&!action){
      const b=await body(req);if(Object.keys(b).some(k=>!['credential','expected_epoch'].includes(k))||!Number.isSafeInteger(b.expected_epoch)||b.expected_epoch!==row.route_epoch)throw new AppError(409,'平台配置已变化，请重新打开设置');
      if(row.kind==='workbuddy')throw new AppError(400,'WorkBuddy请使用官方网页登录');
      const value=secret(b.credential);cancel(row.id);
      store.db.exec('BEGIN IMMEDIATE');try{saveCredential(row,value);store.run("UPDATE accounts SET route_epoch=route_epoch+1,state='offline',platform_models='[]',last_error=NULL WHERE id=?",row.id);store.audit('account.platform.credential',row.id);store.db.exec('COMMIT');}catch(error){store.db.exec('ROLLBACK');throw error;}
      json(res,200,{ok:true});return true;
    }
    if(method==='GET'&&action==='login'){
      const pending=logins.get(row.id);json(res,200,{state:row.state,message:row.last_error||'',...(pending?.challenge||{}),...(pending?{expiresAt:pending.expiresAt}:{})});return true;
    }
    if(method==='POST'&&action==='login/cancel'){
      if(logins.has(row.id)){cancel(row.id);store.run("UPDATE accounts SET state='offline',route_epoch=route_epoch+1,last_error='登录已取消' WHERE id=?",row.id);store.audit('account.login.cancel',row.id);}
      json(res,200,{ok:true});return true;
    }
    if(method==='POST'&&action==='check'){
      if(logins.has(row.id)||!row.enabled)throw new AppError(409,'请先启用账号并完成登录');
      const controller=new AbortController();controllers.add(controller);
      const task=(async()=>{try{
        const value=await authenticated(row,controller.signal),models=await transport.models(row,value,controller.signal);valid(row);
        store.run("UPDATE accounts SET state='ready',platform_models=?,last_error=NULL WHERE id=?",JSON.stringify(models),row.id);store.audit('account.platform.check',row.id);return models;
      }catch(error){try{valid(row);store.run("UPDATE accounts SET state='offline',last_error='平台检查未通过，请核对认证、入口与CLI版本' WHERE id=?",row.id);}catch{}throw error;}finally{controllers.delete(controller);}})();
      tasks.add(task);try{json(res,200,{ready:true,models:(await task).map(model=>row.kind+'/'+model)});}finally{tasks.delete(task);}return true;
    }
    if(method==='POST'&&action==='login'){
      if(row.kind!=='workbuddy')throw new AppError(400,'此平台请填写官方Key或访问令牌');
      if(!row.enabled||busy.has(row.id))throw new AppError(409,'请先启用账号，等待正在执行的请求完成');
      if(logins.has(row.id)){const pending=logins.get(row.id);json(res,200,{...await pending.challengePromise,expiresAt:pending.expiresAt});return true;}
      store.run("UPDATE accounts SET route_epoch=route_epoch+1,state='login',last_error=NULL,platform_models='[]' WHERE id=?",row.id);row=account(row.id);
      const controller=new AbortController();controllers.add(controller);let resolveChallenge,rejectChallenge;
      const challengePromise=new Promise((resolve,reject)=>{resolveChallenge=resolve;rejectChallenge=reject;});
      const pending={controller,challengePromise,expiresAt:clock()+600000,challenge:null};logins.set(row.id,pending);
      const timeout=setTimeout(()=>controller.abort(),600000);
      const task=(async()=>{try{
        const value=await transport.login({signal:controller.signal,onChallenge:challenge=>{pending.challenge=challenge;resolveChallenge(challenge);}});
        valid(row);if(logins.get(row.id)!==pending||controller.signal.aborted)return;
        // Authentication and model access are separate: never mark a login alone ready.
        const models=await transport.models(row,value,controller.signal);valid(row);if(logins.get(row.id)!==pending||controller.signal.aborted)return;
        saveCredential(row,value);store.run("UPDATE accounts SET state='ready',platform_models=?,last_error=NULL WHERE id=?",JSON.stringify(models),row.id);store.audit('account.login.success',row.id);
      }catch(error){rejectChallenge(error);if(logins.get(row.id)===pending){try{valid(row);store.run("UPDATE accounts SET state='offline',last_error='平台登录未完成，请重新授权或检查模型权限' WHERE id=?",row.id);store.audit('account.login.failed',row.id);}catch{}}}
      finally{clearTimeout(timeout);controllers.delete(controller);if(logins.get(row.id)===pending)logins.delete(row.id);}})();
      tasks.add(task);void task.finally(()=>tasks.delete(task));store.audit('account.login',row.id);
      const challenge=await challengePromise;if(logins.get(row.id)!==pending)throw new AppError(409,'登录流程已结束，请重新打开');json(res,200,{...challenge,expiresAt:pending.expiresAt});return true;
    }
    throw new AppError(405,'不支持的操作');
  }
  function available(){return store.all("SELECT * FROM accounts WHERE auth_mode='platform' AND enabled=1 AND removed_at IS NULL AND state='ready' ORDER BY last_used,id");}
  async function user(req,res,url){
    if(!['/v1/platform/models','/v1/platform/chat/completions'].includes(url.pathname))return false;
    const member=employee(req);
    if(url.pathname==='/v1/platform/models'&&req.method==='GET'){
      const allowed=store.allowedModels(member),models=[...new Set(available().flatMap(row=>JSON.parse(row.platform_models).map(m=>row.kind+'/'+m)))].filter(model=>allowed===null||allowed.includes(model));
      json(res,200,{object:'list',data:models.map(id=>({id,object:'model',owned_by:id.split('/')[0]}))});return true;
    }
    if(req.method!=='POST'||url.pathname!=='/v1/platform/chat/completions')throw new AppError(405,'不支持的操作');
    if(isStopping())throw new AppError(503,'服务停止');
    responseMembers.set(member.id,(responseMembers.get(member.id)||0)+1);preparing.set(member.id,(preparing.get(member.id)||0)+1);let preparingDone=false;
    const releasePreparing=()=>{if(preparingDone)return;preparingDone=true;const left=preparing.get(member.id)-1;if(left)preparing.set(member.id,left);else preparing.delete(member.id);};
    try{
      const payload=validatePlatformChat(await body(req));releasePreparing();
      const {provider,model}=platformModel(payload.model);store.checkModel(member,payload.model);checkLimits(member.id,payload.model);
      let session=payload.session_id?store.get("SELECT * FROM sessions WHERE id=? AND member_id=? AND protocol='platform'",payload.session_id,member.id):null;
      if(payload.session_id&&!session)throw new AppError(404,'平台会话不存在或不属于该员工');
      if(session&&!session.enabled)throw new AppError(403,'会话已禁止访问');
      const row=session?account(session.account_id):available().find(row=>row.kind===provider&&JSON.parse(row.platform_models).includes(model));
      if(!row||!row.enabled||row.state!=='ready')throw new AppError(503,'没有已连接且支持此模型的平台账号');
      if(row.kind!==provider||!JSON.parse(row.platform_models).includes(model))throw new AppError(409,'会话绑定的平台或模型不匹配，请新建会话');
      if(req.aborted||res.destroyed)throw new AppError(499,'客户端已断开');
      session??={id:id(),account_id:row.id,route_version:0};
      const requestId=store.observe(member,req.socket.remoteAddress||'unknown',String(req.headers['user-agent']||'').slice(0,200),payload.model);
      if(!store.get('SELECT id FROM sessions WHERE id=?',session.id))store.run("INSERT INTO sessions(id,member_id,account_id,protocol,nightly_enabled,created,updated) VALUES(?,?,?,'platform',0,?,?)",session.id,member.id,row.id,now(),now());
      store.run('UPDATE requests SET session_id=?,account_id=?,route_version=? WHERE id=?',session.id,row.id,session.route_version,requestId);
      const controller=new AbortController();controllers.add(controller);res.on('close',()=>{if(!res.writableEnded)controller.abort();});busy.set(row.id,(busy.get(row.id)||0)+1);
      const task=(async()=>{let dispatched=false;try{
        const value=await authenticated(row,controller.signal);
        const current=valid(row),currentMember=employee(req);store.checkModel(currentMember,payload.model);checkLimits(currentMember.id,payload.model,{requestId});
        const latest=store.get('SELECT * FROM sessions WHERE id=?',session.id);if(!latest.enabled||latest.route_version!==session.route_version||latest.account_id!==row.id)throw new AppError(409,'会话绑定已变化，请重试');
        if(controller.signal.aborted)throw new AppError(499,'客户端已断开');
        store.run("UPDATE requests SET status='running' WHERE id=?",requestId);store.phase(requestId,current.kind==='qoder'?'cli':'upstream');store.run('UPDATE accounts SET last_used=? WHERE id=?',now(),row.id);dispatched=true;
        const result=await transport.run(current,value,payload,controller.signal);
        store.finish(requestId,result,()=>{store.run("UPDATE sessions SET state='active',updated=? WHERE id=? AND account_id=? AND route_version=?",now(),session.id,row.id,session.route_version);});return result;
      }catch(error){store.finish(requestId,{status:dispatched?'unknown':'rejected',usageKnown:!dispatched,error:error instanceof AppError?error.message:'平台请求失败；用量需核对'});throw error;}
      finally{controllers.delete(controller);const left=busy.get(row.id)-1;if(left)busy.set(row.id,left);else busy.delete(row.id);}})();
      tasks.add(task);let result;try{result=await task;}finally{tasks.delete(task);}
      if(res.destroyed)return true;
      const common={id:'chatcmpl-'+requestId,created:Math.floor(now()/1000),model:payload.model,session_id:session.id,harbor_standard_usage:store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',requestId))},usage=result.usageKnown?{prompt_tokens:result.input,completion_tokens:result.output,total_tokens:result.input+result.output,prompt_tokens_details:{cached_tokens:result.cached}}:null;
      res.setHeader('X-Harbor-Session',session.id);
      if(payload.stream){res.writeHead(200,{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no'});res.write('data: '+JSON.stringify({...common,object:'chat.completion.chunk',choices:[{index:0,delta:{role:'assistant',content:result.text},finish_reason:null}]})+'\n\n');res.end('data: '+JSON.stringify({...common,object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:result.finishReason||'stop'}],usage})+'\n\ndata: [DONE]\n\n');}
      else json(res,200,{...common,object:'chat.completion',choices:[{index:0,message:{role:'assistant',content:result.text},finish_reason:result.finishReason||'stop'}],usage});return true;
    }finally{releasePreparing();const left=responseMembers.get(member.id)-1;if(left)responseMembers.set(member.id,left);else responseMembers.delete(member.id);}
  }
  return {admin,user,cancel,close:()=>{for(const id of logins.keys())cancel(id);},epoch:row=>row.route_epoch};
}
