import http from 'node:http';
import {homedir} from 'node:os';
import {acquireInstance} from './instance.mjs';
import {passwordHash,createAdminAuth} from './auth.mjs';
import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { Store, AppError, id, now, hash } from './store.mjs';
import { runCodex, checkLogin, runnerEnv } from './codex.mjs';
import {readRateLimits,normalizeLimits,accountCapacity} from './rates.mjs';
import {runClaude,checkClaudeLogin,claudeEnv} from './claude.mjs';

const root=dirname(fileURLToPath(import.meta.url));
function integer(value,label,{nullable=false,min=0,max=1e12}={}) {
  if(nullable && (value===null || value===''))return null;
  if(!Number.isSafeInteger(value)||value<min||value>max)throw new AppError(400,label+' 必须是有效整数');return value;
}
function name(value){if(typeof value!=='string'||!value.trim()||value.trim().length>60)throw new AppError(400,'名称需为 1–60 字');return value.trim();}
function expiry(value){return integer(value,'到期时间',{nullable:true,max:9e15});}
function bool(value){if(typeof value!=='boolean')throw new AppError(400,'状态无效');return value?1:0;}
function equals(a,b){const x=Buffer.from(a||''),y=Buffer.from(b||'');return x.length===y.length&&timingSafeEqual(x,y);}
const json=(res,status,body)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(JSON.stringify(body));};
async function body(req){
  if(!String(req.headers['content-type']||'').startsWith('application/json'))throw new AppError(415,'请使用 application/json');
  let size=0,chunks=[];for await(const chunk of req){size+=chunk.length;if(size>128*1024)throw new AppError(413,'请求超过 128 KB');chunks.push(chunk);}
  try{const value=JSON.parse(Buffer.concat(chunks).toString());if(!value||typeof value!=='object'||Array.isArray(value))throw Error();return value;}catch{throw new AppError(400,'请求必须是 JSON 对象');}
}
function discoverCodex(){
  const folder=join(process.env.LOCALAPPDATA||join(homedir(),'AppData','Local'),'OpenAI','Codex','bin');
  if(existsSync(folder))for(const sub of readdirSync(folder).reverse()){const p=join(folder,sub,'codex.exe');if(existsSync(p))return p;}
  return 'codex';
}
export function createGateway(options={}) {
  const data=resolve(options.dataDir||join(root,'data')),runtime=resolve(options.runtimeDir||join(root,'runtime'));
  mkdirSync(data,{recursive:true});mkdirSync(runtime,{recursive:true});
  const configPath=join(data,'settings.json');
  let config=options.config;
  if(!config){
    if(existsSync(configPath))config=JSON.parse(readFileSync(configPath,'utf8'));
    else {config={port:43127,adminToken:randomBytes(32).toString('base64url'),codexBinary:discoverCodex(),reserveTokens:32000};writeFileSync(configPath,JSON.stringify(config,null,2));}
  }
  if(!config.adminPasswordHash&&!options.config){
    const password='Hbr!'+randomBytes(24).toString('base64url');
    config.adminPasswordHash=passwordHash(password);
    writeFileSync(join(root,'管理密码.txt'),'Harbor 管理密码（请勿分享）\r\n\r\n'+password+'\r\n\r\n打开 http://127.0.0.1:'+config.port+'/ 输入密码，无需账号。\r\n可让浏览器密码管理器保存。此文件是本机找回副本，请勿公开。\r\n');
    writeFileSync(configPath,JSON.stringify(config,null,2));
  }
  const adminAuth=createAdminAuth(config);
  const store=new Store(join(data,'gateway.sqlite'));
  const executor=options.executor||runCodex, checker=options.checker||checkLogin,rateReader=options.rateReader||readRateLimits;
  const claudeBinary=config.claudeBinary||join(root,'runtime','claude-cli','node_modules','@anthropic-ai','claude-code','bin','claude.exe');
  const provider=a=>a.kind==='claude'?{binary:claudeBinary,run:options.claudeExecutor||runClaude,check:options.claudeChecker||checkClaudeLogin,env:claudeEnv,loginArgs:['auth','login']}:{binary:config.codexBinary,run:executor,check:checker,env:runnerEnv,loginArgs:['login']};
  const busy=new Set(),queue=[],controllers=new Set(),logins=new Map(),tasks=new Set(),admittedMembers=new Set();let stopping=false;
  const quotaReads=new Map(),authVersions=new Map();
  async function refreshQuota(account,force=false){
    if(account.kind==='claude'){store.run('UPDATE accounts SET quota_error=? WHERE id=?','Claude 官方 CLI 暂无已验证的额度查询；使用本地 Token 预算估算',account.id);return;}
    if(quotaReads.has(account.id))return quotaReads.get(account.id);
    let previous=null;try{previous=JSON.parse(account.quota_json);}catch{}
    if(!force&&previous&&previous.checked>now()-30000&&!previous.windows.some(w=>w.reset&&w.reset<=now()))return;
    const beforeUsed=account.budget_used||0,authVersion=authVersions.get(account.id)||0;
    const pending=(async()=>{
      try{
        const data=normalizeLimits(await rateReader(config.codexBinary,account.home));if(!data)throw Error('官方额度暂不可用，请设置本地预算后再测试');
        if(stopping||(authVersions.get(account.id)||0)!==authVersion)return;
        const current=store.get('SELECT * FROM accounts WHERE id=?',account.id);let perPercent=current.tokens_per_percent;
        const windowId=data.windows.map(w=>w.reset).join(',');
        let baseline=null;try{baseline=JSON.parse(current.quota_calibration);}catch{}
        if(!baseline||baseline.windowId!==windowId||data.remaining>baseline.remaining){baseline={windowId,remaining:data.remaining,used:beforeUsed};}
        else if(baseline.remaining-data.remaining>=1){
          const spend=beforeUsed-baseline.used;
          if(spend>0)perPercent=Math.max(100,Math.min(1e7,spend/(baseline.remaining-data.remaining)));
          baseline={windowId,remaining:data.remaining,used:beforeUsed};
        }
        store.run('UPDATE accounts SET quota_json=?,quota_error=NULL,since_check_tokens=?,tokens_per_percent=?,quota_calibration=? WHERE id=?',JSON.stringify(data),Math.max(0,current.budget_used-beforeUsed),perPercent,JSON.stringify(baseline),account.id);
      }catch(e){if(!stopping&&(authVersions.get(account.id)||0)===authVersion)store.run('UPDATE accounts SET quota_error=? WHERE id=?',e.message,account.id);}
    })().finally(()=>quotaReads.delete(account.id));quotaReads.set(account.id,pending);return pending;
  }
  function expectedTokens(messages){const avg=store.get("SELECT AVG(input+output) n FROM (SELECT input,output FROM requests WHERE status='ok' ORDER BY started DESC LIMIT 10)").n||10000;return Math.ceil(Math.max(10000,avg,JSON.stringify(messages).length/2+9000));}
  function capacity(account,predicted,exclude=''){
    const pending=store.get("SELECT COALESCE(SUM(predicted),0) n FROM requests WHERE account_id=? AND id<>? AND status IN ('queued','running','unknown','interrupted')",account.id,exclude).n;
    return accountCapacity(account,predicted,pending);
  }
  function chooseAccount(predicted,kind='codex'){
    const ranked=store.all("SELECT * FROM accounts WHERE kind=? AND enabled=1 AND state IN ('ready','degraded')",kind).map(a=>({a,c:capacity(a,predicted)})).filter(x=>x.c.eligible).sort((x,y)=>y.c.score-x.c.score||x.a.last_used-y.a.last_used);
    if(!ranked.length)throw new AppError(503,'没有预计额度足够的账号；请刷新官方额度或设置本地预算','no_account_capacity');return ranked[0].a;
  }
  const digestMessages=(seed,messages)=>messages.reduce((value,m)=>hash(value+'\n'+JSON.stringify({role:m.role,content:m.content})),seed);
  async function sessionFor(member,sessionId,messages,predicted,kind){
    if(sessionId){
      if(typeof sessionId!=='string'||sessionId.length>80)throw new AppError(400,'session_id 格式错误');
      let session=store.get('SELECT * FROM sessions WHERE id=? AND member_id=?',sessionId,member.id);if(!session)throw new AppError(404,'会话不存在或不属于该员工');
      if(session.state==='uncertain')throw new AppError(409,'此会话的上次调用状态不确定，请创建新会话','session_uncertain');
      const account=store.get('SELECT * FROM accounts WHERE id=?',session.account_id);
      if(account.kind!==kind)throw new AppError(409,'会话已绑定另一种模型，请创建新会话','session_provider_mismatch');
      if(!account.enabled||!['ready','degraded'].includes(account.state))throw new AppError(503,'会话绑定账号不可用；会话不会自动换号','session_account_unavailable');
      await refreshQuota(account);
      session=store.get('SELECT * FROM sessions WHERE id=? AND member_id=?',sessionId,member.id);
      if(session.state==='uncertain')throw new AppError(409,'此会话的上次调用状态不确定，请创建新会话','session_uncertain');
      if(!capacity(store.get('SELECT * FROM accounts WHERE id=?',account.id),predicted).eligible)throw new AppError(429,'会话绑定账号预计额度不足；请等待恢复或创建新会话','session_quota_exhausted');
      let delta=messages;
      if(session.message_count&&messages.length>1){
        if(messages.length<=session.message_count||digestMessages('',messages.slice(0,session.message_count))!==session.history_digest)throw new AppError(409,'完整历史与会话不匹配；请只发送本轮新消息，或创建新会话');
        delta=messages.slice(session.message_count);
      }
      return {session,delta};
    }
    const accounts=store.all("SELECT * FROM accounts WHERE kind=? AND enabled=1 AND state IN ('ready','degraded')",kind);
    await Promise.all(accounts.map(a=>refreshQuota(a)));
    const session={id:id(),member_id:member.id,account_id:chooseAccount(predicted,kind).id,upstream_id:null,state:'new',message_count:0,history_digest:'',created:now(),updated:now()};
    return {session,delta:messages,isNew:true};
  }
  const authAdmin=req=>{const bearer=typeof config.adminToken==='string'&&config.adminToken.length>0&&equals(req.headers.authorization,'Bearer '+config.adminToken);if(!adminAuth.authenticated(req)&&!bearer)throw new AppError(401,'请先输入管理密码登录','admin_required');};
  const employee=req=>store.authenticate((req.headers.authorization||'').replace(/^Bearer /,''));
  function pump(){
    if(stopping)return;
    while(queue.length){
      const index=queue.findIndex(job=>!busy.has(job.session.account_id));if(index===-1)return;
      const [job]=queue.splice(index,1),account=store.get('SELECT * FROM accounts WHERE id=?',job.session.account_id);clearTimeout(job.queueTimer);busy.add(account.id);
      const task=execute(job,account);tasks.add(task);
      void task.finally(()=>{tasks.delete(task);busy.delete(account.id);pump();}).catch(e=>console.error('[worker]',e.message));
    }
  }
  async function execute(job,account){
    const {requestId,messages,model,resolve:complete,reject,controller,secret,session,predicted}=job;
    if(controller.signal.aborted){store.finish(requestId,{status:'cancelled',error:'排队期间取消'});reject(new AppError(499,'请求已取消'));return;}
    try{
      const currentMember=store.authenticate(secret);
      const held=store.get("SELECT COALESCE(SUM(reserved),0) n FROM requests WHERE member_id=? AND id<>?",currentMember.id,requestId).n;
      const heldKey=store.get("SELECT COALESCE(SUM(reserved),0) n FROM requests WHERE key_id=? AND id<>?",currentMember.key_id,requestId).n;
      if(currentMember.quota!==null&&currentMember.used+held>=currentMember.quota)throw new AppError(429,'排队期间员工额度已耗尽','quota_exceeded');
      if(currentMember.key_quota!==null&&currentMember.key_used+heldKey>=currentMember.key_quota)throw new AppError(429,'排队期间 Key 额度已耗尽','key_quota_exceeded');
    }catch(e){store.finish(requestId,{status:'rejected',error:e.message});reject(e);return;}
    const current=store.get('SELECT * FROM accounts WHERE id=?',account.id);
    if(!current.enabled||!['ready','degraded'].includes(current.state)||!capacity(current,predicted,requestId).eligible){store.finish(requestId,{status:'rejected',error:'绑定账号不可用或预计额度不足'});reject(new AppError(429,'绑定账号不可用或预计额度不足；会话不会换号'));return;}
    store.run("UPDATE requests SET status='running',account_id=? WHERE id=?",account.id,requestId);
    store.run('UPDATE accounts SET last_used=? WHERE id=?',now(),account.id);
    try{
      const upstream=provider(account);
      const result=await upstream.run({binary:upstream.binary,home:account.home,workspace:join(runtime,'jobs',session.id),messages,model,sessionId:session.upstream_id,previousUsage:{input:session.cum_input||0,output:session.cum_output||0,cached:session.cum_cached||0},signal:controller.signal});
      store.finish(requestId,result,()=>{
        store.run("UPDATE accounts SET state='ready',last_error=NULL,budget_used=budget_used+?,since_check_tokens=since_check_tokens+? WHERE id=?",result.input+result.output,result.input+result.output,account.id);
        store.run("UPDATE sessions SET upstream_id=?,state='active',message_count=?,history_digest=?,updated=? WHERE id=?",result.threadId||session.upstream_id,session.message_count+messages.length+1,digestMessages(session.history_digest,[...messages,{role:'assistant',content:result.text}]),now(),session.id);
        const cumulative=result.cumulativeUsage||{input:(session.cum_input||0)+result.input,output:(session.cum_output||0)+result.output,cached:(session.cum_cached||0)+result.cached};
        store.run('UPDATE sessions SET cum_input=?,cum_output=?,cum_cached=? WHERE id=?',cumulative.input,cumulative.output,cumulative.cached,session.id);
      });
      complete({...result,sessionId:session.id});
      void refreshQuota(store.get('SELECT * FROM accounts WHERE id=?',account.id),true);
    }catch(e){
      const known=e.usageKnown===true;
      store.finish(requestId,{input:e.input||0,output:e.output||0,cached:e.cached||0,status:known?'failed':'unknown',error:e.message,usageKnown:known},()=>{
        store.run("UPDATE accounts SET state='degraded',last_error=? WHERE id=?",e.message,account.id);
        store.run('UPDATE accounts SET budget_used=budget_used+?,since_check_tokens=since_check_tokens+? WHERE id=?',(e.input||0)+(e.output||0),(e.input||0)+(e.output||0),account.id);
        store.run("UPDATE sessions SET state='uncertain',upstream_id=COALESCE(?,upstream_id),updated=? WHERE id=?",e.threadId||null,now(),session.id);
      });
      reject(new AppError(502,e.message,'upstream_error'));
    }
  }
  async function handler(req,res){
    res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');
    res.setHeader('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
    try{
      const host=req.headers.host||'';
      if(!/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(host))throw new AppError(403,'本机服务不接受此 Host');
      if(req.headers.origin && !['http://'+host].includes(req.headers.origin))throw new AppError(403,'不允许跨站请求');
      const url=new URL(req.url,'http://localhost'),path=url.pathname,method=req.method;
      if(method==='GET'&&['/','/app.js','/style.css'].includes(path)){
        const file=path==='/'?'index.html':path.slice(1);res.setHeader('Content-Type',file.endsWith('html')?'text/html; charset=utf-8':file.endsWith('js')?'text/javascript; charset=utf-8':'text/css; charset=utf-8');return res.end(readFileSync(join(root,'public',file)));
      }
      if(path==='/health'&&method==='GET')return json(res,200,{ok:true,service:'harbor',version:'1.0.0'});
      if(path==='/api/auth/login'&&method==='POST'){const b=await body(req);adminAuth.login(b.password,res);return json(res,200,{ok:true});}
      if(path==='/api/auth/logout'&&method==='POST'){adminAuth.logout(req,res);return json(res,200,{ok:true});}
      if(path==='/api/auth/session'&&method==='GET')return json(res,200,{authenticated:adminAuth.authenticated(req)});
      if(path.startsWith('/api/admin/')){
        authAdmin(req);
        if(path==='/api/admin/shutdown'&&method==='POST'){
          json(res,202,{ok:true});setTimeout(()=>void close().then(()=>options.onStopped?.()),50);return;
        }
        if(path==='/api/admin/state'&&method==='GET')return json(res,200,{...store.snapshot(),queue:queue.length,busy:busy.size,endpoint:`http://${host}/v1`,mode:'本机 · 订阅测试',reserveTokens:config.reserveTokens});
        if(path==='/api/admin/members'&&method==='POST'){
          const b=await body(req);return json(res,201,store.createMember(name(b.name),integer(b.quota,'额度',{nullable:true}),expiry(b.expires)));
        }
        const memberMatch=path.match(/^\/api\/admin\/members\/([a-f0-9-]+)$/);
        if(memberMatch&&method==='PATCH'){
          const b=await body(req),m=store.get('SELECT * FROM members WHERE id=?',memberMatch[1]);if(!m)throw new AppError(404,'员工不存在');
          store.run('UPDATE members SET name=?,quota=?,enabled=? WHERE id=?',b.name===undefined?m.name:name(b.name),b.quota===undefined?m.quota:integer(b.quota,'额度',{nullable:true}),b.enabled===undefined?m.enabled:bool(b.enabled),m.id);store.audit('member.update',m.id);return json(res,200,{ok:true});
        }
        if(path==='/api/admin/keys'&&method==='POST'){const b=await body(req);return json(res,201,store.createKey(b.memberId,expiry(b.expires),b.quota===undefined?null:integer(b.quota,'Key 额度',{nullable:true})));}
        const keyMatch=path.match(/^\/api\/admin\/keys\/([a-f0-9-]+)$/);
        if(keyMatch&&method==='PATCH'){
          const b=await body(req),k=store.get('SELECT * FROM keys WHERE id=?',keyMatch[1]);if(!k)throw new AppError(404,'Key 不存在');
          store.run('UPDATE keys SET enabled=?,expires=?,quota=? WHERE id=?',b.enabled===undefined?k.enabled:bool(b.enabled),b.expires===undefined?k.expires:expiry(b.expires),b.quota===undefined?k.quota:integer(b.quota,'Key 额度',{nullable:true}),k.id);store.audit('key.update',k.id);return json(res,200,{ok:true});
        }
        if(path==='/api/admin/accounts'&&method==='POST'){
          const b=await body(req),accountId=id(),home=join(runtime,'accounts',accountId);mkdirSync(home,{recursive:true});
          const kind=b.kind||'codex';if(!['codex','claude'].includes(kind))throw new AppError(400,'不支持的账号类型');
          store.run("INSERT INTO accounts(id,name,home,kind,state) VALUES(?,?,?,?,'offline')",accountId,name(b.name),home,kind);store.audit('account.create',accountId);return json(res,201,{id:accountId});
        }
        if(path==='/api/admin/quotas/refresh'&&method==='POST'){await Promise.all(store.all('SELECT * FROM accounts WHERE enabled=1').map(a=>refreshQuota(a,true)));return json(res,200,{ok:true});}
        const accountMatch=path.match(/^\/api\/admin\/accounts\/([a-zA-Z0-9-]+)(?:\/(check|login|quota))?$/);
        if(accountMatch){
          const account=store.get('SELECT * FROM accounts WHERE id=?',accountMatch[1]);if(!account)throw new AppError(404,'账号不存在');
          if(method==='PATCH'&&!accountMatch[2]){const b=await body(req);store.run('UPDATE accounts SET enabled=?,token_budget=?,tokens_per_percent=? WHERE id=?',b.enabled===undefined?account.enabled:bool(b.enabled),b.token_budget===undefined?account.token_budget:integer(b.token_budget,'本地预算',{nullable:true}),b.tokens_per_percent===undefined?account.tokens_per_percent:integer(b.tokens_per_percent,'容量换算',{min:100,max:1e7}),account.id);store.audit('account.update',account.id);pump();return json(res,200,{ok:true});}
          if(method==='POST'&&accountMatch[2]==='quota'){await refreshQuota(account,true);return json(res,200,{ok:true});}
          if(method==='POST'&&accountMatch[2]==='check'){
            if(logins.has(account.id))throw new AppError(409,'账号正在登录，请完成登录后再检查');
            const version=authVersions.get(account.id)||0,upstream=provider(account),ready=await upstream.check(upstream.binary,account.home);
            if(logins.has(account.id)||(authVersions.get(account.id)||0)!==version)throw new AppError(409,'账号登录状态已改变，请完成登录后再检查');
            store.run('UPDATE accounts SET state=?,last_error=? WHERE id=?',ready?'ready':'offline',ready?null:'未检测到订阅登录',account.id);pump();return json(res,200,{ready});
          }
          if(method==='POST'&&accountMatch[2]==='login'){
            if(busy.has(account.id))throw new AppError(409,'账号正在使用，请稍后登录');
            if(logins.has(account.id))return json(res,200,{ok:true,message:'登录流程已启动，请查看浏览器'});
            if(logins.size)throw new AppError(409,'另一个账号正在登录，请先完成该流程');
            const version=(authVersions.get(account.id)||0)+1;authVersions.set(account.id,version);
            store.run("UPDATE accounts SET state='login',quota_json=NULL,quota_calibration=NULL,quota_error=NULL,since_check_tokens=0 WHERE id=?",account.id);
            store.run("UPDATE sessions SET state='uncertain' WHERE account_id=?",account.id);
            const upstream=provider(account),child=spawn(upstream.binary,upstream.loginArgs,{env:upstream.env(account.home),windowsHide:true,stdio:'ignore'});logins.set(account.id,child);
            const timer=setTimeout(()=>child.kill(),180000);
            let ended=false;const finish=async()=>{if(ended)return;ended=true;clearTimeout(timer);try{if(stopping)return;let ready=false;try{ready=await upstream.check(upstream.binary,account.home);}catch{}if(stopping||(authVersions.get(account.id)||0)!==version)return;store.run('UPDATE accounts SET state=? WHERE id=?',ready?'ready':'offline',account.id);}finally{logins.delete(account.id);if(!stopping)pump();}};
            child.on('error',finish);child.on('close',finish);store.audit('account.login',account.id);return json(res,200,{ok:true,message:'已启动官方登录，请在本机浏览器完成'});
          }
        }
        const settleMatch=path.match(/^\/api\/admin\/requests\/([a-f0-9-]+)\/settle$/);
        if(settleMatch&&method==='POST'){
          const b=await body(req),r=store.get('SELECT * FROM requests WHERE id=?',settleMatch[1]);if(!r||!['unknown','interrupted'].includes(r.status))throw new AppError(409,'该请求无需核对');
          const actual=integer(b.tokens,'补记 Token');store.db.exec('BEGIN IMMEDIATE');try{store.run("UPDATE requests SET status='reconciled',reserved=0,input=input+?,error='管理员已核对并补记用量' WHERE id=?",actual,r.id);store.run('UPDATE members SET used=used+? WHERE id=?',actual,r.member_id);store.run('UPDATE keys SET used=used+? WHERE id=?',actual,r.key_id);if(r.account_id)store.run('UPDATE accounts SET budget_used=budget_used+?,since_check_tokens=since_check_tokens+? WHERE id=?',actual,actual,r.account_id);store.audit('request.settle',r.id+': '+actual);store.db.exec('COMMIT');}catch(e){store.db.exec('ROLLBACK');throw e;}
          return json(res,200,{ok:true});
        }
        if(path==='/api/admin/ips'&&method==='GET'){
          const member=url.searchParams.get('member')||'',key=url.searchParams.get('key');
          if(key&&!store.get('SELECT id FROM keys WHERE id=? AND member_id=?',key,member))throw new AppError(404,'Key 不属于该员工');
          return json(res,200,store.all('SELECT ip,COUNT(*) requests,MIN(started) first_seen,MAX(started) last_seen,MAX(agent) agent FROM requests WHERE member_id=?'+(key?' AND key_id=?':'')+' GROUP BY ip ORDER BY last_seen DESC',...[member,...(key?[key]:[])]));
        }
        if(path==='/api/admin/export'&&method==='GET'){
          const rows=store.all('SELECT r.started,m.name,r.ip,r.status,r.input,r.output,r.cached,r.model FROM requests r LEFT JOIN members m ON m.id=r.member_id ORDER BY started DESC LIMIT 10000');
          const cell=v=>{const text=String(v??'');return '"'+(/^[\s\x00-\x1f\x7f]*[=+@-]/.test(text)?"'":"")+text.replaceAll('"','""')+'"';};
          res.setHeader('Content-Type','text/csv; charset=utf-8');res.setHeader('Content-Disposition','attachment; filename="harbor-usage.csv"');return res.end('\ufeff'+['时间,员工,IP,状态,输入Token,输出Token,缓存Token,模型',...rows.map(r=>[new Date(r.started).toISOString(),r.name,r.ip,r.status,r.input,r.output,r.cached,r.model].map(cell).join(','))].join('\r\n'));
        }
        if(path==='/api/admin/backup'&&method==='POST'){
          const backupDir=join(data,'backups');mkdirSync(backupDir,{recursive:true});const filename='gateway-'+now()+'.sqlite';store.db.prepare('VACUUM INTO ?').run(join(backupDir,filename));store.audit('backup.create',filename);return json(res,200,{file:'data/backups/'+filename});
        }
        throw new AppError(404,'接口不存在');
      }
      if(path==='/v1/models'&&method==='GET'){employee(req);return json(res,200,{object:'list',data:['codex','claude'].map(id=>({id,object:'model',owned_by:'local'}))});}
      if(path==='/api/me'&&method==='GET'){
        const m=employee(req);const reserved=store.get('SELECT COALESCE(SUM(reserved),0) n FROM requests WHERE member_id=?',m.id).n;return json(res,200,{id:m.id,name:m.name,quota:m.quota,used:m.used,keyQuota:m.key_quota,keyUsed:m.key_used,reserved,expires:m.expires});
      }
      if(path==='/v1/chat/completions'&&method==='POST'){
        const m=employee(req);
        if(admittedMembers.has(m.id))throw new AppError(429,'该员工已有请求在处理，请稍后再试','member_busy');
        if(stopping||admittedMembers.size>=16)throw new AppError(429,'请求准备队列已满，请稍后重试');
        store.allowance(m,config.reserveTokens);
        admittedMembers.add(m.id);
        try{
        const b=await body(req);
        if(!Array.isArray(b.messages)||!b.messages.length||b.messages.length>100||b.messages.some(v=>!v||!['system','user','assistant','developer'].includes(v.role)||typeof v.content!=='string'))throw new AppError(400,'仅支持 1–100 条纯文本 messages');
        if(b.tools||b.functions)throw new AppError(400,'此版本是纯文本网关，不支持工具调用');
        if(b.model!==undefined&&!['codex','claude'].includes(b.model))throw new AppError(400,'本地模型名为 codex 或 claude');
        const kind=b.model||'codex';
        if(b.max_tokens!==undefined||b.max_completion_tokens!==undefined)throw new AppError(400,'订阅 CLI 无硬输出 Token 上限，暂不支持 max_tokens 参数');
        if(b.stream!==undefined&&typeof b.stream!=='boolean')throw new AppError(400,'stream 必须为布尔值');
        if(stopping||queue.length>=5)throw new AppError(429,'队列已满，请稍后重试');
        if(!store.get("SELECT id FROM accounts WHERE kind=? AND enabled=1 AND state IN ('ready','degraded')",kind))throw new AppError(503,'没有可用的 '+kind+' 订阅账号，请检查账号池');
        const secret=(req.headers.authorization||'').replace(/^Bearer /,'');
        const messages=b.messages.map(({role,content})=>({role,content})),predicted=expectedTokens(messages);
        const {session,delta,isNew}=await sessionFor(m,b.session_id||req.headers['x-harbor-session'],messages,predicted,kind);
        // Recheck queue and credentials after asynchronous quota reads.
        if(req.aborted||res.destroyed)throw new AppError(499,'客户端已断开');
        if(stopping||queue.length>=5)throw new AppError(429,'队列已满，请稍后重试');store.authenticate(secret);
        if(isNew)session.account_id=chooseAccount(predicted,kind).id;
        const requestId=store.reserve(m,req.socket.remoteAddress||'unknown',String(req.headers['user-agent']||'').slice(0,200),config.reserveTokens,kind);
        if(isNew)store.run('INSERT INTO sessions(id,member_id,account_id,created,updated) VALUES(?,?,?,?,?)',session.id,m.id,session.account_id,session.created,session.updated);
        store.run('UPDATE requests SET session_id=?,account_id=?,predicted=? WHERE id=?',session.id,session.account_id,predicted,requestId);
        const controller=new AbortController();controllers.add(controller);res.on('close',()=>{if(!res.writableEnded)controller.abort();});
        try{
          const result=await new Promise((resolve,reject)=>{
            const job={requestId,messages:delta,model:kind,resolve,reject,controller,secret,session,predicted};
            function cancelQueued(message){const at=queue.indexOf(job);if(at!==-1){queue.splice(at,1);clearTimeout(job.queueTimer);store.finish(requestId,{status:'cancelled',error:message});reject(new AppError(503,message));}}
            job.queueTimer=setTimeout(()=>cancelQueued('排队超过 2 分钟，请稍后重试'),120000);
            controller.signal.addEventListener('abort',()=>cancelQueued('排队期间取消'),{once:true});
            queue.push(job);pump();
          });
          if(res.destroyed)return;
          const usage={prompt_tokens:result.input,completion_tokens:result.output,total_tokens:result.input+result.output,prompt_tokens_details:{cached_tokens:result.cached}};
          const common={id:'chatcmpl-'+requestId,created:Math.floor(now()/1000),model:kind,session_id:session.id};res.setHeader('X-Harbor-Session',session.id);
          if(b.stream){
            res.writeHead(200,{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no'});
            res.write('data: '+JSON.stringify({...common,object:'chat.completion.chunk',choices:[{index:0,delta:{role:'assistant',content:result.text},finish_reason:null}]})+'\n\n');
            res.write('data: '+JSON.stringify({...common,object:'chat.completion.chunk',choices:[{index:0,delta:{},finish_reason:'stop'}],usage})+'\n\n');res.end('data: [DONE]\n\n');
          }else json(res,200,{...common,object:'chat.completion',choices:[{index:0,message:{role:'assistant',content:result.text},finish_reason:'stop'}],usage});
        }finally{controllers.delete(controller);}
        }finally{admittedMembers.delete(m.id);}
        return;
      }
      throw new AppError(404,'接口不存在');
    }catch(e){
      if(!res.headersSent&&!res.destroyed)json(res,e.status||500,{error:{message:e.status?e.message:'服务内部错误，请查看本机日志',type:e.code||'server_error'}});
      if(!e.status)console.error('[server]',e.message);
    }
  }
  const server=http.createServer(handler);server.requestTimeout=15000;server.headersTimeout=10000;
  let closePromise;
  function close(){if(closePromise)return closePromise;closePromise=(async()=>{stopping=true;for(const c of controllers)c.abort();for(const c of logins.values())c.kill();while(queue.length){const job=queue.shift();clearTimeout(job.queueTimer);store.finish(job.requestId,{status:'cancelled',error:'服务停止'});job.reject(new AppError(503,'服务停止'));}await new Promise(resolve=>server.close(resolve));await Promise.allSettled([...tasks]);store.close();})();return closePromise;}
  return {server,store,config,close,data,runtime};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const instance=await acquireInstance(join(root,'data'));
  const app=createGateway({onStopped:()=>process.exit(0)});app.server.listen(app.config.port,'127.0.0.1',()=>console.log(`Harbor ready at http://127.0.0.1:${app.config.port}`));
  for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>app.close().then(()=>process.exit(0)));
}
