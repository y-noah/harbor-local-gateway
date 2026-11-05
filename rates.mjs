import {spawn} from 'node:child_process';
import {StringDecoder} from 'node:string_decoder';
import {runnerEnv,codexAuthArgs} from './codex.mjs';
import {normalizeCodexEntitlements,codexQuotaDenials} from './codex-entitlements.mjs';
export function readRateLimits(binary,home,{spawnProcess=spawn,timeoutMs=15000,signal}={}){
  return new Promise((resolve,reject)=>{
    if(signal?.aborted)return reject(Error('额度查询已取消'));
    const child=spawnProcess(binary,['app-server','--listen','stdio://','--disable','plugins','--disable','apps',...codexAuthArgs()],{env:runnerEnv(home),windowsHide:true});
    let buffer='',settled=false,size=0;const decoder=new StringDecoder('utf8');
    const finish=(err,value)=>{if(settled)return;settled=true;clearTimeout(timer);signal?.removeEventListener('abort',abort);child.kill();err?reject(err):resolve(value);};
    const abort=()=>finish(Error('额度查询已取消'));
    const timer=setTimeout(()=>finish(Error('官方额度读取超时')),timeoutMs);
    const send=value=>child.stdin.write(JSON.stringify(value)+'\n');
    child.on('error',()=>finish(Error('无法启动额度查询')));child.on('close',()=>{if(!settled)finish(Error('额度查询未完成'));});
    child.stderr.on('data',()=>{});child.stdin.on('error',()=>{});
    child.stdout.on('data',bytes=>{if(settled)return;size+=bytes.length;if(size>2*1024*1024)return finish(Error('官方额度响应超过限制'));buffer+=decoder.write(bytes);let at;while(!settled&&(at=buffer.indexOf('\n'))!==-1){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);let event;try{event=JSON.parse(line);}catch{continue;}
      if(event.id===1){if(event.error)return finish(Error('额度查询初始化失败'));send({method:'initialized',params:{}});send({method:'account/rateLimits/read',id:2});}
      if(event.id===2){if(event.error)return finish(Error('官方暂未提供额度数据'));finish(null,event.result);}
    }});
    signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted){abort();return;}
    send({method:'initialize',id:1,params:{clientInfo:{name:'harbor_local_gateway',title:'Harbor Local Gateway',version:'1.0.0'}}});
  });
}
export function normalizeLimits(value,at=Date.now()){
  const by=value?.rateLimitsByLimitId;const selected=by?(by.codex||Object.values(by).find(x=>x?.limitId==='codex')):value?.rateLimits;
  const rate=selected||{};
  const windows=[rate.primary,rate.secondary].filter(w=>w&&Number.isFinite(w.usedPercent)).map(w=>({used:Math.max(0,Math.min(100,w.usedPercent)),minutes:w.windowDurationMins,reset:w.resetsAt? w.resetsAt*1000:null}));
  const entitlements=normalizeCodexEntitlements(value,rate,at);
  if(!windows.length&&!entitlements&&!rate.rateLimitReachedType&&typeof rate.spendControlReached!=='boolean')return null;
  const limitReachedType=['rate_limit_reached','workspace_owner_credits_depleted','workspace_member_credits_depleted','workspace_owner_usage_limit_reached','workspace_member_usage_limit_reached'].includes(rate.rateLimitReachedType)?rate.rateLimitReachedType:null;
  return {windows,remaining:windows.length?Math.min(...windows.map(w=>100-w.used)):null,checked:at,plan:rate.planType||null,limitReached:!!rate.rateLimitReachedType,limitReachedType,ordinaryUsageAllowed:typeof value?.ordinaryUsageAllowed==='boolean'?value.ordinaryUsageAllowed:null,spendControlReached:typeof rate.spendControlReached==='boolean'?rate.spendControlReached:null,entitlements};
}
export function accountCapacity(account,predictedTokens,pendingTokens=0,at=Date.now()){
  let snapshot=null;try{snapshot=JSON.parse(account.quota_json);}catch{}
  if(Object.values(codexQuotaDenials(snapshot)).some(Boolean))return {eligible:false,score:-1,source:'official_denied',remaining:null};
  const localRemaining=account.token_budget===null?null:Math.max(0,account.token_budget-account.budget_used-pendingTokens-predictedTokens);
  if(account.token_budget!==null&&account.budget_used+pendingTokens+predictedTokens>account.token_budget)return {eligible:false,score:-1,source:'local',remaining:0};
  if(snapshot&&snapshot.checked>at-300000&&Array.isArray(snapshot.windows)&&snapshot.windows.length&&Number.isFinite(snapshot.remaining)){
    // A passed reset requires a new official snapshot; do not invent a replenishment.
    if(snapshot.windows.some(w=>w.reset&&w.reset<=at))return {eligible:false,score:-1,source:'expired',remaining:null};
    const remaining=Math.max(0,snapshot.remaining-(pendingTokens+predictedTokens+(account.since_check_tokens||0))/(account.tokens_per_percent||10000));
    const tokens=remaining*(account.tokens_per_percent||10000);
    return {eligible:remaining>0&&!snapshot.limitReached,score:localRemaining===null?tokens:Math.min(tokens,localRemaining),source:'official',remaining:snapshot.remaining};
  }
  if(localRemaining!==null)return {eligible:localRemaining>=0,score:localRemaining,source:'local',remaining:100*Math.max(0,account.token_budget-account.budget_used)/Math.max(1,account.token_budget)};
  return {eligible:false,score:-1,source:'unknown',remaining:null};
}
