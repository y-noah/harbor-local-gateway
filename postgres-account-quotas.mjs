import {AppError} from './store.mjs';
import {normalizeLimits} from './rates.mjs';
import {retainCodexQuotaDenials} from './codex-entitlements.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';

export function createPostgresAccountQuotas(store,{router,packages,queue,readQuota,clock=Date.now,parallelism=2}={}){
  if(!store||!router||['recover','block','syncWeights'].some(key=>typeof router[key]!=='function')||!queue||typeof queue.run!=='function'||typeof readQuota!=='function'||typeof clock!=='function'||!Number.isInteger(parallelism)||parallelism<1||parallelism>8)throw new AppError(503,'额度刷新依赖或资源配置无效','quota_configuration');
  const pending=new Map(),controllers=new Set(),scans=new Map();let stopping=false;
  const running=()=>{if(stopping)throw new AppError(503,'额度刷新正在关闭','service_stopping');};
  const eligible=account=>account&&!account.removed_at&&account.enabled&&account.auth_mode==='subscription'&&['ready','degraded'].includes(account.state);
  const mutate=fn=>store.transaction(async()=>{await lockPostgresRouting(store);running();return fn();});
  async function load(accountId){const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL',accountId);if(!account)throw new AppError(404,'账号不存在或已移除');return account;}
  async function refresh(account,{force=false,signal:requestSignal}={}){
    if(requestSignal!==undefined&&(!requestSignal||typeof requestSignal.throwIfAborted!=='function'||typeof requestSignal.addEventListener!=='function'))throw new AppError(503,'额度刷新取消信号配置无效','quota_configuration');
    requestSignal?.throwIfAborted();running();if(!eligible(account))return {skipped:true};
    const key=account.id+':'+account.route_epoch;if(pending.has(key))return pending.get(key);
    const operation=queue.run(async()=>{
      requestSignal?.throwIfAborted();running();const before=await mutate(async()=>{
        const current=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',account.id);running();
        requestSignal?.throwIfAborted();
        if(!eligible(current)||current.route_epoch!==account.route_epoch)return null;
        const at=clock();if(!force&&current.quota_attempted_at!==null&&current.quota_attempted_at>at-30000)return null;
        await store.run('UPDATE harbor.accounts SET quota_attempted_at=$1 WHERE id=$2',at,current.id);
        if(current.kind!=='codex'){await store.run('UPDATE harbor.accounts SET quota_error=$1 WHERE id=$2','该订阅尚无已验证的官方额度查询',current.id);requestSignal?.throwIfAborted();return null;}
        requestSignal?.throwIfAborted();
        return current;
      });if(!before)return {skipped:true};
      const controller=new AbortController();controllers.add(controller);const signal=AbortSignal.any([controller.signal,AbortSignal.timeout(15000),requestSignal].filter(Boolean));
      try{
        const data=normalizeLimits(await readQuota(before,{signal}),clock());
        signal.throwIfAborted();
        if(!data||data.plan!==null&&(typeof data.plan!=='string'||data.plan.length>128)||data.windows.some(w=>!Number.isFinite(w.minutes)||w.minutes<=0||w.reset!==null&&!Number.isSafeInteger(w.reset)))throw new AppError(502,'官方额度响应无效','quota_protocol');
        return await mutate(async()=>{
          const current=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',before.id);running();
          signal.throwIfAborted();
          if(!eligible(current)||current.route_epoch!==before.route_epoch)return {skipped:true};
          let previousQuota=null;try{previousQuota=JSON.parse(current.quota_json);}catch{}
          Object.assign(data,retainCodexQuotaDenials(previousQuota,data));
          let perPercent=current.tokens_per_percent,baseline=null;try{baseline=JSON.parse(current.quota_calibration);}catch{}
          if(data.windows.length&&Number.isFinite(data.remaining)){
            const windowId=data.windows.map(w=>w.reset).join(',');
            if(!baseline||baseline.windowId!==windowId||data.remaining>baseline.remaining)baseline={windowId,remaining:data.remaining,used:before.budget_used};
            else if(baseline.remaining-data.remaining>=1){const spend=before.budget_used-baseline.used;if(spend>0)perPercent=Math.max(100,Math.min(1e7,spend/(baseline.remaining-data.remaining)));baseline={windowId,remaining:data.remaining,used:before.budget_used};}
          }
          await store.run("UPDATE harbor.accounts SET state='ready',last_error=NULL,quota_json=$1,quota_error=NULL,since_check_tokens=$2,tokens_per_percent=$3,quota_calibration=$4 WHERE id=$5",JSON.stringify(data),Math.max(0,current.budget_used-before.budget_used),perPercent,JSON.stringify(baseline),current.id);
          await packages?.reconcile(current.id);
          await router.recover(current,data);
          if(data.limitReached||data.remaining!==null&&data.remaining<=0||Object.values(data.officialDenial).some(Boolean))await router.block(await load(current.id),'quota_exhausted',{quotaRefresh:true});
          await router.syncWeights();signal.throwIfAborted();return {refreshed:true};
        });
      }catch(error){
        if(requestSignal?.aborted)return {cancelled:true};
        running();await mutate(async()=>{
          const current=await store.get('SELECT route_epoch FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',before.id);running();
          if(current?.route_epoch===before.route_epoch)await store.run('UPDATE harbor.accounts SET quota_error=$1 WHERE id=$2','官方额度未能刷新，已保留上次结果，请稍后重试',before.id);
        });return {failed:true};
      }finally{controllers.delete(controller);}
    },{signal:requestSignal}).finally(()=>pending.delete(key));pending.set(key,operation);return operation;
  }
  function refreshAll({force=false}={}){
    running();if(scans.has(force))return scans.get(force);
    const operation=(async()=>{
      let cursor='';const counts={ok:true,refreshed:0,failed:0,skipped:0};
      for(;;){running();const rows=await store.all("SELECT * FROM harbor.accounts WHERE id>$1 AND removed_at IS NULL AND enabled=1 AND auth_mode='subscription' AND state IN ('ready','degraded') ORDER BY id LIMIT 256",cursor);if(!rows.length)break;cursor=rows.at(-1).id;
        for(let start=0;start<rows.length;start+=parallelism){running();const results=await Promise.allSettled(rows.slice(start,start+parallelism).map(row=>refresh(row,{force})));
          for(const result of results){if(result.status==='rejected')counts.failed++;else for(const field of ['refreshed','failed','skipped'])if(result.value[field])counts[field]++;}
        }
      }return counts;
    })().finally(()=>scans.delete(force));scans.set(force,operation);return operation;
  }
  const stop=()=>{stopping=true;for(const controller of controllers)controller.abort();};
  return {refresh,refreshAll,load,stop,status:()=>({pending:pending.size,scans:scans.size,stopping}),close:async()=>{stop();await Promise.allSettled([...pending.values(),...scans.values()]);}};
}
