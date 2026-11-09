import {randomUUID} from 'node:crypto';
import {AppError,usageWindow} from './store.mjs';
import seed from './pricing-data.mjs';
import {fetchOfficialPricing,nextPricingSettings,pricingPublication,quoteReference} from './pricing.mjs';
import {postgresPricingStatus} from './postgres-observability.mjs';

const defaults=()=>({enabled:true,refreshHours:24,version:0,manualRates:{},aliases:[]});
// No process cache owns the active book: publications and repairs share this
// transaction lock, and configuration always reloads the latest committed state.
export function createPostgresPricingRuntime(store,{clock=Date.now,fetchImpl=fetch,batchSize=100,readStore=store,queryQueue}={}){
 if(!Number.isInteger(batchSize)||batchSize<1||batchSize>100)throw new AppError(503,'费率回算批次配置无效','pricing_configuration');
 if(queryQueue!==undefined&&typeof queryQueue?.run!=='function')throw new AppError(503,'费率查询队列配置无效','pricing_configuration');
 let closed=false,pending=null,repairPending=null,cursor=null;const abort=new AbortController(),operations=new Set();
 const assertOpen=()=>{if(closed)throw new AppError(503,'服务正在停止','service_stopping');};
 function track(operation){assertOpen();const task=operation();operations.add(task);task.finally(()=>operations.delete(task)).catch(()=>{});return task;}
 const set=(name,value)=>store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));
 async function read(){
  const saved=Object.fromEntries((await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('pricing.settings','pricing.state','pricing.official')")).map(r=>[r.name,JSON.parse(r.value)]));
  const state=saved['pricing.state']||{},book=state.active&&await store.pricingBook(state.active);
  if(!book?.rates)throw new AppError(503,'计价版本尚未准备好','pricing_unavailable');
  return {settings:{...defaults(),...saved['pricing.settings']},state,official:saved['pricing.official']||seed.rates};
 }
 const locked=operation=>store.transaction(async()=>{
  await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-pricing-state',0))");assertOpen();
  await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state' FOR UPDATE");return operation(await read());
 });
 async function publish({settings,state,official}){
  const {id,json}=pricingPublication(official,settings);
  await store.run('INSERT INTO harbor.pricing_versions(id,created,book) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING',id,clock(),json);
  await set('pricing.state',{...state,active:id});return id;
 }
 // Keep seven-day aggregates off the dispatch pool and within its existing
 // bounded admin query queue. The caller retains ownership of both resources.
 const readStatus=()=>queryQueue?queryQueue.run(()=>postgresPricingStatus(readStore,clock())):postgresPricingStatus(readStore,clock());
 const status=()=>track(readStatus);
 function configure(value){return track(async()=>{
  await locked(async current=>{
   const settings=nextPricingSettings(current.settings,current.official,value,clock);
   await set('pricing.settings',settings);const revision=await publish({...current,settings});
   await store.run('INSERT INTO harbor.audit(id,at,action,detail) VALUES($1,$2,$3,$4)',randomUUID(),clock(),'pricing.configure',JSON.stringify({version:settings.version,revision}));
  });return readStatus();
 });}
 function refresh({force=false}={}){
  assertOpen();if(pending)return pending;
  pending=track(async()=>{
   const initial=await read();
   if(!force&&(!initial.settings.enabled||initial.state.checkedAt&&initial.state.checkedAt>clock()-initial.settings.refreshHours*3600000))return;
   try{
    const {prices,sourceHash}=await fetchOfficialPricing({fetchImpl,signal:abort.signal});assertOpen();
    await locked(async current=>{
     const official={...current.official,...prices},at=clock(),state={...current.state,checkedAt:at,lastSuccess:at,error:null,sourceHash};
     await set('pricing.official',official);await publish({...current,official,state});
    });
   }catch(error){
    if(closed)return;
    // A stale failed fetch cannot hide a concurrent successful publication.
    await locked(async current=>{
     if(current.state.active===initial.state.active&&current.state.checkedAt===initial.state.checkedAt)
      await set('pricing.state',{...current.state,checkedAt:clock(),error:'官方费率更新暂未完成，继续使用上次有效版本'});
    });
   }
   return readStatus();
  }).finally(()=>{pending=null;});return pending;
 }
 function backfill(){
  assertOpen();if(repairPending)return repairPending;
  repairPending=track(()=>locked(async current=>{
   const at=clock(),window=usageWindow(at),revision=current.state.active;
   // A finish may have read the previous active book before publication and
   // commit after this pass. Start another bounded pass after completion.
   const prior=cursor?.revision===revision&&cursor.start===window.start&&!cursor.complete?cursor:{revision,start:window.start,after:null,complete:false};
   const active=await store.pricingBook(revision),args=[window.start,window.end,revision,batchSize];
   let after='';if(prior.after){args.push(prior.after.at,prior.after.id);after=' AND (r.usage_at,r.id)>($5,$6)';}
   // Ended usage only. Unknown/manual rows and missing old books are never repaired.
   // Row locks precede projection writes, matching finish/projectPrice ordering.
   const rows=await store.all(`SELECT r.* FROM harbor.requests r
     JOIN harbor.request_prices p ON p.request_id=r.id
     JOIN harbor.pricing_versions b ON b.id=r.price_version
     WHERE r.usage_at>=$1 AND r.usage_at<$2 AND r.price_version<>$3
       AND r.ended IS NOT NULL AND r.manual_cost_atoms IS NULL AND r.usage_unknown=0
       AND p.state='unpriced'${after} ORDER BY r.usage_at,r.id LIMIT $4 FOR UPDATE OF r`,...args);
   const books=new Map();let changed=0;
   for(const row of rows){
    let book=books.get(row.price_version);if(!book){book=await store.pricingBook(row.price_version);books.set(row.price_version,book);}
    if(!book||quoteReference(row,book,row.price_version).state!=='unpriced'||quoteReference(row,active,revision).state!=='priced')continue;
    await store.run('UPDATE harbor.requests SET price_version=$1 WHERE id=$2 AND price_version=$3',revision,row.id,row.price_version);
    await store.projectPrice(row,at);changed++;
   }
   const last=rows.at(-1),next={revision,start:window.start,after:last?{at:last.usage_at,id:last.id}:prior.after,complete:rows.length<batchSize};
   return {scanned:rows.length,changed,complete:next.complete,next};
  })).then(result=>{if(result.next)cursor=result.next;const {next,...publicResult}=result;return publicResult;}).finally(()=>{repairPending=null;});return repairPending;
 }
 async function tick(){assertOpen();await refresh();if(!closed)return backfill();}
 async function close(){closed=true;abort.abort();await Promise.allSettled([...operations]);await Promise.allSettled([pending,repairPending].filter(Boolean));}
 return {status,configure,refresh,tick,backfill,close};
}
