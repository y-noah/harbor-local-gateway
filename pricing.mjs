import {createHash} from 'node:crypto';
import seed from './pricing-data.mjs';

export const referenceMetric='openai-standard-short-usd-v1';
const scale=1000000000000n;
const modelName=v=>typeof v==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(v);
const fail=message=>{throw Object.assign(new Error(message),{status:400,code:'invalid_pricing'});};
const parse=value=>{try{return JSON.parse(value);}catch{return null;}};
export function priceMicros(value){
  if(typeof value!=='string'||!/^\d{1,12}(?:\.\d{1,6})?$/.test(value))fail('费率须为最多六位小数的非负十进制数');
  const [whole,fraction='']=value.split('.');return BigInt(whole)*1000000n+BigInt(fraction.padEnd(6,'0'));
}
function validateRate(value){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['input','cached','output','source'].includes(k)))fail('费率字段无效');
  priceMicros(value.input);priceMicros(value.output);if(value.cached!==null)priceMicros(value.cached);
  return {input:value.input,cached:value.cached,output:value.output,...(value.source?{source:String(value.source).slice(0,500)}:{})};
}
export function parseOfficialPrices(text){
  const begin=text.indexOf('### Standard pricing data'),end=text.indexOf('### Batch pricing data',begin);
  if(!text.startsWith('# Pricing')||begin<0||end<begin)fail('官方定价表格式已变化，保留上次有效费率');
  const primaryHeaders=text.slice(begin,end).split('\n').filter(line=>/^\|\s*Model\s*\|/.test(line)).map(line=>line.replace(/^\||\|$/g,'').split('|').map(x=>x.trim()));
  if(!primaryHeaders.length||primaryHeaders.some(h=>(h.includes('Short context input')?['Short context input','Short context cached input','Short context output']:['Input','Cached input','Output']).some(f=>!h.includes(f))))fail('官方主要费率字段已变化，保留上次有效费率');
  const result={};let header,mode;
  const money=s=>s==='-'?null:/^\$\d+(?:\.\d{1,6})?$/.test(s)?s.slice(1):fail('官方价格字段无法识别');
  for(const original of text.split('\n')){
    const line=original.trim(),speed=line.match(/^(?:###?\s+)?(Standard|Batch|Fast|Ultrafast|Flex)(?: pricing data)?$/);
    if(speed){mode=speed[1];header=null;continue;}
    if(!line.startsWith('|'))continue;const cells=line.replace(/^\||\|$/g,'').split('|').map(x=>x.trim());
    if(cells.includes('Model')){header=cells;continue;}
    if(mode!=='Standard'||!header||cells[0].startsWith('---')||header.includes('Modality')||header.includes('Training'))continue;
    if(!header.includes('Model'))continue;
    const fields=header.includes('Short context input')?['Short context input','Short context cached input','Short context output']:['Input','Cached input','Output'];
    // Per-minute, image/audio modalities, tools, training and rows without text output cannot share this token reference.
    if(fields.some(f=>!header.includes(f)))continue;if(cells.length!==header.length)fail('官方费率表不完整');
    if(cells[header.indexOf(fields[0])]==='-'||cells[header.indexOf(fields[2])]==='-')continue;
    const name=cells[header.indexOf('Model')].split(' (')[0];if(!modelName(name))fail('官方模型标识无法识别');
    const [input,cached,output]=fields.map(f=>money(cells[header.indexOf(f)]));if(input===null||output===null)fail('官方输入或输出费率缺失');
    const rate=validateRate({input,cached,output});if(result[name]&&JSON.stringify(result[name])!==JSON.stringify(rate))fail('官方模型费率冲突');result[name]=rate;
  }
  if(!Object.keys(result).length)fail('未取得可用的官方费率');return result;
}
export const exactUsd=atoms=>{
  const n=BigInt(atoms),whole=n/scale,fraction=(n%scale).toString().padStart(12,'0').replace(/0+$/,'');return whole.toString()+(fraction?'.'+fraction:'');
};
const fields=['input','output','cached','adjustment','total'];
const empty=(state,model=null,revision=null)=>({state,model,revision,...Object.fromEntries(fields.map(k=>[k+'Atoms',null]))});
const legacyManual=row=>row.status==='reconciled'&&row.manual_cost_atoms==null;
export const needsReferenceReconciliation=(row,start)=>['unknown','interrupted'].includes(row.status)||legacyManual(row)&&row.ended>=start;
export function referenceModel(chosen,at,book){
  if(typeof chosen!=='string'||!book||!book.rates)return null;
  if(Object.hasOwn(book.rates,chosen))return chosen;
  const alias=(book.aliases||[]).find(a=>a.name===chosen&&at>=a.from&&(a.to===null||at<a.to));
  return alias&&Object.hasOwn(book.rates,alias.target)?alias.target:null;
}
export function quoteReference(row,book={rates:seed.rates,aliases:[]},revision=null){
  const chosen=row.billing_model||row.model;
  const alias=(book.aliases||[]).find(a=>a.name===chosen&&row.started>=a.from&&(a.to===null||row.started<a.to));
  const model=referenceModel(chosen,row.started,book),rate=model?book.rates[model]:null;
  const modelSource=!Object.hasOwn(book.rates,chosen)&&alias?'configured_alias':row.billing_model?'upstream':model?'requested':'unknown';
  if(row.manual_cost_atoms!==null&&row.manual_cost_atoms!==undefined){
    if(!/^\d+$/.test(row.manual_cost_atoms))return empty('unmetered',chosen,revision);
    const total=BigInt(row.manual_cost_atoms);return {state:'manual',model:chosen,modelSource:'manual',revision,inputAtoms:0n,outputAtoms:0n,cachedAtoms:0n,adjustmentAtoms:total,totalAtoms:total};
  }
  if(legacyManual(row))return empty('legacy_manual',chosen,revision);
  if(!row.ended&&['queued','running'].includes(row.status))return empty('pending',model||chosen,revision);
  if(row.usage_unknown)return empty('unmetered',model||chosen,revision);
  const {input=0,output=0,cached=0}=row;
  if(![input,output,cached].every(n=>Number.isSafeInteger(n)&&n>=0)||cached>input)return empty('unmetered',chosen,revision);
  if(input===0&&output===0)return {state:'priced',model:chosen,modelSource,revision,...Object.fromEntries(fields.map(k=>[k+'Atoms',0n]))};
  if(!rate||cached>0&&rate.cached===null)return empty('unpriced',chosen,revision);
  const cachedAtoms=BigInt(cached)*(rate.cached===null?0n:priceMicros(rate.cached));
  const inputAtoms=BigInt(input-cached)*priceMicros(rate.input)+cachedAtoms,outputAtoms=BigInt(output)*priceMicros(rate.output);
  return {state:'priced',model,modelSource,revision,inputAtoms,outputAtoms,cachedAtoms,adjustmentAtoms:0n,totalAtoms:inputAtoms+outputAtoms};
}
export function publicUsage(value){
  return {metric:referenceMetric,currency:'USD',state:value.state,model:value.model,model_source:value.modelSource||null,revision:value.revision,
    ...Object.fromEntries(fields.flatMap(k=>[[k,value[k+'Atoms']===null?null:Number(value[k+'Atoms'])/Number(scale)],[k+'_exact',value[k+'Atoms']===null?null:exactUsd(value[k+'Atoms'])]]))};
}
export function createPricing(store,{clock=Date.now,fetchImpl=fetch,windowStart=()=>0}={}){
  store.db.exec('CREATE TABLE IF NOT EXISTS pricing_versions(id TEXT PRIMARY KEY,created INTEGER NOT NULL,book TEXT NOT NULL);');
  const get=name=>parse(store.get('SELECT value FROM gateway_settings WHERE name=?',name)?.value);
  const set=(name,value)=>store.run('INSERT INTO gateway_settings(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value',name,JSON.stringify(value));
  let settings={enabled:true,refreshHours:24,version:0,manualRates:{},aliases:[],...get('pricing.settings')};
  let official=get('pricing.official')||seed.rates,state=get('pricing.state')||{},closed=false,pending=null;
  const cache=new Map();const abort=new AbortController();
  const book=id=>{if(!id)return null;if(!cache.has(id)){const r=store.get('SELECT book FROM pricing_versions WHERE id=?',id);if(r)cache.set(id,parse(r.book));}return cache.get(id)||null;};
  function publish(){
    const rates=Object.fromEntries(Object.entries({...official,...settings.manualRates}).sort(([a],[b])=>a.localeCompare(b))),value={rates,aliases:settings.aliases};
    const json=JSON.stringify(value),id=createHash('sha256').update(json).digest('hex');
    store.run('INSERT OR IGNORE INTO pricing_versions(id,created,book) VALUES(?,?,?)',id,clock(),json);cache.set(id,value);state={...state,active:id};set('pricing.state',state);return id;
  }
  if(!state.active||!book(state.active))publish();
  function rawQuote(row,{at=clock(),retention=true}={}){
    if(retention&&row.ended&&row.ended<windowStart(at))return empty('expired',row.billing_model||row.model,row.price_version);
    if(row.price_version&&!book(row.price_version))return empty('unpriced',row.billing_model||row.model,row.price_version);
    return quoteReference(row,book(row.price_version)||book(state.active),row.price_version||state.active);
  }
  const usage=(row,at=clock())=>publicUsage(rawQuote(row,{at}));
  const quoteAt=(row,revision=row.price_version||state.active)=>book(revision)?quoteReference(row,book(revision),revision):empty('unpriced',row.billing_model||row.model,revision);
  function stamp(id){store.run('UPDATE requests SET price_version=? WHERE id=? AND price_version IS NULL',state.active,id);}
  function canReprice(row){
    return row.price_version!==state.active&&row.manual_cost_atoms==null&&book(row.price_version)&&rawQuote(row).state==='unpriced'&&quoteReference(row,book(state.active),state.active).state==='priced';
  }
  function capture(id,actualModel){
    if(modelName(actualModel))store.run('UPDATE requests SET billing_model=? WHERE id=?',actualModel,id);stamp(id);
    const row=store.get('SELECT * FROM requests WHERE id=?',id);
    if(row&&canReprice(row))store.run('UPDATE requests SET price_version=? WHERE id=? AND price_version=?',state.active,id,row.price_version);
  }
  function backfill({transaction=true}={}){
    const start=windowStart(clock()),end=start+7*86400000;
    // Initialize legacy rows in one statement: per-row WAL commits would delay startup on real histories.
    let changed=Number(store.run('UPDATE requests SET price_version=? WHERE price_version IS NULL AND COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',state.active,start,end).changes);
    const rows=store.all('SELECT * FROM requests WHERE COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',start,end),updates=[];
    for(const row of rows){
      if(canReprice(row))updates.push(row);
    }
    if(updates.length){if(transaction)store.db.exec('BEGIN IMMEDIATE');try{for(const row of updates)changed+=Number(store.run('UPDATE requests SET price_version=? WHERE id=? AND price_version=?',state.active,row.id,row.price_version).changes);if(transaction)store.db.exec('COMMIT');}catch(e){if(transaction)store.db.exec('ROLLBACK');throw e;}}
    return changed;
  }
  backfill();
  function summary(rows,at=clock(),quotes){
    const sums=Object.fromEntries(fields.map(k=>[k+'Atoms',0n])),revisions=new Set();let known=0,unmetered=0,unpriced=0,pendingCount=0,expired=0;
    for(const row of rows){const q=quotes?.get(row.id)||rawQuote(row,{at});
      if(q.totalAtoms===null){if(q.state==='unpriced')unpriced++;else if(q.state==='pending')pendingCount++;else if(q.state==='expired')expired++;else unmetered++;continue;}
      known++;for(const k of fields)sums[k+'Atoms']+=q[k+'Atoms'];if(q.revision)revisions.add(q.revision);
    }
    const absent=rows.length>0&&known===0;
    const value={...sums,state:unmetered+unpriced+expired?'partial':pendingCount?'pending':'priced',model:null,revision:null};if(absent)for(const k of fields)value[k+'Atoms']=null;
    return {...publicUsage(value),requests:rows.length,known,unmetered,unpriced,pending:pendingCount,expired,revisions:[...revisions].sort()};
  }
  function decorate(row,at=clock()){
    const {input,output,cached,reserved,predicted,price_version,manual_cost_atoms,billing_model,...meta}=row;
    return {...meta,has_legacy_reservation:Number(reserved)>0,can_reconcile:needsReferenceReconciliation(row,windowStart(at)),requested_model:row.model,model:billing_model||row.model,standard:usage(row,at)};
  }
  function inWindow(column,value,at=clock()){
    if(!['member_id','key_id','account_id','session_id'].includes(column))fail('用量维度无效');
    return summary(store.all(`SELECT * FROM requests WHERE ${column}=? AND COALESCE(ended,started)>=? AND COALESCE(ended,started)<?`,value,windowStart(at),windowStart(at)+7*86400000),at);
  }
  function snapshot(value,at){
    const rows=store.all('SELECT * FROM requests WHERE COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',windowStart(at),windowStart(at)+7*86400000);
    const quotes=new Map(rows.map(r=>[r.id,rawQuote(r,{at})])),groups=new Map();
    const group=(field,key)=>{const id=field+':'+key;if(!groups.has(id))groups.set(id,[]);return groups.get(id);};
    for(const r of rows){for(const f of ['member_id','key_id','account_id','session_id'])group(f,r[f]).push(r);group('day',new Date((r.ended??r.started)+28800000).toISOString().slice(0,10)).push(r);}
    const sum=(f,key)=>summary(group(f,key),at,quotes);
    const sumRecent=(f,key)=>summary(group(f,key).filter(r=>(r.ended??r.started)>=at-12*3600000&&(r.ended??r.started)<at),at,quotes);
    const clean=({used,used_12h,budget_used,unmetered,reserved,token_budget,quota,...meta})=>meta;
    const {tokens,reserved,unmetered,...totals}=value.totals;
    return {...value,usageMetric:referenceMetric,
      members:value.members.map(m=>({...clean(m),standard:sum('member_id',m.id),standard_12h:sumRecent('member_id',m.id)})),
      keys:value.keys.map(k=>({...clean(k),standard:sum('key_id',k.id),standard_12h:sumRecent('key_id',k.id)})),
      accounts:value.accounts.map(a=>({...clean(a),standard:sum('account_id',a.id)})),
      sessions:value.sessions.map(s=>({...s,standard:sum('session_id',s.id)})),
      requests:value.requests.map(r=>decorate(r,at)),totals:{...totals,standard:summary(rows,at,quotes)},
      daily:value.daily.map(({tokens,...d})=>({...d,standard:sum('day',d.day)}))};
  }
  function status(){
    const current=book(state.active),missing=new Map();
    for(const r of store.all('SELECT * FROM requests WHERE COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',windowStart(clock()),windowStart(clock())+7*86400000)){if(rawQuote(r).state==='unpriced'){const name=r.billing_model||r.model||'未记录';missing.set(name,(missing.get(name)||0)+1);}}
    return {metric:referenceMetric,currency:'USD',basis:seed.basis,source:seed.source,seedCaptured:seed.captured,state:{...state},settings:{...settings},
      rates:Object.entries(current.rates).map(([model,rate])=>({model,...rate,manual:Object.hasOwn(settings.manualRates,model)})),unpricedModels:[...missing].map(([model,requests])=>({model,requests}))};
  }
  function configure(value){
    if(closed)throw Object.assign(new Error('服务正在停止'),{status:503,code:'service_stopping'});
    if(!value||value.expectedVersion!==settings.version)fail('费率配置已变化，请刷新后重试');
    if(Object.keys(value).some(k=>!['expectedVersion','enabled','refreshHours','manualRates','aliases'].includes(k)))fail('费率配置字段无效');
    const next={...settings,version:settings.version+1,manualRates:{...settings.manualRates}};
    if(value.enabled!==undefined){if(typeof value.enabled!=='boolean')fail('自动同步开关无效');next.enabled=value.enabled;}
    if(value.refreshHours!==undefined){if(!Number.isInteger(value.refreshHours)||value.refreshHours<1||value.refreshHours>168)fail('同步间隔须为1至168小时');next.refreshHours=value.refreshHours;}
    if(value.manualRates!==undefined){
      if(!value.manualRates||typeof value.manualRates!=='object'||Array.isArray(value.manualRates))fail('手动费率无效');
      for(const [name,rate] of Object.entries(value.manualRates)){if(!modelName(name))fail('模型名称无效');if(rate===null)delete next.manualRates[name];else next.manualRates[name]=validateRate(rate);}
    }
    if(value.aliases!==undefined){
      if(!Array.isArray(value.aliases)||value.aliases.length>1000)fail('模型映射无效');
      next.aliases=value.aliases.map(a=>{
        const from=a?.from??clock(),to=a?.to??null;
        if(!a||!modelName(a.name)||!modelName(a.target)||!Number.isSafeInteger(from)||from<0||to!==null&&(!Number.isSafeInteger(to)||to<=from)||typeof a.source!=='string'||!a.source.trim()||a.source.length>500)fail('映射须提供明确模型、有效时间及来源说明');
        if(!Object.hasOwn({...official,...next.manualRates},a.target))fail('映射目标尚无费率');return {name:a.name,target:a.target,from,to,source:a.source};
      });
      for(const a of next.aliases)if(next.aliases.some(b=>b!==a&&a.name===b.name&&a.from<(b.to??Infinity)&&b.from<(a.to??Infinity)))fail('同一模型映射有效时间不能重叠');
    }
    const prior=settings,priorState=state;
    store.db.exec('BEGIN IMMEDIATE');try{settings=next;set('pricing.settings',settings);publish();backfill({transaction:false});store.audit('pricing.configure',JSON.stringify({version:settings.version,revision:state.active}));store.db.exec('COMMIT');}catch(e){store.db.exec('ROLLBACK');settings=prior;state=priorState;throw e;}
    return status();
  }
  async function refresh({force=false}={}){
    if(closed)return;if(pending)return pending;
    if(!force&&(!settings.enabled||state.checkedAt&&state.checkedAt>clock()-settings.refreshHours*3600000))return;
    pending=(async()=>{
      try{
        const r=await fetchImpl(seed.source,{headers:{Accept:'text/markdown'},redirect:'error',signal:AbortSignal.any([abort.signal,AbortSignal.timeout(15000)])});
        if(!r.ok)throw Error('official unavailable');const reader=r.body.getReader();let size=0;const chunks=[];
        try{for(;;){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>3000000)throw Error('official page too large');chunks.push(value);}}catch(e){await reader.cancel().catch(()=>{});throw e;}finally{reader.releaseLock();}
        const text=Buffer.concat(chunks).toString('utf8'),prices=parseOfficialPrices(text);if(closed)return;
        const prior=official,priorState=state;store.db.exec('BEGIN IMMEDIATE');
        try{official={...official,...prices};set('pricing.official',official);state={...state,checkedAt:clock(),lastSuccess:clock(),error:null,sourceHash:createHash('sha256').update(text).digest('hex')};publish();backfill({transaction:false});store.db.exec('COMMIT');}
        catch(e){store.db.exec('ROLLBACK');official=prior;state=priorState;throw e;}
        return status();
      }catch{if(!closed){state={...state,checkedAt:clock(),error:'官方费率更新暂未完成，继续使用上次有效版本'};set('pricing.state',state);return status();}}
    })().finally(()=>pending=null);return pending;
  }
  return {usage,rawQuote,quoteAt,stamp,capture,backfill,summary,decorate,inWindow,snapshot,status,configure,refresh,current:()=>state.active,
    book,close:async()=>{closed=true;abort.abort();await pending;}};
}
