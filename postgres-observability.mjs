import {canonicalStreamTiming} from './request-stream-timing-facts.mjs';
import {AppError,requestPhases,usageWindow} from './store.mjs';
import seed from './pricing-data.mjs';
import {modelReleaseRegistry,modelReleaseSetting} from './model-release-dates.mjs';
import {joins,validPrice} from './postgres-reference-query.mjs';
import {decorateReferenceRequest,publicUsage,referenceMetric} from './pricing.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {createQueryDepartments,departmentPredicate} from './postgres-query-departments.mjs';

const observationDepartments=new WeakMap();
export async function postgresRequestObservation(store,id,at=Date.now(),{scope}={}){
  if(!observationDepartments.has(store))observationDepartments.set(store,createQueryDepartments(store));
  const authorization=await observationDepartments.get(store).resolve(scope),args=[id],predicate=departmentPredicate(authorization,'r',args);
  return store.transaction(async()=>{
    const row=await store.get(`SELECT r.*,m.name member_name,a.name account_name,k.prefix key_prefix,k.removed_at key_removed_at
      FROM harbor.requests r LEFT JOIN harbor.members m ON m.id=r.member_id LEFT JOIN harbor.accounts a ON a.id=r.account_id
      LEFT JOIN harbor.keys k ON k.id=r.key_id WHERE r.id=$1 AND ${predicate}`,...args);
    const window=usageWindow(at),time=row?.ended??row?.started;
    if(!row||time<window.start||time>=window.end)throw new AppError(404,'请求记录不存在或超出近7天可见范围');
    let phases;try{phases=JSON.parse(row.phase_history);}catch{}
    const timeline=(Array.isArray(phases)?phases:[]).filter(p=>p&&requestPhases.has(p.phase)&&Number.isSafeInteger(p.at)).map(({phase,at})=>({phase,at}));
    const value=decorateReferenceRequest(row,publicUsage(await store.quote(row)),window.start);
    return {...value,error:row.error?sanitizeRecord(row.error,{maxBytes:2048}):null,phase_history:undefined,observedAt:at,streamTiming:canonicalStreamTiming(Array.isArray(phases)?phases.at(-1)?.stream_timing:undefined)??null,
      timeline:timeline.map((p,i)=>({...p,duration:Math.max(0,(timeline[i+1]?.at??row.ended??at)-p.at)}))};
  },{level:'REPEATABLE READ',readOnly:true});
}

export async function postgresPricingStatus(store,at=Date.now()){
  return store.transaction(async()=>{
    const settingsRows=await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('pricing.state','pricing.settings','model.release_dates')"),saved=Object.fromEntries(settingsRows.map(r=>[r.name,JSON.parse(r.value)]));
    const state=saved['pricing.state'],settings={enabled:true,refreshHours:24,version:0,manualRates:{},aliases:[],...saved['pricing.settings']};
    const book=state?.active&&await store.pricingBook(state.active);if(!book?.rates)throw new AppError(503,'计价版本尚未准备好','pricing_unavailable');
    const window=usageWindow(at),missing=await store.all(`SELECT COALESCE(r.billing_model,r.model,'未记录') model,COUNT(*) requests FROM harbor.requests r${joins}
      WHERE r.usage_at>=$1 AND r.usage_at<$2 AND (NOT COALESCE(${validPrice},false) OR p.state='unpriced') GROUP BY COALESCE(r.billing_model,r.model,'未记录') ORDER BY model`,window.start,window.end);
    const releases=saved[modelReleaseSetting],dates=modelReleaseRegistry(releases,at);
    return {metric:referenceMetric,currency:'USD',basis:seed.basis,source:seed.source,seedCaptured:seed.captured,state,settings,modelReleaseState:releases?{checkedAt:releases.checkedAt,lastSuccess:releases.lastSuccess,error:releases.error}:null,
      rates:Object.entries(book.rates).map(([model,rate])=>({model,...rate,manual:Object.hasOwn(settings.manualRates,model),release_date:dates[model]?.date??null,release_date_source:dates[model]?.source??null})),unpricedModels:missing};
  },{level:'REPEATABLE READ',readOnly:true});
}
