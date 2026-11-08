import {AsyncLocalStorage} from 'node:async_hooks';
import {codexQuotaDenials} from './codex-entitlements.mjs';
import {isResponsesDispatchSource} from './responses.mjs';
import {AppError} from './store.mjs';
import {quotaFreshnessMs,balanceSettings} from './balancing.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {createPostgresAccountExtraUsage} from './postgres-account-extra-usage.mjs';
export const EXTRA_USAGE_DISPATCH_CONTRACT='v1';
const fail=reason=>{throw new AppError(503,'账号当前不可分配，请检查手动权重和账号状态',reason);};
const dispatchReads=new AsyncLocalStorage(),dispatchPreferenceReaders=new WeakSet();
export function registerExtraDispatchPreferenceReader(reader){dispatchPreferenceReaders.add(reader);return reader;}
// Only the current final-dispatch transaction may reuse routing-protected inputs.
// Account rows, balances, permissions and time-dependent decisions never enter it.
const readScope=store=>{const scope=dispatchReads.getStore(),transaction=store.context?.getStore();return scope?.reuse===true&&scope.store===store&&transaction?.open===true&&scope.transaction===transaction?scope:null;};
const clearReads=scope=>{scope.settings=null;scope.selections=new Map();};
export function noteExtraDispatchRoutingLock(store){const scope=dispatchReads.getStore();if(scope?.store===store){scope.transaction=store.context.getStore();clearReads(scope);}}
export async function readExtraDispatchSettings(store,load){const scope=readScope(store);if(!scope)return load();if(!scope.settings)scope.settings=await load();return new Map([...scope.settings].map(([key,value])=>[key,structuredClone(value)]));}
export async function decorateExtraDispatchPackages(store,packages,accounts){const scope=readScope(store);if(!scope)return packages.decorate(accounts);const key=JSON.stringify(accounts.map(account=>account.id));if(!scope.selections.has(key)){const decorated=await packages.decorate(accounts);scope.selections.set(key,new Map(decorated.map(account=>[account.id,structuredClone(account.package_selection)])));}const selections=scope.selections.get(key);return accounts.map(account=>({...account,package_selection:structuredClone(selections.get(account.id))}));}
const routingSettings=async store=>new Map((await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('balancing','routing_strategy','routing_weights','routing_auto','routing_schedule','account_package_policy')")).map(row=>[row.name,JSON.parse(row.value)]));

// The quota reader's existing five-minute freshness contract is shared here.
export function classifyExtraPackage(account,{at=Date.now(),protocol,model,packagePolicy={enabled:false}}={}){
 if(account.auth_mode==='openai_api')return {mode:'api',packageAvailable:null,reason:'extra_not_applicable'};
 const scoped=accountPackageBudget(account,{at,enabled:packagePolicy.enabled,protocol,model});if(scoped.reason==='package_scope_mismatch')return {mode:'blocked',packageAvailable:null,reason:scoped.reason};
 let q;try{q=typeof account.quota_json==='string'?JSON.parse(account.quota_json):account.quota_json;}catch{}
 const denial=codexQuotaDenials(q);if(denial.spend_control||denial.workspace)return {mode:'blocked',packageAvailable:null,reason:'official_spending_denied'};if(denial.ordinary)return {mode:'extra',packageAvailable:false,reason:'official_package_denied'};
 const zero=window=>window?.absolute?.source==='official'?window.absolute.remaining===0:Number.isFinite(window?.used)&&window.used>=100;
 const knownExhausted=account.route_block==='quota_exhausted'||q?.limitReached===true||q?.remaining===0&&(!Array.isArray(q?.windows)||q.windows.length===0)||Array.isArray(q?.windows)&&q.windows.some(zero);
 // An explicit upstream refusal stays authoritative until trusted recovery.
 // Expiring a zero snapshot must never launder it into legacy compatibility.
 if(account.route_block==='quota_exhausted')return {mode:'extra',packageAvailable:false,reason:'quota_exhausted'};
 const fresh=q&&Number.isSafeInteger(q.checked)&&q.checked<=at&&at-q.checked<=quotaFreshnessMs&&Array.isArray(q.windows)&&q.windows.length>0&&q.windows.every(w=>w&&Number.isSafeInteger(w.reset)&&w.reset>at&&(w.absolute?.source==='official'?Number.isFinite(w.absolute.remaining)&&w.absolute.remaining>=0:Number.isFinite(w.used)&&w.used>=0&&w.used<=100));
 if(fresh&&knownExhausted)return {mode:'extra',packageAvailable:false,reason:'quota_exhausted'};
 if(fresh&&!knownExhausted)return {mode:'package',packageAvailable:true,reason:null};
 if(knownExhausted)return {mode:'blocked',packageAvailable:null,reason:'package_availability_unknown'};
 return {mode:'package_compatibility',packageAvailable:null,reason:'package_availability_unknown_existing_compatibility'};
}

export function createPostgresExtraRuntime(store,{clock=Date.now,service=createPostgresAccountExtraUsage(store,{clock}),packages=createPostgresAccountPackages(store,{clock})}={}){
 // Manual positive weights authorize an upstream attempt, not a fabricated
 // balance or a local extra-use reservation. Upstream remains the authority.
 function classifyManual(account,options,manualPositive){
  const value=classifyExtraPackage(account,options);
  if(manualPositive&&account.kind==='codex'&&account.auth_mode==='subscription'&&(value.mode==='extra'||value.reason==='package_availability_unknown'))return {...value,mode:'manual_extra',packageAvailable:false,reason:null};
  return value.mode==='extra'?{...value,mode:'blocked',reason:'manual_positive_weight_required'}:value;
 }
 async function classifyAccounts(accounts,{model,protocol}={}){
  const saved=await readExtraDispatchSettings(store,()=>routingSettings(store)),policy=saved.get('account_package_policy')??{enabled:false},weights=saved.get('routing_weights'),legacy=saved.get('balancing')??{},settings=balanceSettings({...legacy,weighted:saved.get('routing_strategy')?.weighted??legacy.weighted??false}),manualActive=weights?.mode==='manual'&&weights.expires>clock()&&(settings.weighted||policy.enabled),decorated=await decorateExtraDispatchPackages(store,packages,accounts);
  return new Map(decorated.map(account=>[account.id,{...classifyManual(account,{at:clock(),model,protocol,packagePolicy:policy},manualActive&&weights.weights?.[account.id]>0),protocol}]));
 }
 async function classify(account,options){return (await classifyAccounts([account],options)).get(account.id);}
 async function routingView(accounts,{model,protocol,packagePolicy,distribution}={}){
  const manual=distribution.subscriptionMode==='manual',classifications=new Map(accounts.map(account=>[account.id,classifyManual(account,{at:clock(),model,protocol,packagePolicy},manual&&distribution.configuration.weights[account.id]>0)]));
  const eligible=distribution.eligible.filter(account=>['api','package','package_compatibility','manual_extra'].includes(classifications.get(account.id).mode)),weights=new Map(distribution.weights),decisions=new Map(accounts.map(account=>[account.id,{decision:{allowed:eligible.some(row=>row.id===account.id),mode:classifications.get(account.id).mode,reason:classifications.get(account.id).reason,estimated:false},allocation:null}]));
  for(const account of accounts)if(!eligible.some(row=>row.id===account.id))weights.set(account.id,0);
  const sum=eligible.reduce((n,account)=>n+(weights.get(account.id)||0),0);if(sum>0&&eligible.length!==distribution.eligible.length)for(const account of eligible)weights.set(account.id,weights.get(account.id)/sum*100);
  return {classifications,decisions,distribution:{...distribution,eligible,weights,accounts:distribution.accounts.map(row=>({...row,effective:row.effective===null?null:weights.get(row.id),extra:decisions.get(row.id).decision}))}};
 }
 async function admit({account,model,protocol}){const selected=await classify(account,{model,protocol});if(selected.mode==='blocked')fail(selected.reason);return {...selected,extraReserved:false};}
 function boundary({requestId,protocol,admittedExtra=false,fixed=false,packagePreference,prepare,validate}){
  let unusedReleased=false,used=false,attempted=false,grant=null,sent=false,uncertain=false,extra=admittedExtra,cleanupFailed=false;
  const beforeDispatch=async()=>{if(attempted)fail('extra_duplicate_dispatch_authorization');attempted=true;try{grant=await dispatchReads.run({store,reuse:dispatchPreferenceReaders.has(packagePreference),transaction:null,settings:null,selections:new Map()},()=>service.authorizeRequestDispatch(requestId,{allowExtraLedger:false,prepare:typeof prepare==='function'?async request=>{const checked=await prepare(request),scope=dispatchReads.getStore();if(scope)clearReads(scope);return checked;}:prepare,assess:async(account,request)=>{const scope=dispatchReads.getStore();if(scope)clearReads(scope);await validate?.(account,request);if(scope)clearReads(scope);let currentPreference=packagePreference?await packagePreference(request):null;if(packagePreference&&!dispatchPreferenceReaders.has(packagePreference)){if(scope)clearReads(scope);currentPreference=await packagePreference(request);}if(scope&&!dispatchPreferenceReaders.has(packagePreference))clearReads(scope);const selected=await classify(account,{model:request.model,protocol});if(selected.mode!=='api'&&currentPreference?.manual&&!currentPreference.manualPositive)fail('extra_manual_weight_zero');if(selected.mode==='manual_extra'&&currentPreference&&!currentPreference.manualPositive)fail('manual_positive_weight_required');if(selected.mode==='blocked')fail(selected.reason);return selected;}}));extra||=grant.mode==='extra';}catch(error){if(error.dispatchUncertain===true){uncertain=true;extra=true;}throw error;}};
  const onDispatch=()=>{if(!grant)fail('extra_dispatch_source_unproven');service.markRequestDispatched(requestId,grant.permit);sent=true;};
  return Object.freeze({
   run:async(operation,{source}={})=>{
    const reliable=isResponsesDispatchSource(source);
    if(used||typeof operation!=='function')fail('extra_duplicate_dispatch');used=true;
    try{if(!reliable){await beforeDispatch();onDispatch();}const result=await operation({beforeDispatch:reliable?beforeDispatch:async()=>{},onDispatch:reliable?onDispatch:()=>{}});if(reliable&&!sent)fail('extra_dispatch_source_unproven');return result;}
    catch(error){if(uncertain){error.dispatchUncertain=true;error.usageKnown=false;}throw error;}
    finally{
     // This point is reached only after the actual adapter promise settles.
     // The source revokes not-sent eligibility synchronously before fetch.
     if(!sent&&!uncertain)try{if(grant)await service.confirmRequestNotDispatched(requestId,grant.permit);else if(extra)await service.releaseUndispatched(requestId,{confirmed:true});}catch{cleanupFailed=true;}
    }
   },
   releaseUnused:async()=>{if(used||unusedReleased||!extra)return;unusedReleased=true;try{await service.releaseUndispatched(requestId,{confirmed:true});}catch{cleanupFailed=true;}},
   settle:async()=>{if(!extra)return {state:'not_applicable'};try{return await service.settle(requestId);}catch{return {state:'pending',reason:'extra_settlement_pending'};}},
   status:()=>({dispatched:sent||uncertain,uncertain,extra,cleanupFailed})
  });
 }
 return Object.freeze({service,classify,classifyAccounts,routingView,admit,boundary});
}
