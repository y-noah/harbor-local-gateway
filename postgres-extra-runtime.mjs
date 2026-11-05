import {AsyncLocalStorage} from 'node:async_hooks';
import {codexQuotaDenials} from './codex-entitlements.mjs';
import {isResponsesDispatchSource} from './responses.mjs';
import {AppError} from './store.mjs';
import {quotaFreshnessMs} from './balancing.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {createPostgresAccountExtraUsage,readExtraCandidatePolicyContext} from './postgres-account-extra-usage.mjs';
export const EXTRA_USAGE_DISPATCH_CONTRACT='v1';
const fail=reason=>{throw new AppError(503,'账号额外额度尚未获许可或预算不可用，请查看账号状态',reason);};
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
 async function classify(account,{model,protocol}={}){const policy=readScope(store)?(await readExtraDispatchSettings(store,()=>routingSettings(store))).get('account_package_policy')??{enabled:false}:await packages.policy(),[decorated]=await decorateExtraDispatchPackages(store,packages,[account]);return {...classifyExtraPackage(decorated,{at:clock(),model,protocol,packagePolicy:policy}),protocol};}
 async function routingView(accounts,{model,protocol,packagePolicy,distribution}={}){
  const classifications=new Map(accounts.map(account=>[account.id,classifyExtraPackage(account,{at:clock(),model,protocol,packagePolicy})])),views=model?await service.evaluateAccounts(accounts,{model,protocol,packageAvailability:new Map(accounts.map(account=>[account.id,classifications.get(account.id).packageAvailable]))}):null,decisions=new Map((views?.accounts??[]).map(row=>[row.account_id,row]));
  const isApi=account=>account.auth_mode==='openai_api',manual=distribution.subscriptionMode==='manual',normal=distribution.eligible.filter(account=>!isApi(account)&&['package','package_compatibility'].includes(classifications.get(account.id).mode)),apis=distribution.eligible.filter(isApi),extras=accounts.filter(account=>!isApi(account)&&decisions.get(account.id)?.decision.allowed&&decisions.get(account.id).decision.mode==='extra'&&(!manual||distribution.configuration.weights[account.id]>0));
  let selected=manual?[...normal,...extras]:normal.length?normal:extras,reason=distribution.reason,extraAuto=!manual&&!normal.length&&extras.length>0;
  // Reference budgets share the existing Standard USD basis. Provider units
  // require an explicit adapter comparison contract; matching "credits" alone
  // cannot establish one, so multiple official candidates need manual ratios.
  if(extraAuto&&extras.length>1&&extras.some(account=>{const row=decisions.get(account.id).allocation;return row?.basis!=='reference_estimate'||row.unit!=='reference_usd_atoms';})){selected=[];extraAuto=false;reason='extra_budget_incomparable';}
  const apiShare=apis.reduce((sum,account)=>sum+account.api_weight,0);let weights=new Map(accounts.map(account=>[account.id,0]));
  const desired=account=>extraAuto?Number(decisions.get(account.id).allocation.available_atoms):distribution.weights.get(account.id)||0,total=selected.reduce((sum,account)=>sum+desired(account),0);
  for(const account of selected)weights.set(account.id,total>0?Math.max(0,100-apiShare)*desired(account)/total:0);for(const account of apis)weights.set(account.id,account.api_weight);
  let eligible=[...selected,...apis];const unchanged=!extraAuto&&eligible.length===distribution.eligible.length&&eligible.every(account=>distribution.eligible.some(previous=>previous.id===account.id));const sum=eligible.reduce((n,account)=>n+weights.get(account.id),0);if(unchanged){eligible=distribution.eligible;weights=new Map(distribution.weights);}else if(sum>0)for(const account of eligible)weights.set(account.id,weights.get(account.id)/sum*100);
  return {classifications,decisions,distribution:{...distribution,eligible,weights,mode:extraAuto?(apis.length?'api_mixed_extra':'extra_auto'):distribution.mode,reason,accounts:distribution.accounts.map(row=>({...row,effective:row.effective===null?null:weights.get(row.id),...(decisions.has(row.id)?{extra:decisions.get(row.id).decision}:{})}))}};
 }
 async function admit({requestId,account,model,protocol}){const selected=await classify(account,{model,protocol});if(selected.mode==='blocked')fail(selected.reason);if(selected.mode==='extra'){const reserved=await service.reserve(requestId,{accountId:account.id,model,protocol});if(!reserved.allowed)fail(reserved.reason);return {...selected,extraReserved:true};}return {...selected,extraReserved:false};}
 function boundary({requestId,protocol,admittedExtra=false,fixed=false,packagePreference,prepare,validate}){
  let unusedReleased=false,used=false,attempted=false,grant=null,sent=false,uncertain=false,extra=admittedExtra,cleanupFailed=false;
  let preferenceLocked=false,globalOffObserved=false;
  const beforeDispatch=async()=>{if(attempted)fail('extra_duplicate_dispatch_authorization');attempted=true;try{grant=await dispatchReads.run({store,reuse:dispatchPreferenceReaders.has(packagePreference),transaction:null,settings:null,selections:new Map()},()=>service.authorizeRequestDispatch(requestId,{prepare:typeof prepare==='function'?async request=>{const checked=await prepare(request),scope=dispatchReads.getStore();if(scope)clearReads(scope);return checked;}:prepare,candidateAccountIds:packagePreference&&!admittedExtra&&!fixed?async function discoverCandidates(request,context){if(dispatchPreferenceReaders.has(packagePreference)&&typeof service.readPolicy==='function'){const snapshot=readExtraCandidatePolicyContext(context,store,discoverCandidates),policy=snapshot?.enabled===false?snapshot:await service.readPolicy();if(policy.enabled===false){globalOffObserved=true;return [];}}const view=await packagePreference(request);if(view.mode!=='extra'||view.manualPositive)return [];preferenceLocked=true;return view.accountIds;}:undefined,assess:async(account,request)=>{const scope=dispatchReads.getStore();if(scope)clearReads(scope);await validate?.(account,request);if(scope)clearReads(scope);const selected=await classify(account,{model:request.model,protocol}),currentPreference=packagePreference?await packagePreference(request):null;if(selected.mode!=='api'&&currentPreference?.manual&&!currentPreference.manualPositive)fail('extra_manual_weight_zero');if(selected.mode==='extra'&&packagePreference&&!admittedExtra&&!fixed){const view=currentPreference;if(!view.manualPositive&&globalOffObserved&&!preferenceLocked&&!view.packageAlternatives.length&&(await service.readPolicy()).enabled===false)fail('extra_global_disabled');if(!view.manualPositive&&(!preferenceLocked||view.packageAlternatives.length))fail(view.packageAlternatives.length?'extra_package_alternative_available':'extra_dispatch_tier_changed');}return selected;}}));extra||=grant.mode==='extra';}catch(error){if(error.dispatchUncertain===true){uncertain=true;extra=true;}throw error;}};
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
 return Object.freeze({service,classify,routingView,admit,boundary});
}
