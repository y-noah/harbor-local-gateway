import {X509Certificate} from 'node:crypto';

const dayMs=86400000,maximumMs=365*dayMs-1000;
const fail=code=>{throw new Error(code);};
export function capacityRunLifetime(value,{required=false}={}){
  if(value===undefined){if(required)fail('capacity_run_lifetime_required');return undefined;}
  // Issuer accepts at most 365 days. Reserve X509's one-second precision.
  if(!Number.isSafeInteger(value)||value<1||value>maximumMs)fail('capacity_run_lifetime_invalid');
  return value;
}
export function capacityRunBudget({plan,runLifetimeMs,apiMaxDurationMs,uiMaxDurationMs}={}){
  capacityRunLifetime(runLifetimeMs,{required:plan?.profile==='formal'});
  const durations=(plan?.scenarios??[]).map(scene=>scene.durationMs);
  for(const value of [...durations,apiMaxDurationMs??0,uiMaxDurationMs??0])if(!Number.isSafeInteger(value)||value<0)fail('capacity_run_budget_invalid');
  const scheduledMs=durations.reduce((sum,value)=>sum+value,0)+(apiMaxDurationMs??0)+(uiMaxDurationMs??0);
  if(!Number.isSafeInteger(scheduledMs))fail('capacity_run_budget_invalid');
  if(runLifetimeMs!==undefined&&runLifetimeMs<=scheduledMs)fail('capacity_run_lifetime_below_budget');
  return Object.freeze({runLifetimeMs:runLifetimeMs??null,scheduledMs,preparationAndFinalizationAllowanceMs:runLifetimeMs===undefined?null:runLifetimeMs-scheduledMs});
}

// One lifecycle begins before target preparation. Cancellation asks existing
// operations to stop; it never pretends callbacks or cleanup have completed.
export function createCapacityCertificateLifetime({runLifetimeMs,signal}={}){
  capacityRunLifetime(runLifetimeMs);
  const wallStart=Date.now(),monoStart=performance.now(),controller=new AbortController(),certificates=new Map();let authority,timer,disposed=false;
  const expires=()=>{if(runLifetimeMs!==undefined&&performance.now()-monoStart>=runLifetimeMs&&!controller.signal.aborted)controller.abort(new Error('capacity_run_lifetime_exhausted'));return controller.signal.aborted;};
  const arm=()=>{if(disposed||expires()||runLifetimeMs===undefined)return;timer=setTimeout(arm,Math.min(2147483647,Math.max(1,Math.ceil(runLifetimeMs-(performance.now()-monoStart)))));timer.unref();};arm();
  const combined=AbortSignal.any([signal,controller.signal].filter(Boolean));
  const remaining=()=>runLifetimeMs===undefined?null:Math.max(0,runLifetimeMs-(performance.now()-monoStart));
  const requiredUntil=()=>Date.now()+(remaining()??0);
  const assertCoverage=({minimumRemainingMs=0}={})=>{
    expires();combined.throwIfAborted();
    if(runLifetimeMs!==undefined&&remaining()<=minimumRemainingMs)fail('capacity_run_lifetime_remaining_below_budget');
    if(!authority)fail('capacity_certificate_authority_missing');
    const now=Date.now(),until=requiredUntil();
    for(const certificate of certificates.values()){
      const from=Date.parse(certificate.validFrom),to=Date.parse(certificate.validTo);
      if(!Number.isFinite(from)||!Number.isFinite(to)||from>now||to<=until)fail('capacity_certificate_lifetime_insufficient');
      if(certificate!==authority&&(!certificate.checkIssued(authority)||!certificate.verify(authority.publicKey)))fail('capacity_certificate_chain_invalid');
    }
    return {checkedCertificates:certificates.size,requiredUntil:until,earliestNotAfter:Math.min(...[...certificates.values()].map(cert=>Date.parse(cert.validTo)))};
  };
  return Object.freeze({
    runLifetimeMs,signal:combined,days:runLifetimeMs===undefined?1:Math.ceil((runLifetimeMs+1000)/dayMs),
    registerAuthority:pem=>{if(authority)fail('capacity_certificate_authority_already_registered');const parsed=new X509Certificate(pem);if(!parsed.ca||!parsed.verify(parsed.publicKey))fail('capacity_certificate_chain_invalid');authority=parsed;certificates.set('ca',parsed);assertCoverage();},
    registerCertificate:(id,pem)=>{if(typeof id!=='string'||!id||certificates.has(id))fail('capacity_certificate_identity_invalid');const certificate=new X509Certificate(pem);certificates.set(id,certificate);try{assertCoverage();}catch(error){certificates.delete(id);throw error;}},
    assertRegisteredCertificate:(id,pem)=>{const parsed=new X509Certificate(pem);if(!certificates.get(id)?.raw.equals(parsed.raw))fail('capacity_certificate_material_changed');return assertCoverage();},
    assertCoverage,
    evidence:()=>({runLifetimeMs:runLifetimeMs??null,startedAt:wallStart,observedMs:performance.now()-monoStart,deadlineReached:expires(),checkedCertificates:certificates.size}),
    dispose:()=>{disposed=true;clearTimeout(timer);}
  });
}
