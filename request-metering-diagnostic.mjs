const reasons=new Set(['usage_received','not_dispatched','upstream_rejected','terminal_usage_missing','terminal_usage_invalid','interrupted_without_terminal','startup_interrupted','settlement_usage_invalid','settlement_uncertain','unclassified_unknown']);
const terminals=new Set(['none','completed','failed','incomplete','json']);
const interruptions=new Set(['none','client_cancelled','gateway_shutdown','upstream','startup']);
const fields=['version','reason','terminal','interruption','usage_known'];
// Diagnostic metadata must never introduce free text or affect settlement.
export function canonicalMeteringDiagnostic(value){
  try{
    if(!value||typeof value!=='object'||Array.isArray(value))return null;
    const descriptors=Object.getOwnPropertyDescriptors(value);
    if(Reflect.ownKeys(descriptors).length!==fields.length||fields.some(key=>!descriptors[key]||!Object.hasOwn(descriptors[key],'value')))return null;
    const data=Object.fromEntries(fields.map(key=>[key,descriptors[key].value]));
    return data.version===1&&reasons.has(data.reason)&&terminals.has(data.terminal)&&interruptions.has(data.interruption)&&typeof data.usage_known==='boolean'?data:null;
  }catch{return null;}
}
export function responseUsageState(response){
  const usage=response?.usage;
  if(usage===undefined||usage===null)return {state:'missing',usage:null};
  if(!['input_tokens','output_tokens'].every(key=>Number.isSafeInteger(usage[key])&&usage[key]>=0))return {state:'invalid',usage:null};
  const cached=usage.input_tokens_details?.cached_tokens??0;
  if(!Number.isSafeInteger(cached)||cached<0||cached>usage.input_tokens)return {state:'invalid',usage:null};
  return {state:'valid',usage:{input:usage.input_tokens,output:usage.output_tokens,cached}};
}
export function transportMeteringDiagnostic({usageKnown,usageState='missing',terminal='none',dispatched=false,rejected=false,interruption='none'}={}){
  const reason=usageState==='valid'?'usage_received':terminal!=='none'?(usageState==='invalid'?'terminal_usage_invalid':'terminal_usage_missing'):!dispatched?'not_dispatched':rejected?'upstream_rejected':interruption!=='none'?'interrupted_without_terminal':'unclassified_unknown';
  return canonicalMeteringDiagnostic({version:1,reason,terminal,interruption,usage_known:usageKnown});
}

// Apply the same final numeric facts before both durable spool and SQL writes.
// Missing historical diagnostics stay missing; diagnostic validation never
// changes the actual settlement amounts or rejects a valid settlement.
export function settlementMeteringDiagnostic(value,{input=0,output=0,cached=0,usageKnown=true}={}){
  const diagnostic=canonicalMeteringDiagnostic(value);if(!diagnostic)return null;
  const valid=[input,output,cached].every(n=>Number.isSafeInteger(n)&&n>=0)&&cached<=input;
  const known=valid&&!!usageKnown;
  return {...diagnostic,usage_known:known,reason:!valid?'settlement_usage_invalid':!known&&diagnostic.usage_known?'settlement_uncertain':diagnostic.reason};
}
