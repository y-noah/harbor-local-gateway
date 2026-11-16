import {AppError} from './store.mjs';

// Diagnostic-only recognition is not parser support. Codex rust-v0.167.2:
// https://github.com/openai/codex/blob/rust-v0.167.2/codex-rs/protocol/src/models.rs#L1094
// agent_message.content may contain encrypted children; never inspect them here.
export const recordingInputProtocolTypes=Object.freeze(['message','additional_tools','function_call','function_call_output','custom_tool_call','custom_tool_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output','code_interpreter_call','mcp_call','mcp_list_tools','mcp_approval_request','mcp_approval_response','web_search_call','file_search_call','computer_call','computer_call_output','item_reference','compaction','reasoning','agent_message']);
const protocols=new Set(recordingInputProtocolTypes),roles=new Set(['user','assistant','system','developer','tool']);
const fields=Object.freeze(['type','role','content','input','output','call_id','tools','encrypted_content','summary']);
const shapes=new Set(['null','array','object','string','number','boolean','undefined','other']);
const unknownBuckets=new Set(['missing','non_string','empty','unknown_token','oversize','invalid_chars']);
const defaults={enabled:false,maxRequests:100,maxGroups:64,maxItemsPerRequest:256,ttlMs:600000};
const limits={maxRequests:10000,maxGroups:256,maxItemsPerRequest:4096,ttlMs:3600000};
const variables={maxRequests:'MAX_REQUESTS',maxGroups:'MAX_GROUPS',maxItemsPerRequest:'MAX_ITEMS_PER_REQUEST',ttlMs:'TTL_MS'};
const invalid=()=>{throw new AppError(503,'输入结构诊断配置无效','recording_configuration');};
export function recordingInputDiagnosticsConfiguration({env={},configuration}={}){
 if(configuration!==undefined&&(!configuration||typeof configuration!=='object'||Array.isArray(configuration)||Object.keys(configuration).some(key=>!Object.hasOwn(defaults,key))))invalid();
 const flag=env.HARBOR_RECORDING_INPUT_DIAGNOSTICS;
 if(flag!==undefined&&!['0','1'].includes(flag))invalid();
 if(configuration?.enabled!==undefined&&typeof configuration.enabled!=='boolean')invalid();
 const result={...defaults,enabled:configuration?.enabled??flag==='1'};
 if(typeof result.enabled!=='boolean')invalid();
 for(const [key,suffix] of Object.entries(variables)){
  const value=env['HARBOR_RECORDING_INPUT_DIAGNOSTICS_'+suffix];
  if(value!==undefined&&(typeof value!=='string'||!/^\d+$/.test(value)||!Number.isSafeInteger(Number(value))||Number(value)<1||Number(value)>limits[key]))invalid();
  if(configuration?.[key]!==undefined&&typeof configuration[key]!=='number')invalid();
  result[key]=configuration?.[key]??(value===undefined?defaults[key]:Number(value));
  if(!Number.isSafeInteger(result[key])||result[key]<1||result[key]>limits[key])invalid();
 }
 return result;
}
const shape=value=>value===null?'null':Array.isArray(value)?'array':['object','string','number','boolean','undefined'].includes(typeof value)?typeof value:'other';
function discriminator(value,known){
 if(value===undefined)return 'missing';if(typeof value!=='string')return 'non_string';if(!value.length)return 'empty';
 if(value.length>256)return 'oversize';if(known.has(value))return value;
 return /^[a-z][a-z0-9_]*$/.test(value)?'unknown_token':'invalid_chars';
}
// Construct before invoking any observer. No raw value, arbitrary field name,
// identifier, text, hash or per-request index can enter the diagnostic DTO.
export function recordingInputShape(value,source,location){
 const object=value!==null&&typeof value==='object'&&!Array.isArray(value);
 return Object.freeze({source,location,shape:shape(value),type:discriminator(object?value.type:undefined,protocols),role:discriminator(object?value.role:undefined,roles),...Object.fromEntries(fields.map(name=>['has_'+name,object&&Object.hasOwn(value,name)]))});
}
export function observeRecordingInputShape(observer,value,source,location){
 if(typeof observer!=='function')return;
 try{if(typeof observer.canObserve==='function'&&!observer.canObserve())return;const result=observer(recordingInputShape(value,source,location));if(result&&typeof result.then==='function')void Promise.resolve(result).catch(()=>{});}catch{}
}
function validShape(value){
 return value&&Object.keys(value).length===5+fields.length&&Object.keys(value).every(key=>['source','location','shape','type','role',...fields.map(name=>'has_'+name)].includes(key))&&['input','messages'].includes(value.source)&&['array_item','top_level'].includes(value.location)&&shapes.has(value.shape)&&(protocols.has(value.type)||unknownBuckets.has(value.type))&&(roles.has(value.role)||unknownBuckets.has(value.role))&&fields.every(name=>typeof value['has_'+name]==='boolean');
}
export function createRecordingInputDiagnostics({clock=Date.now,...configuration}={}){
 const config=recordingInputDiagnosticsConfiguration({configuration});if(typeof clock!=='function')invalid();
 const groups=new Map();let closed=false,requests=0,overflow=0,itemLimitRequests=0;
 const startedAt=clock(),expiresAt=startedAt+config.ttlMs;
 const expired=()=>clock()>=expiresAt;
 function beginRequest(){
  if(closed||!config.enabled||expired()||requests>=config.maxRequests)return undefined;
  requests++;let items=0,limited=false;
  const canObserve=()=>{
   if(closed||expired())return false;
   if(items>=config.maxItemsPerRequest){if(!limited){limited=true;itemLimitRequests++;}return false;}
   return true;
  };
  const observe=value=>{
   if(!canObserve()||!validShape(value))return false;
   items++;
   const key=JSON.stringify(value),entry=groups.get(key);
   if(entry){entry.count++;return true;}
   if(groups.size>=config.maxGroups){overflow++;return false;}
   groups.set(key,{...value,count:1});return true;
  };
  Object.defineProperty(observe,'canObserve',{value:canObserve});return Object.freeze(observe);
 }
 return Object.freeze({beginRequest,status:()=>closed||!config.enabled?undefined:{version:1,state:expired()?'expired':requests>=config.maxRequests?'request_limit':'active',startedAt,expiresAt,requests,limits:{maxRequests:config.maxRequests,maxGroups:config.maxGroups,maxItemsPerRequest:config.maxItemsPerRequest,ttlMs:config.ttlMs},overflow,itemLimitRequests,truncated:overflow>0||itemLimitRequests>0,groups:[...groups.values()].map(value=>({...value}))},close:()=>{closed=true;groups.clear();requests=0;overflow=0;itemLimitRequests=0;}});
}
