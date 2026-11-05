import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
const invalid=message=>{throw new AppError(400,message,'package_configuration_invalid');};
const record=(value,fields)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>fields.includes(key));
const text=(value,max=128)=>typeof value==='string'&&value.trim()===value&&value.length>0&&value.length<=max&&!/[\x00-\x1f]/.test(value);
const positive=value=>Number.isFinite(value)&&value>0;
const list=(value,max=128)=>Array.isArray(value)&&value.length>0&&value.length<=max&&value.every(item=>text(item,200))&&new Set(value).size===value.length;
const canonical=value=>Array.isArray(value)?value.map(canonical):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
export const packageFingerprint=value=>createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
// Values are configuration, not a built-in directory of commercial plans.
export function packageDefinition(value){
 if(!record(value,['name','platform','auth_modes','protocols','models','comparison','official','source','effective_at','windows'])||!text(value.name)||!text(value.platform)||!list(value.auth_modes,8)||value.auth_modes.some(mode=>!['subscription','platform'].includes(mode))||!list(value.protocols,16)||!list(value.models,512)||!Number.isSafeInteger(value.effective_at)||value.effective_at<0)invalid('套餐名称、平台、适用范围或生效时间无效');
 const c=value.comparison;if(!record(c,['group','unit','rule_id','rule_version','units'])||![c.group,c.unit,c.rule_id].every(v=>text(v))||!Number.isSafeInteger(c.rule_version)||c.rule_version<1||!c.units||typeof c.units!=='object'||Array.isArray(c.units)||!Object.keys(c.units).length||Object.keys(c.units).length>64||Object.entries(c.units).some(([unit,factor])=>!text(unit)||!positive(factor)))invalid('须配置版本化的共同容量单位及完整换算规则');
 const official=value.official;if(!record(official,['ids','granularity'])||!Array.isArray(official.ids)||official.ids.length>128||official.ids.some(id=>!text(id))||new Set(official.ids).size!==official.ids.length||!['exact','family','unknown'].includes(official.granularity))invalid('官方识别规则无效');
 const source=value.source;if(!record(source,['kind','reference','confirmed'])||!['administrator','official_document'].includes(source.kind)||!text(source.reference,2048)||typeof source.confirmed!=='boolean')invalid('须填写容量依据及确认状态');
 if(!Array.isArray(value.windows)||!value.windows.length||value.windows.length>32||new Set(value.windows.map(w=>w?.id)).size!==value.windows.length||value.windows.some(w=>!record(w,['id','minutes','capacity','unit'])||!text(w.id)||!positive(w.minutes)||w.minutes>5256000||w.capacity!==null&&!positive(w.capacity)||!Object.hasOwn(c.units,w.unit)))invalid('额度窗口须有稳定标识、时长、容量或官方绝对量及已知换算单位');
 return canonical({...value,auth_modes:[...value.auth_modes].sort(),protocols:[...value.protocols].sort(),models:[...value.models].sort(),official:{...official,ids:[...official.ids].sort()},windows:[...value.windows].sort((a,b)=>a.id.localeCompare(b.id))});
}
export function packageOverride(value,definition){
 if(value===null||value===undefined)return null;if(!record(value,['reason','capacities','confirmed'])||!text(value.reason,1024)||value.confirmed!==true||!value.capacities||typeof value.capacities!=='object'||Array.isArray(value.capacities)||!Object.keys(value.capacities).length||Object.entries(value.capacities).some(([key,capacity])=>!definition.windows.some(w=>w.id===key)||!positive(capacity)))invalid('个体容量覆盖须确认原因且仅指定已有窗口');return canonical(value);
}
function snapshot(account){try{const q=typeof account.quota_json==='string'?JSON.parse(account.quota_json):account.quota_json;return q&&typeof q==='object'?q:null;}catch{return null;}}
export function packageRecognition(definition,selection,plan){
 const known=typeof plan==='string'&&plan.length>0,matched=known&&definition.official.ids.includes(plan),status=matched?definition.official.granularity==='exact'?'match':'partial':known&&definition.official.ids.length?'conflict':'unknown',confirmation=selection.official_confirmation;
 const confirmed=confirmation?.plan===(known?plan:null)&&confirmation?.template_version===selection.template_version&&Number.isSafeInteger(confirmation?.recognition_version)&&confirmation.recognition_version===(selection.recognition_version??0)&&confirmation?.basis_confirmed===true;
 return {plan:known?plan:null,status,capacity_confirmed:definition.source.confirmed&&(status==='match'||confirmed),exception_confirmed:status==='conflict'&&confirmed};
}
export function accountPackageBudget(account,{at=Date.now(),enabled=false,protocol,model,freshnessMs=300000}={}){
 const selection=account.package_selection??{mode:'unknown'},q=snapshot(account),plan=typeof q?.plan==='string'?q.plan:null;
 const identity=packageFingerprint({account:account.id,kind:account.kind,auth_mode:account.auth_mode,route_epoch:account.route_epoch??0,selection:{mode:selection.mode,template_id:selection.template_id??null,template_version:selection.template_version??null,revision:selection.revision??0,recognition_version:selection.recognition_version??0,observed_plan:selection.observed_plan??null,capacity_override:selection.capacity_override?{capacities:selection.capacity_override.capacities,confirmed:selection.capacity_override.confirmed}:null,official_confirmation:selection.official_confirmation?{plan:selection.official_confirmation.plan,template_version:selection.official_confirmation.template_version,basis_confirmed:selection.official_confirmation.basis_confirmed,recognition_version:selection.official_confirmation.recognition_version}:null,definition:selection.definition??null},plan,quota_scope:Array.isArray(q?.windows)?q.windows.map(w=>w&&typeof w==='object'&&!Array.isArray(w)?{id:w.id??null,minutes:w.minutes??null,absolute_unit:w.absolute?.source==='official'?w.absolute.unit??null:null}:null).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b))):null});
 const result={id:account.id,mode:'unknown',eligible:false,score:null,comparison_key:null,identity,reason:null,windows:[],recognition:null,estimate:true};
 const rejected=(reason,mode='excluded')=>({...result,mode,reason});
 if(account.auth_mode==='openai_api')return rejected('api_manual_share','manual_api');
 if(!enabled)return {...result,mode:'legacy',reason:'package_policy_disabled'};
 if(!account.enabled||account.removed_at)return rejected('disabled');if(!['ready','degraded'].includes(account.state)||account.route_block&&account.route_block!=='quota_exhausted')return rejected(account.route_block==='login_expired'?'login_expired':'not_ready');
 const exhausted=account.route_block==='quota_exhausted'||q?.limitReached||Array.isArray(q?.windows)&&q.windows.some(w=>w&&typeof w==='object'&&(w.absolute?.source==='official'?w.absolute.remaining===0:Number.isFinite(w.used)&&w.used>=100));
 result.exhausted=!!exhausted;
 if(selection.mode==='legacy'&&exhausted)return rejected('quota_exhausted','legacy');
 if(selection.mode==='legacy')return {...result,mode:'legacy',reason:'legacy_capacity_unconfigured'};
 if(selection.mode!=='selected'||!selection.definition)return rejected('package_unknown','unknown');
 let definition,override;try{definition=packageDefinition(selection.definition);override=packageOverride(selection.capacity_override,definition);}catch{return rejected('package_configuration_invalid','unknown');}
 result.recognition=packageRecognition(definition,selection,plan);
 if(definition.platform!==account.kind||!definition.auth_modes.includes(account.auth_mode)||protocol&&!definition.protocols.includes(protocol)||model&&!definition.models.includes('*')&&!definition.models.includes(model))return rejected('package_scope_mismatch');
 if(definition.effective_at>at)return rejected('package_not_effective','unknown');
 if(!result.recognition.capacity_confirmed)return rejected(result.recognition.status==='conflict'?'official_plan_conflict':'capacity_basis_unconfirmed','unknown');
 if(!q||!Number.isFinite(q.checked)||q.checked>at||at-q.checked>freshnessMs||!Array.isArray(q.windows)||!q.windows.length)return rejected('quota_unavailable','unknown');
 result.comparison_key=packageFingerprint({platform:definition.platform,auth_mode:account.auth_mode,protocols:definition.protocols,models:definition.models,comparison:definition.comparison});
 const matched=new Set();let estimated=false;
 for(const window of q.windows){
  if(!window||typeof window!=='object'||Array.isArray(window))return rejected('quota_unavailable','unknown');
  const candidates=definition.windows.filter(w=>window.id?w.id===window.id:w.minutes===window.minutes);
  if(candidates.length!==1||matched.has(candidates[0].id))return rejected('window_conversion_unavailable','unknown');const rule=candidates[0];matched.add(rule.id);
  if(!Number.isFinite(window.minutes)||window.minutes!==rule.minutes||!Number.isSafeInteger(window.reset)||window.reset<=at)return rejected('quota_expired','unknown');
  const absolute=window.absolute;let remaining,source;
  if(absolute?.source==='official'){
   if(!Number.isFinite(absolute.remaining)||absolute.remaining<0||!Object.hasOwn(definition.comparison.units,absolute.unit))return rejected('absolute_unit_unavailable','unknown');remaining=absolute.remaining*definition.comparison.units[absolute.unit];source='official_absolute';
  }else{
   const capacity=override?.capacities[rule.id]??rule.capacity;if(capacity===null||!Number.isFinite(window.used)||window.used<0||window.used>100)return rejected('window_capacity_unavailable','unknown');remaining=capacity*(100-window.used)/100*definition.comparison.units[rule.unit];source=override?.capacities[rule.id]!==undefined?'account_estimate':'package_estimate';estimated=true;
  }
  const hours=(window.reset-at)/3600000,denominator=Math.max(hours,24),score=remaining/denominator;if(!Number.isFinite(remaining)||!Number.isFinite(score))return rejected('capacity_overflow','unknown');result.windows.push({id:rule.id,minutes:window.minutes,remaining,unit:definition.comparison.unit,hours,denominator,score,source,reset:window.reset});
 }
 if(matched.size!==definition.windows.length)return rejected('quota_window_missing','unknown');
 if(exhausted)return rejected('quota_exhausted');
 result.mode='capacity';result.eligible=true;result.score=Math.min(...result.windows.map(w=>w.score));result.estimate=estimated;result.reason=null;result.comparison_key=packageFingerprint({platform:definition.platform,auth_mode:account.auth_mode,protocols:definition.protocols,models:definition.models,comparison:definition.comparison});return result;
}
export function packageBudget(accounts,options={}){
 const items=accounts.map(account=>accountPackageBudget(account,options)),groups=new Map();for(const item of items)if(item.eligible){if(!groups.has(item.comparison_key))groups.set(item.comparison_key,[]);groups.get(item.comparison_key).push(item);}
 return {version:1,smoothing_hours:24,items,groups:[...groups].map(([key,rows])=>{const largest=Math.max(...rows.map(row=>row.score)),total=rows.reduce((sum,row)=>sum+row.score/largest,0);return {key,accounts:rows.map(row=>row.id),weights:Object.fromEntries(rows.map(row=>[row.id,(row.score/largest)/total*100]))};})};
}
