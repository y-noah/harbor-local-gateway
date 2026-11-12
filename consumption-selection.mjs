import {AppError} from './store.mjs';
import {normalizeConsumptionAttempts} from './postgres-consumption-policy.mjs';
const code='consumption_rebind_commit_unconfirmed';
const invalid=()=>{throw new AppError(400,'消耗选择恢复凭据无效','consumption_selection_invalid');};
const conflict=()=>{throw new AppError(409,'消耗选择恢复与已保存事实不一致','consumption_selection_conflict');};
const integer=value=>Number.isSafeInteger(value)&&value>=0;
const identifier=value=>typeof value==='string'&&value.length>=1&&value.length<=80&&!/[^a-zA-Z0-9-]/.test(value);
function object(value,required,optional=[]){
 if(!value||typeof value!=='object'||![Object.prototype,null].includes(Object.getPrototypeOf(value)))invalid();
 const descriptors=Object.getOwnPropertyDescriptors(value),names=Reflect.ownKeys(descriptors);
 if(required.some(name=>!Object.hasOwn(descriptors,name))||names.some(name=>typeof name!=='string'||!required.includes(name)&&!optional.includes(name)||!Object.hasOwn(descriptors[name],'value')||!descriptors[name].enumerable))invalid();
 return Object.fromEntries(names.map(name=>[name,descriptors[name].value]));
}
function firstAttempt(value){
 const item=object(value,['source','account_id','state','usage_known','started','ended','error_code','input','output','cached'],['dispatched']);
 if(item.source!=='api'||!identifier(item.account_id)||item.state!=='rejected'||item.usage_known!==true||item.error_code!=='upstream_quota_exhausted'||!integer(item.started)||!integer(item.ended)||item.ended<item.started||['input','output','cached'].some(key=>item[key]!==0)||Object.hasOwn(item,'dispatched')&&item.dispatched!==true)invalid();
 // Missing dispatched and explicit true carry the same existing sent fact.
 return {source:'api',account_id:item.account_id,state:'rejected',usage_known:true,started:item.started,ended:item.ended,error_code:'upstream_quota_exhausted',input:0,output:0,cached:0};
}
const stable=value=>JSON.stringify(value,(_,item)=>item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.entries(item).sort(([a],[b])=>a.localeCompare(b))):item);
export function canonicalConsumptionSelection(value,attempts){
 if(value===undefined)return undefined;
 const item=object(value,['version','request_id','member_id','key_id','session_id','consumption_policy','consumption_policy_version','from','to','prepared_at','failed_at','error_code','first_attempt']),from=object(item.from,['account_id','route_version']),to=object(item.to,['account_id','route_version']);
 if(item.version!==1||['request_id','member_id','key_id','session_id'].some(key=>!identifier(item[key]))||item.consumption_policy!=='api_first'||!integer(item.consumption_policy_version)||item.error_code!==code||!identifier(from.account_id)||!identifier(to.account_id)||from.account_id===to.account_id||!integer(from.route_version)||!integer(to.route_version)||to.route_version!==from.route_version+1||!integer(item.prepared_at)||!integer(item.failed_at))invalid();
 const first=firstAttempt(item.first_attempt);
 if(first.account_id!==from.account_id||item.prepared_at<first.ended||item.failed_at<item.prepared_at||!Array.isArray(attempts)||attempts.length!==1||stable(firstAttempt(attempts[0]))!==stable(first))invalid();
 const result={version:1,request_id:item.request_id,member_id:item.member_id,key_id:item.key_id,session_id:item.session_id,consumption_policy:'api_first',consumption_policy_version:item.consumption_policy_version,from:{account_id:from.account_id,route_version:from.route_version},to:{account_id:to.account_id,route_version:to.route_version},prepared_at:item.prepared_at,failed_at:item.failed_at,error_code:code,first_attempt:first};
 if(Buffer.byteLength(JSON.stringify(result),'utf8')>2048)invalid();return result;
}
export function assertConsumptionSelectionUsage(result){
 if(['input','output','cached'].some(key=>result[key]!==undefined&&result[key]!==0))invalid();
}
// Called only with the locked historical request row. Current session routing
// may legitimately have advanced, and is never consulted or changed here.
export function resolveConsumptionSelection(row,selection){
 if(!row||['request_id','member_id','key_id','session_id','consumption_policy','consumption_policy_version'].some(key=>(key==='request_id'?row.id:row[key])!==selection[key])||['input','output','cached'].some(key=>row[key]!==0)||row.manual_cost_atoms!==null&&row.manual_cost_atoms!==undefined)conflict();
 let previous;
 try{previous=normalizeConsumptionAttempts(row.consumption_attempts);if(!previous.length||stable(firstAttempt(previous[0]))!==stable(selection.first_attempt))conflict();}catch{conflict();}
 if(row.consumption_source!=='api'||row.consumption_fallback!==false)conflict();
 const from=row.account_id===selection.from.account_id&&row.route_version===selection.from.route_version,to=row.account_id===selection.to.account_id&&row.route_version===selection.to.route_version;
 if(!from&&!to)conflict();
 const second={source:'subscription',account_id:selection.to.account_id,state:'failed',dispatched:false,usage_known:true,input:0,output:0,cached:0,started:selection.prepared_at,ended:selection.failed_at,error_code:code};
 if(previous.length!==1&&(!to||previous.length!==2||stable(previous[1])!==stable(second)))conflict();
 return from?[previous[0]]:[previous[0],second];
}
