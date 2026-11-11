import {AppError,id,modelAllowlist,usageWindow} from './store.mjs';
import {name,bool} from './admin-input.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';

const fail=(status,message,code='invalid_member_type')=>{throw new AppError(status,message,code);};
const version=value=>Number.isSafeInteger(value)&&value>=0;
const description=value=>{if(typeof value!=='string'||value.trim().length>400)fail(400,'使用类型说明最多400字');return value.trim();};
function typeCode(number){let code='';for(;number>0;number=Math.floor((number-1)/26))code=String.fromCharCode(65+(number-1)%26)+code;return code;}
export function createPostgresMemberTypes(store,{readStore=store,clock=Date.now,queryQueue}={}){
  const limits=createPostgresMemberLimits(store,{clock});
  const read=operation=>queryQueue?queryQueue.run(operation):operation();
  const row=(target,typeId,lock='')=>target.get('SELECT * FROM harbor.member_types WHERE id=$1'+lock,typeId);
  async function template(target,type){
    return type.default_version===null?null:await target.get('SELECT version,created,allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=$2',type.id,type.default_version);
  }
  async function validateModels(value){
    const allowed=modelAllowlist(value);if(allowed===null)return null;
    const current=await store.get("SELECT p.book FROM harbor.gateway_settings s JOIN harbor.pricing_versions p ON p.id=(s.value::jsonb->>'active') WHERE s.name='pricing.state'");
    const book=current&&JSON.parse(current.book),platforms=await store.all("SELECT kind,platform_models FROM harbor.accounts WHERE removed_at IS NULL AND auth_mode='platform'");
    const known=new Set(Object.keys(book?.rates||{}));
    for(const account of platforms){let models;try{models=JSON.parse(account.platform_models);}catch{}for(const model of Array.isArray(models)?models:[]){const value=typeof model==='string'?model:model?.id;if(typeof value==='string')known.add(account.kind+'/'+value);}}
    if(allowed.some(model=>!known.has(model)))fail(400,'请选择本地模型目录中的具体模型ID','member_type_model_unknown');return allowed;
  }
  async function list(){
    return read(()=>readStore.transaction(async()=>{
      const rows=await readStore.all(`SELECT t.*,p.created configured_at,p.allowed_models,p.usage_limits,
        (SELECT COUNT(*) FROM harbor.members m WHERE m.usage_type_id=t.id AND m.removed_at IS NULL) members
        FROM harbor.member_types t LEFT JOIN harbor.member_type_templates p ON p.type_id=t.id AND p.version=t.default_version ORDER BY t.code,t.id`);
      return {types:rows.map(({allowed_models,usage_limits,...type})=>({...type,configured:type.default_version!==null,defaults:type.default_version===null?null:{allowed_models,usage_limits}}))};
    },{level:'REPEATABLE READ',readOnly:true}));
  }
  async function create(payload){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['name','description'].includes(key)))fail(400,'使用类型字段无效');
    const title=name(payload.name),detail=description(payload.description??'');
    return store.transaction(async()=>{
      await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor.member-types.create',0))");
      const used=new Set((await store.all('SELECT code FROM harbor.member_types')).map(row=>row.code));
      let ordinal=1;while(used.has(typeCode(ordinal)))ordinal++;
      const value={id:id(),code:typeCode(ordinal),name:title,description:detail,enabled:1,version:0,default_version:null,updated:clock()};
      await store.run('INSERT INTO harbor.member_types(id,code,name,description,enabled,version,default_version,updated) VALUES($1,$2,$3,$4,$5,$6,$7,$8)',value.id,value.code,value.name,value.description,value.enabled,value.version,value.default_version,value.updated);
      await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'member.type.create',JSON.stringify({type:value.id,code:value.code}));
      return {...value,configured:false,defaults:null,members:0};
    });
  }
  async function save(typeId,payload){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['name','description','enabled','defaults','restore_version','expected_version'].includes(key))||!version(payload.expected_version)||!['name','description','enabled','defaults','restore_version'].some(key=>Object.hasOwn(payload,key)))fail(400,'使用类型配置字段无效');
    if(Object.hasOwn(payload,'restore_version')&&(!version(payload.restore_version)||payload.restore_version<1||Object.keys(payload).some(key=>!['restore_version','expected_version'].includes(key))))fail(400,'恢复版本须单独提交，不可混合其他配置');
    return store.transaction(async()=>{
      const previous=await row(store,typeId,' FOR UPDATE');if(!previous)fail(404,'使用类型不存在');if(previous.version!==payload.expected_version)fail(409,'使用类型配置已变化，请重新核对','member_type_conflict');
      let nextDefault=previous.default_version,defaults,restored;
      if(payload.restore_version!==undefined){const window=usageWindow(clock());restored=await store.get('SELECT allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=$2 AND created>=$3 AND created<$4',typeId,payload.restore_version,window.start,window.end);if(!restored)fail(404,'历史模板不存在或超出近7天范围','member_type_template_not_found');}
      if(payload.defaults!==undefined||restored){const input=restored||payload.defaults;if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['allowed_models','usage_limits'].includes(k))||!Object.hasOwn(input,'allowed_models')||!Object.hasOwn(input,'usage_limits'))fail(400,'默认模型和员工限额均须明确配置');
        defaults={allowed_models:await validateModels(input.allowed_models),usage_limits:await limits.validate(input.usage_limits)};nextDefault=(previous.default_version??0)+1;
        await store.run('INSERT INTO harbor.member_type_templates VALUES($1,$2,$3,$4::jsonb,$5::jsonb)',typeId,nextDefault,clock(),JSON.stringify(defaults.allowed_models),JSON.stringify(defaults.usage_limits));
      }
      const next={name:payload.name===undefined?previous.name:name(payload.name),description:payload.description===undefined?previous.description:description(payload.description),enabled:payload.enabled===undefined?previous.enabled:bool(payload.enabled),version:previous.version+1,default_version:nextDefault,updated:clock()};
      await store.run('UPDATE harbor.member_types SET name=$1,description=$2,enabled=$3,version=$4,default_version=$5,updated=$6 WHERE id=$7',next.name,next.description,next.enabled,next.version,nextDefault,next.updated,typeId);
      await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'member.type.configure',JSON.stringify({type:typeId,code:previous.code,version:next.version,template:nextDefault,previous:previous.default_version,...(restored?{restored_from:payload.restore_version}:{}),...(payload.name!==undefined?{name_changed:next.name!==previous.name}:{}),...(payload.description!==undefined?{description_changed:next.description!==previous.description}:{}),...(payload.enabled!==undefined?{enabled_before:!!previous.enabled,enabled_after:!!next.enabled}:{})}));
      return {id:typeId,code:previous.code,...next,configured:nextDefault!==null,...(defaults?{defaults}:{})};
    });
  }
  // Called inside employee creation. These are trusted server snapshots, never
  // client-provided provenance or a live template consulted during inference.
  async function creation(payload){
    if(payload.type_id===undefined)return null;
    if(!store.context.getStore())fail(503,'类型继承须在创建员工的事务内执行','database_transaction_required');
    if(typeof payload.type_id!=='string'||!payload.type_id||!version(payload.expected_type_version))fail(400,'请选择使用类型并核对版本');
    const type=await row(store,payload.type_id,' FOR SHARE');if(!type||!type.enabled)fail(400,'使用类型不存在或已停用');if(type.version!==payload.expected_type_version)fail(409,'使用类型配置已变化，请重新预览','member_type_conflict');
    const modelMode=payload.models_mode??'template',limitMode=payload.limits_mode??'template';if(![modelMode,limitMode].every(mode=>['template','custom'].includes(mode)))fail(400,'请选择类型默认或显式自定义');
    const inherited=modelMode==='template'||limitMode==='template',saved=await template(store,type);
    if(inherited&&(!saved||payload.expected_template_version!==type.default_version))fail(409,saved?'默认规则已变化，请重新预览':'此类型默认规则待配置，请明确自定义模型和限额','member_type_template_conflict');
    if(modelMode==='custom'&&!Object.hasOwn(payload,'allowed_models')||limitMode==='custom'&&!Object.hasOwn(payload,'usage_limits')||modelMode==='template'&&Object.hasOwn(payload,'allowed_models')||limitMode==='template'&&Object.hasOwn(payload,'usage_limits'))fail(400,'自定义字段与配置来源不一致');
    const allowed=modelMode==='template'?modelAllowlist(saved.allowed_models):await validateModels(payload.allowed_models),configuration=await limits.validate(limitMode==='template'?saved.usage_limits:payload.usage_limits);
    const source=mode=>mode==='template'?{kind:'type',type_id:type.id,template_version:type.default_version}:{kind:'custom'};
    return {type,allowed,limits:configuration,sources:{models:source(modelMode),limits:source(limitMode)}};
  }
  async function history(typeId,params){
    if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1)||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))fail(400,'模板历史分页无效');
    const after=Number(params.get('after')??0),limit=Number(params.get('limit')??25);if(!version(after)||!Number.isSafeInteger(limit)||limit<1||limit>100)fail(400,'模板历史分页无效');
    return read(()=>readStore.transaction(async()=>{
      const type=await row(readStore,typeId);if(!type)fail(404,'使用类型不存在');const at=clock(),window=usageWindow(at);
      const rows=await readStore.all('SELECT version,created,octet_length(allowed_models::text)+octet_length(usage_limits::text) bytes FROM harbor.member_type_templates WHERE type_id=$1 AND ($2::bigint=0 OR version<$2) AND created>=$3 AND created<$4 ORDER BY version DESC LIMIT $5',typeId,after,window.start,window.end,limit+1);
      let bytes=0;const selected=[];for(const entry of rows.slice(0,limit)){if(entry.bytes+1024>262144)fail(503,'单个模板超出历史读取容量','member_type_history_capacity');if(selected.length&&bytes+entry.bytes+1024>262144)break;bytes+=entry.bytes+1024;selected.push(entry.version);}
      const data=selected.length?await readStore.all('SELECT version,created,allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=ANY($2::bigint[]) ORDER BY version DESC',typeId,selected):[];
      return {type:{id:type.id,code:type.code,name:type.name,version:type.version,default_version:type.default_version},items:data.map(({allowed_models,usage_limits,...item})=>({...item,defaults:{allowed_models,usage_limits}})),after:selected.at(-1)??after,has_more:rows.length>selected.length,window,observedAt:at};
    },{level:'REPEATABLE READ',readOnly:true}));
  }
  async function handle(path,method,payload,params=new URLSearchParams()){
    if(path==='/api/admin/member-types'&&method==='GET')return {status:200,value:await list()};
    if(path==='/api/admin/member-types'&&method==='POST')return {status:201,value:await create(payload)};
    const versions=path.match(/^\/api\/admin\/member-types\/([a-zA-Z0-9-]{1,80})\/templates$/);if(versions&&method==='GET')return {status:200,value:await history(versions[1],params)};
    const match=path.match(/^\/api\/admin\/member-types\/([a-zA-Z0-9-]{1,80})$/);if(match&&method==='PATCH')return {status:200,value:await save(match[1],payload)};
    return null;
  }
  async function changeType(member,payload){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['type_id','expected_type_version','expected_usage_type_version','apply_defaults'].includes(key))||payload.apply_defaults!==false||!version(payload.expected_usage_type_version)||typeof payload.type_id!=='string'||!version(payload.expected_type_version))fail(400,'请选择使用类型、保留现有配置并提供版本');
    if(payload.expected_usage_type_version!==member.usage_type_version)fail(409,'员工使用类型已变化，请重新预览','member_type_conflict');
    const type=await row(store,payload.type_id,' FOR SHARE');if(!type||!type.enabled)fail(400,'使用类型不存在或已停用');if(type.version!==payload.expected_type_version)fail(409,'使用类型配置已变化，请重新预览','member_type_conflict');
    const retained=source=>({kind:'retained',...(source.type_id?{type_id:source.type_id,template_version:source.template_version}:{})});
    const sources={models:retained(member.configuration_source.models),limits:retained(member.configuration_source.limits)};
    await store.run('UPDATE harbor.members SET usage_type_id=$1,usage_type_version=usage_type_version+1,configuration_source=$2::jsonb,defaults_version=defaults_version+1 WHERE id=$3',type.id,JSON.stringify(sources),member.id);
    await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'member.type.change',JSON.stringify({member:member.id,from:member.usage_type_id,to:type.id,version:member.usage_type_version+1,apply_defaults:false}));
    return {type_id:type.id,usage_type_version:member.usage_type_version+1};
  }
  return {handle,list,creation,changeType};
}
