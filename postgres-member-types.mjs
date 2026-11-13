import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {AppError,id,modelAllowlist,usageWindow} from './store.mjs';
import {name,bool} from './admin-input.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';

const fail=(status,message,code='invalid_member_type')=>{throw new AppError(status,message,code);};
const version=value=>Number.isSafeInteger(value)&&value>=0;
const description=value=>{if(typeof value!=='string'||value.trim().length>400)fail(400,'使用类型说明最多400字');return value.trim();};
function typeCode(number){let code='';for(;number>0;number=Math.floor((number-1)/26))code=String.fromCharCode(65+(number-1)%26)+code;return code;}
export function createPostgresMemberTypes(store,{readStore=store,clock=Date.now,queryQueue}={}){
  const departments=createAdminDepartments(store);
  const limits=createPostgresMemberLimits(store,{clock});
  const read=(operation,{signal}={})=>queryQueue?queryQueue.run(operation,{signal}):operation();
  const row=(target,typeId,lock='',scope=null)=>target.get('SELECT * FROM harbor.member_types WHERE id=$1'+(scope?.view==='department'?' AND department_id=$2':'')+lock,typeId,...(scope?.view==='department'?[scope.department_id]:[]));
  async function template(target,type){
    return type.default_version===null?null:await target.get('SELECT version,created,allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=$2'+(type.department_id?' AND department_id=$3':''),type.id,type.default_version,...(type.department_id?[type.department_id]:[]));
  }
  async function knownModels(target,scope){
    const current=await target.get("SELECT p.book FROM harbor.gateway_settings s JOIN harbor.pricing_versions p ON p.id=(s.value::jsonb->>'active') WHERE s.name='pricing.state'");
    const book=current&&JSON.parse(current.book),platforms=await target.all("SELECT kind,platform_models FROM harbor.accounts WHERE removed_at IS NULL AND auth_mode='platform'"+(scope?.view==='department'?' AND department_id=$1':''),...(scope?.view==='department'?[scope.department_id]:[]));
    const known=new Set(Object.keys(book?.rates||{}));
    for(const account of platforms){let models;try{models=JSON.parse(account.platform_models);}catch{}for(const model of Array.isArray(models)?models:[]){const value=typeof model==='string'?model:model?.id;if(typeof value==='string')known.add(account.kind+'/'+value);}}
    return known;
  }
  async function validateModels(value,{scope=null}={}){
    const allowed=modelAllowlist(value);if(allowed===null)return null;
    const known=await knownModels(store,scope);
    if(allowed.some(model=>!known.has(model)))fail(400,'请选择本地模型目录中的具体模型ID','member_type_model_unknown');return allowed;
  }
  async function list({signal,scope}={}){
    const trusted=await departments.resolve(scope);
    return read(()=>readStore.transaction(async()=>{
      const rows=await readStore.all(`SELECT t.*,p.created configured_at,p.allowed_models,p.usage_limits,
        (SELECT COUNT(*) FROM harbor.members m WHERE m.usage_type_id=t.id AND m.removed_at IS NULL${trusted?' AND m.department_id=t.department_id':''}) members
        FROM harbor.member_types t LEFT JOIN harbor.member_type_templates p ON p.type_id=t.id AND p.version=t.default_version${trusted?' AND p.department_id=t.department_id':''}${trusted?.view==='department'?' WHERE t.department_id=$1':''} ORDER BY t.code,t.id`,...(trusted?.view==='department'?[trusted.department_id]:[]));
      return {models:[...await knownModels(readStore,trusted)].sort(),types:rows.map(({allowed_models,usage_limits,...type})=>({...type,configured:type.default_version!==null,defaults:type.default_version===null?null:{allowed_models,usage_limits}}))};
    },{level:'REPEATABLE READ',readOnly:true}),{signal});
  }
  async function create(payload,{scope}={}){
    const trusted=await departments.resolve(scope,{write:true});
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['name','description'].includes(key)))fail(400,'使用类型字段无效');
    const title=name(payload.name),detail=description(payload.description??'');
    return store.transaction(async()=>{
      await departments.current(trusted,{lock:true,write:true});
      await store.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',['harbor.member-types.create'+(trusted?':'+trusted.department_id:'')]);
      const used=new Set((await store.all('SELECT code FROM harbor.member_types'+(trusted?' WHERE department_id=$1':''),...(trusted?[trusted.department_id]:[]))).map(row=>row.code));
      let ordinal=1;while(used.has(typeCode(ordinal)))ordinal++;
      const value={id:id(),code:typeCode(ordinal),name:title,description:detail,enabled:1,version:0,default_version:null,updated:clock()};
      await store.run('INSERT INTO harbor.member_types(id,code,name,description,enabled,version,default_version,updated'+(trusted?',department_id':'')+') VALUES($1,$2,$3,$4,$5,$6,$7,$8'+(trusted?',$9':'')+')',value.id,value.code,value.name,value.description,value.enabled,value.version,value.default_version,value.updated,...(trusted?[trusted.department_id]:[]));
      await createPostgresAudit(store,{clock}).write(trusted,{action:'member.type.create',detail:{type:value.id,code:value.code},subject:{kind:'type',id:value.id}});
      return {...value,configured:false,defaults:null,members:0};
    });
  }
  async function save(typeId,payload,{scope}={}){
    const trusted=await departments.resolve(scope,{write:true});
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['name','description','enabled','defaults','restore_version','expected_version'].includes(key))||!version(payload.expected_version)||!['name','description','enabled','defaults','restore_version'].some(key=>Object.hasOwn(payload,key)))fail(400,'使用类型配置字段无效');
    if(Object.hasOwn(payload,'restore_version')&&(!version(payload.restore_version)||payload.restore_version<1||Object.keys(payload).some(key=>!['restore_version','expected_version'].includes(key))))fail(400,'恢复版本须单独提交，不可混合其他配置');
    return store.transaction(async()=>{
      await departments.current(trusted,{lock:true,write:true});
      const previous=await row(store,typeId,' FOR UPDATE',trusted);if(!previous)fail(404,'使用类型不存在');if(previous.version!==payload.expected_version)fail(409,'使用类型配置已变化，请重新核对','member_type_conflict');
      let nextDefault=previous.default_version,defaults,restored;
      if(payload.restore_version!==undefined){const window=usageWindow(clock());restored=await store.get('SELECT allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=$2 AND created>=$3 AND created<$4'+(trusted?' AND department_id=$5':''),typeId,payload.restore_version,window.start,window.end,...(trusted?[trusted.department_id]:[]));if(!restored)fail(404,'历史模板不存在或超出近7天范围','member_type_template_not_found');}
      if(payload.defaults!==undefined||restored){const input=restored||payload.defaults;if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).some(k=>!['allowed_models','usage_limits'].includes(k))||!Object.hasOwn(input,'allowed_models')||!Object.hasOwn(input,'usage_limits'))fail(400,'默认模型和员工限额均须明确配置');
        defaults={allowed_models:await validateModels(input.allowed_models,{scope:trusted}),usage_limits:await limits.validate(input.usage_limits)};nextDefault=(previous.default_version??0)+1;
        await store.run('INSERT INTO harbor.member_type_templates(type_id,version,created,allowed_models,usage_limits'+(trusted?',department_id':'')+') VALUES($1,$2,$3,$4::jsonb,$5::jsonb'+(trusted?',$6':'')+')',typeId,nextDefault,clock(),JSON.stringify(defaults.allowed_models),JSON.stringify(defaults.usage_limits),...(trusted?[trusted.department_id]:[]));
      }
      const next={name:payload.name===undefined?previous.name:name(payload.name),description:payload.description===undefined?previous.description:description(payload.description),enabled:payload.enabled===undefined?previous.enabled:bool(payload.enabled),version:previous.version+1,default_version:nextDefault,updated:clock()};
      await store.run('UPDATE harbor.member_types SET name=$1,description=$2,enabled=$3,version=$4,default_version=$5,updated=$6 WHERE id=$7',next.name,next.description,next.enabled,next.version,nextDefault,next.updated,typeId);
      await createPostgresAudit(store,{clock}).write(trusted,{action:'member.type.configure',detail:{type:typeId,code:previous.code,version:next.version,template:nextDefault,previous:previous.default_version,...(restored?{restored_from:payload.restore_version}:{}),...(payload.name!==undefined?{name_changed:next.name!==previous.name}:{}),...(payload.description!==undefined?{description_changed:next.description!==previous.description}:{}),...(payload.enabled!==undefined?{enabled_before:!!previous.enabled,enabled_after:!!next.enabled}:{})},subject:{kind:'type',id:typeId}});
      return {id:typeId,code:previous.code,...next,configured:nextDefault!==null,...(defaults?{defaults}:{})};
    });
  }
  // Called inside employee creation. These are trusted server snapshots, never
  // client-provided provenance or a live template consulted during inference.
  async function creation(payload,{scope}={}){
    const trusted=await departments.resolve(scope,{write:true});
    if(payload.type_id===undefined)return null;
    if(!store.context.getStore())fail(503,'类型继承须在创建员工的事务内执行','database_transaction_required');
    if(payload.type_id===null){
      if(payload.models_mode!=='custom'||payload.limits_mode!=='custom'||!Object.hasOwn(payload,'allowed_models')||!Object.hasOwn(payload,'usage_limits')||payload.expected_type_version!==undefined||payload.expected_template_version!==undefined)fail(400,'未分类员工须明确自定义模型和限额');
      return {type:null,allowed:await validateModels(payload.allowed_models,{scope:trusted}),limits:await limits.validate(payload.usage_limits),sources:{models:{kind:'custom'},limits:{kind:'custom'}}};
    }
    if(typeof payload.type_id!=='string'||!payload.type_id||!version(payload.expected_type_version))fail(400,'请选择使用类型并核对版本');
    const type=await row(store,payload.type_id,' FOR SHARE',trusted);if(type)departments.same(trusted,type);if(!type)fail(404,'使用类型不存在或无权访问','department_object_not_found');if(!type.enabled)fail(400,'使用类型不存在或已停用');if(type.version!==payload.expected_type_version)fail(409,'使用类型配置已变化，请重新预览','member_type_conflict');
    const modelMode=payload.models_mode??'template',limitMode=payload.limits_mode??'template';if(![modelMode,limitMode].every(mode=>['template','custom'].includes(mode)))fail(400,'请选择类型默认或显式自定义');
    const inherited=modelMode==='template'||limitMode==='template',saved=await template(store,type);
    if(inherited&&(!saved||payload.expected_template_version!==type.default_version))fail(409,saved?'默认规则已变化，请重新预览':'此类型默认规则待配置，请明确自定义模型和限额','member_type_template_conflict');
    if(modelMode==='custom'&&!Object.hasOwn(payload,'allowed_models')||limitMode==='custom'&&!Object.hasOwn(payload,'usage_limits')||modelMode==='template'&&Object.hasOwn(payload,'allowed_models')||limitMode==='template'&&Object.hasOwn(payload,'usage_limits'))fail(400,'自定义字段与配置来源不一致');
    const allowed=modelMode==='template'?modelAllowlist(saved.allowed_models):await validateModels(payload.allowed_models,{scope:trusted}),configuration=await limits.validate(limitMode==='template'?saved.usage_limits:payload.usage_limits);
    const source=mode=>mode==='template'?{kind:'type',type_id:type.id,template_version:type.default_version}:{kind:'custom'};
    return {type,allowed,limits:configuration,sources:{models:source(modelMode),limits:source(limitMode)}};
  }
  async function history(typeId,params,{signal,scope}={}){
    const trusted=await departments.resolve(scope);
    if([...params.keys()].some(key=>!['after','limit'].includes(key)||params.getAll(key).length!==1)||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))fail(400,'模板历史分页无效');
    const after=Number(params.get('after')??0),limit=Number(params.get('limit')??20);if(!version(after)||![20,50,100].includes(limit))fail(400,'模板历史分页无效');
    return read(()=>readStore.transaction(async()=>{
      const type=await row(readStore,typeId,'',trusted);if(!type)fail(404,'使用类型不存在');const at=clock(),window=usageWindow(at);
      const rows=await readStore.all('SELECT version,created,octet_length(allowed_models::text)+octet_length(usage_limits::text) bytes FROM harbor.member_type_templates WHERE type_id=$1 AND ($2::bigint=0 OR version<$2) AND created>=$3 AND created<$4'+(trusted?.view==='department'?' AND department_id=$6':'')+' ORDER BY version DESC LIMIT $5',typeId,after,window.start,window.end,limit+1,...(trusted?.view==='department'?[trusted.department_id]:[]));
      let bytes=0;const selected=[];for(const entry of rows.slice(0,limit)){if(entry.bytes+1024>262144)fail(503,'单个模板超出历史读取容量','member_type_history_capacity');if(selected.length&&bytes+entry.bytes+1024>262144)break;bytes+=entry.bytes+1024;selected.push(entry.version);}
      const data=selected.length?await readStore.all('SELECT version,created,allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 AND version=ANY($2::bigint[])'+(trusted?.view==='department'?' AND department_id=$3':'')+' ORDER BY version DESC',typeId,selected,...(trusted?.view==='department'?[trusted.department_id]:[])):[];
      return {type:{id:type.id,code:type.code,name:type.name,version:type.version,default_version:type.default_version},items:data.map(({allowed_models,usage_limits,...item})=>({...item,defaults:{allowed_models,usage_limits}})),after:selected.at(-1)??after,has_more:rows.length>selected.length,window,observedAt:at};
    },{level:'REPEATABLE READ',readOnly:true}),{signal});
  }
  async function handle(path,method,payload,params=new URLSearchParams(),{signal,scope}={}){
    if(path==='/api/admin/member-types'&&method==='GET')return {status:200,value:await list({signal,scope})};
    if(path==='/api/admin/member-types'&&method==='POST')return {status:201,value:await create(payload,{scope})};
    const versions=path.match(/^\/api\/admin\/member-types\/([a-zA-Z0-9-]{1,80})\/templates$/);if(versions&&method==='GET')return {status:200,value:await history(versions[1],params,{signal,scope})};
    const match=path.match(/^\/api\/admin\/member-types\/([a-zA-Z0-9-]{1,80})$/);if(match&&method==='PATCH')return {status:200,value:await save(match[1],payload,{scope})};
    return null;
  }
  async function changeType(member,payload,{scope}={}){
    const trusted=await departments.resolve(scope,{write:true});departments.same(trusted,member);
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['type_id','expected_type_version','expected_usage_type_version','apply_defaults'].includes(key))||payload.apply_defaults!==false||!version(payload.expected_usage_type_version)||typeof payload.type_id!=='string'||!version(payload.expected_type_version))fail(400,'请选择使用类型、保留现有配置并提供版本');
    if(payload.expected_usage_type_version!==member.usage_type_version)fail(409,'员工使用类型已变化，请重新预览','member_type_conflict');
    const type=await row(store,payload.type_id,' FOR SHARE',trusted);if(type)departments.same(trusted,type);if(!type)fail(404,'使用类型不存在或无权访问','department_object_not_found');if(!type.enabled)fail(400,'使用类型不存在或已停用');if(type.version!==payload.expected_type_version)fail(409,'使用类型配置已变化，请重新预览','member_type_conflict');
    const retained=source=>({kind:'retained',...(source.type_id?{type_id:source.type_id,template_version:source.template_version}:{})});
    const sources={models:retained(member.configuration_source.models),limits:retained(member.configuration_source.limits)};
    await store.run('UPDATE harbor.members SET usage_type_id=$1,usage_type_version=usage_type_version+1,configuration_source=$2::jsonb,defaults_version=defaults_version+1 WHERE id=$3',type.id,JSON.stringify(sources),member.id);
    await createPostgresAudit(store,{clock}).write(trusted,{action:'member.type.change',detail:{member:member.id,from:member.usage_type_id,to:type.id,version:member.usage_type_version+1,apply_defaults:false},subject:{kind:'member',id:member.id}});
    return {type_id:type.id,usage_type_version:member.usage_type_version+1};
  }
  return {handle,list,creation,changeType};
}
