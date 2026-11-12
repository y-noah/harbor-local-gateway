import {AppError,id,token,hash,modelAllowlist,storedModelAllowlist,encryptStoredKey} from './store.mjs';
import {integer,name,expiry,bool} from './admin-input.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';
import {createPostgresMemberTypes} from './postgres-member-types.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';
import {createPostgresMemberBatches} from './postgres-member-batches.mjs';
import {createPostgresMemberActions} from './postgres-member-actions.mjs';
import {createPostgresMemberQueries,memberQuery} from './postgres-member-query.mjs';
import {normalizeConsumptionPolicy,normalizeConsumptionPolicyVersion} from './consumption-policy.mjs';
import {createPostgresConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';

function consumptionInput(payload,{create=false}={}){
  const invalid=()=>{throw new AppError(400,'消耗来源和预期版本须按操作成对提供','consumption_policy_invalid');};
  if(!payload||typeof payload!=='object'||Array.isArray(payload))invalid();
  const policy=Object.hasOwn(payload,'consumption_policy'),expected=Object.hasOwn(payload,'expected_consumption_policy_version');
  if(Object.hasOwn(payload,'consumption_policy_version')||create&&expected||!create&&policy!==expected)invalid();
  if(!policy)return undefined;
  if(payload.consumption_policy===undefined)invalid();
  return {consumption_policy:normalizeConsumptionPolicy(payload.consumption_policy),...(create?{}:{expected_consumption_policy_version:normalizeConsumptionPolicyVersion(payload.expected_consumption_policy_version)})};
}

// All permission mutations lock member before Key, matching admission and
// settlement. No lock crosses request-body reading or upstream execution.
export function createPostgresMemberAdmin(store,{encryptionKey,readStore=store,clock=Date.now,isBusy=()=>false,queryQueue=createPostgresQueryQueue(),consumptionReady}={}){
  const types=createPostgresMemberTypes(store,{readStore,clock,queryQueue});
  const batches=createPostgresMemberBatches(store,{readStore,clock,queryQueue});
  const actions=createPostgresMemberActions(store,{readStore,clock,queryQueue});
  const queries=createPostgresMemberQueries(readStore,{clock,encryptionKey,queryQueue});
  const limits=createPostgresMemberLimits(store,{clock}),readLimits=createPostgresMemberLimits(readStore,{clock});
  if(consumptionReady!==undefined&&typeof consumptionReady!=='function')throw new AppError(503,'消耗来源运行就绪依赖无效','consumption_runtime_not_ready');
  const consumption=createPostgresConsumptionPolicy(store,{clock});
  const requireConsumptionReady=async(source,available)=>{
    if(!source||consumptionReady===undefined||await consumptionReady()===true)return;
    if(!(available??await verifyConsumptionPolicyAvailability(store)))throw new AppError(503,'请先安装Key消耗来源迁移','database_migration_required');
    throw new AppError(503,'消耗来源扩展已安装，当前网关尚未启用；请受控重启后重试','consumption_runtime_not_ready');
  };
  const audit=(action,detail)=>store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),action,typeof detail==='string'?detail:JSON.stringify(detail));
  const activeMember=async memberId=>{
    const row=await store.get('SELECT * FROM harbor.members WHERE id=$1 AND removed_at IS NULL FOR UPDATE',memberId);
    if(!row)throw new AppError(404,'员工不存在或已移除');return row;
  };
  const keyMember=async keyId=>{
    const owner=await store.get('SELECT member_id FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId);if(!owner)throw new AppError(404,'Key 不存在或已删除');
    await activeMember(owner.member_id);
    const key=await store.get('SELECT * FROM harbor.keys WHERE id=$1 AND removed_at IS NULL FOR UPDATE',keyId);if(!key)throw new AppError(404,'Key 不存在或已删除');return key;
  };
  async function createKey(memberId,expires,quota,allowedModels,expectedDefaults,source){
    if(!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32)throw new AppError(503,'员工凭据加密材料尚未配置','key_configuration');
    const consumptionAvailable=await verifyConsumptionPolicyAvailability(store);
    if(source&&!consumptionAvailable)throw new AppError(503,'请先安装Key消耗来源迁移','database_migration_required');
    await requireConsumptionReady(source,consumptionAvailable);
    const owner=await activeMember(memberId),inherited=allowedModels===undefined&&!!owner.default_models_set;
    if(expectedDefaults!==undefined){if(!Number.isSafeInteger(expectedDefaults)||expectedDefaults<0)throw new AppError(400,'员工默认版本无效');if(expectedDefaults!==owner.defaults_version)throw new AppError(409,'员工默认已变化，请重新打开新增Key并核对','employee_defaults_conflict');}
    if(allowedModels===undefined)allowedModels=inherited?storedModelAllowlist(owner.default_models):null;
    const modelSource=inherited?{...owner.configuration_source.models,kind:'employee',defaults_version:owner.defaults_version}:{kind:owner.default_models_set?'custom':'legacy'},keyId=id(),secret=token();
    const policy=source?.consumption_policy??'inherit';
    await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,expires,created,quota,secret_cipher,allowed_models,model_source'+(consumptionAvailable?',consumption_policy,consumption_policy_version':'')+') VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb'+(consumptionAvailable?',$11,0':'')+')',
      keyId,memberId,hash(secret),secret.slice(0,11),expires,clock(),quota,encryptStoredKey(encryptionKey,secret,keyId),allowedModels===null?null:JSON.stringify(allowedModels),JSON.stringify(modelSource),...(consumptionAvailable?[policy]:[]));
    await audit('key.create',keyId);if(allowedModels!==null)await audit('key.models',{key:keyId,allowed_models:allowedModels});
    if(source)await audit('key.consumption_policy',{key:keyId,member:memberId,previous:null,consumption_policy:policy,consumption_policy_version:0});
    return {keyId,secret,...(consumptionAvailable?{consumption_policy:policy,consumption_policy_version:0}:{})};
  }
  async function handle(path,method,payload,params,{signal}={}){
    if(method==='GET'){
      params??=new URLSearchParams();
      if(path==='/api/admin/members')return {status:200,value:await queries.list(memberQuery(params),{signal})};
      if(path==='/api/admin/members/selection'){if(params.has('view'))throw new AppError(400,'筛选选择不接受视图参数');return {status:200,value:await queries.selection(memberQuery(params),{signal})};}
      if(path==='/api/admin/keys')return {status:200,value:await queries.keys(memberQuery(params,{keys:true}),{signal})};
      const copyPolicy=path.match(/^\/api\/admin\/keys\/([a-f0-9-]{36})\/policy$/);
      if(copyPolicy){if([...params.keys()].length)throw new AppError(400,'Key权限摘要不接受查询参数');return {status:200,value:await queries.keyPolicy(copyPolicy[1],{signal})};}
      const detail=path.match(/^\/api\/admin\/members\/([a-f0-9-]{36})(\/keys)?$/);
      if(detail){if(!detail[2]&&[...params.keys()].length)throw new AppError(400,'员工详情不接受查询参数');return {status:200,value:detail[2]?await queries.keys(memberQuery(params,{keys:true}),{memberId:detail[1],reveal:true,signal}):await queries.detail(detail[1],{signal})};}
    }
    const actionResult=await actions.handle(path,method,payload,params,method==='GET'?{signal}:{});if(actionResult)return actionResult;
    const batchResult=await batches.handle(path,method,payload,params,method==='GET'?{signal}:{});if(batchResult)return batchResult;
    const typeResult=await types.handle(path,method,payload,params,method==='GET'?{signal}:{});if(typeResult)return typeResult;
    if(path==='/api/admin/members'&&method==='POST'){
      const b=payload,source=consumptionInput(b,{create:true}),memberName=name(b.name),quota=integer(b.quota,'额度',{nullable:true}),expires=expiry(b.expires),allowed=b.type_id===undefined?modelAllowlist(b.allowed_models===undefined?null:b.allowed_models):undefined;
      return {status:201,value:await store.transaction(async()=>{
        const configuration=await types.creation(b),memberId=id();
        await store.run('INSERT INTO harbor.members(id,name,quota,created,usage_type_id,usage_type_version,default_models,default_models_set,defaults_version,configuration_source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)',memberId,memberName,quota,clock(),configuration?.type.id??null,configuration?1:0,configuration?.allowed===undefined||configuration.allowed===null?null:JSON.stringify(configuration.allowed),configuration?1:0,configuration?1:0,JSON.stringify(configuration?.sources||{models:{kind:'legacy'},limits:{kind:'legacy'}}));
        if(configuration)await limits.save(memberId,{...configuration.limits,expected_version:0},{source:configuration.sources.limits});
        const key=await createKey(memberId,expires,null,allowed,undefined,source);await audit('member.create',memberName);return {memberId,...key};
      })};
    }
    const typeMatch=path.match(/^\/api\/admin\/members\/([a-f0-9-]+)\/type$/);
    if(typeMatch&&method==='PATCH')return {status:200,value:await store.transaction(async()=>types.changeType(await activeMember(typeMatch[1]),payload))};
    const limitMatch=path.match(/^\/api\/admin\/members\/([a-f0-9-]+)\/usage-limits$/);
    if(limitMatch&&['GET','PATCH'].includes(method)){
      return {status:200,value:await queryQueue.run(async()=>{if(method==='PATCH')await limits.save(limitMatch[1],payload);return readLimits.status(limitMatch[1]);},method==='GET'?{signal}:{})};
    }
    const memberMatch=path.match(/^\/api\/admin\/members\/([a-f0-9-]+)$/);
    if(memberMatch&&['PATCH','DELETE'].includes(method))return {status:200,value:await store.transaction(async()=>{
      const member=await activeMember(memberMatch[1]),b=payload;
      if(b&&['consumption_policy','consumption_policy_version','expected_consumption_policy_version'].some(field=>Object.hasOwn(b,field)))throw new AppError(400,'请在对应Key中设置消耗来源','consumption_policy_invalid');
      if(method==='DELETE'){
        if(isBusy(member.id)||await store.get("SELECT id FROM harbor.requests WHERE member_id=$1 AND status IN ('queued','running') LIMIT 1",member.id))throw new AppError(409,'该员工仍有请求正在准备、排队或处理，请完成后再移除');
        const at=clock();await store.run('UPDATE harbor.members SET enabled=0,removed_at=$1 WHERE id=$2',at,member.id);
        await store.run("UPDATE harbor.keys SET enabled=0,secret_cipher=NULL,removed_at=COALESCE(removed_at,$1),digest='removed:'||id WHERE member_id=$2",at,member.id);
        await store.run("UPDATE harbor.sessions SET state='uncertain',updated=$1 WHERE member_id=$2",at,member.id);await audit('member.remove',member.id);
      }else{
        await store.run('UPDATE harbor.members SET name=$1,quota=$2,enabled=$3 WHERE id=$4',b.name===undefined?member.name:name(b.name),b.quota===undefined?member.quota:integer(b.quota,'额度',{nullable:true}),b.enabled===undefined?member.enabled:bool(b.enabled),member.id);await audit('member.update',member.id);
      }
      return {ok:true};
    })};
    if(path==='/api/admin/keys'&&method==='POST'){
      const source=consumptionInput(payload,{create:true});
      return {status:201,value:await store.transaction(()=>createKey(payload.memberId,expiry(payload.expires),payload.quota===undefined?null:integer(payload.quota,'Key 额度',{nullable:true}),payload.allowed_models===undefined?undefined:modelAllowlist(payload.allowed_models),payload.expected_defaults_version,source))};
    }
    const keyMatch=path.match(/^\/api\/admin\/keys\/([a-f0-9-]+)$/);
    if(keyMatch&&['PATCH','DELETE'].includes(method))return {status:200,value:await store.transaction(async()=>{
      const key=await keyMember(keyMatch[1]),b=payload,source=consumptionInput(b);
      if(method==='DELETE'){
        if(source)throw new AppError(400,'删除Key不接受消耗来源设置','consumption_policy_invalid');
        if(isBusy(key.member_id)||await store.get("SELECT id FROM harbor.requests WHERE key_id=$1 AND status IN ('queued','running') LIMIT 1",key.id))throw new AppError(409,'该员工仍有请求正在准备、排队或处理，请完成后再删除');
        await store.run('UPDATE harbor.keys SET enabled=0,secret_cipher=NULL,removed_at=$1,digest=$2 WHERE id=$3',clock(),'removed:'+key.id,key.id);await audit('key.remove',key.id);
      }else{
        const changesModels=b.allowed_models!==undefined;
        if(changesModels&&b.expected_model_policy_version!==key.model_policy_version)throw new AppError(409,'模型权限已变化，请重新打开设置','model_policy_conflict');
        const allowed=changesModels?(b.allowed_models===null?null:JSON.stringify(modelAllowlist(b.allowed_models))):key.allowed_models;
        await requireConsumptionReady(source);
        const sourceResult=source?await consumption.set(key.id,source):undefined;
        await store.run('UPDATE harbor.keys SET enabled=$1,expires=$2,quota=$3,allowed_models=$4,model_policy_version=$5,model_source=$6::jsonb WHERE id=$7',b.enabled===undefined?key.enabled:bool(b.enabled),b.expires===undefined?key.expires:expiry(b.expires),b.quota===undefined?key.quota:integer(b.quota,'Key 额度',{nullable:true}),allowed,key.model_policy_version+(changesModels?1:0),JSON.stringify(changesModels?{kind:'custom'}:key.model_source),key.id);
        await audit('key.update',key.id);if(changesModels)await audit('key.models',{key:key.id,allowed_models:b.allowed_models});
        if(sourceResult)return {ok:true,...sourceResult};
      }
      return {ok:true};
    })};
    return null;
  }
  return {handle,close:()=>queryQueue.close()};
}
