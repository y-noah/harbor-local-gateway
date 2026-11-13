import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {departmentPredicate} from './postgres-query-departments.mjs';
import {verifyUsageRollupAvailability,usageRollupsReady,usageRollupMoneySql} from './postgres-usage-rollups.mjs';
import {AppError,usageWindow,storedModelAllowlist,revealStoredKey} from './store.mjs';
import {fields,validPrice,joins,aggregate,referencePartialAggregateSql,referenceRollupAggregateSql} from './postgres-reference-query.mjs';
import {createPostgresMemberLimits,postgresMemberLimitConfiguration} from './postgres-member-limits.mjs';
import {readConsumptionPolicy} from './consumption-policy.mjs';
import {verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';

const fail=(status,message,code='member_query_invalid')=>{throw new AppError(status,message,code);};
export function memberQuery(params,{keys=false}={}){
  const allowed=keys?['page','page_size','q','member_id']:['page','page_size','q','type_id','status','view'];
  if([...params.keys()].some(key=>!allowed.includes(key)||params.getAll(key).length!==1))fail(400,'员工查询参数无效');
  const value={page:1,pageSize:20,q:params.get('q')??'',typeId:params.get('type_id')??'',status:params.get('status')??'',memberId:params.get('member_id')??''};
  if(params.has('view')){if(params.get('view')!=='list')fail(400,'员工查询视图无效');value.view='list';}
  for(const [key,field] of [['page','page'],['page_size','pageSize']])if(params.has(key)){if(!/^[1-9]\d*$/.test(params.get(key))||!Number.isSafeInteger(Number(params.get(key))))fail(400,'员工分页参数无效');value[field]=Number(params.get(key));}
  if(value.pageSize>100||value.q.length>100||value.q.includes('\0')||!['','enabled','disabled'].includes(value.status)||value.typeId&&!/^(?:unclassified|[a-zA-Z0-9-]{1,80})$/.test(value.typeId)||value.memberId&&!/^[a-f0-9-]{36}$/.test(value.memberId))fail(400,'员工查询参数无效');
  value.q=value.q.trim();return value;
}
export function memberConditions(query,{alias='m',start=1}={}){
  const values=[],where=[alias+'.removed_at IS NULL'],add=(sql,value)=>{values.push(value);where.push(sql.replace('?',String(start+values.length-1)));};
  if(query.q)add(`position(lower($?::text) in lower(${alias}.name))>0`,query.q);
  if(query.typeId==='unclassified')where.push(alias+'.usage_type_id IS NULL');else if(query.typeId)add(alias+'.usage_type_id=$?',query.typeId);
  if(query.status)add(alias+'.enabled=$?',query.status==='enabled'?1:0);
  return {where:where.join(' AND '),values};
}
const memberColumns='m.id,m.name,m.enabled,m.access_version,m.device_mode,m.device_policy_version,m.created,m.usage_limits,m.usage_limits_version,m.usage_type_id,m.usage_type_version,m.default_models,m.default_models_set,m.defaults_version,m.configuration_source,t.code usage_type_code,t.name usage_type_name,t.description usage_type_description';
export function createPostgresMemberQueries(store,{clock=Date.now,encryptionKey,queryQueue,pageBytes=524288,maxSelection=1000}={}){
  const departments=createAdminDepartments(store);
  let rollupInitialization,consumptionInitialization,consumptionAvailable=false;
  const limits=createPostgresMemberLimits(store,{clock}),read=(op,{signal,consumption=false}={})=>{const execute=async()=>{
    const error=await (rollupInitialization??=verifyUsageRollupAvailability(store).then(()=>null,error=>error));if(error)throw error;
    if(consumption){const initialized=await (consumptionInitialization??=verifyConsumptionPolicyAvailability(store).then(available=>({available}),error=>{consumptionInitialization=undefined;throw error;}));consumptionAvailable=initialized.available;}
    return store.transaction(op,{level:'REPEATABLE READ',readOnly:true});
  };return queryQueue?queryQueue.run(execute,{signal}):execute();};
  const consumptionView=row=>{if(!consumptionAvailable)return {};try{return readConsumptionPolicy(row,{allowLegacy:false});}catch{fail(503,'Key消耗来源配置无效','consumption_policy_invalid');}};
  if(!Number.isSafeInteger(pageBytes)||pageBytes<1024||pageBytes>2097152||!Number.isSafeInteger(maxSelection)||maxSelection<1||maxSelection>10000)fail(503,'员工页面字节预算无效','member_query_configuration');
  const empty=aggregate({requests:0,known:0,success:0,active:0,unmetered:0,unpriced:0,pending:0,expired:0,...Object.fromEntries(fields.map(field=>[field,'0']))}).standard;
  async function usage(scope,ids,at){
    if(!ids.length)return new Map();const window=usageWindow(at);
    const useRollups=await usageRollupsReady(store);
    const rollupSql=`WITH identity AS (
      SELECT r.${scope}_id owner_id,COUNT(DISTINCT r.ip) ips,COUNT(DISTINCT r.device_id) used_devices,
        COUNT(*) FILTER(WHERE r.device_auth_state IN ('compatible_unverified','historical_unknown')) unverified_requests,MAX(r.started) last_seen
      FROM harbor.requests r WHERE r.${scope}_id=ANY($1::text[]) AND r.usage_at>=$2 AND r.usage_at<$3 GROUP BY r.${scope}_id
    ),money AS (${usageRollupMoneySql(scope)}) SELECT i.*,m.* FROM identity i JOIN money m USING(owner_id)`;
    const rows=await store.all(useRollups?rollupSql:`WITH identity AS (
      SELECT r.${scope}_id owner_id,COUNT(DISTINCT r.ip) ips,COUNT(DISTINCT r.device_id) used_devices,
        COUNT(*) FILTER(WHERE r.device_auth_state IN ('compatible_unverified','historical_unknown')) unverified_requests,MAX(r.started) last_seen
      FROM harbor.requests r WHERE r.${scope}_id=ANY($1::text[]) AND r.usage_at>=$2 AND r.usage_at<$3 GROUP BY r.${scope}_id
    ), priced AS (
      SELECT r.${scope}_id owner_id,(r.usage_at>=$4 AND r.usage_at<$5) recent,r.price_version revision,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
        ${fields.map(field=>`CASE WHEN ${validPrice} THEN p.${field}_atoms END ${field}`).join(',')}
      FROM harbor.requests r${joins} WHERE r.${scope}_id=ANY($1::text[]) AND r.usage_at>=$2 AND r.usage_at<$3
    ), partial AS MATERIALIZED (
      SELECT owner_id,revision,recent,${referencePartialAggregateSql({standardOnly:true})} FROM priced GROUP BY owner_id,revision,recent
    ), money AS (
      SELECT owner_id,${referenceRollupAggregateSql({standardOnly:true})},${referenceRollupAggregateSql({standardOnly:true,condition:'recent',prefix:'recent_'})} FROM partial GROUP BY owner_id
    ) SELECT i.*,m.* FROM identity i JOIN money m USING(owner_id)`,ids,window.start,window.end,at-12*3600000,at);
    return new Map(rows.flatMap(row=>[
      [row.owner_id+':false',{standard:aggregate(row).standard,ips:row.ips,used_devices:row.used_devices,unverified_requests:row.unverified_requests,last_seen:row.last_seen}],
      [row.owner_id+':true',{standard:aggregate(Object.fromEntries(Object.entries(row).filter(([key])=>key.startsWith('recent_')).map(([key,value])=>[key.slice(7),value]))).standard}]
    ]));
  }
  const used=(data,id)=>({standard:data.get(id+':false')?.standard??empty,standard_12h:data.get(id+':true')?.standard??empty,ips:data.get(id+':false')?.ips??0,used_devices:data.get(id+':false')?.used_devices??0,unverified_requests:data.get(id+':false')?.unverified_requests??0,last_seen:data.get(id+':false')?.last_seen??null});
  async function registered(scope,ids){
    if(!ids.length)return new Map();
    const rows=scope==='member'?await store.all('SELECT member_id owner_id,COUNT(*) total FROM harbor.employee_devices WHERE member_id=ANY($1::text[]) GROUP BY member_id',ids):await store.all(`SELECT k.id owner_id,COUNT(d.id) total FROM harbor.keys k LEFT JOIN harbor.employee_devices d ON d.member_id=k.member_id AND (d.allowed_key_ids IS NULL OR d.allowed_key_ids ? k.id) AND (k.device_ids IS NULL OR k.device_ids ? d.id) WHERE k.id=ANY($1::text[]) GROUP BY k.id`,ids);
    return new Map(rows.map(row=>[row.owner_id,row.total]));
  }
  async function keyExpirations(ids,at){
    if(!ids.length)return new Map();
    const rows=await store.all(`SELECT member_id,
      COUNT(*) FILTER(WHERE expires IS NULL) permanent,
      COUNT(*) FILTER(WHERE expires IS NOT NULL AND expires<=$2) expired,
      MIN(expires) FILTER(WHERE expires>$2) next,
      MAX(expires) FILTER(WHERE expires IS NOT NULL AND expires<=$2) last_expired
      FROM harbor.keys WHERE member_id=ANY($1::text[]) AND removed_at IS NULL GROUP BY member_id`,ids,at);
    return new Map(rows.map(({member_id,...value})=>[member_id,value]));
  }
  async function member(row,data,at,deviceCounts,{metadata=false}={}){
    const {default_models,usage_limits,usage_limits_version,...view}=row,configuration=postgresMemberLimitConfiguration({usage_limits,usage_limits_version});
    return {...view,default_models:storedModelAllowlist(default_models),usage_limits:configuration,...(metadata?{}:{limit_usage:configuration.enabled?(await limits.status(row.id,at)).usage:[],...used(data,row.id)}),registered_devices:(deviceCounts??await registered('member',[row.id])).get(row.id)??0};
  }
  const budget=value=>{if(Buffer.byteLength(JSON.stringify(value))>pageBytes)fail(503,'完整员工页超过字节预算，请减小页大小','member_query_capacity');return value;};
  async function list(query,{signal,scope}={}){return read(async()=>{
    const trusted=await departments.resolve(scope),at=clock(),condition=memberConditions(query);condition.where+=' AND '+departmentPredicate(trusted,'m',condition.values);
    const typeArgs=[],typeScope=departmentPredicate(trusted,'t',typeArgs),types=await store.all('SELECT id,code,name,description,enabled FROM harbor.member_types t WHERE '+typeScope+' ORDER BY code,id',...typeArgs);
    if(query.typeId&&query.typeId!=='unclassified'&&!types.some(type=>type.id===query.typeId))fail(400,'使用类型不存在');
    const total=(await store.get('SELECT COUNT(*) total FROM harbor.members m WHERE '+condition.where,...condition.values)).total,totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=Math.min(query.page,totalPages),n=condition.values.length;
    const rows=await store.all(`SELECT ${memberColumns},
      (SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL) key_count,
      (SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL AND k.model_source->>'kind'='custom') key_exceptions
      FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE ${condition.where} ORDER BY m.created DESC,m.id DESC LIMIT $${n+1} OFFSET $${n+2}`,...condition.values,query.pageSize,(page-1)*query.pageSize);
    const metadata=query.view==='list',ids=rows.map(row=>row.id),data=metadata?null:await usage('member',ids,at),deviceCounts=await registered('member',ids),expirations=await keyExpirations(ids,at),items=[];for(const row of rows)items.push(await member({...row,key_expiration:expirations.get(row.id)??{permanent:0,expired:0,next:null,last_expired:null}},data,at,deviceCounts,{metadata}));
    return budget({rows:items,types,pagination:{page,pageSize:query.pageSize,total,totalPages},filters:query,observedAt:at,...(metadata?{usagePending:items.length>0}:{})});
  },{signal});}
  async function detail(memberId,{signal,scope}={}){return read(async()=>{
    const trusted=await departments.resolve(scope);if(trusted)departments.same(trusted,await store.get('SELECT id,department_id FROM harbor.members WHERE id=$1 AND removed_at IS NULL',memberId));
    const at=clock(),row=await store.get(`SELECT ${memberColumns},(SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL) key_count FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE m.id=$1 AND m.removed_at IS NULL`,memberId);if(!row)fail(404,'员工不存在或已移除','member_not_found');
    return budget(await member(row,await usage('member',[memberId],at),at));
  },{signal});}
  async function keys(query,{memberId=null,reveal=false,signal,scope}={}){return read(async()=>{
    const trusted=await departments.resolve(scope);
    if(trusted&&(memberId??query.memberId))departments.same(trusted,await store.get('SELECT * FROM harbor.members WHERE id=$1 AND removed_at IS NULL',memberId??query.memberId));
    const at=clock(),values=[],where=['k.removed_at IS NULL','m.removed_at IS NULL'];where.push(departmentPredicate(trusted,'k',values));const add=(sql,value)=>{values.push(value);where.push(sql.replaceAll('?',String(values.length)));};
    if(memberId??query.memberId)add('m.id=$?',memberId??query.memberId);
    if(query.q)add('(position(lower($?::text) in lower(m.name))>0 OR position(lower($?::text) in lower(k.prefix))>0)',query.q);
    const from=' FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id WHERE '+where.join(' AND ');
    const owner=memberId?await store.get(`SELECT ${memberColumns} FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE m.id=$1 AND m.removed_at IS NULL`,memberId):null;if(memberId&&!owner)fail(404,'员工不存在或已移除','member_not_found');
    const total=(await store.get('SELECT COUNT(*) total'+from,...values)).total,totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=Math.min(query.page,totalPages),n=values.length;
    const rows=await store.all(`SELECT k.id,k.member_id,k.prefix,k.expires,k.enabled,k.access_version,k.device_ids,k.device_policy_version,k.created,m.name member_name,m.enabled member_enabled${consumptionAvailable?',k.consumption_policy,k.consumption_policy_version':''}${reveal?',k.allowed_models,k.model_policy_version,k.model_source,k.digest,k.secret_cipher':''}${from} ORDER BY k.created DESC,k.id DESC LIMIT $${n+1} OFFSET $${n+2}`,...values,query.pageSize,(page-1)*query.pageSize),data=await usage('key',rows.map(row=>row.id),at),deviceCounts=await registered('key',rows.map(row=>row.id));
    return budget({rows:rows.map(({digest,secret_cipher,allowed_models,...row})=>({...row,...consumptionView(row),...(reveal?{allowed_models:storedModelAllowlist(allowed_models),full_key:revealStoredKey(encryptionKey,{id:row.id,digest,secret_cipher})}:{credentials_deferred:true}),...used(data,row.id),registered_devices:deviceCounts.get(row.id)??0})),...(owner?{member:await member(owner,await usage('member',[memberId],at),at)}:{}),pagination:{page,pageSize:query.pageSize,total,totalPages},observedAt:at});
  },{signal,consumption:true});}
  async function selection(query,{signal,scope}={}){return read(async()=>{
    const trusted=await departments.resolve(scope);if(trusted&&query.typeId&&query.typeId!=='unclassified')departments.same(trusted,await store.get('SELECT * FROM harbor.member_types WHERE id=$1',query.typeId));
    if(query.typeId&&query.typeId!=='unclassified'&&!await store.get('SELECT id FROM harbor.member_types WHERE id=$1',query.typeId))fail(400,'使用类型不存在');
    const condition=memberConditions(query);condition.where+=' AND '+departmentPredicate(trusted,'m',condition.values);const rows=await store.all('SELECT m.id,m.access_version,m.usage_type_version,m.defaults_version FROM harbor.members m WHERE '+condition.where+' ORDER BY m.id LIMIT $'+(condition.values.length+1),...condition.values,maxSelection+1);
    if(rows.length>maxSelection)fail(413,'筛选结果超过批量容量，请缩小范围','member_selection_capacity');
    return budget({members:rows,total:rows.length,filters:query,observedAt:clock()});
  },{signal});}
  async function keyPolicy(keyId,{signal,scope}={}){return read(async()=>{
    const trusted=await departments.resolve(scope);if(trusted)departments.same(trusted,await store.get('SELECT id,department_id FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId));
    const row=await store.get('SELECT k.allowed_models,m.usage_limits,m.usage_limits_version'+(consumptionAvailable?',k.id key_id,k.member_id,k.model_policy_version,k.consumption_policy,k.consumption_policy_version':'')+' FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id WHERE k.id=$1 AND k.removed_at IS NULL AND m.removed_at IS NULL',keyId);
    if(!row)fail(404,'Key不存在或已移除','key_not_found');
    return {allowed_models:storedModelAllowlist(row.allowed_models),usage_limits:postgresMemberLimitConfiguration(row),...(consumptionAvailable?{key_id:row.key_id,member_id:row.member_id,model_policy_version:row.model_policy_version,...consumptionView(row)}:{})};
  },{signal,consumption:true});}
  return {list,detail,keys,selection,keyPolicy};
}
