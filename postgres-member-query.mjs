import {AppError,usageWindow,storedModelAllowlist,revealStoredKey} from './store.mjs';
import {fields,validPrice,joins,aggregate,aggregateSql,referenceAggregateSql} from './postgres-reference-query.mjs';
import {createPostgresMemberLimits,postgresMemberLimitConfiguration} from './postgres-member-limits.mjs';

const fail=(status,message,code='member_query_invalid')=>{throw new AppError(status,message,code);};
export function memberQuery(params,{keys=false}={}){
  const allowed=keys?['page','page_size','q','member_id']:['page','page_size','q','type_id','status'];
  if([...params.keys()].some(key=>!allowed.includes(key)||params.getAll(key).length!==1))fail(400,'员工查询参数无效');
  const value={page:1,pageSize:25,q:params.get('q')??'',typeId:params.get('type_id')??'',status:params.get('status')??'',memberId:params.get('member_id')??''};
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
  const limits=createPostgresMemberLimits(store,{clock}),read=op=>queryQueue?queryQueue.run(()=>store.transaction(op,{level:'REPEATABLE READ',readOnly:true})):store.transaction(op,{level:'REPEATABLE READ',readOnly:true});
  if(!Number.isSafeInteger(pageBytes)||pageBytes<1024||pageBytes>2097152||!Number.isSafeInteger(maxSelection)||maxSelection<1||maxSelection>10000)fail(503,'员工页面字节预算无效','member_query_configuration');
  const empty=aggregate({requests:0,known:0,success:0,active:0,unmetered:0,unpriced:0,pending:0,expired:0,...Object.fromEntries(fields.map(field=>[field,'0']))}).standard;
  async function usage(scope,ids,at){
    if(!ids.length)return new Map();const window=usageWindow(at);
    const rows=await store.all(`WITH data AS (
      SELECT r.${scope}_id owner_id,r.ip,r.device_id,r.device_auth_state,r.started,r.ended,r.status,(r.usage_at>=$4 AND r.usage_at<$5) recent,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,CASE WHEN ${validPrice} THEN p.revision END revision,
        ${fields.map(field=>`CASE WHEN ${validPrice} THEN p.${field}_atoms END ${field}`).join(',')}
      FROM harbor.requests r${joins} WHERE r.${scope}_id=ANY($1::text[]) AND r.usage_at>=$2 AND r.usage_at<$3
    )
    SELECT owner_id,COUNT(DISTINCT ip) ips,COUNT(DISTINCT device_id) used_devices,COUNT(*) FILTER(WHERE device_auth_state IN ('compatible_unverified','historical_unknown')) unverified_requests,MAX(started) last_seen,${aggregateSql},${referenceAggregateSql({condition:'recent',prefix:'recent_'})} FROM data GROUP BY owner_id`,ids,window.start,window.end,at-12*3600000,at);
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
  async function member(row,data,at,deviceCounts){
    const {default_models,usage_limits,usage_limits_version,...view}=row,configuration=postgresMemberLimitConfiguration({usage_limits,usage_limits_version});
    return {...view,default_models:storedModelAllowlist(default_models),usage_limits:configuration,limit_usage:configuration.enabled?(await limits.status(row.id,at)).usage:[],...used(data,row.id),registered_devices:(deviceCounts??await registered('member',[row.id])).get(row.id)??0};
  }
  const budget=value=>{if(Buffer.byteLength(JSON.stringify(value))>pageBytes)fail(503,'完整员工页超过字节预算，请减小页大小','member_query_capacity');return value;};
  async function list(query){return read(async()=>{
    const at=clock(),condition=memberConditions(query),types=await store.all('SELECT id,code,name,description,enabled FROM harbor.member_types ORDER BY code,id');
    if(query.typeId&&query.typeId!=='unclassified'&&!types.some(type=>type.id===query.typeId))fail(400,'使用类型不存在');
    const total=(await store.get('SELECT COUNT(*) total FROM harbor.members m WHERE '+condition.where,...condition.values)).total,totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=Math.min(query.page,totalPages),n=condition.values.length;
    const rows=await store.all(`SELECT ${memberColumns},
      (SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL) key_count,
      (SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL AND k.model_source->>'kind'='custom') key_exceptions
      FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE ${condition.where} ORDER BY m.created DESC,m.id DESC LIMIT $${n+1} OFFSET $${n+2}`,...condition.values,query.pageSize,(page-1)*query.pageSize);
    const ids=rows.map(row=>row.id),data=await usage('member',ids,at),deviceCounts=await registered('member',ids),items=[];for(const row of rows)items.push(await member(row,data,at,deviceCounts));
    return budget({rows:items,types,pagination:{page,pageSize:query.pageSize,total,totalPages},filters:query,observedAt:at});
  });}
  async function detail(memberId){return read(async()=>{
    const at=clock(),row=await store.get(`SELECT ${memberColumns},(SELECT COUNT(*) FROM harbor.keys k WHERE k.member_id=m.id AND k.removed_at IS NULL) key_count FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE m.id=$1 AND m.removed_at IS NULL`,memberId);if(!row)fail(404,'员工不存在或已移除','member_not_found');
    return budget(await member(row,await usage('member',[memberId],at),at));
  });}
  async function keys(query,{memberId=null,reveal=false}={}){return read(async()=>{
    const at=clock(),values=[],where=['k.removed_at IS NULL','m.removed_at IS NULL'];const add=(sql,value)=>{values.push(value);where.push(sql.replaceAll('?',String(values.length)));};
    if(memberId??query.memberId)add('m.id=$?',memberId??query.memberId);
    if(query.q)add('(position(lower($?::text) in lower(m.name))>0 OR position(lower($?::text) in lower(k.prefix))>0)',query.q);
    const from=' FROM harbor.keys k JOIN harbor.members m ON m.id=k.member_id WHERE '+where.join(' AND ');
    const owner=memberId?await store.get(`SELECT ${memberColumns} FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE m.id=$1 AND m.removed_at IS NULL`,memberId):null;if(memberId&&!owner)fail(404,'员工不存在或已移除','member_not_found');
    const total=(await store.get('SELECT COUNT(*) total'+from,...values)).total,totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=Math.min(query.page,totalPages),n=values.length;
    const rows=await store.all(`SELECT k.id,k.member_id,k.prefix,k.expires,k.enabled,k.access_version,k.device_ids,k.device_policy_version,k.created,m.name member_name,m.enabled member_enabled${reveal?',k.allowed_models,k.model_policy_version,k.model_source,k.digest,k.secret_cipher':''}${from} ORDER BY k.created DESC,k.id DESC LIMIT $${n+1} OFFSET $${n+2}`,...values,query.pageSize,(page-1)*query.pageSize),data=await usage('key',rows.map(row=>row.id),at),deviceCounts=await registered('key',rows.map(row=>row.id));
    return budget({rows:rows.map(({digest,secret_cipher,allowed_models,...row})=>({...row,...(reveal?{allowed_models:storedModelAllowlist(allowed_models),full_key:revealStoredKey(encryptionKey,{id:row.id,digest,secret_cipher})}:{credentials_deferred:true}),...used(data,row.id),registered_devices:deviceCounts.get(row.id)??0})),...(owner?{member:await member(owner,await usage('member',[memberId],at),at)}:{}),pagination:{page,pageSize:query.pageSize,total,totalPages},observedAt:at});
  });}
  async function selection(query){return read(async()=>{
    if(query.typeId&&query.typeId!=='unclassified'&&!await store.get('SELECT id FROM harbor.member_types WHERE id=$1',query.typeId))fail(400,'使用类型不存在');
    const condition=memberConditions(query),rows=await store.all('SELECT m.id,m.access_version,m.usage_type_version,m.defaults_version FROM harbor.members m WHERE '+condition.where+' ORDER BY m.id LIMIT $'+(condition.values.length+1),...condition.values,maxSelection+1);
    if(rows.length>maxSelection)fail(413,'筛选结果超过批量容量，请缩小范围','member_selection_capacity');
    return budget({members:rows,total:rows.length,filters:query,observedAt:clock()});
  });}
  return {list,detail,keys,selection};
}
