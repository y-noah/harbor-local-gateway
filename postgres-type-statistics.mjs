import {createHash} from 'node:crypto';
import {AppError,usageWindow} from './store.mjs';
import {fields,validPrice,joins,aggregate,aggregateSql} from './postgres-reference-query.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';

const states=new Set(['queued','running','ok','failed','unknown','interrupted','cancelled','rejected']);
const invalid=(message='使用类型统计筛选无效')=>{throw new AppError(400,message,'type_statistics_invalid');};
const capacity=()=>{throw new AppError(503,'完整统计超过资源预算，请缩小范围或页大小','type_statistics_capacity');};
const names={group_by:'groupBy',page_size:'pageSize',model_missing:'modelMissing',member_missing:'memberMissing'};
const allowed=new Set(['type','member','model','status','from','to','groupBy','page','pageSize','modelMissing','memberMissing']);
function normalize(value={}){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!allowed.has(key)))invalid();
  const query={type:'',member:'',model:'',status:'',from:null,to:null,groupBy:'type',page:1,pageSize:25,modelMissing:false,memberMissing:false,...value};
  if([query.type,query.member,query.model,query.status,query.groupBy].some(v=>typeof v!=='string')||query.type&&!/^[a-zA-Z0-9-]{1,100}$/.test(query.type)&&!['unclassified','historical_unknown'].includes(query.type)||query.member&&!/^[a-zA-Z0-9-]{1,100}$/.test(query.member)||query.model.length>200||/[\x00-\x1f]/.test(query.model)||query.status&&!states.has(query.status)||!['type','model','member'].includes(query.groupBy))invalid();
  if(!Number.isSafeInteger(query.page)||query.page<1||query.page>1e7||!Number.isSafeInteger(query.pageSize)||query.pageSize<1||query.pageSize>100||[query.from,query.to].some(v=>v!==null&&(!Number.isSafeInteger(v)||v<0))||query.from!==null&&query.to!==null&&query.from>=query.to||typeof query.modelMissing!=='boolean'||typeof query.memberMissing!=='boolean'||query.modelMissing&&!!query.model||query.memberMissing&&!!query.member)invalid();
  return query;
}
export function typeStatisticsQuery(params){
  const query={};
  for(const [key,value] of params){const field=names[key]??key;if(!allowed.has(field)||params.getAll(key).length!==1||Object.hasOwn(query,field))invalid();
    if(['page','pageSize','from','to'].includes(field)){if(!/^\d+$/.test(value)||!Number.isSafeInteger(Number(value)))invalid();query[field]=Number(value);}
    else if(['modelMissing','memberMissing'].includes(field)){if(!['true','false'].includes(value))invalid();query[field]=value==='true';}
    else query[field]=value;
  }
  return normalize(query);
}
// Uses request-time identity only. Callers can reuse these exact conditions for
// list/export drill-down; current employee classification never enters SQL.
export function typeStatisticsConditions(value,{at=Date.now(),alias='r',start=1}={}){
  const query=normalize(value);if(!Number.isSafeInteger(at)||at<0||!Number.isSafeInteger(start)||start<1||start>1000||!/^[a-z][a-z0-9_]{0,20}$/.test(alias))invalid();
  const visible=usageWindow(at),window={start:query.from??visible.start,end:query.to??visible.end};
  if(window.start<visible.start||window.end>visible.end||window.start>=window.end)invalid('统计仅支持近7个北京时间自然日');
  const values=[window.start,window.end],where=[`${alias}.usage_at>=$${start}`,`${alias}.usage_at<$${start+1}`],add=(sql,v)=>{values.push(v);where.push(sql+'$'+(start+values.length-1));};
  if(query.type==='unclassified')where.push(`${alias}.usage_type_id IS NULL AND ${alias}.usage_type_version IS NOT NULL`);
  else if(query.type==='historical_unknown')where.push(`${alias}.usage_type_id IS NULL AND ${alias}.usage_type_version IS NULL`);
  else if(query.type)add(`${alias}.usage_type_id=`,query.type);
  if(query.memberMissing)where.push(`${alias}.member_id IS NULL`);else if(query.member)add(`${alias}.member_id=`,query.member);
  if(query.modelMissing)where.push(`COALESCE(NULLIF(${alias}.billing_model,''),NULLIF(${alias}.model,'')) IS NULL`);else if(query.model)add(`COALESCE(NULLIF(${alias}.billing_model,''),NULLIF(${alias}.model,''))=`,query.model);
  if(query.status==='unknown')where.push(`${alias}.business_status NOT IN ('ok','failed','queued','running','cancelled','rejected','interrupted')`);else if(query.status)add(`${alias}.business_status=`,query.status);
  return {where:where.join(' AND '),values,window,filters:{...query,from:window.start,to:window.end}};
}
const counters=`COUNT(*) FILTER(WHERE status='failed') failed,COUNT(*) FILTER(WHERE status='queued') queued,COUNT(*) FILTER(WHERE status='running') running,
  COUNT(*) FILTER(WHERE status='cancelled') cancelled,COUNT(*) FILTER(WHERE status='rejected') rejected,COUNT(*) FILTER(WHERE status='interrupted') interrupted,
  COUNT(*) FILTER(WHERE status NOT IN ('ok','failed','queued','running','cancelled','rejected','interrupted')) unresolved`;
const typeKey="CASE WHEN r.usage_type_id IS NOT NULL THEN 'type:'||r.usage_type_id WHEN r.usage_type_version IS NULL THEN 'historical_unknown' ELSE 'unclassified' END";
const groupKeys={type:'type_key',model:"CASE WHEN chosen_model IS NULL THEN '0' ELSE '1'||chosen_model END",member:"CASE WHEN member_id IS NULL THEN '0' ELSE '1'||member_id END"};
function usage(row){
  const value=aggregate(row),outcomes={success:row.success,failed:row.failed,queued:row.queued,running:row.running,cancelled:row.cancelled,rejected:row.rejected,interrupted:row.interrupted,unknown:row.unresolved},denominator=row.success+row.failed;
  return {...value,outcomes,failureRate:denominator?row.failed/denominator:null};
}
export function createPostgresTypeStatistics(store,{clock=Date.now,queryQueue,pageBytes=524288,maxVersions=1000,maxConcurrent=2,maxQueued=8,queueTimeoutMs=15000}={}){
  if(!store||['get','all','transaction'].some(key=>typeof store[key]!=='function')||typeof clock!=='function'||!Number.isSafeInteger(pageBytes)||pageBytes<1024||pageBytes>2097152||!Number.isSafeInteger(maxVersions)||maxVersions<1||maxVersions>10000||queryQueue&&['run','close','status'].some(key=>typeof queryQueue[key]!=='function'))throw new AppError(503,'类型统计资源配置无效','type_statistics_configuration');
  const ownQueue=createPostgresQueryQueue({maxConcurrent,maxQueued,queueTimeoutMs}),queue=queryQueue??ownQueue,operations=new Set();let closed=false;
  async function execute(query){
    const at=clock(),scope=typeStatisticsConditions(query,{at}),args=scope.values,key=groupKeys[query.groupBy];
    return store.transaction(async()=>{
      const visibility=(await store.get('SELECT pg_current_snapshot()::text visibility')).visibility;
      // Valid projections reference an existing catalogue row in this same
      // snapshot, so the bounded catalogue is an upper bound for used versions.
      const catalog=await store.all('SELECT id FROM harbor.pricing_versions LIMIT $1',maxVersions+1);
      if(catalog.length>maxVersions){
        const versions=await store.all(`SELECT DISTINCT p.revision FROM harbor.requests r${joins} WHERE ${scope.where} AND ${validPrice} AND p.total_atoms IS NOT NULL ORDER BY p.revision LIMIT $${args.length+1}`,...args,maxVersions+1);if(versions.length>maxVersions)capacity();
      }
      const data=`WITH data AS (SELECT ${typeKey} type_key,r.usage_type_id type_id,r.usage_type_code snapshot_code,r.usage_type_name snapshot_name,r.member_id,COALESCE(NULLIF(r.billing_model,''),NULLIF(r.model,'')) chosen_model,r.business_status status,r.started,r.ended,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,CASE WHEN ${validPrice} THEN p.revision END revision,
        ${fields.map(field=>`CASE WHEN ${validPrice} THEN p.${field}_atoms END ${field}`).join(',')}
        FROM harbor.requests r${joins} WHERE ${scope.where})`;
      // Aggregate the complete filtered window and its groups together. Only the
      // small grouped relation is materialized/reused for counting and paging.
      const resultRows=await store.all(`${data},keyed AS(SELECT data.*,${key} group_key FROM data),
        grouped AS MATERIALIZED(SELECT GROUPING(group_key) is_total,group_key,MIN(type_id) type_id,MIN(snapshot_code) snapshot_code,MIN(snapshot_name) snapshot_name,MIN(chosen_model) model,MIN(member_id) member_id,${aggregateSql},${counters}
          FROM keyed GROUP BY GROUPING SETS((),(group_key))),
        counts AS(SELECT COUNT(*) FILTER(WHERE is_total=0) group_count FROM grouped),
        paged AS(SELECT * FROM grouped WHERE is_total=0 ORDER BY group_key COLLATE "C" LIMIT $${args.length+1}
          OFFSET (SELECT (LEAST($${args.length+2}::bigint,GREATEST(1,CEIL(group_count::numeric/$${args.length+1})))::bigint-1)*$${args.length+1} FROM counts))
        SELECT g.*,c.group_count,t.code current_code,t.name current_name,m.name member_name
          FROM (SELECT * FROM grouped WHERE is_total=1 UNION ALL SELECT * FROM paged) g CROSS JOIN counts c
          LEFT JOIN harbor.member_types t ON t.id=g.type_id LEFT JOIN harbor.members m ON m.id=g.member_id
          ORDER BY g.is_total DESC,g.group_key COLLATE "C"`,...args,query.pageSize,query.page);
      const [total,...rows]=resultRows,totalPages=Math.max(1,Math.ceil(total.group_count/query.pageSize)),page=Math.min(query.page,totalPages);
      const filters={...scope.filters,page:1},items=rows.map(row=>{
        let identity,drilldown;
        if(query.groupBy==='type'){const special=row.group_key==='unclassified'?'未分类':row.group_key==='historical_unknown'?'历史类型未知':null;identity={typeId:row.type_id,type:row.type_id??row.group_key,code:row.current_code??row.snapshot_code??null,name:special??row.current_name??row.snapshot_name??'类型名称未记录',classificationSource:'request_snapshot',labelSource:special?'system':row.current_name!==null?'current_type_catalog':'request_snapshot'};drilldown={...filters,type:identity.type};}
        else if(query.groupBy==='model'){identity={model:row.model,name:row.model??'未记录模型'};drilldown={...filters,model:row.model??'',modelMissing:row.model===null};}
        else{identity={memberId:row.member_id,name:row.member_name??'历史员工未知'};drilldown={...filters,member:row.member_id??'',memberMissing:row.member_id===null};}
        return {id:row.group_key,...identity,...usage(row),filters:drilldown,requestFilters:{type:drilldown.type,member:drilldown.member,model:drilldown.model,outcome:drilldown.status,from:drilldown.from,to:drilldown.to,member_missing:drilldown.memberMissing,model_missing:drilldown.modelMissing}};
      });
      const result={window:scope.window,observedAt:at,watermark:createHash('sha256').update(visibility).digest('hex').slice(0,24),watermarkKind:'postgres_visibility',classificationSource:'request_snapshot',filters:scope.filters,summary:usage(total),rows:items,pagination:{page,pageSize:query.pageSize,total:total.group_count,totalPages}};
      if(Buffer.byteLength(JSON.stringify(result))>pageBytes)capacity();return result;
    },{level:'REPEATABLE READ',readOnly:true});
  }
  return {read:value=>{if(closed)return Promise.reject(new AppError(503,'类型统计查询已关闭','query_closed'));const query=normalize(value),operation=queue.run(()=>execute(query));operations.add(operation);operation.then(()=>operations.delete(operation),()=>operations.delete(operation));return operation;},close:async()=>{closed=true;if(!queryQueue)await queue.close();await Promise.allSettled([...operations]);},status:()=>({...queue.status(),closed:closed||queue.status().closed}),queue};
}
