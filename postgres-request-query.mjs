import {createHash,randomUUID} from 'node:crypto';
import {AppError,usageWindow} from './store.mjs';
import {fields,validPrice,joins,decorate,aggregate,referencePartialAggregateSql,referenceRollupAggregateSql} from './postgres-reference-query.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';
import {typeStatisticsConditions} from './postgres-type-statistics.mjs';

const invalid=()=>{throw new AppError(400,'请求分页快照已失效，请刷新列表','invalid_request_snapshot');};
const encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
function cursor(value){try{if(typeof value!=='string'||value.length>2048||!/^[A-Za-z0-9_-]+$/.test(value))invalid();return JSON.parse(Buffer.from(value,'base64url'));}catch{invalid();}}
export function createPostgresRequestQueries(store,{clock=Date.now,snapshotTtlMs=3600000,maxSnapshots=4096,maxConcurrent=2,maxQueued=8,queueTimeoutMs=15000,queryQueue,businessOutcomes=false,readInputPreviews}={}){
  if(typeof businessOutcomes!=='boolean'||!Number.isSafeInteger(snapshotTtlMs)||snapshotTtlMs<60000||snapshotTtlMs>86400000||!Number.isInteger(maxSnapshots)||maxSnapshots<8||maxSnapshots>65536||!Number.isInteger(maxConcurrent)||maxConcurrent<1||maxConcurrent>8||!Number.isInteger(maxQueued)||maxQueued<0||maxQueued>64||!Number.isInteger(queueTimeoutMs)||queueTimeoutMs<100||queueTimeoutMs>60000)throw new AppError(503,'管理查询资源配置无效','query_configuration');
  const queue=queryQueue||createPostgresQueryQueue({maxConcurrent,maxQueued,queueTimeoutMs});
  async function scope(query,at){
    const typeScope=typeStatisticsConditions({type:query.type||'',member:query.member||'',model:query.model||'',status:query.outcome||'',from:query.from??null,to:query.to??null,memberMissing:query.memberMissing??false,modelMissing:query.modelMissing??false},{at});
    const window=typeScope.window,extraFilters=[query.type||'',query.outcome||'',query.memberMissing??false,query.modelMissing??false,query.from??null,query.to??null,query.device||''],identity=JSON.stringify([query.member||'',query.model||'',query.status||'',...(extraFilters.some(value=>value!==''&&value!==false&&value!==null)?extraFilters:[])]);
    const visible=(await store.get('SELECT pg_current_snapshot()::text visibility')).visibility;
    let snapshot=query.snapshot,visibility;
    if(snapshot){
      if(!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(snapshot))invalid();
      const saved=await store.get('SELECT visibility::text visibility,window_start,window_end,filters FROM harbor.request_query_snapshots WHERE id=$1 AND expires_at>$2',snapshot,at);
      if(!saved||saved.window_start!==window.start||saved.window_end!==window.end||saved.filters!==identity)invalid();visibility=saved.visibility;
    }else{
      snapshot=randomUUID();visibility=visible;
      await store.run('INSERT INTO harbor.request_query_snapshots VALUES($1,$2::pg_snapshot,$3,$4,$5,$6,$7)',snapshot,visibility,window.start,window.end,identity,at,Math.min(usageWindow(at).end,at+snapshotTtlMs));
    }
    const clauses=[typeScope.where],args=[...typeScope.values];args.push(visibility);clauses.push('pg_visible_in_snapshot(r.inserted_xid,$'+args.length+'::pg_snapshot)');
    if(query.device){if(query.device==='unknown')clauses.push('r.device_id IS NULL');else{if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(query.device))throw new AppError(400,'请求设备筛选无效');args.push(query.device);clauses.push('r.device_id=$'+args.length);}}
    if(query.status){args.push(query.status);clauses.push('r.status=$'+args.length);}
    return {window,snapshot,args,where:' WHERE '+clauses.join(' AND '),observedAt:at,watermark:createHash('sha256').update(visible).digest('hex').slice(0,24),watermarkKind:'postgres_visibility'};
  }
  async function list(query,scope,total){
    const {where,args,window,snapshot}=scope;if(total===undefined)total=(await store.get('SELECT COUNT(*) n FROM harbor.requests r'+where,...args)).n;
    const totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=query.cursor?query.page:Math.min(query.page,totalPages),extra=[];let seek='';
    if(query.cursor){const key=cursor(query.cursor);if(!key||key.snapshot!==snapshot||!Number.isSafeInteger(key.started)||typeof key.id!=='string'||!key.id.length||key.id.length>100||/[\x00-\x1f]/.test(key.id))invalid();extra.push(key.started,key.id);seek=` AND (r.started,r.id)<($${args.length+1},$${args.length+2})`;}
    const values=[...args,...extra,query.pageSize+1],limit='$'+values.length;
    if(!query.cursor)values.push((page-1)*query.pageSize);
    const rows=await store.all(`SELECT r.*,m.name member_name,a.name account_name,p.state projected_state,p.model projected_model,
      p.model_source projected_source,p.revision projected_revision,v.id IS NOT NULL book_exists,${fields.map(f=>'p.'+f+'_atoms::text q_'+f).join(',')}
      FROM harbor.requests r LEFT JOIN harbor.members m ON m.id=r.member_id LEFT JOIN harbor.accounts a ON a.id=r.account_id${joins}${where}${seek}
      ORDER BY r.started DESC,r.id DESC LIMIT ${limit}${query.cursor?'':' OFFSET $'+values.length}`,...values);
    const selected=rows.slice(0,query.pageSize),last=selected.at(-1),previews=readInputPreviews?await readInputPreviews(selected.map(row=>row.id)):null;
    return {pagination:{page,pageSize:query.pageSize,total,totalPages,cursorMode:!!query.cursor,nextCursor:rows.length>query.pageSize?encode({snapshot,started:last.started,id:last.id}):null},rows:selected.map(r=>({...decorate(r,window.start),...previews?.get(r.id)}))};
  }
  async function statistics(query,scope){
    const rows=await store.all(`WITH data AS (
      SELECT r.member_id,m.name member_name,COALESCE(NULLIF(r.billing_model,''),NULLIF(r.model,'')) chosen_model,${businessOutcomes?'r.business_status':'r.status'} status,r.started,r.ended,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
        r.price_version revision,
        ${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms ELSE NULL END ${f}`).join(',')}
      FROM harbor.requests r LEFT JOIN harbor.members m ON m.id=r.member_id${joins}${scope.where}
    ),partial AS MATERIALIZED(SELECT chosen_model,member_id,MIN(member_name) member_name,revision,${referencePartialAggregateSql()}
      FROM data GROUP BY chosen_model,member_id,revision)
    SELECT GROUPING(chosen_model) g_model,GROUPING(member_id) g_member,chosen_model,member_id,MAX(member_name) member_name,
      ${referenceRollupAggregateSql()}
      FROM partial GROUP BY GROUPING SETS((),(chosen_model),(member_id))`,...scope.args);
    const byModel=[],byMember=[];let summary;
    for(const row of rows){const value=aggregate(row);if(row.g_model&&row.g_member)summary=value;else if(!row.g_model)byModel.push({model:row.chosen_model,...value});else byMember.push({member_id:row.member_id,member_name:row.member_name,...value});}
    const sort=key=>(a,b)=>{const x=a.standard.total_exact===null?null:a.standard.total_exact,y=b.standard.total_exact===null?null:b.standard.total_exact;
      if(x!==y){if(x===null)return 1;if(y===null)return -1;const atoms=v=>{const [whole,fraction='']=v.split('.');return BigInt(whole)*1000000000000n+BigInt(fraction.padEnd(12,'0'));};const dx=atoms(x),dy=atoms(y);if(dx!==dy)return dx>dy?-1:1;}
      return String(a[key]).localeCompare(String(b[key]));};
    byModel.sort(sort('model'));byMember.sort(sort('member_id'));
    const models=(await store.all("SELECT DISTINCT COALESCE(billing_model,model) model FROM harbor.requests WHERE usage_at>=$1 AND usage_at<$2 AND COALESCE(billing_model,model) IS NOT NULL AND COALESCE(billing_model,model)<>'' ORDER BY model",scope.window.start,scope.window.end)).map(r=>r.model);
    const members=await store.all('SELECT m.id,m.name,m.removed_at FROM harbor.members m WHERE EXISTS(SELECT 1 FROM harbor.requests r WHERE r.member_id=m.id AND r.usage_at>=$1 AND r.usage_at<$2) ORDER BY m.name,m.id',scope.window.start,scope.window.end);
    return {summary,byModel,byMember,models,members};
  }
  const execute=async query=>{
    const at=clock();
    // Cache cleanup is an autocommit READ COMMITTED statement, outside the
    // repeatable business read. Concurrent readers may delete the same expired
    // cache entries without turning routine eviction into serialization errors.
    if(!query.snapshot)await store.run('DELETE FROM harbor.request_query_snapshots WHERE expires_at<=$1 OR id IN (SELECT id FROM harbor.request_query_snapshots WHERE expires_at>$1 ORDER BY created DESC,id DESC OFFSET $2)',at,maxSnapshots-1);
    return store.transaction(async()=>{
      const current=await scope(query,at),{args,where,...metadata}=current;
      const stats=query.view==='list'?null:await statistics(query,current);
      return {...metadata,...(query.view==='summary'?{}:await list(query,current,stats?.summary.requests)),...(stats??{})};
    },{level:'REPEATABLE READ'});
  };
  return {read:(query,{signal}={})=>{const input={...query};return queue.run(()=>execute(input),{signal});},close:()=>queue.close(),status:()=>queue.status(),queue};
}
