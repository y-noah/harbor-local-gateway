import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {AppError,usageWindow,participatesNightly,revealStoredKey,storedModelAllowlist} from './store.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {referenceMetric} from './pricing.mjs';
import {postgresMemberLimitConfiguration} from './postgres-member-limits.mjs';
import {fields,validPrice,joins,decorate,aggregate,aggregateSql} from './postgres-reference-query.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';

export function stateQuery(params){
  const value={sessionPage:1,sessionPageSize:25};
  if(params.has('view')){if(params.getAll('view').length!==1||!['full','overview'].includes(params.get('view')))throw new AppError(400,'状态视图无效');value.view=params.get('view');}
  if(params.has('people')){if(params.getAll('people').length!==1||!['all','none'].includes(params.get('people')))throw new AppError(400,'员工加载模式无效');value.people=params.get('people');}
  for(const [name,field] of [['session_page','sessionPage'],['session_page_size','sessionPageSize']]){
    const entries=params.getAll(name);
    if(entries.length>1||entries.length&&(!/^[1-9]\d*$/.test(entries[0])||!Number.isSafeInteger(Number(entries[0]))))throw new AppError(400,'会话分页参数无效');
    if(entries.length)value[field]=Number(entries[0]);
  }
  for(const [name,field] of [['session_q','sessionQ'],['session_type','sessionType'],['session_account','sessionAccount'],['session_status','sessionStatus']]){
    if(params.has(name)){if(params.getAll(name).length!==1)throw new AppError(400,'会话筛选参数无效');value[field]=params.get(name).trim();}
  }
  validateSessionFilters(value);
  if(value.sessionPageSize>100)throw new AppError(400,'每页最多100个会话');
  return value;
}

function validateSessionFilters({sessionQ='',sessionType='',sessionAccount='',sessionStatus=''}={}){
  if(typeof sessionQ!=='string'||sessionQ.length>100||sessionQ.includes('\0')||typeof sessionType!=='string'||sessionType&&!/^[a-zA-Z0-9-]{1,100}$/.test(sessionType)||typeof sessionAccount!=='string'||sessionAccount&&!/^[a-zA-Z0-9-]{1,100}$/.test(sessionAccount)||!['','enabled','disabled'].includes(sessionStatus))throw new AppError(400,'会话筛选参数无效');
}

// The page identities and every usage dimension share one read snapshot. Only
// aggregates and this page's request/session metadata leave PostgreSQL.
export function createPostgresStateQueries(store,{clock=Date.now,encryptionKey,queryQueue,balancingReader,maxPeople=1000}={}){
  const queue=queryQueue||createPostgresQueryQueue();
  if(!Number.isSafeInteger(maxPeople)||maxPeople<1||maxPeople>10000)throw new AppError(503,'兼容员工状态容量无效','state_configuration');
  async function execute({sessionPage=1,sessionPageSize=25,people='all',view='full',sessionQ='',sessionType='',sessionAccount='',sessionStatus=''}={}){
    validateSessionFilters({sessionQ,sessionType,sessionAccount,sessionStatus});sessionQ=sessionQ.trim();
    if(!['full','overview'].includes(view))throw new AppError(400,'状态视图无效');
    if(!['all','none'].includes(people))throw new AppError(400,'员工加载模式无效');
    if(!Number.isSafeInteger(sessionPage)||sessionPage<1||!Number.isSafeInteger(sessionPageSize)||sessionPageSize<1||sessionPageSize>100)throw new AppError(400,'会话分页参数无效');
    const at=clock(),window=usageWindow(at),recentUsageWindow={start:at-12*3600000,end:at,hours:12};
    return store.transaction(async()=>{
      if(view==='overview'){
        const counts=await store.get('SELECT COUNT(*) member_count FROM harbor.members WHERE removed_at IS NULL');
        const accounts=await store.all('SELECT id,name,kind,auth_mode,enabled,state FROM harbor.accounts WHERE removed_at IS NULL ORDER BY id');
        const rows=await store.all(`SELECT to_char(to_timestamp(usage_at/1000.0) AT TIME ZONE 'Asia/Shanghai','YYYY-MM-DD') AS day,
          COUNT(*) requests,COUNT(*) FILTER(WHERE status='ok') success FROM harbor.requests
          WHERE usage_at>=$1 AND usage_at<$2 GROUP BY 1 ORDER BY 1`,window.start,window.end);
        return {view:'overview',usageWindow:window,observedAt:at,peopleLoaded:false,memberCount:counts.member_count,
          accounts,members:[],keys:[],sessions:[],requests:[],audit:[],
          totals:rows.reduce((total,row)=>({requests:total.requests+Number(row.requests),success:total.success+Number(row.success)}),{requests:0,success:0}),
          daily:rows.map(row=>({day:row.day,requests:Number(row.requests)}))};
      }
      const members=people==='none'?[]:await store.all('SELECT m.id,m.name,m.enabled,m.created,m.removed_at,m.usage_limits,m.usage_limits_version,m.usage_type_id,m.usage_type_version,m.defaults_version,m.configuration_source,m.default_models_set,m.default_models,t.code usage_type_code,t.name usage_type_name FROM harbor.members m LEFT JOIN harbor.member_types t ON t.id=m.usage_type_id WHERE m.removed_at IS NULL ORDER BY m.created DESC,m.id DESC LIMIT $1',maxPeople+1);
      const keys=people==='none'?[]:await store.all('SELECT id,member_id,digest,prefix,secret_cipher,expires,allowed_models,model_policy_version,model_source,enabled,created FROM harbor.keys WHERE removed_at IS NULL ORDER BY created DESC,id DESC LIMIT $1',maxPeople+1);
      if(members.length>maxPeople||keys.length>maxPeople)throw new AppError(503,'兼容状态超过员工或Key容量，请使用分页接口','state_people_capacity');
      const counts=await store.get('SELECT (SELECT COUNT(*) FROM harbor.members WHERE removed_at IS NULL) member_count,(SELECT COUNT(*) FROM harbor.keys WHERE removed_at IS NULL) key_count');
      const packages=createPostgresAccountPackages(store,{clock}),packagePolicy=await packages.policy(),accounts=await packages.decorate(await store.all('SELECT id,name,kind,auth_mode,route_epoch,platform_config,platform_models,api_weight,api_secret_digest,enabled,state,last_used,last_error,route_block,quota_json,quota_error FROM harbor.accounts WHERE removed_at IS NULL ORDER BY id'));
      const sessionArgs=[window.start,window.end],conditions=['s.updated>=$1','s.updated<$2'];
      const filter=(sql,value)=>{sessionArgs.push(value);conditions.push(sql.replaceAll('?',String(sessionArgs.length)));};
      if(sessionQ)filter("(position(lower($?::text) in lower(m.name))>0 OR position(lower($?::text) in lower(m.id))>0 OR position(lower($?::text) in lower(COALESCE(s.client_session_id,'')))>0 OR position(lower($?::text) in lower(s.id))>0)",sessionQ);
      if(sessionType==='unclassified')conditions.push('m.usage_type_id IS NULL');else if(sessionType)filter('m.usage_type_id=$?',sessionType);
      if(sessionAccount)filter('s.account_id=$?',sessionAccount);
      if(sessionStatus)filter('s.enabled=$?',sessionStatus==='enabled'?1:0);
      const from='FROM harbor.sessions s JOIN harbor.members m ON m.id=s.member_id JOIN harbor.accounts a ON a.id=s.account_id WHERE '+conditions.join(' AND '),n=sessionArgs.length;
      const total=(await store.get('SELECT COUNT(*) n '+from,...sessionArgs)).n,totalPages=Math.max(1,Math.ceil(total/sessionPageSize)),page=Math.min(sessionPage,totalPages);
      const sessions=await store.all(`SELECT s.id,s.client_session_id,s.member_id,s.account_id,s.upstream_id,s.state,s.created,s.updated,s.protocol,s.enabled,s.route_version,s.nightly_enabled,s.nightly_expires,s.upstream_thread,m.name member_name,a.name account_name ${from} ORDER BY s.updated DESC,s.id DESC LIMIT $${n+1} OFFSET $${n+2}`,...sessionArgs,sessionPageSize,(page-1)*sessionPageSize);
      // The paged UI omits people. Its DTO uses only seven-day company,
      // account, visible-session and day totals; no twelve-hour groups escape.
      const loadPeople=people==='all',usageArgs=loadPeople?
        [window.start,window.end,recentUsageWindow.start,at,members.map(r=>r.id),keys.map(r=>r.id),accounts.map(r=>r.id),sessions.map(r=>r.id)]:
        [window.start,window.end,accounts.map(r=>r.id),sessions.map(r=>r.id)];
      const rows=await store.all(`WITH source AS ${loadPeople?'MATERIALIZED ':''}(
        SELECT ${loadPeople?`CASE WHEN r.member_id=ANY($5::text[]) THEN r.member_id END member_id,
          CASE WHEN r.key_id=ANY($6::text[]) THEN r.key_id END key_id,`:''}
          CASE WHEN r.account_id=ANY($${loadPeople?7:3}::text[]) THEN r.account_id END account_id,
          CASE WHEN r.session_id=ANY($${loadPeople?8:4}::text[]) THEN r.session_id END session_id,
          to_char(to_timestamp(r.usage_at/1000.0) AT TIME ZONE 'Asia/Shanghai','YYYY-MM-DD') AS day,
          r.usage_at,r.ip,r.status,r.started,r.ended,
          CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
          CASE WHEN ${validPrice} THEN p.revision END revision,
          ${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms END ${f}`).join(',')}
        FROM harbor.requests r${joins} WHERE r.usage_at>=$1 AND r.usage_at<$2
      ), data AS (
        ${loadPeople?`SELECT source.*,period.recent FROM source CROSS JOIN (VALUES(false),(true)) period(recent)
        WHERE NOT period.recent OR (usage_at>=$3 AND usage_at<$4)`:'SELECT source.*,false recent FROM source'}
      ) SELECT recent,${loadPeople?'GROUPING(member_id) g_member,GROUPING(key_id) g_key':'1 g_member,1 g_key'},GROUPING(account_id) g_account,GROUPING(session_id) g_session,GROUPING(day) g_day,
        ${loadPeople?'member_id,key_id':'NULL::text member_id,NULL::text key_id'},account_id,session_id,day,COUNT(DISTINCT ip) ips,MAX(started) last_seen,${aggregateSql}
        FROM data GROUP BY GROUPING SETS((recent),${loadPeople?'(recent,member_id),(recent,key_id),':''}(recent,account_id),(recent,session_id),(recent,day))`,...usageArgs);
      const groups=new Map(),daily=[];let totals;
      for(const row of rows){
        const dimension=['member','key','account','session','day'].find(name=>!row['g_'+name]);
        const value=aggregate(row);
        if(!dimension){if(!row.recent)totals={requests:value.requests,success:value.success,standard:value.standard};continue;}
        const id=row[dimension==='day'?'day':dimension+'_id'];
        if(id===null)continue;
        groups.set(`${dimension}:${id}:${row.recent}`,{standard:value.standard,ips:row.ips,last_seen:row.last_seen,request_count:value.requests});
        if(dimension==='day'&&!row.recent)daily.push({day:id,requests:value.requests,standard:value.standard});
      }
      const empty=aggregate({requests:0,known:0,success:0,active:0,unmetered:0,unpriced:0,pending:0,expired:0,...Object.fromEntries(fields.map(f=>[f,'0']))}).standard;
      const usage=(dimension,id,recent=false)=>groups.get(`${dimension}:${id}:${recent}`)||{standard:empty,ips:0,last_seen:null,request_count:0};
      const requests=await store.all(`SELECT r.*,m.name member_name,a.name account_name,k.prefix key_prefix,k.removed_at key_removed_at,
        p.state projected_state,p.model projected_model,p.model_source projected_source,p.revision projected_revision,v.id IS NOT NULL book_exists,
        ${fields.map(f=>'p.'+f+'_atoms::text q_'+f).join(',')}
        FROM harbor.requests r LEFT JOIN harbor.members m ON m.id=r.member_id LEFT JOIN harbor.accounts a ON a.id=r.account_id LEFT JOIN harbor.keys k ON k.id=r.key_id${joins}
        WHERE r.usage_at>=$1 AND r.usage_at<$2 ORDER BY r.started DESC,r.id DESC LIMIT 200`,window.start,window.end);
      // The compact overview needs action/time only. Structured operation detail
      // belongs to the dedicated history endpoint; never expose raw audit text.
      const audit=await store.all('SELECT id,at,action FROM harbor.audit WHERE at>=$1 AND at<$2 ORDER BY at DESC,id DESC LIMIT 30',window.start,window.end);
      return {...(balancingReader?{balancing:await balancingReader(at)}:{}),usageMetric:referenceMetric,usageWindow:window,recentUsageWindow,observedAt:at,
        sessionsPagination:{page,pageSize:sessionPageSize,total,totalPages},memberCount:counts.member_count,keyCount:counts.key_count,peopleLoaded:people==='all',
        members:members.map(({usage_limits,usage_limits_version,default_models,...row})=>({ ...row,default_models:storedModelAllowlist(default_models),usage_limits:postgresMemberLimitConfiguration({usage_limits,usage_limits_version}),ips:usage('member',row.id).ips,last_seen:usage('member',row.id).last_seen,standard:usage('member',row.id).standard,standard_12h:usage('member',row.id,true).standard})),
        keys:keys.map(({digest,secret_cipher,allowed_models,...row})=>({...row,allowed_models:storedModelAllowlist(allowed_models),full_key:revealStoredKey(encryptionKey,{id:row.id,digest,secret_cipher}),ips:usage('key',row.id).ips,last_seen:usage('key',row.id).last_seen,standard:usage('key',row.id).standard,standard_12h:usage('key',row.id,true).standard})),
        accounts:accounts.map(({api_secret_digest,...row})=>({...row,package:accountPackageBudget(row,{at,enabled:packagePolicy.enabled}),last_error:row.last_error?sanitizeRecord(row.last_error,{maxBytes:2048}):null,quota_error:row.quota_error?sanitizeRecord(row.quota_error,{maxBytes:2048}):null,api_key_configured:!!api_secret_digest,api_key_fingerprint:api_secret_digest?.slice(0,8)||null,standard:usage('account',row.id).standard})),
        sessions:sessions.map(row=>({...row,nightly_enabled:Number(participatesNightly(row,at)),upstream_thread:row.protocol==='responses'?(row.upstream_thread||row.id):row.upstream_id,standard:usage('session',row.id).standard,request_count:usage('session',row.id).request_count})),
        requests:requests.map(row=>decorate(row,window.start)),totals:totals||{requests:0,success:0,standard:empty},daily:daily.sort((a,b)=>a.day.localeCompare(b.day)),audit};
    },{level:'REPEATABLE READ',readOnly:true});
  }
  return {read:query=>{const input={...query};return queue.run(()=>execute(input));},close:()=>queue.close(),status:()=>queue.status()};
}
