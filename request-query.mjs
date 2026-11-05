import {referenceAccumulator} from './pricing.mjs';

const invalid=()=>{throw Object.assign(new Error('请求分页快照已失效，请刷新列表'),{status:400,code:'invalid_request_snapshot'});};
const encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
function decode(value){
  try{if(typeof value!=='string'||value.length>2048||!/^[A-Za-z0-9_-]+$/.test(value))invalid();return JSON.parse(Buffer.from(value,'base64url'));}catch{invalid();}
}
export function requestScope(db,query,window){
  const identity=JSON.stringify([query.member||'',query.model||'',query.status||'']);
  let snapshot;
  if(query.snapshot){
    snapshot=decode(query.snapshot);
    if(!snapshot||snapshot.start!==window.start||snapshot.end!==window.end||snapshot.filters!==identity||!Number.isSafeInteger(snapshot.fence)||snapshot.fence<0)invalid();
  }else snapshot={...window,filters:identity,fence:db.prepare('SELECT COALESCE(MAX(query_sequence),0) n FROM requests').get().n};
  const clauses=['COALESCE(r.ended,r.started)>=?','COALESCE(r.ended,r.started)<?','r.query_sequence<=?'],args=[window.start,window.end,snapshot.fence];
  for(const [field,value] of [['member_id',query.member],['model',query.model],['status',query.status]])if(value){clauses.push((field==='model'?'COALESCE(r.billing_model,r.model)':`r.${field}`)+'=?');args.push(value);}
  return {where:' WHERE '+clauses.join(' AND '),args,snapshot:encode(snapshot)};
}
export function requestList(store,query,window,at){
  const scope=requestScope(store.db,query,window),{where,args}=scope;
  const total=store.get('SELECT COUNT(*) n FROM requests r'+where,...args).n;
  const totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=query.cursor?query.page:Math.min(query.page,totalPages);
  const index=query.member?'request_member_page':query.model?'request_model_page':query.status?'request_status_page':'request_stable_page';
  let seek='',extra=[];
  if(query.cursor){
    const cursor=decode(query.cursor);
    if(!cursor||cursor.snapshot!==scope.snapshot||!Number.isSafeInteger(cursor.started)||typeof cursor.id!=='string'||cursor.id.length>100)invalid();
    seek=' AND (r.started,r.id)<(?,?)';extra=[cursor.started,cursor.id];
  }
  const rows=store.all(`SELECT r.*,m.name member_name,a.name account_name FROM requests r INDEXED BY ${index} LEFT JOIN members m ON m.id=r.member_id LEFT JOIN accounts a ON a.id=r.account_id${where}${seek} ORDER BY r.started DESC,r.id DESC LIMIT ?${query.cursor?'':' OFFSET ?'}`,...args,...extra,query.pageSize,...(query.cursor?[]:[(page-1)*query.pageSize]));
  const last=rows.at(-1),hasNext=last&&!!store.get(`SELECT 1 n FROM requests r INDEXED BY ${index}${where} AND (r.started,r.id)<(?,?) LIMIT 1`,...args,last.started,last.id);
  return {window,observedAt:at,watermark:store.get('SELECT version FROM request_query_version WHERE singleton=1').version,
    snapshot:scope.snapshot,pagination:{page,pageSize:query.pageSize,total,totalPages,cursorMode:!!query.cursor,nextCursor:hasNext?encode({snapshot:scope.snapshot,started:last.started,id:last.id}):null},
    rows:rows.map(r=>store.pricing.decorate(r,at))};
}
function bucket(){
  const standard=referenceAccumulator();let requests=0,success=0,active=0,duration=0,finished=0;
  return {add(row,q){requests++;success+=Number(row.status==='ok');active+=Number(['queued','running'].includes(row.status));if(row.ended!==null&&row.ended>=row.started){finished++;duration+=row.ended-row.started;}standard.add(q);},
    value(){const usage=standard.value();return {requests,success,active,average_ms:finished?duration/finished:0,unmetered:usage.unmetered,unpriced:usage.unpriced,standard:usage};}};
}
// Iterate once and quote once per request; never materialize the full window or phase history.
export function requestStatistics(db,query,window,quote,at){
  const {where,args,snapshot}=requestScope(db,query,window),summary=bucket(),models=new Map(),members=new Map();
  const columns=['id','member_id','model','billing_model','started','ended','status','input','output','cached','usage_unknown','price_version','manual_cost_atoms'];
  for(const row of db.prepare('SELECT '+columns.map(c=>'r.'+c).join(',')+' FROM requests r'+where).iterate(...args)){
    const q=quote(row);summary.add(row,q);
    for(const [map,key] of [[models,row.billing_model||row.model],[members,row.member_id]]){if(!map.has(key))map.set(key,bucket());map.get(key).add(row,q);}
  }
  const names=new Map(db.prepare('SELECT id,name FROM members').all().map(m=>[m.id,m.name]));
  const grouped=(map,field)=>[...map].map(([key,b])=>({[field]:key,...(field==='member_id'?{member_name:names.get(key)??null}:{}),...b.value()})).sort((a,b)=>(b.standard.total??-1)-(a.standard.total??-1)||String(a[field]).localeCompare(String(b[field])));
  const base='COALESCE(r.ended,r.started)>=? AND COALESCE(r.ended,r.started)<?';
  return {window,observedAt:at,snapshot,watermark:db.prepare('SELECT version FROM request_query_version WHERE singleton=1').get().version,summary:summary.value(),byModel:grouped(models,'model'),byMember:grouped(members,'member_id'),
    models:db.prepare(`SELECT DISTINCT COALESCE(r.billing_model,r.model) model FROM requests r WHERE ${base} AND COALESCE(r.billing_model,r.model) IS NOT NULL AND COALESCE(r.billing_model,r.model)<>'' ORDER BY model`).all(window.start,window.end).map(r=>r.model),
    members:db.prepare(`SELECT DISTINCT m.id,m.name,m.removed_at FROM requests r JOIN members m ON m.id=r.member_id WHERE ${base} ORDER BY m.name,m.id`).all(window.start,window.end).map(row=>({...row}))};
}
