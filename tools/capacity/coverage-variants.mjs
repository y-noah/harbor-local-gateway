import {usageWindow} from '../../store.mjs';

const listSpecs=Object.freeze({
 'request-list':{time:'started',stable:true},'request-all':{time:'started',stable:true},
 'employee-list':{time:'created'},'key-list':{time:'created'},'device-list':{time:'created'},
 'audit-history':{time:'at'},'session-list':{time:'updated',rows:'sessions',pagination:'sessionsPagination',size:'session_page_size',page:'session_page'}
});
const coldQueries=new Set(['request-summary','request-all','type-statistics','recording-search']);
const filteredQueries=new Set(['request-list','request-all','employee-list','key-list','device-list','audit-history']);
const failures=new WeakMap();
export const getCoverageVariantFailure=error=>failures.get(error);
const fail=code=>{const error=new Error(code);failures.set(error,code);throw error;};
const urlFor=(path,params)=>{const url=new URL(path,'http://capacity.invalid');for(const [key,value] of Object.entries(params))url.searchParams.set(key,String(value));return url.pathname+url.search;};
const compare=(a,b,time)=>a[time]===b[time]?(a.id===b.id?0:a.id>b.id?-1:1):a[time]>b[time]?-1:1;
const ordered=(rows,time)=>rows.every((row,index)=>typeof row.id==='string'&&Number.isSafeInteger(row[time])&&(!index||compare(rows[index-1],row,time)<0));
const pageOf=(json,spec)=>({rows:json?.[spec.rows??'rows'],pagination:json?.[spec.pagination??'pagination']});
function pageValid(json,spec,{page=1,size=25}={}){
 const {rows,pagination:p}=pageOf(json,spec);
 return Array.isArray(rows)&&!!p&&p.page===page&&p.pageSize===size&&Number.isSafeInteger(p.total)&&p.total>=0&&p.totalPages===Math.max(1,Math.ceil(p.total/size))&&rows.length===Math.min(size,Math.max(0,p.total-(page-1)*size))&&ordered(rows,spec.time);
}

// Variants change actual controller inputs. This module neither changes a
// production query nor warms the measured page to manufacture its evidence.
export function createCoverageVariants({mode='steady',request,device,memberId,clock=Date.now}={}){
 if(!['steady','expanded'].includes(mode)||typeof clock!=='function')fail('capacity_coverage_variants_configuration');
 const chains=new Map(),evidence=new Map();let coldSequence=0;
 const variants=id=>mode==='steady'||id==='device-detail'?[id==='device-detail'?'filtered-sorted':'steady']:[...new Set(['steady',...(listSpecs[id]?['default','maximum','deep']:[]),...(filteredQueries.has(id)?['filtered-sorted']:[]),...(coldQueries.has(id)?['cold']:[])])];
 const summary=(id,variant)=>{const key=id+'/'+variant;let state=evidence.get(key);if(!state){state={operationId:id,variant,measured:0,verified:0,firstPageAuxiliary:0,maxDepth:0,completedChains:0,liveAdjacentOverlap:0,...(variant==='cold'?{scope:'first application SQL for a previously unexecuted filter; PostgreSQL/OS cache state unverified',databaseColdPassed:null,operatingSystemColdPassed:null}:{})};evidence.set(key,state);}return state;};
 const metadata=id=>variants(id).map(variant=>({variant,batchCapacity:variant==='deep'?1:undefined}));
 function filters(id,target){
  if(id.startsWith('request-'))return {member:request.member_id,model:request.billing_model||request.model,status:request.status};
  if(id==='employee-list')return {q:'容量',status:'enabled'};
  if(id==='key-list')return {member_id:memberId};
  if(id==='device-list')return {member_id:target?.memberId??device.memberId,state:target?'pending':'approved'};
  if(id==='audit-history')return {action:'member.create'};
  return {};
 }
 function matches(id,row,target){
  if(id.startsWith('request-'))return row.member_id===request.member_id&&(row.billing_model||row.model)===(request.billing_model||request.model)&&row.status===request.status;
  if(id==='employee-list')return typeof row.name==='string'&&row.name.includes('容量')&&row.enabled===1;
  if(id==='key-list')return row.member_id===memberId;
  if(id==='device-list')return row.member_id===(target?.memberId??device.memberId)&&row.state===(target?'pending':'approved');
  if(id==='audit-history')return row.action==='member.create';
  return false;
 }
 async function prepare(binding,variant,{callPrepared,signal,target}={}){
  if(!variants(binding.id).includes(variant))fail('capacity_coverage_variant_invalid');
  if(variant==='steady'||binding.id==='device-detail')return null;
  const id=binding.id,spec=listSpecs[id],row=summary(id,variant);let path=binding.path,verify;
  if(['default','maximum','filtered-sorted'].includes(variant)){
   if(!spec)fail('capacity_coverage_variant_invalid');const size=variant==='maximum'?100:25;
   path=urlFor(path,{...(variant==='maximum'?{[spec.size??'page_size']:100}:{}),...(variant==='filtered-sorted'?filters(id,target):{})});
   verify=json=>pageValid(json,spec,{size})&&(variant!=='filtered-sorted'||pageOf(json,spec).rows.length>0&&pageOf(json,spec).rows.every(item=>matches(id,item,target)));
  }else if(variant==='deep'){
   if(!spec)fail('capacity_coverage_variant_invalid');let state=chains.get(id);
   if(!state||state.ended){signal?.throwIfAborted();const first=await callPrepared(path,{signal});row.firstPageAuxiliary++;if(!first.success)fail('capacity_coverage_variant_first_page_failed');if(!pageValid(first.json,spec))fail('capacity_coverage_variant_first_page_contract_failed');if(pageOf(first.json,spec).pagination.totalPages<2)fail('capacity_coverage_variant_data_incomplete');const firstPage=pageOf(first.json,spec);state={page:2,total:firstPage.pagination.total,snapshot:first.json.snapshot,cursor:firstPage.pagination.nextCursor,last:firstPage.rows.at(-1),previousIds:new Set(firstPage.rows.map(item=>item.id)),ended:false};if(spec.stable&&(typeof state.snapshot!=='string'||!state.snapshot||typeof state.cursor!=='string'||!state.cursor))fail('capacity_coverage_variant_cursor_missing');chains.set(id,state);}
   const expected=state.page;path=urlFor(path,{[spec.page??'page']:expected,...(spec.stable?{snapshot:state.snapshot,cursor:state.cursor}:{})});
   verify=json=>{if(!pageValid(json,spec,{page:expected}))return false;const current=pageOf(json,spec),first=current.rows[0];if(spec.stable&&(json.snapshot!==state.snapshot||current.pagination.total!==state.total||!current.pagination.cursorMode||first&&compare(state.last,first,spec.time)>=0||(expected<current.pagination.totalPages?typeof current.pagination.nextCursor!=='string'||!current.pagination.nextCursor:current.pagination.nextCursor!==null)))return false;row.liveAdjacentOverlap+=spec.stable?0:current.rows.filter(item=>state.previousIds.has(item.id)).length;row.maxDepth=Math.max(row.maxDepth,expected);state.last=current.rows.at(-1);state.previousIds=new Set(current.rows.map(item=>item.id));state.page++;state.cursor=current.pagination.nextCursor;state.ended=spec.stable?!state.cursor:expected>=current.pagination.totalPages;if(state.ended)row.completedChains++;return true;};
  }else if(variant==='cold'){
   const window=usageWindow(clock()),from=window.start+coldSequence+1,to=window.end;if(!Number.isSafeInteger(request?.usage_at)||from>request.usage_at||request.usage_at>=to)fail('capacity_coverage_variant_data_incomplete');coldSequence++;
   path=urlFor(path,{from,to});
   verify=json=>id==='recording-search'?Array.isArray(json?.items)&&json.items.length>0:!!json?.summary&&json.summary.requests>0;
  }else fail('capacity_coverage_variant_invalid');
  return {path,method:'GET',verify:json=>{row.measured++;const passed=verify(json)===true;if(passed)row.verified++;return passed;}};
 }
 return Object.freeze({metadata,variants,prepare,evidence:()=>Object.freeze([...evidence.values()].map(row=>Object.freeze({...row})))});
}
