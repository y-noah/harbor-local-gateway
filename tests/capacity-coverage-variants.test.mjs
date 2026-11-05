import test from 'node:test';
import assert from 'node:assert/strict';
import {registerHooks} from 'node:module';
import {randomUUID} from 'node:crypto';
import {createCoverageVariants,getCoverageVariantFailure} from '../tools/capacity/coverage-variants.mjs';
import {requestsQuery,historyQuery} from '../observability.mjs';
import {memberQuery} from '../postgres-member-query.mjs';
import {stateQuery} from '../postgres-state-query.mjs';
import {usageWindow} from '../store.mjs';

const at=Date.now(),identity='00000000-0000-4000-8000-000000000001',snapshot='00000000-0000-4000-8000-000000000002';
const fixture=mode=>createCoverageVariants({mode,request:{member_id:identity,model:'gpt-6.1-sol',status:'ok',usage_at:at-10000},memberId:identity,device:{memberId:identity},clock:()=>at});
const binding=id=>({id,path:id.startsWith('request-')?'/api/admin/requests?view='+({'request-list':'list','request-all':'all','request-summary':'summary'}[id]):id==='session-list'?'/api/admin/state?people=none':id==='employee-list'?'/api/admin/members':id==='key-list'?'/api/admin/keys':id==='audit-history'?'/api/admin/history':'/api/admin/devices'});
const rows=(start,count)=>Array.from({length:count},(_,index)=>({id:String(start+index).padStart(8,'0'),started:10000-start-index,created:10000-start-index,updated:10000-start-index,at:10000-start-index,member_id:identity,model:'gpt-6.1-sol',status:'ok',enabled:1,name:'容量合成',state:'approved',action:'member.create'}));
const page=(number=1,{size=25,total=80,cursor=false}={})=>({rows:rows((number-1)*size,Math.min(size,Math.max(0,total-(number-1)*size))),snapshot,pagination:{page:number,pageSize:size,total,totalPages:Math.max(1,Math.ceil(total/size)),cursorMode:cursor,nextCursor:number*size<total?'cursor-'+number:null}});

test('expanded variants are route-specific while the default metadata contract stays unchanged',()=>{
 const old=fixture('steady');assert.deepEqual(old.variants('request-list'),['steady']);assert.deepEqual(old.variants('device-detail'),['filtered-sorted']);
 const expanded=fixture('expanded');assert.deepEqual(expanded.variants('health'),['steady']);assert.deepEqual(expanded.variants('account-list'),['steady']);assert.deepEqual(expanded.variants('request-list'),['steady','default','maximum','deep','filtered-sorted']);assert.deepEqual(expanded.variants('request-summary'),['steady','cold']);assert.equal(expanded.metadata('request-list').find(row=>row.variant==='deep').batchCapacity,1);assert.throws(()=>fixture('invented'),/variants_configuration/);
});

test('default and maximum use actual query fields and verify page capacity, order and content',async()=>{
 for(const id of ['request-list','request-all','employee-list','key-list','audit-history','session-list']){
  const variants=fixture('expanded'),normal=await variants.prepare(binding(id),'default'),maximum=await variants.prepare(binding(id),'maximum');const a=new URL(normal.path,'http://fixture'),b=new URL(maximum.path,'http://fixture');assert.equal(a.searchParams.has(id==='session-list'?'session_page_size':'page_size'),false);assert.equal(b.searchParams.get(id==='session-list'?'session_page_size':'page_size'),'100');
  const parse=id.startsWith('request-')?requestsQuery:id==='employee-list'?memberQuery:id==='key-list'?params=>memberQuery(params,{keys:true}):id==='audit-history'?historyQuery:stateQuery;assert.equal(parse(b.searchParams)[id==='session-list'?'sessionPageSize':'pageSize'],100);
  const response=(number,options)=>{const value=page(number,options);return id==='session-list'?{sessions:value.rows,sessionsPagination:value.pagination}:value;};assert.equal(normal.verify(response(1)),true);assert.equal(maximum.verify(response(1,{size:100,total:80})),true);assert.equal(maximum.verify(response(1)),false);const unordered=response(1);(unordered.rows??unordered.sessions).reverse();assert.equal(normal.verify(unordered),false);
 }
});

test('filtered variants send controller-supported filters and reject mismatched or empty rows',async()=>{
 const variants=fixture('expanded');for(const id of ['request-list','employee-list','key-list','device-list','audit-history']){const descriptor=await variants.prepare(binding(id),'filtered-sorted'),url=new URL(descriptor.path,'http://fixture');assert.ok([...url.searchParams.keys()].some(key=>['member','q','member_id','action'].includes(key)));assert.equal(url.searchParams.has('sort'),false);assert.equal(descriptor.verify(page()),true);const wrong=page();Object.assign(wrong.rows[0],{member_id:'other',model:'other',name:'other',action:'other',state:'disabled'});assert.equal(descriptor.verify(wrong),false);assert.equal(descriptor.verify(page(1,{total:0})),false);}
});

test('deep requests derive each measured cursor from the previous actual response without pre-reading that page',async()=>{
 const variants=fixture('expanded');let aux=0;const initial=async path=>{aux++;assert.equal(path,binding('request-list').path);return {success:true,json:page()};};
 const second=await variants.prepare(binding('request-list'),'deep',{callPrepared:initial});assert.equal(aux,1);let params=new URL(second.path,'http://fixture').searchParams;assert.equal(params.get('page'),'2');assert.equal(params.get('snapshot'),snapshot);assert.equal(params.get('cursor'),'cursor-1');assert.equal(second.verify(page(2,{cursor:true})),true);
 const third=await variants.prepare(binding('request-list'),'deep',{callPrepared:initial});assert.equal(aux,1);params=new URL(third.path,'http://fixture').searchParams;assert.equal(params.get('page'),'3');assert.equal(params.get('cursor'),'cursor-2');const duplicate=page(3,{cursor:true});duplicate.rows[0]=rows(49,1)[0];assert.equal(third.verify(duplicate),false);assert.equal(third.verify(page(3,{cursor:true})),true);
 const last=await variants.prepare(binding('request-list'),'deep',{callPrepared:initial});assert.equal(last.verify(page(4,{cursor:true})),true);assert.equal(variants.evidence()[0].completedChains,1);await variants.prepare(binding('request-list'),'deep',{callPrepared:initial});assert.equal(aux,2);assert.equal(variants.evidence()[0].maxDepth,4);
});

test('live offset pagination reports adjacent drift without claiming a stable snapshot',async()=>{
 const variants=fixture('expanded'),descriptor=await variants.prepare(binding('employee-list'),'deep',{callPrepared:async()=>({success:true,json:page()})});assert.equal(new URL(descriptor.path,'http://fixture').searchParams.get('page'),'2');const shifted=page(2);shifted.rows=rows(24,25);assert.equal(descriptor.verify(shifted),true);assert.equal(variants.evidence()[0].liveAdjacentOverlap,1);
 await assert.rejects(fixture('expanded').prepare(binding('request-list'),'deep',{callPrepared:async()=>({success:true,json:page(1,{total:25})})}),/variant_data_incomplete/);
});

test('stable deep pagination rejects early missing and late extra cursors without advancing the chain',async()=>{
 const variants=fixture('expanded'),initial=async()=>({success:true,json:page(1,{total:75})}),second=await variants.prepare(binding('request-list'),'deep',{callPrepared:initial}),early=page(2,{total:75,cursor:true});early.pagination.nextCursor=null;assert.equal(second.verify(early),false);assert.equal(variants.evidence()[0].completedChains,0);assert.equal(second.verify(page(2,{total:75,cursor:true})),true);
 const last=await variants.prepare(binding('request-list'),'deep',{callPrepared:initial}),extra=page(3,{total:75,cursor:true});extra.pagination.nextCursor='unexpected';assert.equal(last.verify(extra),false);assert.equal(variants.evidence()[0].completedChains,0);assert.equal(last.verify(page(3,{total:75,cursor:true})),true);assert.equal(variants.evidence()[0].completedChains,1);
});

test('a malformed deep first page is a contract failure while valid short data remains incomplete',async()=>{
 const bad=page();bad.rows.reverse();await assert.rejects(fixture('expanded').prepare(binding('request-list'),'deep',{callPrepared:async()=>({success:true,json:bad})}),error=>getCoverageVariantFailure(error)==='capacity_coverage_variant_first_page_contract_failed');
 await assert.rejects(fixture('expanded').prepare(binding('request-list'),'deep',{callPrepared:async()=>({success:true,json:page(1,{total:25})})}),error=>getCoverageVariantFailure(error)==='capacity_coverage_variant_data_incomplete');
});

test('cold queries use unique real nonempty time filters and retain database and OS cold-state gaps',async()=>{
 const variants=fixture('expanded'),one=await variants.prepare(binding('request-summary'),'cold'),two=await variants.prepare(binding('request-summary'),'cold');const a=new URL(one.path,'http://fixture'),b=new URL(two.path,'http://fixture');assert.equal(requestsQuery(a.searchParams,{usageTypes:true}).from,usageWindow(at).start+1);assert.equal(Number(b.searchParams.get('from')),Number(a.searchParams.get('from'))+1);assert.equal(one.verify({summary:{requests:1}}),true);assert.equal(two.verify({summary:{requests:0}}),false);const evidence=variants.evidence()[0];assert.equal(evidence.databaseColdPassed,null);assert.equal(evidence.operatingSystemColdPassed,null);assert.equal(evidence.measured,2);assert.equal(evidence.verified,1);assert.ok(Object.isFrozen(evidence));
 const shared=await variants.prepare(binding('request-all'),'cold');assert.equal(Number(new URL(shared.path,'http://fixture').searchParams.get('from')),Number(b.searchParams.get('from'))+1);
});

test('factory snapshots the variant before asynchronous preparation and rejects mutation before dispatch',async()=>{
 // Isolate the factory's capability binding; this lease boundary stub does not
 // claim to validate transport, HTTP or real handler settlement.
 const entry=new URL('../tools/capacity/coverage-factory.mjs?variant-race='+randomUUID(),import.meta.url).href,hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./inventory-adapter.mjs')return {url:'data:text/javascript,export const getPreparedCoverageLease=()=>({count:1});',shortCircuit:true};return next(specifier,context);}});let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);let prepared,dispatches=0;
 try{const {createCapacityCoverageFactory}=await import(entry);hooks.deregister();const scope={kind:'normal',id:'variant-race'},measurement={kind:'normal',id:'variant-race-measurement'},base={operationId:'request-list',method:'GET',canonicalHttp:'GET /api/admin/requests?view=list',workloadClass:'list',purposes:['request-list']},group={...base,variant:'default'},handle={};
  const factory=createCapacityCoverageFactory({getBusiness:()=>({inventory:{coverageInventory:()=>['default','maximum'].map(variant=>({...base,variant})),prepareCoverageBatch:async spec=>{prepared=spec;return [handle];},executePrepared:async()=>{dispatches++;return {};}}}),getAdminActors:()=>Array.from({length:5},()=>({headers:{cookie:'synthetic-cookie'}})),getCandidateActors:()=>Array.from({length:2},()=>({memberId:identity,keyId:identity,deviceId:identity,agent:{},origin:'https://fixture.invalid',headers:{authorization:'synthetic'}})),getNormalActorCount:()=>1,prepareCandidates:async()=>{},ownsScope:value=>value===scope||value===measurement,validateOwnedOperation:()=>{},runOwnedOperation:async(_scope,operation)=>{entered();await gate;return operation();},readHandlerWitness:()=>null,readTrace:()=>null,readTraceEvidence:()=>null,store:{get:async()=>null,all:async()=>[]}});
  await factory.getApiCoverageInventory({prepare:true});const pending=factory.prepareApiCoverageBatch({scope,group,count:1});await started;group.variant='maximum';group.purposes.push('employee-list');release();await pending;assert.equal(prepared.variant,'default');assert.deepEqual(prepared.purposes,['request-list']);assert.ok(Object.isFrozen(prepared));await assert.rejects(factory.executeApiCoverageHandle({scope:{...scope},group,handle,actorIndex:0}),/scope_not_owned/);await assert.rejects(factory.executeApiCoverageHandle({scope:measurement,group,handle,actorIndex:0}),/factory_handle_invalid/);assert.equal(dispatches,0);
 }finally{release?.();hooks.deregister();}
});
