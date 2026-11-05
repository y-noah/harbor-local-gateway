import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityInventoryAdapter} from '../tools/capacity/inventory-adapter.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,real={skip:!binDirectory?'需本任务私有PG/gateway/worker；仅核直接inventory边界，真实factory另验':false,timeout:180000};
// Direct inventory tests isolate API/DB behavior. Factory tests separately prove
// real handler witnesses; this explicit boundary stub is not that proof.
const options=(target,store=target.store)=>({origin:target.gateway.ready.tlsOrigin,agent:target.employeeActors[0].agent,adminActors:target.adminActors,employeeActors:target.employeeActors,store,awaitCoverageTransport:async()=>({passed:true,settled:true})});
const starts=target=>{const count=target.eventCounts?.byKind?.['gateway-request-start'];assert.ok(Number.isSafeInteger(count),'factory必须提供完整单调事件计数，诊断ring不作历史证明');return count;};
const group=(inventory,id)=>inventory.coverageInventory().find(row=>row.operationId===id);

test('prepared inventory uses real isolated APIs with one measured HTTP and concrete task ids',real,async()=>{
 let target;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:2});const inventory=await createCapacityInventoryAdapter(options(target)),metadata=inventory.coverageInventory();assert.equal(metadata.length,48);assert.deepEqual(metadata.filter(row=>!row.available).map(row=>row.operationId).sort(),['extra-refresh-status','extra-refresh-submit','handoff-status','handoff-submit']);
  const actorIds=target.employeeActors.map(row=>row.memberId),before=await target.store.all('SELECT id,name,enabled,usage_limits_version FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id',actorIds);
  for(const spec of metadata){if(!spec.available)continue;const handles=await inventory.prepareCoverageBatch(spec,1),start=starts(target);try{const result=await inventory.executePrepared(handles[0]);assert.equal(result.success,true,spec.operationId+' status='+result.status+' code='+result.json?.error?.code);assert.equal(result.businessVerified,true,spec.operationId);assert.equal(starts(target)-start,1,spec.operationId+' measured HTTP');await assert.rejects(inventory.executePrepared(handles[0]),/handle_invalid/);}finally{await inventory.releasePrepared(handles);}}
  assert.deepEqual(await target.store.all('SELECT id,name,enabled,usage_limits_version FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id',actorIds),before);
 }finally{await target?.close();}
});

test('private handles reject copies, expire and cannot execute twice or unlock another lease',real,async()=>{
 let target;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});let at=Date.now();const inventory=await createCapacityInventoryAdapter({...options(target),configurationTargetCount:1,coverageTargetCount:1,coverageClock:()=>at,coverageHandleTtlMs:20}),spec=group(inventory,'employee-save');
  const [token]=await inventory.prepareCoverageBatch(spec,1),fake={...token};await inventory.releasePrepared([fake]);await assert.rejects(inventory.prepareCoverageBatch(spec,1),/preparation_busy/);await assert.rejects(inventory.executePrepared(fake),/handle_invalid/);at+=21;const before=starts(target);await assert.rejects(inventory.executePrepared(token),/handle_expired/);assert.equal(starts(target),before);await inventory.releasePrepared([token]);
  const [next]=await inventory.prepareCoverageBatch(spec,1),start=starts(target);assert.equal((await inventory.executePrepared(next)).businessVerified,true);await assert.rejects(inventory.executePrepared(next),/handle_invalid/);assert.equal(starts(target)-start,1);await inventory.releasePrepared([next]);const [abandoned]=await inventory.prepareCoverageBatch(spec,1);at+=21;const reclaimed=await inventory.prepareCoverageBatch(spec,1);await assert.rejects(inventory.executePrepared(abandoned),/handle_invalid/);await inventory.releasePrepared(reclaimed);
 }finally{await target?.close();}
});

test('release waits for in-progress DB verification and a failed verification remains single-consumption',real,async()=>{
 let target,release;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});let enteredResolve,blocking=true;const entered=new Promise(resolve=>enteredResolve=resolve),gate=new Promise(resolve=>release=resolve);
  const store=new Proxy(target.store,{get(object,key){if(key==='get')return async(sql,...args)=>{if(sql==='SELECT name FROM harbor.members WHERE id=$1'){enteredResolve();await gate;return {name:'synthetic-wrong-value'};}return object.get(sql,...args);};const value=Reflect.get(object,key);return typeof value==='function'?value.bind(object):value;}}),inventory=await createCapacityInventoryAdapter({...options(target,store),configurationTargetCount:1,coverageTargetCount:1}),spec=group(inventory,'employee-save'),[handle]=await inventory.prepareCoverageBatch(spec,1),before=starts(target),executing=inventory.executePrepared(handle);await entered;
  let released=false;const releasing=inventory.releasePrepared([handle]).then(()=>{released=true;});await delay(20);assert.equal(released,false);await assert.rejects(inventory.prepareCoverageBatch(spec,1),/preparation_busy/);release();assert.equal((await executing).businessVerified,false);await releasing;assert.equal(released,true);await assert.rejects(inventory.executePrepared(handle),/handle_invalid/);assert.equal(starts(target)-before,1);
  const retry=await inventory.prepareCoverageBatch(spec,1);await inventory.releasePrepared(retry);
 }finally{release?.();await target?.close();}
});

test('a partially failed preparation releases both created handles and unissued reservations',real,async()=>{
 let target;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});let reads=0,failSecond=true;
  const store=new Proxy(target.store,{get(object,key){if(key==='get')return async(sql,...args)=>{if(sql==='SELECT model_policy_version FROM harbor.keys WHERE id=$1'&&failSecond&&++reads===2)throw Error('synthetic-preparation-failure');return object.get(sql,...args);};const value=Reflect.get(object,key);return typeof value==='function'?value.bind(object):value;}}),inventory=await createCapacityInventoryAdapter({...options(target,store),configurationTargetCount:1,coverageTargetCount:3}),spec=group(inventory,'model-permissions-save');
  await assert.rejects(inventory.prepareCoverageBatch(spec,2),/synthetic-preparation-failure/);failSecond=false;const handles=await inventory.prepareCoverageBatch(spec,3);assert.equal(handles.length,3);for(const handle of handles)assert.equal((await inventory.executePrepared(handle)).businessVerified,true);await inventory.releasePrepared(handles);
 }finally{await target?.close();}
});

test('a cancelled connection cannot release a target while the real server transaction waits on a row lock',real,async()=>{
 let target,unlock,blocking;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});let allowWitness=true;const inventory=await createCapacityInventoryAdapter({...options(target),configurationTargetCount:1,coverageTargetCount:1,awaitCoverageTransport:async()=>allowWitness?{passed:true,settled:true}:{passed:null,settled:null}});
  const [reading]=await inventory.prepareCoverageBatch(group(inventory,'employee-detail'),1),detail=await inventory.executePrepared(reading),memberId=detail.json.id;await inventory.releasePrepared([reading]);const previous=detail.json.name;
  let lockedResolve;const locked=new Promise(resolve=>lockedResolve=resolve),gate=new Promise(resolve=>unlock=resolve);blocking=target.store.transaction(async()=>{await target.store.get('SELECT id FROM harbor.members WHERE id=$1 FOR UPDATE',memberId);lockedResolve();await gate;});await locked;
  const spec=group(inventory,'employee-save'),[handle]=await inventory.prepareCoverageBatch(spec,1),abort=new AbortController(),executing=inventory.executePrepared(handle,{signal:abort.signal}),deadline=Date.now()+5000;
  for(;;){const row=await target.store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%harbor.members%' AND pid<>pg_backend_pid()");if(row.n>0)break;assert.ok(Date.now()<deadline,'真实管理事务必须先到行锁等待状态');await delay(10);}
  allowWitness=false;abort.abort();const result=await executing;assert.equal(result.error,'client_cancelled');assert.equal(result.httpEnded,false);await assert.rejects(inventory.releasePrepared([handle]),/transport_settlement_incomplete/);await assert.rejects(inventory.prepareCoverageBatch(spec,1),/transport_settlement_incomplete/);assert.equal((await target.store.get('SELECT name FROM harbor.members WHERE id=$1',memberId)).name,previous);
  unlock();await blocking;const completedDeadline=Date.now()+5000;for(;;){const row=await target.store.get('SELECT name FROM harbor.members WHERE id=$1',memberId);if(row.name!==previous)break;assert.ok(Date.now()<completedDeadline,'客户端取消后事务仍按真实结果提交，不能将连接关闭当作回滚');await delay(10);}
  await assert.rejects(inventory.releasePrepared([handle]),/transport_settlement_incomplete/);await assert.rejects(inventory.prepareCoverageBatch(spec,1),/transport_settlement_incomplete/);
 }finally{unlock?.();await blocking?.catch(()=>{});await target?.close();}
});

test('completed async status alone cannot release a target with mismatched applied configuration',real,async()=>{
 let target;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});const inventory=await createCapacityInventoryAdapter({...options(target),configurationTargetCount:1,coverageTargetCount:1}),spec=group(inventory,'batch-submit'),[handle]=await inventory.prepareCoverageBatch(spec,1),submitted=await inventory.executePrepared(handle);assert.equal(submitted.businessVerified,true);const batchId=submitted.json.id,deadline=Date.now()+10000;
  for(;;){const row=await target.store.get('SELECT status FROM harbor.member_config_batches WHERE id=$1',batchId);if(row.status==='completed')break;assert.ok(Date.now()<deadline);await delay(10);}
  const item=await target.store.get('SELECT member_id FROM harbor.member_config_batch_items WHERE batch_id=$1',batchId);await target.store.run('UPDATE harbor.members SET usage_type_version=usage_type_version+1 WHERE id=$1',item.member_id);
  await assert.rejects(inventory.releasePrepared([handle]),/batch_effect_mismatch/);const next=await inventory.prepareCoverageBatch(group(inventory,'employee-save'),1);await inventory.releasePrepared(next);
 }finally{await target?.close();}
});
