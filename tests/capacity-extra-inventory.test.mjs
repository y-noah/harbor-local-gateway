import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityInventoryAdapter} from '../tools/capacity/inventory-adapter.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('prepared extra reads and writes verify real account facts without adding model-routing candidates',{
  skip:!binDirectory?'需本任务私有PG、真实HTTP及Worker':false,timeout:120000,
},async()=>{
  let target;
  try{
    target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});
    const before=await target.store.all("SELECT id,enabled,state,route_epoch,quota_json,budget_used FROM harbor.accounts WHERE enabled=1 AND state='ready' ORDER BY id");
    // This direct inventory test isolates endpoint/lease contracts. Actual
    // handler-witness integrity is checked by the owned factory tests.
    const inventory=await createCapacityInventoryAdapter({origin:target.gateway.ready.tlsOrigin,agent:target.employeeActors[0].agent,adminActors:target.adminActors,employeeActors:target.employeeActors,store:target.store,coverageTargetCount:1,awaitCoverageTransport:async()=>({passed:true,settled:true})});
    for(const id of ['extra-account-save','extra-manual-save','extra-policy-read','extra-coverage-read','extra-account-read','extra-records-read']){
      const spec=inventory.coverageInventory().find(row=>row.operationId===id),group={...spec,purposes:[id]},handles=await inventory.prepareCoverageBatch(group,1);
      assert.equal(handles.length,1);
      try{const result=await inventory.executePrepared(handles[0]);assert.equal(result.success,true,id);assert.equal(result.businessVerified,true,id);}
      finally{await inventory.releasePrepared(handles);}
    }
    const configured=await target.store.all("SELECT a.enabled,a.state,e.mode,e.manual_record FROM harbor.accounts a JOIN harbor.account_extra_usage e ON e.account_id=a.id WHERE a.name='容量独立权益配置'");
    assert.ok(configured.some(row=>row.manual_record?.state==='unverified'));
    assert.ok(configured.every(row=>row.enabled===0&&row.state==='offline'));
    assert.deepEqual(await target.store.all("SELECT id,enabled,state,route_epoch,quota_json,budget_used FROM harbor.accounts WHERE enabled=1 AND state='ready' ORDER BY id"),before);
    assert.equal((await target.store.get('SELECT enabled FROM harbor.extra_usage_policy WHERE singleton=1')).enabled,false);
    assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.account_extra_holds')).n,0);
  }finally{await target?.close();}
});
