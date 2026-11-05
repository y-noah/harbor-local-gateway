import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityInventoryAdapter} from '../tools/capacity/inventory-adapter.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('capacity inventory binds final real HTTP reads and independent configuration / task writes',{skip:!binDirectory?'需独立私有PG工具路径':false,timeout:120000},async()=>{
  let owned;try{
    owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});
    // Exercise privately observed refreshes before raw fixture preparation sends unscoped HTTP.
    for(const id of ['extra-refresh-submit','extra-refresh-status']){const result=await owned.sendOperation(id);assert.equal(result.success,true,id);assert.equal(result.businessVerified,true,id);}
    const inventory=await createCapacityInventoryAdapter({origin:owned.gateway.ready.tlsOrigin,agent:owned.employeeActors[0].agent,adminActors:owned.adminActors,employeeActors:owned.employeeActors,store:owned.store});
    assert.equal(inventory.bindings.size,48);assert.equal(inventory.fixture.configurationTargets.length,owned.adminActors.length);assert.ok(inventory.fixture.configurationTargets.every(target=>!owned.employeeActors.some(actor=>actor.memberId===target.memberId||actor.keyId===target.keyId||actor.deviceId===target.deviceId)));for(const id of ['account-list','session-list','account-detail','session-detail']){assert.equal(inventory.bindings.get(id).workloadClass,'summary');assert.deepEqual(inventory.bindings.get(id).apiBudget,{p95:500,p99:1000});}
    assert.ok((await owned.store.all('SELECT enabled,state FROM harbor.accounts WHERE id=ANY($1::text[])',inventory.fixture.configurationTargets.map(row=>row.extraAccountId))).every(row=>row.enabled===0&&row.state==='offline'));
    for(const [id,binding] of inventory.bindings){if(id.startsWith('extra-refresh-')){await assert.rejects(inventory.execute(id),/cleanup_not_owned/);continue;}const result=await inventory.execute(id);assert.equal(result.success,true,id+' status='+result.status+' code='+result.json?.error?.code);if(binding.method!=='GET')assert.equal(result.businessVerified,true,id);}
    assert.equal((await inventory.qualifyAsync()).passed,true);
    const results=await Promise.all(['employee-save','model-permissions-save','limits-save','type-default-save','device-state-save'].map(id=>inventory.execute(id)));assert.ok(results.every(result=>result.success&&result.businessVerified));
  }finally{await owned?.close();}
});
