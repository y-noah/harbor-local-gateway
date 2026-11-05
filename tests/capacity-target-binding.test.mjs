import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {createTarget,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('only an issued target can expose frozen original source and administrator bindings',{skip:!binDirectory?'需隔离PG工具路径':false,timeout:120000},async()=>{
  let owned;try{
    owned=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory,libraryDirectory});const issued=ownedCapacityTarget(owned);assert.equal(issued.origin,owned.gateway.ready.origin);assert.ok(Object.isFrozen(issued));assert.ok(Object.isFrozen(issued.adminActors[0].headers));assert.equal(Object.isFrozen(owned),false);assert.equal(Object.isFrozen(owned.gateway.ready),false);assert.throws(()=>ownedCapacityTarget({...owned}),/target_not_owned/);
    const mutations=[
      [owned.gateway.ready,'origin','http://127.0.0.1:1'],[owned.gateway.ready,'tlsOrigin','https://127.0.0.1:1'],[owned,'laboratory',{}],[owned,'gateway',{...owned.gateway}],[owned.gateway,'child',owned.worker.child],[owned.worker,'child',owned.gateway.child],[owned,'upstream',{...owned.upstream}],[owned.upstream,'origin','http://127.0.0.1:1'],[owned.adminActors[0].headers,'cookie','harbor_session=synthetic-not-issued']
    ];for(const [target,key,replacement] of mutations){const original=target[key];try{target[key]=replacement;assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/,key);assert.equal(issued.adminActors[0].headers.cookie.includes('not-issued'),false);}finally{target[key]=original;}assert.equal(ownedCapacityTarget(owned),issued);}
    await delay(1200);for(const process of [owned.gateway,owned.worker]){const measured=await process.command('resource-metrics');assert.equal(measured.schema,'harbor-capacity-child-resource-v1');assert.equal(measured.pid,process.child.pid);assert.match(measured.node,/^v24\./);assert.ok(measured.events.some(row=>row.kind==='runtime'&&row.process.pid===process.child.pid));assert.equal(measured.dropped,0);const drained=await process.command('resource-metrics');assert.equal(drained.events.some(row=>row.kind==='event-loop-window'),false,'incomplete five-minute window cannot invent percentiles');}
    await owned.close();assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);
  }finally{await owned?.close();}
});

for(const role of ['gateway','worker'])test('setup callbacks cannot reissue the original listener against another live '+role+' child',{skip:!binDirectory,timeout:120000},async()=>{
  let owned,original,replacement;
  try{
    owned=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory,libraryDirectory,onSetup:async({laboratory,gateway,worker})=>{const selected={gateway,worker}[role];original=selected.child;replacement=laboratory.runtime.start(process.execPath,['-e','setInterval(()=>{},1000)'],{role});selected.child=replacement;}});
    assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);owned[role].child=original;const issued=ownedCapacityTarget(owned);assert.equal(issued[role+'Child'],original);await owned.laboratory.runtime.stop(original);assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);owned[role].child=replacement;assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);
  }finally{await owned?.close();}
});

test('replacing the public runtime proof cannot keep a stopped or closed target signed',{skip:!binDirectory,timeout:120000},async()=>{
  let owned;try{
    owned=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory,libraryDirectory,onSetup:async({laboratory,gateway,worker})=>{laboratory.runtime.proof=child=>({role:child===gateway.child?'gateway':child===worker.child?'worker':'database',pid:child.pid});}});const snapshot=ownedCapacityTarget(owned);assert.equal(snapshot.gatewayChild,owned.gateway.child);await owned.laboratory.runtime.stop(owned.gateway.child);assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);await owned.close();assert.throws(()=>ownedCapacityTarget(owned),/target_not_owned/);
  }finally{await owned?.close();}
});
