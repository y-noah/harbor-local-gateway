import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {withLegacyConsumption,legacyBoundaryIntegration as integration} from './legacy-consumption-fixture.mjs';
const until=async predicate=>{const end=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<end,'synthetic legacy condition deadline');await new Promise(resolve=>setTimeout(resolve,5));}};

test('all explicit source policies reject legacy chat, platform chat and platform catalog before dispatch',integration,async()=>{
 for(const policy of ['api_only','subscription_only','api_first'])await withLegacyConsumption(async f=>{
  for(const [path,body] of [['/v1/chat/completions',f.chatBody],['/v1/platform/chat/completions',f.platformBody],['/v1/platform/models',undefined]]){
   const result=await f.call(path,{body,headers:{'x-harbor-consumption-policy':'inherit'}});assert.equal(result.status,400);assert.equal(result.value.error.code,'consumption_policy_protocol_unsupported');
  }
  assert.equal(f.calls.length,0);assert.equal((await f.store.get('SELECT count(*) count FROM harbor.requests')).count,0);assert.equal((await f.store.get('SELECT count(*) count FROM harbor.sessions')).count,0);
 },{policy});
});
test('inherit and absent-schema legacy calls retain successful behavior and only installed schema records admission snapshots',integration,async()=>{
 for(const install of [false,true])await withLegacyConsumption(async f=>{
  assert.equal((await f.call('/v1/platform/models')).status,200);assert.equal((await f.call('/v1/chat/completions',{body:f.chatBody})).status,200);assert.equal((await f.call('/v1/platform/chat/completions',{body:f.platformBody})).status,200);assert.deepEqual(f.calls.map(row=>row.protocol),['chat','platform']);
  if(install){const rows=await f.store.all('SELECT consumption_policy,consumption_policy_version,consumption_source,consumption_fallback FROM harbor.requests');assert.equal(rows.length,2);assert.ok(rows.every(row=>row.consumption_policy==='inherit'&&row.consumption_policy_version===0&&row.consumption_source===null&&row.consumption_fallback===false));}
 },{install});
});
test('source policy changed during body upload is checked again in both real admission transactions',integration,async()=>{
 for(const [path,kind] of [['/v1/chat/completions','chatBody'],['/v1/platform/chat/completions','platformBody']])await withLegacyConsumption(async f=>{
  let initialPolicyRead=false;const query=f.store.query.bind(f.store);f.store.query=async(sql,args)=>{const result=await query(sql,args);if(sql.startsWith('SELECT consumption_policy,consumption_policy_version FROM harbor.keys')&&!sql.includes('FOR SHARE'))initialPolicyRead=true;return result;};
  const raw=Buffer.from(JSON.stringify(f[kind]));let request;
  const pending=new Promise((resolve,reject)=>{request=http.request(new URL(path,f.origin),{method:'POST',headers:{authorization:'Bearer '+f.member.secret,'content-type':'application/json','content-length':raw.length}},response=>{let text='';response.on('data',part=>text+=part);response.on('end',()=>resolve({status:response.statusCode,value:JSON.parse(text)}));});request.on('error',reject);request.write(raw.subarray(0,5));});
  try{await until(()=>initialPolicyRead);await f.setPolicy('api_only',0);request.end(raw.subarray(5));const result=await pending;assert.equal(result.status,400);assert.equal(result.value.error.code,'consumption_policy_protocol_unsupported');assert.equal(f.calls.length,0);assert.equal((await f.store.get('SELECT count(*) count FROM harbor.requests')).count,0);}finally{request?.destroy();await pending.catch(()=>{});f.store.query=query;}
 });
});
test('a policy update does not interrupt already dispatched legacy calls but denies subsequent new calls',integration,async()=>{
 for(const [path,kind] of [['/v1/chat/completions','chatBody'],['/v1/platform/chat/completions','platformBody']])await withLegacyConsumption(async f=>{
  const gate=f.gate();f.setModelGate(gate.promise);const pending=f.call(path,{body:f[kind]});
  try{await until(()=>f.calls.length===1);await f.setPolicy('api_only',0);const denied=await f.call(path,{body:f[kind]});assert.equal(denied.status,400);gate.resolve();assert.equal((await pending).status,200);assert.equal(f.calls.length,1);const row=await f.store.get('SELECT status,consumption_policy,consumption_policy_version FROM harbor.requests');assert.equal(row.status,'ok');assert.equal(row.consumption_policy,'inherit');assert.equal(row.consumption_policy_version,0);}finally{gate.resolve();await pending.catch(()=>{});}
 });
});
test('queued chat and prepared platform requests keep their admitted inherit snapshot across later source changes',integration,async()=>{
 await withLegacyConsumption(async f=>{
  const gate=f.gate();f.setModelGate(gate.promise);const first=f.call('/v1/chat/completions',{body:f.chatBody});let second;
  try{await until(()=>f.calls.length===1);const employee=(await f.admin.handle('/api/admin/members','POST',{name:'合成排队员工',quota:null,expires:null,consumption_policy:'inherit'})).value;second=f.call('/v1/chat/completions',{body:f.chatBody,secret:employee.secret});await until(async()=>!!await f.store.get("SELECT id FROM harbor.requests WHERE key_id=$1 AND status='queued'",employee.keyId));await f.admin.handle('/api/admin/keys/'+employee.keyId,'PATCH',{consumption_policy:'subscription_only',expected_consumption_policy_version:0});gate.resolve();assert.equal((await first).status,200);assert.equal((await second).status,200);assert.equal(f.calls.length,2);const row=await f.store.get('SELECT consumption_policy,consumption_policy_version,status FROM harbor.requests WHERE key_id=$1',employee.keyId);assert.equal(row.consumption_policy,'inherit');assert.equal(row.consumption_policy_version,0);assert.equal(row.status,'ok');}finally{gate.resolve();await first.catch(()=>{});await second?.catch(()=>{});}
 });
 await withLegacyConsumption(async f=>{
  const gate=f.gate();f.setCredentialGate(gate.promise);const pending=f.call('/v1/platform/chat/completions',{body:f.platformBody});
  try{await until(async()=>!!await f.store.get("SELECT id FROM harbor.requests WHERE status='queued'"));await f.setPolicy('api_first',0);gate.resolve();assert.equal((await pending).status,200);assert.equal(f.calls.length,1);const row=await f.store.get('SELECT consumption_policy,consumption_policy_version,status FROM harbor.requests');assert.equal(row.consumption_policy,'inherit');assert.equal(row.consumption_policy_version,0);assert.equal(row.status,'ok');}finally{gate.resolve();await pending.catch(()=>{});}
 });
});
test('failed schema initialization can recover without permitting explicit policies or bypassing authentication',integration,()=>withLegacyConsumption(async f=>{
 const marker=await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='schema.key-consumption-policy'");await f.store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.key-consumption-policy'");
 const rejected=await f.call('/v1/platform/models');assert.equal(rejected.status,503);assert.equal(rejected.value.error.code,'database_migration_conflict');assert.equal((await f.call('/v1/platform/models',{secret:'synthetic-invalid'})).status,401);
 await f.store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='schema.key-consumption-policy'",marker.value);assert.equal((await f.call('/v1/platform/models')).status,200);
}));
