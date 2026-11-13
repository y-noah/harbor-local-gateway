import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresAccountExtraUsage} from '../postgres-account-extra-usage.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需独占私有PG18，不访问线上':false},model='gpt-6.1-sol',protocol='responses',actor={session_ref:createHash('sha256').update('synthetic-batch-admin').digest('hex')};
test('batch extra-use decisions read one real PostgreSQL snapshot without trusting account DTO state or creating rows',integration,async t=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),store=new PostgresStore({connection:lab.connection}),other=new PostgresStore({connection:lab.connection});
 try{
  await store.migrate();await store.initializePricing();const ids=Array.from({length:64},()=>randomUUID());for(const accountId of ids)await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,'合成批量账号','synthetic-batch','ready')",accountId);
  const service=createPostgresAccountExtraUsage(store),accounts=ids.map(id=>({id,route_epoch:0})),allPackages=new Map(ids.map(id=>[id,true]));
  await service.readPolicy();
  await t.test('two data reads plus constant authorization checks cover 64 accounts without writes or per-account SQL',async()=>{
   const originalQuery=store.query.bind(store);let statements=0,dataReads=0;try{store.query=async(sql,...args)=>{assert.match(sql,/^SELECT/);statements++;if(sql.includes('FROM harbor.extra_usage_policy')||sql.includes('LEFT JOIN harbor.account_extra_usage'))dataReads++;return originalQuery(sql,...args);};await service.evaluateAccounts(accounts.slice(0,1),{model,protocol,packageAvailability:allPackages});const singleStatements=statements;statements=0;dataReads=0;const out=await service.evaluateAccounts(accounts,{model,protocol,packageAvailability:allPackages});assert.equal(out.accounts.length,64);assert.ok(out.accounts.every(row=>row.decision.mode==='package'));assert.deepEqual(out.snapshot,{source:'top_level',isolation:'repeatable read',read_only:true});assert.equal(dataReads,2);assert.equal(statements,singleStatements);}finally{store.query=originalQuery;}
   assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.account_extra_usage')).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.account_extra_entitlements')).n,0);
  });
  await t.test('actual disabled, removed and API rows cannot be laundered by forged caller fields or package=true',async()=>{
   await store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',ids[0]);await store.run('UPDATE harbor.accounts SET removed_at=1 WHERE id=$1',ids[1]);await store.run("UPDATE harbor.accounts SET auth_mode='openai_api' WHERE id=$1",ids[2]);const out=await service.evaluateAccounts(accounts.slice(0,4).map(row=>({...row,enabled:1,removed_at:null,state:'ready'})),{model,protocol,packageAvailability:allPackages});assert.equal(out.accounts[0].decision.reason,'account_unavailable');assert.equal(out.accounts[1].decision.reason,'account_unavailable');assert.equal(out.accounts[2].decision.reason,'extra_not_applicable');assert.equal(out.accounts[3].decision.mode,'package');
  });
  await t.test('expected identity is independent, missing or changed epochs cannot self-certify',async()=>{
   const out=await service.evaluateAccounts([{id:ids[3]},{id:ids[4],route_epoch:1},{id:ids[5],route_epoch:0,kind:'forged'},{id:randomUUID(),route_epoch:0}],{model,protocol,packageAvailability:allPackages});assert.deepEqual(out.accounts.map(row=>row.decision.reason),['extra_identity_unavailable','extra_identity_changed','extra_identity_changed','account_unavailable']);
  });
  await t.test('unknown package state does not fall back to extra even with verified entitlement and enabled budget',async()=>{
   const extra=ids[6],readEntitlement=async owner=>({capability:{supported:true,platform:owner.kind,auth_mode:owner.auth_mode},entitlement:{state:'verified',source:'official',remaining_atoms:'1000',unit:'credits',models:[model],protocols:[protocol],verified_at:Date.now(),valid_until:Date.now()+60000,expires_at:null}}),ready=createPostgresAccountExtraUsage(store,{readEntitlement});await ready.savePolicy({enabled:true,expected_version:0},actor);await ready.saveAccount(extra,{mode:'inherit',budget:{basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:'100',request_reserve_atoms:'10'},expected_version:0,expected_identity_epoch:0},actor);await ready.refresh(extra);
   for(const unknown of [undefined,null,'unknown']){const map=new Map();if(unknown!==undefined)map.set(extra,unknown);const row=(await ready.evaluateAccounts([accounts[6]],{model,protocol,packageAvailability:map})).accounts[0];assert.equal(row.decision.reason,'package_availability_unknown');}
   assert.equal((await ready.evaluateAccounts([accounts[6]],{model,protocol,packageAvailability:new Map([[extra,false]])})).accounts[0].decision.mode,'extra');assert.equal((await ready.evaluateAccounts([accounts[6]],{model,protocol,packageAvailability:new Map([[extra,true]])})).accounts[0].decision.mode,'package');
  });
  await t.test('top-level RR is stable across concurrent account changes and the next snapshot sees the changed epoch',async()=>{
   const target=ids[7],original=store.get.bind(store);let changed=false;try{store.get=async(sql,...args)=>{const result=await original(sql,...args);if(sql.includes("current_setting('transaction_isolation')")&&!changed){changed=true;await other.run('UPDATE harbor.accounts SET route_epoch=1,enabled=0 WHERE id=$1',target);}return result;};const row=await service.evaluateAccounts([accounts[7]],{model,protocol,packageAvailability:allPackages});assert.equal(row.accounts[0].identity_epoch,0);assert.equal(row.accounts[0].decision.mode,'package');}finally{store.get=original;}assert.equal((await service.evaluateAccounts([accounts[7]],{model,protocol,packageAvailability:allPackages})).accounts[0].decision.reason,'extra_identity_changed');
  });
  await t.test('existing READ COMMITTED context is reused without a savepoint or a false RR claim',async()=>{
   const original=store.transaction.bind(store);await original(async()=>{store.transaction=()=>{throw Error('nested transaction forbidden');};try{const out=await service.evaluateAccounts([accounts[8]],{model,protocol,packageAvailability:allPackages});assert.deepEqual(out.snapshot,{source:'existing_transaction',isolation:'read committed',read_only:false});}finally{store.transaction=original;}});
  });
  await t.test('sparse account arrays reject before opening any transaction or issuing a query',async()=>{
   const original=store.transaction.bind(store);let called=0;store.transaction=()=>{called++;throw Error('invalid batch reached transaction');};try{for(const sparse of [Array(1),[accounts[10],,],[,accounts[11]],Array(1001)])await assert.rejects(service.evaluateAccounts(sparse,{model,protocol,packageAvailability:allPackages}),error=>error.code==='extra_decision_batch_invalid');assert.equal(called,0);}finally{store.transaction=original;}
  });
  await t.test('input collections are snapshotted before awaits; bounded invalid batches cannot query',async()=>{
   const target=ids[9],inputs=[{id:target,route_epoch:0}],map=new Map([[target,false]]),original=store.get.bind(store);try{store.get=async(sql,...args)=>{if(sql.includes("current_setting('transaction_isolation')")){inputs[0].route_epoch=99;map.set(target,true);}return original(sql,...args);};const out=await service.evaluateAccounts(inputs,{model,protocol,packageAvailability:map});assert.equal(out.accounts[0].decision.allowed,false);assert.notEqual(out.accounts[0].decision.reason,'extra_identity_changed');}finally{store.get=original;}
   const bounded=createPostgresAccountExtraUsage(store,{maxDecisionAccounts:1});for(const batch of [[accounts[10],accounts[11]],[accounts[10],accounts[10]],[{id:'bad id',route_epoch:0}]])await assert.rejects(bounded.evaluateAccounts(batch,{model,protocol,packageAvailability:allPackages}),error=>error.code==='extra_decision_batch_invalid');
   const empty=await service.evaluateAccounts([],{model,protocol,packageAvailability:new Map()});assert.deepEqual(empty.accounts,[]);
  });
 }finally{await other.close();await store.close();await lab.close();}
});
