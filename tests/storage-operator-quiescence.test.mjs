import test from 'node:test';
import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';
import {assertStorageOperatorTargetIdle} from '../storage-operator-quiescence.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';

test('ordinary promotion still refuses the first foreign client without waiting or identifying it by application name',async()=>{
  let reads=0;const store={get:async()=>{reads++;return {present:1};},query:()=>assert.fail('default mode must not add a statistics polling query')};
  await assert.rejects(assertStorageOperatorTargetIdle(store),{code:'storage_operator_target_has_other_connections'});assert.equal(reads,1);
  await assertStorageOperatorTargetIdle({get:async()=>undefined});
});

test('invalid, wall-clock, expired or cancelled managed deadlines cannot create an unbounded wait',async()=>{
  const store={get:()=>assert.fail('invalid configuration must not query'),query:()=>assert.fail('invalid configuration must not query')};
  for(const options of [{allowWait:'yes'},{allowWait:true},{allowWait:true,deadline:Infinity},{allowWait:true,deadline:Date.now()+120000},{allowWait:true,deadline:performance.now()+121000},{pollMs:0}])await assert.rejects(assertStorageOperatorTargetIdle(store,options),{code:'storage_operator_quiescence_configuration'});
  await assert.rejects(assertStorageOperatorTargetIdle(store,{allowWait:true,deadline:performance.now()-1}),{code:'storage_operator_target_has_other_connections'});
  await assert.rejects(assertStorageOperatorTargetIdle(store,{allowWait:true,deadline:performance.now()+1000,signal:AbortSignal.abort()}),{name:'AbortError'});
});

test('real PG transaction observes a genuinely closed publisher-named client after its statistics snapshot is cleared',{skip:!process.env.HARBOR_CAPACITY_PG_BIN?'requires private PG18 tools':false,timeout:30000},async()=>{
  const lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB}),store=new PostgresStore({connection:lab.connection,poolSize:1}),other=new PostgresStore({connection:{...lab.connection,application_name:'harbor_infrastructure_publisher'},poolSize:1});
  let otherClosed=false;
  try{
    await other.get('SELECT 1');
    await assert.rejects(store.transaction(()=>assertStorageOperatorTargetIdle(store)),{code:'storage_operator_target_has_other_connections'});
    await store.transaction(async()=>{
      assert.ok(await store.get("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() LIMIT 1"));
      let observedForeign,reads=0;const reached=new Promise(resolve=>{observedForeign=resolve;}),get=store.get.bind(store),wrapped={query:store.query.bind(store),get:async(...args)=>{const row=await get(...args);if(row&&++reads===1)observedForeign();return row;}};
      const controller=new AbortController(),pending=assertStorageOperatorTargetIdle(wrapped,{allowWait:true,deadline:performance.now()+5000,signal:controller.signal,pollMs:5});pending.catch(()=>{});
      try{await Promise.race([reached,pending.then(()=>assert.fail('an open client cannot be classified as idle'))]);await other.close();otherClosed=true;await pending;}
      finally{controller.abort();await pending.catch(()=>{});}
      await store.query('SELECT pg_stat_clear_snapshot()');assert.equal(await get("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() LIMIT 1"),undefined);
    });
  }finally{if(!otherClosed)await other.close();await store.close();await lab.close();}
});

test('a persistent actual client is never exempted and reaches the bounded managed deadline',{skip:!process.env.HARBOR_CAPACITY_PG_BIN?'requires private PG18 tools':false,timeout:30000},async()=>{
  const lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB}),store=new PostgresStore({connection:lab.connection,poolSize:1}),other=new PostgresStore({connection:{...lab.connection,application_name:'harbor_infrastructure_publisher'},poolSize:1});
  try{await other.get('SELECT 1');await assert.rejects(store.transaction(()=>assertStorageOperatorTargetIdle(store,{allowWait:true,deadline:performance.now()+80,pollMs:5})),{code:'storage_operator_target_has_other_connections'});assert.equal((await other.get('SELECT 1 AS alive')).alive,1);}
  finally{await other.close();await store.close();await lab.close();}
});
