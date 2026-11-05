import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresAccountQuotas} from '../postgres-account-quotas.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';

test('official entitlement observations and refusal latches survive real quota refresh transactions',{
  skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需要专用PG18，不访问真实上游':false,timeout:60000,
},async()=>{
  let lab,store,quotas,queue;
  try{
    lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
    store=new PostgresStore({connection:lab.connection});await store.migrate();
    const now=1800000000000,id=randomUUID(),recovered=[],blocks=[];
    await store.run("INSERT INTO harbor.accounts(id,name,home,state,kind,auth_mode) VALUES($1,'合成权益观察','synthetic-entitlement-read','ready','codex','subscription')",id);
    let result;
    const router={recover:async(_account,data)=>recovered.push(structuredClone(data)),block:async()=>blocks.push(true),syncWeights:async()=>{}};
    queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:2});
    quotas=createPostgresAccountQuotas(store,{router,queue,clock:()=>now,readQuota:async()=>result});
    const read=async input=>{result={rateLimits:{planType:'pro',primary:{usedPercent:10,windowDurationMins:300,resetsAt:1800000600},...input.rateLimits},...input};if(input.rateLimits)result.rateLimits={planType:'pro',primary:{usedPercent:10,windowDurationMins:300,resetsAt:1800000600},...input.rateLimits};const outcome=await quotas.refresh(await quotas.load(id),{force:true});assert.deepEqual(outcome,{refreshed:true});return JSON.parse((await quotas.load(id)).quota_json);};
    const denied=await read({ordinaryUsageAllowed:false,rateLimits:{spendControlReached:true,rateLimitReachedType:'workspace_owner_credits_depleted',credits:{hasCredits:true,unlimited:false,balance:'12.34567890123456789'}},rateLimitResetCredits:{availableCount:7,credits:null}});
    assert.deepEqual(denied.officialDenial,{ordinary:true,spend_control:true,workspace:true});
    assert.equal(denied.entitlements.credits.balance,'12.34567890123456789');assert.equal(denied.entitlements.credits.unit,null);
    assert.equal(denied.entitlements.reset_cards.remaining,7);assert.equal(denied.entitlements.reset_cards.cards,null);
    assert.equal(denied.entitlements.dispatch_permission_verified,false);
    const sparse=await read({ordinaryUsageAllowed:null});
    assert.equal(sparse.remaining,90);assert.deepEqual(sparse.officialDenial,denied.officialDenial);
    assert.deepEqual(recovered.at(-1).officialDenial,denied.officialDenial);
    const recoveredQuota=await read({ordinaryUsageAllowed:true,rateLimits:{spendControlReached:false}});
    assert.deepEqual(recoveredQuota.officialDenial,{ordinary:false,spend_control:false,workspace:false});
    // Read-only observations cannot create an official extra-use authorization.
    assert.equal((await store.get('SELECT count(*)::int n FROM harbor.account_extra_entitlements WHERE account_id=$1',id)).n,0);
    result={rateLimits:{},ordinaryUsageAllowed:false,rateLimitResetCredits:{availableCount:7,credits:null}};
    assert.deepEqual(await quotas.refresh(await quotas.load(id),{force:true}),{refreshed:true});
    const noWindows=JSON.parse((await quotas.load(id)).quota_json);
    assert.deepEqual(noWindows.windows,[]);assert.equal(noWindows.remaining,null);assert.equal(noWindows.entitlements.reset_cards.remaining,7);
    assert.equal(noWindows.officialDenial.ordinary,true);
    result={rateLimits:{},ordinaryUsageAllowed:null};
    assert.deepEqual(await quotas.refresh(await quotas.load(id),{force:true}),{refreshed:true});
    assert.equal(JSON.parse((await quotas.load(id)).quota_json).officialDenial.ordinary,true);
    // After an explicit recovery an unknown, windowless read is not a zero.
    await read({ordinaryUsageAllowed:true,rateLimits:{spendControlReached:false}});const count=blocks.length;
    result={rateLimits:{},ordinaryUsageAllowed:null};
    assert.deepEqual(await quotas.refresh(await quotas.load(id),{force:true}),{refreshed:true});assert.equal(blocks.length,count);
    result={rateLimits:{rateLimitReachedType:'rate_limit_reached'}};
    assert.deepEqual(await quotas.refresh(await quotas.load(id),{force:true}),{refreshed:true});
    assert.equal(JSON.parse((await quotas.load(id)).quota_json).officialDenial.ordinary,true);
    result={rateLimits:{},ordinaryUsageAllowed:null};await quotas.refresh(await quotas.load(id),{force:true});
    assert.equal(JSON.parse((await quotas.load(id)).quota_json).officialDenial.ordinary,true);
  }finally{quotas?.stop();await queue?.close();await quotas?.close();await store?.close();await lab?.close();}
});
