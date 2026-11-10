import test from 'node:test';
import assert from 'node:assert/strict';
import seed from '../pricing-data.mjs';
import {fetchOfficialPricing,pricingPublication} from '../pricing.mjs';
import {createPostgresPricingRuntime} from '../postgres-pricing-runtime.mjs';
import {createControlledUpstream} from '../tools/capacity/controlled-upstream.mjs';
import {capacityPricingPage,createCapacityPricingFetch} from '../tools/capacity/pricing-source.mjs';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {requestCapacityTarget} from '../tools/capacity/http-client.mjs';

test('controlled HTTP pricing preserves every shipped rate through the real official parser',async t=>{
 const upstream=await createControlledUpstream(),requests=[],actualFetch=globalThis.fetch;
 const mock=t.mock.method(globalThis,'fetch',(url,options)=>{requests.push(String(url));return actualFetch(url,options);});
 try{
  const fetchImpl=createCapacityPricingFetch(upstream.origin),result=await fetchOfficialPricing({fetchImpl});
  assert.deepEqual(result.prices,seed.rates);assert.match(result.sourceHash,/^[a-f0-9]{64}$/);
  assert.deepEqual(requests,[upstream.origin+'/pricing']);assert.equal(upstream.status().pricingRequests,1);
  assert.equal(pricingPublication(result.prices,{manualRates:{},aliases:[]}).id,pricingPublication(seed.rates,{manualRates:{},aliases:[]}).id);
  assert.equal(typeof capacityPricingPage(),'string');
  assert.throws(()=>fetchImpl('https://unexpected.invalid/pricing',{}),/source_unexpected/);
  assert.equal(requests.length,1,'an unexpected source must never fall back to the Internet');
  const controller=new AbortController();controller.abort(Error('synthetic-pricing-cancel'));
  await assert.rejects(fetchOfficialPricing({fetchImpl,signal:controller.signal}),/synthetic-pricing-cancel/);
 }finally{mock.mock.restore();await upstream.close();}
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('actual capacity gateway refresh publishes the frozen fixture without changing price generation',{skip:!binDirectory?'需本任务独占PG18':false,timeout:120000},async()=>{
 let owned,runAuxiliary,primary;
 try{
  owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1,onSetup:options=>{runAuxiliary=options.runAuxiliaryProbe;}});
  const original=await owned.store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'"),active=JSON.parse(original.value).active;
  const marker=await owned.store.get('SELECT pricing_revision FROM harbor_capacity.fixture WHERE singleton=1');
  assert.equal(marker.pricing_revision,active);
  assert.equal(active,pricingPublication(seed.rates,{manualRates:{},aliases:[]}).id);
  for(let index=0;index<2;index++){
   const before=owned.upstream.status().pricingRequests;
   const response=await runAuxiliary(()=>requestCapacityTarget({origin:owned.gateway.ready.origin,path:'/api/admin/pricing/refresh',method:'POST',headers:{...owned.adminActors[0].headers,origin:owned.gateway.ready.origin,'content-type':'application/json'},body:{},mode:'json'}));
   assert.equal(response.status,200,JSON.stringify({error:response.error,status:response.status}));assert.equal(response.success,true);
   assert.equal(owned.upstream.status().pricingRequests,before+1,'real child runtime must consume the owned HTTP source');
   const state=JSON.parse((await owned.store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value);
   assert.equal(state.active,active);assert.equal(state.error,null);assert.ok(state.checkedAt>0&&state.lastSuccess>0);assert.match(state.sourceHash,/^[a-f0-9]{64}$/);
  }
  const settings=await owned.store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.settings'");
  assert.equal(settings?JSON.parse(settings.value).enabled:true,true,'automatic refresh remains enabled');
  let at=Date.now()+25*3600000;const runtime=createPostgresPricingRuntime(owned.store,{clock:()=>at,fetchImpl:createCapacityPricingFetch(owned.upstream.origin)});
  try{
   const before=owned.upstream.status().pricingRequests;
   await runtime.tick();assert.equal(owned.upstream.status().pricingRequests,before+1,'a due background tick performs the real refresh');
   await runtime.tick();assert.equal(owned.upstream.status().pricingRequests,before+1,'a fresh publication retains the normal refresh interval');
   await runtime.configure({expectedVersion:0,enabled:false});at+=25*3600000;
   await runtime.tick();assert.equal(owned.upstream.status().pricingRequests,before+1,'the source adapter cannot bypass disabled synchronization');
   await runtime.configure({expectedVersion:1,enabled:true});
  }finally{await runtime.close();}
  const prices=await owned.store.get('SELECT COUNT(*) bad FROM harbor.requests WHERE price_version IS DISTINCT FROM $1',active);assert.equal(prices.bad,0);
 }catch(error){primary=error;throw error;}finally{try{await owned?.close();}catch(error){if(!primary)throw error;primary.cleanupFailed=true;}}
});
