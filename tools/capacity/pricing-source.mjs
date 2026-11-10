import seed from '../../pricing-data.mjs';
import {pricingPublication} from '../../pricing.mjs';

// The capacity laboratory exercises normal parsing/publication against the
// shipped public snapshot. It never fetches or invents live model prices.
const publication=pricingPublication(seed.rates,{manualRates:{},aliases:[]});
const page=['# Pricing','## Standard pricing data','| Model | Input | Cached input | Output |','| --- | --- | --- | --- |',
 ...Object.entries(seed.rates).map(([model,rate])=>'| '+[model,...['input','cached','output'].map(field=>rate[field]===null?'-':'$'+rate[field])].join(' | ')+' |'),
 '## Batch pricing data'].join('\n');
export const capacityPricingPage=()=>page;

// Use the same canonical publication before any fixture price is frozen.
export async function initializeCapacityPricing(store){
 await store.initializePricing();
 return store.transaction(async()=>{
  const saved=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state' FOR UPDATE"),state=JSON.parse(saved.value),book=await store.pricingBook(state.active);
  if(!book||pricingPublication(book.rates,{manualRates:{},aliases:book.aliases}).id!==publication.id)throw Error('capacity_pricing_fixture_mismatch');
  await store.run('INSERT INTO harbor.pricing_versions(id,created,book) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING',publication.id,Date.now(),publication.json);
  await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({...state,active:publication.id}));
  return publication.id;
 });
}

export function createCapacityPricingFetch(origin){
 const target=new URL('/pricing',origin);
 return (url,options)=>{
  if(String(url)!==seed.source)throw Error('capacity_pricing_source_unexpected');
  return fetch(target,options);
 };
}
