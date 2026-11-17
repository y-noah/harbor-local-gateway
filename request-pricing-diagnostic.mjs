import {referenceModel} from './pricing.mjs';

const calculationStates=new Set(['priced','manual','pending','unmetered','legacy_manual','expired']);
const amountFields=['input','output','cached','adjustment','total'];
function calculationReason(request,quote,book){
 if(calculationStates.has(quote?.state))return quote.state;
 if(quote?.state!=='unpriced')return 'unclassified';
 if(!quote.revision&&!request?.price_version)return 'price_version_missing';
 if(book===null)return 'price_book_missing';
 if(!book||typeof book.rates!=='object'||book.rates===null)return 'unpriced_unclassified';
 try{
  const model=referenceModel(request.billing_model||request.model,request.started,book);
  if(!model)return 'model_rate_missing';
  if(request.cached>0&&book.rates[model]?.cached===null)return 'cached_rate_missing';
 }catch{return 'unpriced_unclassified';}
 return 'unpriced_unclassified';
}
function atoms(value){
 if(typeof value==='bigint')return value;
 if(typeof value==='number')return Number.isSafeInteger(value)?BigInt(value):undefined;
 if(typeof value==='string'&&/^-?\d+$/.test(value)){try{return BigInt(value);}catch{return undefined;}}
 return undefined;
}
function sameAmount(actual,expected){
 if(actual===null||expected===null)return actual===null&&expected===null;
 const left=atoms(actual),right=atoms(expected);return left!==undefined&&right!==undefined&&left===right;
}
function projectionReason(request,quote,projection,book){
 if(!request?.price_version)return 'request_version_missing';
 if(!projection)return 'missing';
 if(projection.revision!==request.price_version)return 'revision_mismatch';
 if(quote?.state==='unpriced'&&book===null)return 'book_missing';
 if(projection.state!==quote?.state)return 'state_mismatch';
 if(amountFields.some(field=>!sameAmount(projection[field+'_atoms'],quote?.[field+'Atoms'])))return 'amount_mismatch';
 return 'consistent';
}
// Explain a single authorized detail without changing its price or persisted facts.
export function requestPricingDiagnostic({request,quote,projection,book}={}){
 return {version:1,calculation:calculationReason(request,quote,book),projection:projectionReason(request,quote,projection,book)};
}
