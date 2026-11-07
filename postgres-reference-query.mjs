import {sanitizeRecord} from './record-redaction.mjs';
import {decorateReferenceRequest,emptyReferenceQuote,publicUsage} from './pricing.mjs';

export const fields=['input','output','cached','adjustment','total'];
export const validPrice='p.revision=r.price_version AND v.id IS NOT NULL';
export const joins=' LEFT JOIN harbor.request_prices p ON p.request_id=r.id LEFT JOIN harbor.pricing_versions v ON v.id=r.price_version';
export function decorate(row,start){
  const {projected_state,projected_model,projected_source,projected_revision,book_exists,phase_history,q_input,q_output,q_cached,q_adjustment,q_total,...raw}=row;
  const amounts=[q_input,q_output,q_cached,q_adjustment,q_total];
  const quote=book_exists&&projected_revision===raw.price_version&&projected_state?
    {state:projected_state,model:projected_model,modelSource:projected_source,revision:projected_revision,...Object.fromEntries(fields.map((f,i)=>[f+'Atoms',amounts[i]===null?null:BigInt(amounts[i])]))}:
    emptyReferenceQuote('unpriced',raw.billing_model||raw.model,raw.price_version);
  return {...decorateReferenceRequest(raw,publicUsage(quote),start),error:raw.error?sanitizeRecord(raw.error,{maxBytes:2048}):null};
}
export function aggregate(row){
  const absent=row.requests>0&&row.known===0,state=row.unmetered+row.unpriced+row.expired?'partial':row.pending?'pending':'priced';
  const standard={...publicUsage({state,model:null,revision:null,...Object.fromEntries(fields.map(f=>[f+'Atoms',absent?null:BigInt(row[f])]))}),
    requests:row.requests,known:row.known,unmetered:row.unmetered,unpriced:row.unpriced,pending:row.pending,expired:row.expired,revisions:(row.revisions||[]).sort()};
  return {requests:row.requests,success:row.success,active:row.active,average_ms:Number(row.average_ms||0),unmetered:row.unmetered,unpriced:row.unpriced,standard};
}

// Fixed SQL callers can aggregate several windows in one scan without duplicating
// request rows. Every amount remains NUMERIC until its text/BigInt boundary.
export function referenceAggregateSql({condition='',prefix=''}={}){
  const filter=predicate=>condition||predicate?` FILTER(WHERE ${[condition,predicate].filter(Boolean).map(value=>'('+value+')').join(' AND ')})`:'',
    count=(name,predicate)=>`COUNT(*)${filter(predicate)} ${prefix}${name}`;
  return [count('requests'),count('success',"status='ok'"),count('active',"status IN ('queued','running')"),
    `AVG(ended-started)${filter('ended>=started')} ${prefix}average_ms`,count('known','total IS NOT NULL'),
    count('pending',"total IS NULL AND price_state='pending'"),count('unpriced',"total IS NULL AND (price_state='unpriced' OR price_state IS NULL)"),
    count('expired',"total IS NULL AND price_state='expired'"),count('unmetered',"total IS NULL AND price_state NOT IN ('pending','unpriced','expired')"),
    ...fields.map(field=>`COALESCE(SUM(${field})${filter()},0)::text ${prefix}${field}`),
    `ARRAY_AGG(DISTINCT revision)${filter('total IS NOT NULL AND revision IS NOT NULL')} ${prefix}revisions`].join(',');
}
export const aggregateSql=referenceAggregateSql();
