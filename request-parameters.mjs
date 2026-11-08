import {sanitizeRecord} from './record-redaction.mjs';

// Observation only: never rewrite the forwarded payload or infer defaults.
// Keep bounded parameter identifiers; credential-like values remain unrecorded.
function parameter(value){
  if(typeof value!=='string'||!/^[A-Za-z0-9_.-]{1,128}$/.test(value))return null;
  return sanitizeRecord(value,{maxBytes:256})===value?value:null;
}
export function requestParameters(payload){
  return {effort:parameter(payload?.reasoning?.effort),service_tier:parameter(payload?.service_tier)};
}

export function safeUpstreamRequestId(value){return parameter(value);}
