import {createHmac} from 'node:crypto';

// These capabilities and facts are process-local. In particular, neither the
// upstream account identifier nor the internal correlation reference belongs
// in the quota JSON returned to a department administrator.
const identities=new WeakMap(),subjects=new WeakMap();
const unknown=reason=>Object.freeze({state:'unknown',reason,ordinary_scope:'unknown',extra_scope:'unknown'});
const unavailable=unknown('trusted_source_unavailable');
const opaque=(value,max)=>typeof value==='string'&&value.length>0&&value.length<=max&&value===value.trim()&&!/[\x00-\x1f\x7f]/.test(value);

export function createCodexQuotaSubjectIdentity(key,{schemaVersion}={}){
  if(!Buffer.isBuffer(key)||key.length!==32||typeof schemaVersion!=='string'||!/^\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?$/.test(schemaVersion))throw Error('供应方额度身份来源配置无效');
  const identity=Object.freeze({});identities.set(identity,{key:Buffer.from(key),schemaVersion});return identity;
}

// The offline-checked Codex CLI 0.160.1 and 0.167.2 schemas describe accountId as
// the backend account associated with this snapshot and the map keys as
// metered limit_id values. This establishes only an ordinary metered bucket,
// not ownership of all workspace, project, spend-control or credit balances.
export function attachCodexQuotaSubject(snapshot,value,{rate,bucketKey,identity}={}){
  if(!snapshot||typeof snapshot!=='object')return;
  const source=identities.get(identity);if(!source)return;
  const save=facts=>subjects.set(snapshot,facts);
  if(!opaque(value?.accountId,1024)){save(unknown('backend_account_unavailable'));return;}
  const explicit=rate?.limitId;
  if(explicit!==undefined&&explicit!==null&&!opaque(explicit,256)){save(unknown('metered_bucket_invalid'));return;}
  if(bucketKey!==undefined&&!opaque(bucketKey,256)){save(unknown('metered_bucket_invalid'));return;}
  if(bucketKey!==undefined&&explicit!==undefined&&explicit!==null&&bucketKey!==explicit){save(unknown('metered_bucket_conflict'));return;}
  const bucket=bucketKey??explicit;
  if(!opaque(bucket,256)){save(unknown('metered_bucket_unavailable'));return;}
  const ref=createHmac('sha256',source.key).update(JSON.stringify(['harbor.codex.ordinary-metered-bucket.v1',value.accountId,bucket])).digest('hex');
  save(Object.freeze({state:'known',ref,ordinary_scope:'account_metered_bucket',extra_scope:'unknown',source:'codex_account_rate_limits_read',schema_version:source.schemaVersion,checked:snapshot.checked}));
}

export function codexQuotaSubject(snapshot){return subjects.get(snapshot)||unavailable;}

export function apiCredentialQuotaSubject(identity,digest){
  const source=identities.get(identity);
  if(!source||typeof digest!=='string'||!/^[a-f0-9]{64}$/.test(digest))return unavailable;
  const ref=createHmac('sha256',source.key).update(JSON.stringify(['harbor.openai.same-credential.v1',digest])).digest('hex');
  return Object.freeze({state:'known',ref,ordinary_scope:'same_credential',extra_scope:'unknown',source:'stored_api_secret_digest'});
}
