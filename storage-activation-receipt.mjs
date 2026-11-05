import {createHmac,timingSafeEqual,createHash} from 'node:crypto';
import {artifactKey,canonical,transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError('storage_activation_'+code);};
const digest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value),oid=value=>typeof value==='string'&&/^[1-9]\d{0,9}$/.test(value),uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const keys=(value,expected)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...expected].sort().join(',');
const factsKeys=['runId','bindingDigest','configurationDigest','targetDigest','cluster','database','roles','candidate','materialDigest','keyMaterialDigest','deviceCaDigest','backupId','checkpointDigest','receiverResultDigest','sourceMachine','receiverMachine','publicOrigin','deviceOrigin','stack','deviceInventory'];
export function validateStorageActivationFacts(facts){
 if(!keys(facts,factsKeys)||!uuid(facts.runId)||!uuid(facts.backupId)||!/^\d{1,20}$/.test(facts.cluster??'')||!['bindingDigest','configurationDigest','targetDigest','materialDigest','keyMaterialDigest','deviceCaDigest','checkpointDigest','receiverResultDigest','sourceMachine','receiverMachine'].every(key=>digest(facts[key]))||facts.sourceMachine===facts.receiverMachine)fail('facts');
 if(!keys(facts.database,['name','oid'])||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(facts.database.name??'')||!oid(facts.database.oid)||!keys(facts.roles,['owner','runtime']))fail('identity');
 for(const kind of ['owner','runtime'])if(!keys(facts.roles[kind],['name','oid'])||!new RegExp('^harbor_'+kind+'_[a-f0-9]{24}$').test(facts.roles[kind].name??'')||!oid(facts.roles[kind].oid))fail('identity');
 if(facts.roles.owner.oid===facts.roles.runtime.oid||!keys(facts.candidate,['manifestDigest','configDigest','revision','schemaDigest','sourceDigest'])||!['manifestDigest','configDigest'].every(key=>/^sha256:[a-f0-9]{64}$/.test(facts.candidate[key]??''))||!/^[a-f0-9]{40}$/.test(facts.candidate.revision??'')||!digest(facts.candidate.schemaDigest)||!digest(facts.candidate.sourceDigest))fail('candidate');
 for(const key of ['publicOrigin','deviceOrigin']){let url;try{url=new URL(facts[key]);}catch{fail('origin');}if(url.origin!==facts[key]||url.username||url.password||!(key==='deviceOrigin'?url.protocol==='https:':['http:','https:'].includes(url.protocol)))fail('origin');}
 if(facts.stack!=='/opt/product-development/harbor-local-gateway'||!keys(facts.deviceInventory,['count','sha256','mode'])||!Number.isSafeInteger(facts.deviceInventory.count)||facts.deviceInventory.count<0||!digest(facts.deviceInventory.sha256)||!['authenticated-empty','existing-client-challenge'].includes(facts.deviceInventory.mode)||facts.deviceInventory.count===0!==(facts.deviceInventory.mode==='authenticated-empty'))fail('device_inventory');
 return facts;
}
const mac=(payload,key)=>createHmac('sha256',artifactKey(key)).update('harbor-storage-activation-v1\0').update(canonical(payload)).digest('hex');
// A signature is only an envelope. The issuer must obtain actual private
// qualification; the deployment consumer still verifies live DB generations.
export function signStorageActivationReceipt({facts,nonce,issuedMs,expiresMs},key){
 validateStorageActivationFacts(facts);if(!uuid(nonce)||!Number.isSafeInteger(issuedMs)||!Number.isSafeInteger(expiresMs)||issuedMs<0||expiresMs<=issuedMs||expiresMs-issuedMs>86400000)fail('time');
 const payload={version:1,purpose:'harbor-storage-activation-v1',facts:structuredClone(facts),nonce,issuedMs,expiresMs};return {payload,signature:mac(payload,key)};
}
export function verifyStorageActivationReceipt(receipt,key,{expectedFacts}={}){
 const payload=receipt?.payload;if(!keys(receipt,['payload','signature'])||!keys(payload,['version','purpose','facts','nonce','issuedMs','expiresMs'])||payload.version!==1||payload.purpose!=='harbor-storage-activation-v1'||typeof receipt.signature!=='string'||!/^[a-f0-9]{64}$/.test(receipt.signature)||!timingSafeEqual(Buffer.from(receipt.signature,'hex'),Buffer.from(mac(payload,key),'hex')))fail('receipt');
 const checked=signStorageActivationReceipt(payload,key).payload;if(expectedFacts&&canonical(checked.facts)!==canonical(validateStorageActivationFacts(expectedFacts)))fail('binding');return structuredClone(checked);
}
export const storageActivationReceiptDigest=receipt=>createHash('sha256').update(canonical(receipt)).digest('hex');
