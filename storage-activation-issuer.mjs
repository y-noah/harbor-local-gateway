import {randomUUID} from 'node:crypto';
import {artifactKey,canonical,transferError} from './migration-artifacts.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {inspectStorageActivationPending} from './storage-activation-state.mjs';
import {signStorageActivationReceipt,validateStorageActivationFacts} from './storage-activation-receipt.mjs';
const fail=code=>{throw transferError('storage_activation_'+code);};
export async function issueStorageActivation({context,qualification,activationKey,backupKey,signal,ttlMs=1800000}={}){
 const trusted=storageOperatorContext(context);if(process.platform!=='linux'||process.getuid()!==0||trusted.scope!=='docker-systemd')fail('root_required');artifactKey(activationKey);artifactKey(backupKey);if(activationKey.equals(backupKey))fail('independent_key_required');if(!Number.isSafeInteger(ttlMs)||ttlMs<1000||ttlMs>86400000)fail('ttl');signal?.throwIfAborted();
 const {inspectStorageOperatorActivationProof}=await import('./storage-operator-activation-proof.mjs');
 const facts=structuredClone(validateStorageActivationFacts(await inspectStorageOperatorActivationProof(qualification,{context,signal})));
 if(facts.configurationDigest!==trusted.configurationDigest||facts.cluster!==trusted.configuration.expectedCluster||facts.publicOrigin!==trusted.configuration.candidate.publicOrigin||facts.deviceOrigin!==trusted.configuration.candidate.deviceOrigin||facts.candidate.manifestDigest!==trusted.configuration.candidate.imageDigest)fail('root_configuration');
 const admin=new PostgresStore({...trusted.adminOptions,poolSize:1});let store;try{const raw=admin.pool.options;store=new PostgresStore({connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database:facts.database.name,...(raw.ssl?{ssl:raw.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true});const {now}=await inspectStorageActivationPending(store,facts);signal?.throwIfAborted();const fresh=await inspectStorageOperatorActivationProof(qualification,{context,signal});if(canonical(facts)!==canonical(fresh))fail('proof_changed');signal?.throwIfAborted();return signStorageActivationReceipt({facts,nonce:randomUUID(),issuedMs:now,expiresMs:now+ttlMs},activationKey);}finally{await store?.close();await admin.close();}
}
