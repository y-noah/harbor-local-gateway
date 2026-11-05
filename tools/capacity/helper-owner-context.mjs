import {PostgresStore} from '../../postgres-store.mjs';
import {revealStoredKey} from '../../store.mjs';
import {loadCapacityTrustedRuntimeArtifact} from './runtime-artifact.mjs';
import {capacityWireSignature} from './gateway-probe.mjs';
import {mkdir,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {isDeepStrictEqual as equal} from 'node:util';
import {isOwnedLaboratory} from './owned-runtime.mjs';
import {fixtureId,fixtureDatabase} from './dataset.mjs';
import {capacityAgent,requestCapacityTarget,credentialFingerprints} from './http-client.mjs';
import {compileCapacityWindowsHelper,enrollCapacityWindowsDevice,capacityDeviceDeliveryBinding,inspectCompiledCapacityWindowsHelper,releaseCompiledCapacityWindowsHelper,loadCapacityTrustedWindowsHelper} from './device-adapter.mjs';
import {capacityWindowsHelperObserverBinding} from './windows-helper-observer.mjs';
const owners=new WeakMap(),fail=code=>{throw Error('capacity_helper_owner_'+code);};
const digest=value=>createHash('sha256').update(value).digest('hex');
export function capacityHelperOwnerBinding(value){return owners.get(value)??null;}

// This owner is constructed inside the factory. Its source handles never cross
// IPC. A catalog challenge proves actual device TLS, not a successful model run.
export function createCapacityHelperOwner({manifest,laboratory,store,encryptionKey,adminPassword,directory,runLifetime,isOpen,signal}={}){
 if(!isOwnedLaboratory(laboratory)||!store||!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32||typeof adminPassword!=='string'||!adminPassword||typeof directory!=='string'||typeof isOpen!=='function')fail('configuration');
 const sql=new PostgresStore({connection:{...laboratory.connection,database:fixtureDatabase(manifest),query_timeout:10000},poolSize:2,statementTimeoutMs:10000});
 // These qualification queries use the existing 10s liveness budget, not the
 // seed store's 10min bulk-write budget. Destroy errored physical clients so a
 // read timeout cannot later execute queued writes/COMMIT after cleanup retries.
 sql.pool.on('connect',client=>{const query=client.query.bind(client),failed=error=>{if(error)void Promise.resolve(client.end()).catch(()=>{});};client.query=(text,values,callback)=>{if(typeof values==='function'){callback=values;values=undefined;}if(typeof callback==='function')return query(text,values,(error,result)=>{failed(error);callback(error,result);});return query(text,values).catch(error=>{failed(error);throw error;});};});
 let current=null,closed=false,closing=false,helper,closePromise,activationPromise;const ownerController=new AbortController(),ownerSignal=AbortSignal.any([signal,ownerController.signal].filter(Boolean)),challenges=new Map();const history=[];
 const alive=()=>{ownerSignal.throwIfAborted();runLifetime.assertCoverage();if(closed||closing||!isOpen())fail('closed');};
 const marker=async({empty=false,cleanup=false}={})=>{if(!cleanup)alive();else if(!isOwnedLaboratory(laboratory))fail('cleanup_owner');const row=await sql.get('SELECT current_database() database,run_id,owner_nonce,manifest_hash,metadata_cursor,body_cursor,(SELECT system_identifier::text FROM pg_control_system()) cluster FROM harbor_capacity.fixture WHERE singleton=1');if(row?.database!==fixtureDatabase(manifest)||row.run_id!==manifest.runId||row.owner_nonce!==manifest.ownerNonce||row.manifest_hash!==manifest.manifestHash||row.cluster!==current?.binding.cluster)fail('dataset');if(empty)Object.assign(row,await sql.get('SELECT (SELECT COUNT(*) FROM harbor.requests) requests,(SELECT COUNT(*) FROM harbor.recordings) recordings'));if(empty&&(row.metadata_cursor!==0||row.body_cursor!==0||row.requests!==0||row.recordings!==0))fail('preseed_not_empty');return row;};
 const http=async(row,path,{method='GET',body,headers=row.adminHeaders,agent=row.agent,origin=row.origin}={})=>{alive();const result=await requestCapacityTarget({origin,path,method,body,headers:{'content-type':'application/json',...headers},agent,mode:'json',signal:ownerSignal,timeoutMs:30000});if(!result.success)fail('health_http');return result.json;};
 const observe=event=>{const wire=challenges.get(event?.trace);if(!wire)return false;if(event.kind==='gateway-request-start'){wire.started++;wire.id??=event.wireId;if(event.signature!==wire.signature||event.credentials?.authorization!==wire.authorization||event.credentials?.cookie!==wire.cookie)wire.invalid=true;}else if(event.kind==='gateway-response-end'){wire.ended++;if(event.wireId!==wire.id||!wire.statuses.includes(event.status))wire.invalid=true;}else if(event.kind==='gateway-handler-settled'){wire.settled++;if(event.wireId!==wire.id||event.outcome!=='resolved')wire.invalid=true;}else if(['gateway-response-abort','gateway-handler-unobservable'].includes(event.kind))wire.invalid=true;if(wire.started>1||wire.ended>1||wire.settled>1)wire.invalid=true;return true;};
 async function ownedRequest(row,options,statuses=[200]){
  if(challenges.size>=2)fail('challenge_budget');const trace=randomUUID(),headers={...options.headers,'x-capacity-trace':trace},credentials=credentialFingerprints(headers),wire={started:0,ended:0,settled:0,id:null,invalid:false,signature:capacityWireSignature(options.method??'GET',options.path),authorization:credentials.authorization,cookie:credentials.cookie,statuses};challenges.set(trace,wire);
  try{const result=await requestCapacityTarget({...options,headers}),deadline=performance.now()+5000;while(!wire.invalid&&(wire.started!==1||wire.ended!==1||wire.settled!==1)){alive();if(performance.now()>=deadline)fail('wire_timeout');await new Promise(resolve=>setTimeout(resolve,5));}if(wire.invalid)fail('wire_invalid');return result;}finally{challenges.delete(trace);}
 }
 async function login(row){let cookie;const result=await ownedRequest(row,{origin:row.origin,path:'/api/auth/login',method:'POST',body:{password:adminPassword},headers:{'content-type':'application/json'},agent:row.agent,mode:'json',signal:ownerSignal,timeoutMs:30000,onResponseHeaders:headers=>{cookie=headers['set-cookie']?.find(value=>value.startsWith('harbor_session='))?.split(';')[0];}});if(!result.success||!cookie)fail('admin_login');row.adminHeaders={cookie};}
 async function catalogChallenge(row,actor){const result=await ownedRequest(row,{origin:actor.origin,path:'/v1/models',agent:actor.agent??null,headers:actor.headers,mode:'json',signal:ownerSignal,timeoutMs:30000});if(!result.success)fail('catalog');return result.json;}
 async function health(row,actor){
  const binding=capacityDeviceDeliveryBinding(actor);if(!binding||!capacityWindowsHelperObserverBinding(binding.observer))fail('source_unowned');
  const before=await marker({empty:row.phase==='pre-seed'});await binding.observer.sample();const catalog=await catalogChallenge(row,actor);if(!Array.isArray(catalog.models)||!catalog.models.some(model=>manifest.models.includes(model.slug)))fail('catalog');await binding.observer.sample();
  const after=await marker({empty:row.phase==='pre-seed'});if(row.phase==='pre-seed'&&(before.requests!==after.requests||before.recordings!==after.recordings))fail('preseed_mutated');
  const key=await sql.get('SELECT device_ids,device_policy_version FROM harbor.keys WHERE id=$1 AND member_id=$2',actor.keyId,actor.memberId);const restricted=Array.isArray(key?.device_ids)&&key.device_ids.includes(actor.deviceId);if(row.phase==='pre-seed'&&!restricted)fail('key_unrestricted');
  const unsigned=await ownedRequest(row,{origin:row.origin,path:'/v1/models',headers:{authorization:'Bearer '+actor.secret,'x-harbor-device-id':actor.deviceId},agent:row.agent,mode:'json',signal:ownerSignal,timeoutMs:30000},[401,403]);if(unsigned.success||![401,403].includes(unsigned.status))fail('unsigned_not_rejected');
  return {binding,actor,keyVersion:key.device_policy_version};
 }
 async function inspectRow(row,{kind='helper'}={}){
  alive();if(current!==row||row.retired||!['helper','clock'].includes(kind))fail('phase_revoked');await marker({empty:row.phase==='pre-seed'});
  const summaries=[];
  for(const source of row.sources){
   const {binding,actor}=source;if(capacityDeviceDeliveryBinding(actor)!==binding||!capacityWindowsHelperObserverBinding(binding.observer)||actor.deviceId!==binding.deviceId||actor.memberId!==binding.memberId||actor.keyId!==binding.keyId||actor.headers?.authorization!=='Bearer '+actor.secret)fail('source_revoked');const binary=await inspectCompiledCapacityWindowsHelper(binding.helper);
   const device=await sql.get('SELECT d.member_id,d.state,d.hardware_state,d.credential_generation,d.verified_request_at,c.expires,c.revoked FROM harbor.employee_devices d JOIN harbor.device_credentials c ON c.device_id=d.id AND c.generation=d.credential_generation WHERE d.id=$1',binding.deviceId);const identity=await sql.authenticate(actor.secret);
   if(identity.id!==actor.memberId||identity.key_id!==actor.keyId||device?.member_id!==actor.memberId||device.state!=='approved'||device.revoked!==null||device.expires<=Date.now())fail('device_changed');
   const key=await sql.get('SELECT device_ids FROM harbor.keys WHERE id=$1',actor.keyId);const member=await sql.get('SELECT device_mode FROM harbor.members WHERE id=$1',actor.memberId);if(row.phase==='pre-seed'?!equal(key.device_ids,[actor.deviceId]):member.device_mode!=='enforced')fail('device_policy_changed');
   if(row.phase!=='pre-seed'){
    if(binding.healthProof!=='model'||!device.verified_request_at)fail('model_unverified');const proof=await sql.get("SELECT id FROM harbor.requests WHERE member_id=$1 AND key_id=$2 AND device_id=$3 AND device_generation=$4 AND status='ok' AND device_auth_state IN ('authenticated','unverified_hardware') LIMIT 1",actor.memberId,actor.keyId,actor.deviceId,device.credential_generation);if(!proof)fail('model_unverified');
   }
   const credential=await binding.inspectCredential();if(!credential.ready||!['platform-nonexportable','software-nonexportable'].includes(credential.keyProtection))fail('cng_unavailable');
   if(kind==='clock'){await binding.observer.sample();await binding.observer.sample();}
   const clock=binding.observer.status();if(clock.fault||!clock.sourceOpen||clock.samples<2||!clock.clockEpoch)fail('clock_unproven');
   summaries.push({source:row.phase==='pre-seed'?'catalog-device-tls-only':'actual-model-verified-device',keyProtection:credential.keyProtection,upstreamOriginHash:digest(binding.upstreamOrigin),hardwareState:device.hardware_state,hardwareAttested:false,epochHash:digest(clock.clockEpoch),samples:clock.samples,binarySha256:binary.binarySha256,sourceHashes:binary.sources,trustedHarnessSha256:binary.trustedHarnessSha256});
  }
  if(!row.sources.length||row.phase!=='pre-seed'&&row.sources.length!==row.expectedActors)fail('actor_count');alive();if(current!==row||row.retired)fail('phase_revoked');return Object.freeze({phase:row.phase,actors:row.sources.length,endpoint:'helper-socket-write-complete',clockScope:'helper-qpc-to-controller-causal-envelope',remoteClockCovered:false,modelVerified:row.phase!=='pre-seed',sources:Object.freeze(summaries)});
 }
 async function retire(row){
  if(!row||row.retired)return;row.retired=true;
  if(row.phase!=='pre-seed'||!row.memberId){row.agent?.destroy();return;}
  // Cleanup also handles a partially enrolled device. The snapshot belongs to
  // this single private pre-seed operation; no other enrollment runs in it.
  let primary;for(const cleanup of row.cleanups)try{await cleanup();}catch(error){primary??=error;}
  try{
   await marker({empty:true,cleanup:true});if(row.actor){await sql.transaction(async()=>{const key=await sql.get('SELECT device_ids,device_policy_version FROM harbor.keys WHERE id=$1 FOR UPDATE',row.actor.keyId);if(equal(key.device_ids,[row.actor.deviceId])){const restored=await sql.run('UPDATE harbor.keys SET device_ids=$1::jsonb,device_policy_version=device_policy_version+1 WHERE id=$2 AND device_policy_version=$3',row.originalKey.device_ids===null?null:JSON.stringify(row.originalKey.device_ids),row.actor.keyId,key.device_policy_version);if(restored.changes!==1)fail('policy_cleanup_conflict');}else if(!equal(key.device_ids,row.originalKey.device_ids))fail('policy_cleanup_conflict');});}
  }catch(error){primary??=error;}
  if(!primary)try{await sql.transaction(async()=>{await marker({empty:true,cleanup:true});const devices=await sql.all('SELECT id FROM harbor.employee_devices WHERE member_id=$1',row.memberId),created=devices.filter(value=>!row.beforeDevices.includes(value.id)).map(value=>value.id);if(created.length>1||row.actor&&created.some(id=>id!==row.actor.deviceId)||row.cleanupDevices&&created.some(id=>!row.cleanupDevices.includes(id)))fail('cleanup_identity');row.cleanupDevices??=created;const invitations=await sql.all('SELECT id FROM harbor.device_invitations WHERE member_id=$1',row.memberId),invited=invitations.filter(value=>!row.beforeInvitations.includes(value.id)).map(value=>value.id);if(invited.length>1||row.cleanupInvitations&&invited.some(id=>!row.cleanupInvitations.includes(id)))fail('cleanup_identity');row.cleanupInvitations??=invited;if(created.length){await sql.run('DELETE FROM harbor.device_credentials WHERE device_id=ANY($1::text[])',created);await sql.run('DELETE FROM harbor.employee_devices WHERE id=ANY($1::text[])',created);}if(invited.length)await sql.run('DELETE FROM harbor.device_invitations WHERE id=ANY($1::text[])',invited);});}catch(error){primary=error;}
  row.agent?.destroy();if(primary){row.retired=false;throw primary;}history.push({phase:row.phase,retired:true,preseedModelRows:0});
 }
 async function activate(binding,{phase,configuration,certificates,actors}={}){
  alive();if(!['pre-seed','final','restore'].includes(phase)||binding.phase!==phase||binding.laboratory!==laboratory||binding.store!==store||binding.manifest!==manifest||current&&!current.retired&&current.phase===phase)fail('phase');
  if(current&&!current.retired){if(current.phase==='pre-seed')fail('preseed_not_retired');await retire(current);}
  const origin=configuration.publicOrigin??binding.gateway.ready.tlsOrigin,row={binding,phase,origin,agent:capacityAgent(origin,{ca:certificates.ca}),sources:[],cleanups:[],retired:false,expectedActors:actors?.length??1};current=row;
  try{
   await login(row);await marker({empty:phase==='pre-seed'});
   if(phase==='pre-seed'){
    const memberId=fixtureId(manifest.runId,'member',0),keyId=fixtureId(manifest.runId,'key',0),key=await sql.get('SELECT id,member_id,digest,secret_cipher FROM harbor.keys WHERE id=$1 AND member_id=$2',keyId,memberId),secret=key&&revealStoredKey(encryptionKey,key);if(!secret)fail('actor_key_unreadable');const actor={memberId,keyId,secret,synthetic:true};row.memberId=actor.memberId;row.beforeDevices=(await sql.all('SELECT id FROM harbor.employee_devices WHERE member_id=$1',actor.memberId)).map(value=>value.id);row.beforeInvitations=(await sql.all('SELECT id FROM harbor.device_invitations WHERE member_id=$1',actor.memberId)).map(value=>value.id);row.originalKey=await sql.get('SELECT device_ids,device_policy_version FROM harbor.keys WHERE id=$1',actor.keyId);
    const root=join(directory,'helper-proof');await mkdir(root,{mode:0o700});await writeFile(join(root,'ca.pem'),certificates.ca,{mode:0o600,flag:'wx'});helper=manifest.profile==='formal'?await loadCapacityTrustedWindowsHelper(await loadCapacityTrustedRuntimeArtifact()):await compileCapacityWindowsHelper(root);alive();
    row.actor=await enrollCapacityWindowsDevice({directory:root,origin:row.origin,ca:certificates.ca,adminAgent:row.agent,adminHeaders:row.adminHeaders,actor,runtime:laboratory.runtime,helper,healthProof:'catalog',observeDelivery:true,signal:ownerSignal,registerCleanup:cleanup=>row.cleanups.push(cleanup)});
    await http(row,'/api/admin/keys/'+actor.keyId+'/devices',{method:'PATCH',body:{device_ids:[row.actor.deviceId],expected_version:row.originalKey.device_policy_version}});row.policyChanged=true;row.sources.push(await health(row,row.actor));
   }else{
    if(!Array.isArray(actors)||actors.length<1||actors.length>60||manifest.profile==='formal'&&actors.length!==manifest.employees)fail('actors');for(const actor of actors)row.sources.push(await health(row,actor));
   }
   const evidence=await inspectRow(row);history.push({phase,actors:row.sources.length,ready:true});return evidence;
  }catch(error){try{await retire(row);}catch{}throw error;}
 }
 const api=Object.freeze({activate:(...args)=>{if(activationPromise)throw Error('capacity_helper_owner_phase_pending');activationPromise=activate(...args).finally(()=>{activationPromise=null;});return activationPromise;},observe,executable:async()=>{alive();if(!helper)fail('not_prepared');await inspectCompiledCapacityWindowsHelper(helper);return helper;},assertLease:binding=>{alive();if(current?.binding!==binding||current.retired)fail('binding');for(const source of current.sources)if(capacityDeviceDeliveryBinding(source.actor)!==source.binding||!capacityWindowsHelperObserverBinding(source.binding.observer))fail('source_revoked');return true;},inspect:async(binding,options)=>{const row=current;if(!row||row.binding!==binding)fail('binding');return inspectRow(row,options);},retirePreSeed:()=>retire(current?.phase==='pre-seed'?current:null),close:()=>{if(closePromise)return closePromise;closePromise=(async()=>{closing=true;ownerController.abort();await activationPromise?.catch(()=>{});if(current)await retire(current);await sql.close();if(helper)releaseCompiledCapacityWindowsHelper(helper);closed=true;})().catch(error=>{closePromise=null;throw error;});return closePromise;},evidence:()=>Object.freeze({scope:'same-factory-owned-cng-health',phases:structuredClone(history),formalQualified:false})});owners.set(api,Object.freeze({manifest,laboratory,store,isOpen:()=>!closed&&isOpen()}));return api;
}
