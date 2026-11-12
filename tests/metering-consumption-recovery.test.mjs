import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID,createDecipheriv,hkdfSync} from 'node:crypto';
import {mkdtemp,readdir,readFile,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {setTimeout as delay} from 'node:timers/promises';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,startOwnedCapacityChild} from '../tools/capacity/owned-runtime.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
const attempts=()=>[{source:'api',account_id:'api',state:'rejected',usage_known:true,started:10,ended:20,error_code:'upstream_quota_exhausted',input:0,output:0,cached:0},{source:'subscription',account_id:'subscription',state:'completed',usage_known:true,started:21,ended:30,input:11,output:7,cached:3}];
const unsent=()=>({...attempts()[1],state:'failed',usage_known:true,dispatched:false,error_code:'upstream_credentials_unavailable',input:0,output:0,cached:0});
async function spool(t,store){
 const directory=await mkdtemp(join(tmpdir(),'harbor-consumption-metering-')),key=randomBytes(32);let recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});
 t.after(async()=>{await recovery.close();key.fill(0);await rm(directory,{recursive:true,force:true});});return {directory,key,get recovery(){return recovery;},restart:async next=>{await recovery.close();recovery=await MeteringRecovery.create(next,{encryptionKey:key,directory,flushMs:10000});return recovery;}};
}
function decrypt(bytes,name,key){const derived=Buffer.from(hkdfSync('sha256',key,Buffer.alloc(0),'Harbor metering recovery v1',32)),cipher=createDecipheriv('aes-256-gcm',derived,bytes.subarray(0,12));cipher.setAAD(Buffer.from('metering:'+name));cipher.setAuthTag(bytes.subarray(-16));return JSON.parse(Buffer.concat([cipher.update(bytes.subarray(12,-16)),cipher.final()]).toString('utf8'));}

test('canonical attempts enter only authenticated ciphertext and replay exact sent/unsent facts after restart',async t=>{
 const calls=[],unavailable={finish:async()=>{throw Error('synthetic outage');}},f=await spool(t,unavailable),result={status:'failed',input:0,output:0,cached:0,usageKnown:true,consumptionAttempts:[attempts()[0],unsent()]};
 assert.equal(f.recovery.settle('request',result,50),true);await f.recovery.flush();assert.equal(f.recovery.status().files,1);
 const name=(await readdir(f.directory))[0],bytes=await readFile(join(f.directory,name));assert.equal(bytes.includes(Buffer.from('consumptionAttempts')),false);assert.equal(bytes.includes(Buffer.from('upstream_credentials_unavailable')),false);
 const raw=decrypt(bytes,name,f.key);assert.deepEqual(raw.result.consumptionAttempts,result.consumptionAttempts);assert.equal(raw.type,'settle');assert.deepEqual(f.recovery.buffer.repository.open(bytes,name).result.consumptionAttempts,result.consumptionAttempts);
 const next=await f.restart({finish:async(...args)=>calls.push(args)});await next.flush();assert.deepEqual(calls,[['request',result,50]]);assert.equal(next.status().files,0);
});
test('extra attempt fields, fabricated known zero and invalid unsent facts reject before queue or encryption',async t=>{
 const f=await spool(t,{finish:async()=>{throw Error('must not dispatch');}});
 const invalid=[{...attempts()[0],headers:{authorization:'synthetic-secret'}},{...attempts()[0],body:'synthetic-private-body'},{...attempts()[0],source_identity:'forged'},{...attempts()[0],state:'unknown',usage_known:false},{...unsent(),state:'completed'},{...unsent(),dispatched:'false'}];
 for(const value of invalid){assert.throws(()=>f.recovery.settle('invalid',{consumptionAttempts:[value]},50),{code:'consumption_metadata_invalid'});assert.equal(f.recovery.status().queued,0);assert.equal(f.recovery.status().files,0);assert.deepEqual(await readdir(f.directory),[]);}
 const unknown={source:'api',account_id:'api',state:'unknown',usage_known:false,started:10};assert.equal(f.recovery.settle('unknown',{status:'unknown',usageKnown:false,consumptionAttempts:[unknown]},51),true);await f.recovery.flush();const file=(await readdir(f.directory))[0],saved=decrypt(await readFile(join(f.directory,file)),file,f.key);assert.deepEqual(saved.result.consumptionAttempts,[unknown]);assert.equal(saved.result.usageKnown,false);assert.equal(Object.hasOwn(saved.result.consumptionAttempts[0],'input'),false);
});

const bin=process.env.HARBOR_CAPACITY_PG_BIN,lib=process.env.HARBOR_CAPACITY_PG_LIB;
test('actual owned PostgreSQL crash, restart and encrypted spool replay retain evidence without duplicate billing',{skip:!bin?'需独占PG18完整工具链':false,timeout:90000},async t=>{
 const lab=await createPrivatePostgres({binDirectory:bin,libraryDirectory:lib});let store=new PostgresStore({connection:lab.connection,poolSize:2,connectionTimeoutMs:200,statementTimeoutMs:1000}),recovery;
 t.after(async()=>{await recovery?.close();await store.close();await lab.close();});
 await store.migrate();await store.initializePricing();await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0);INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic','synthetic',0);INSERT INTO harbor.accounts(id,name,home) VALUES('api','synthetic','synthetic'),('subscription','synthetic','synthetic');INSERT INTO harbor.requests(id,member_id,key_id,account_id,started,status,ip) VALUES('request','m','k','subscription',10,'running','synthetic'),('unsent','m','k','subscription',10,'running','synthetic'),('legacy','m','k','api',10,'running','synthetic')");
 const policy=createPostgresConsumptionPolicy(store);await policy.set('k',{consumption_policy:'api_first',expected_consumption_policy_version:0});for(const id of ['request','unsent'])await store.transaction(()=>policy.recordAdmission(id,'k',{consumption_policy:'api_first',consumption_policy_version:1}));
 const directory=join(lab.directory,'consumption-spool'),key=randomBytes(32);t.after(()=>key.fill(0));recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});
 assert.throws(()=>recovery.settle('request',{consumptionAttempts:[{...attempts()[0],raw_body:'must-never-enter-spool'}]},50),{code:'consumption_metadata_invalid'});assert.equal(recovery.status().queued,0);assert.deepEqual(await readdir(directory),[]);
 // Crash only the process tree returned by this test's owned PG capability.
 await lab.runtime.crashDatabase(lab.process);await assert.rejects(store.get('SELECT 1'));
 const result={status:'ok',input:11,output:7,cached:3,usageKnown:true,consumptionAttempts:attempts()},failed={status:'failed',input:0,output:0,cached:0,usageKnown:true,consumptionAttempts:[attempts()[0],unsent()]};
 assert.equal(recovery.settle('request',result,50),true);assert.equal(recovery.settle('unsent',failed,51),true);assert.equal(recovery.settle('legacy',{input:2,output:1,cached:0,usageKnown:true},52),true);await recovery.flush();assert.equal(recovery.status().files,3);assert.equal(recovery.status().queued,0);
 const sealed=[];for(const name of await readdir(directory)){const bytes=await readFile(join(directory,name)),command=decrypt(bytes,name,key);assert.equal(bytes.includes(Buffer.from('consumptionAttempts')),false);sealed.push({name,bytes,command});}
 assert.deepEqual(sealed.find(item=>item.command.requestId==='request').command.result.consumptionAttempts,attempts());assert.deepEqual(sealed.find(item=>item.command.requestId==='unsent').command.result.consumptionAttempts,failed.consumptionAttempts);assert.equal(Object.hasOwn(sealed.find(item=>item.command.requestId==='legacy').command.result,'consumptionAttempts'),false);
 await recovery.close();await store.close();
 const restarted=startOwnedCapacityChild(lab.runtime,join(bin,'postgres'),['-D',join(lab.directory,'data'),'-k',join(lab.directory,'socket'),'-h','','-p','55482','-c','max_connections=100'],{role:'database',env:lib?{LD_LIBRARY_PATH:lib}:{}});
 store=new PostgresStore({connection:lab.connection,poolSize:2,connectionTimeoutMs:200,statementTimeoutMs:1000});
 const deadline=Date.now()+15000;for(;;){assert.equal(restarted.exitCode,null);try{if((await store.get('SELECT 1 ready')).ready===1)break;}catch{}assert.ok(Date.now()<deadline,'owned PostgreSQL restart timed out');await delay(20);}
 recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});await recovery.flush();assert.equal(recovery.status().files,0);assert.equal(recovery.status().queued,0);
 const read=()=>store.all('SELECT id,used FROM harbor.members UNION ALL SELECT id,used FROM harbor.keys ORDER BY id'),billing=await read();assert.deepEqual(billing,[{id:'k',used:21},{id:'m',used:21}]);
 assert.deepEqual(await store.get("SELECT consumption_source,consumption_fallback,consumption_attempts FROM harbor.requests WHERE id='request'"),{consumption_source:'subscription',consumption_fallback:true,consumption_attempts:attempts()});assert.deepEqual(await store.get("SELECT consumption_source,consumption_fallback,consumption_attempts FROM harbor.requests WHERE id='unsent'"),{consumption_source:'api',consumption_fallback:false,consumption_attempts:failed.consumptionAttempts});assert.deepEqual(await store.get("SELECT consumption_policy,consumption_attempts FROM harbor.requests WHERE id='legacy'"),{consumption_policy:null,consumption_attempts:null});
 const prices=await store.all('SELECT * FROM harbor.request_prices ORDER BY request_id');assert.equal(prices.length,3);
 await recovery.close();for(const saved of sealed)await writeFile(join(directory,saved.name),saved.bytes,{mode:0o600,flag:'wx'});recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});await recovery.flush();assert.equal(recovery.status().files,0);assert.deepEqual(await read(),billing);assert.deepEqual(await store.all('SELECT * FROM harbor.request_prices ORDER BY request_id'),prices);
});
