import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset} from '../tools/capacity/dataset.mjs';
import {createCapacityProfileBinding,assertCapacityProfileBinding} from '../tools/capacity/profile-binding.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需本任务私有PG18，不访问外部数据库':false};
test('profile guards reject caller-owned objects and invalid requested profiles',()=>{
 for(const value of [{profile:'formal',datasetVerified:true,formalDatasetVerified:true},Object.freeze({}),null])assert.throws(()=>assertCapacityProfileBinding(value,'formal'),/binding_not_owned/);
 assert.throws(()=>assertCapacityProfileBinding({},'other'),/profile_invalid/);
});
test('profile capability is bound to actual owned seed and private factory lifetime',integration,async t=>{
 const laboratory=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:laboratory.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck'});let fixture,formalFixture,periodFixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...laboratory.connection,database}})});const store=fixture.store;let opened=true;const options={laboratory,manifest,store,recordingKey,isOpen:()=>opened};
  await t.test('unseeded fixture and fake public verification cannot mint',async()=>{
   await assert.rejects(createCapacityProfileBinding({...options,dataset:{verified:true,formalScale:true}}),/dataset_unverified/);
   await assert.rejects(createCapacityProfileBinding({...options,manifest:{...manifest,profile:'formal'}}),/manifest_invalid/);
   await assert.rejects(createCapacityProfileBinding({...options,store:admin}),/source_mismatch/);
   await assert.rejects(createCapacityProfileBinding({...options,recordingKey:randomBytes(32)}),/source_mismatch/);
  });
  await seedDataset(store,manifest,{recordingKey});const binding=await createCapacityProfileBinding(options);
  await t.test('60 seed actors do not promote a selfcheck profile to formal',()=>{
   assert.deepEqual(assertCapacityProfileBinding(binding,'selfcheck'),{profile:'selfcheck',datasetVerified:true,formalDatasetVerified:false});assert.throws(()=>assertCapacityProfileBinding(binding,'formal'),/profile_mismatch/);
   assert.throws(()=>assertCapacityProfileBinding({...binding,profile:'formal'},'formal'),/binding_not_owned/);assert.deepEqual(Object.keys(binding),[]);
  });
  await t.test('guard uses frozen source identity and does not rescan or trust public metadata',async()=>{
   const originalProfile=manifest.profile,original=store.all.bind(store);store.all=async()=>{throw Error('guard must not rescan');};try{manifest.profile='formal';assert.equal(assertCapacityProfileBinding(binding,'selfcheck').profile,'selfcheck');}finally{manifest.profile=originalProfile;store.all=original;}
   opened=false;assert.throws(()=>assertCapacityProfileBinding(binding,'selfcheck'),/binding_closed/);await assert.rejects(createCapacityProfileBinding(options),/binding_closed/);opened=true;
  });
  await t.test('closure during asynchronous verification prevents mint',async()=>{
   const original=store.all.bind(store);try{store.all=async(sql,...args)=>{const value=await original(sql,...args);if(sql.includes('DISTINCT ON(source_bytes)'))opened=false;return value;};await assert.rejects(createCapacityProfileBinding(options),/binding_closed/);}finally{store.all=original;opened=true;}
  });
  await t.test('real wrong-size and wrong-proportion receipt buckets cannot mint',async()=>{
   const receipt=await store.get('SELECT request_id,source_bytes FROM harbor_capacity.body_receipts WHERE source_bytes=8192 LIMIT 1');
   await store.run('UPDATE harbor_capacity.body_receipts SET source_bytes=source_bytes+1 WHERE source_bytes=8192');try{await assert.rejects(createCapacityProfileBinding(options),/body_distribution_mismatch/);}finally{await store.run('UPDATE harbor_capacity.body_receipts SET source_bytes=8192 WHERE source_bytes=8193');}
   await store.run('UPDATE harbor_capacity.body_receipts SET source_bytes=32768 WHERE request_id=$1',receipt.request_id);try{await assert.rejects(createCapacityProfileBinding(options),/body_distribution_mismatch/);}finally{await store.run('UPDATE harbor_capacity.body_receipts SET source_bytes=8192 WHERE request_id=$1',receipt.request_id);}
  });
  await t.test('full assignment period plus a remainder validates exact bucket counts',async()=>{
   const period=datasetManifest({profile:'selfcheck',metadata30d:130,metadata7d:28,bodyRequests:130});periodFixture=await createDatasetDatabase(admin,period,{laboratory,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...laboratory.connection,database}})});await seedDataset(periodFixture.store,period,{recordingKey});const proof=await createCapacityProfileBinding({...options,manifest:period,store:periodFixture.store});assert.equal(assertCapacityProfileBinding(proof,'selfcheck').datasetVerified,true);
  });
  await t.test('existing transaction is rejected before any nested verification and rollback yields no proof',async()=>{
   let candidate;await assert.rejects(store.transaction(async()=>{await assert.rejects(createCapacityProfileBinding(options),/nested_transaction/);candidate=undefined;throw Error('synthetic outer rollback');}),/synthetic outer rollback/);assert.throws(()=>assertCapacityProfileBinding(candidate,'selfcheck'),/binding_not_owned/);
  });
  await t.test('underfilled genuine formal profile cannot claim formal dataset coverage',async()=>{
   const smallFormal=datasetManifest({profile:'formal',metadata30d:120,metadata7d:28,bodyRequests:32});formalFixture=await createDatasetDatabase(admin,smallFormal,{laboratory,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...laboratory.connection,database}})});await seedDataset(formalFixture.store,smallFormal,{recordingKey});await assert.rejects(createCapacityProfileBinding({...options,manifest:smallFormal,store:formalFixture.store}),/formal_dataset_incomplete/);
  });
  await t.test('foreign cluster, timeout and unconfirmed transaction cannot mint',async()=>{
   const foreign=await createPrivatePostgres({binDirectory,libraryDirectory});try{await assert.rejects(createCapacityProfileBinding({...options,laboratory:foreign}),/source_mismatch/);}finally{await foreign.close();}
   const originalAll=store.all.bind(store);try{store.all=async(sql,...args)=>{if(sql.includes('DISTINCT ON(source_bytes)')){await store.query('SET LOCAL statement_timeout=1');await store.get('SELECT pg_sleep(0.03)');}return originalAll(sql,...args);};await assert.rejects(createCapacityProfileBinding(options),error=>error.message==='capacity_profile_verification_unavailable');}finally{store.all=originalAll;}
   const originalTransaction=store.transaction.bind(store);try{store.transaction=async(...args)=>{await originalTransaction(...args);throw Error('synthetic unconfirmed COMMIT');};await assert.rejects(createCapacityProfileBinding(options),error=>error.message==='capacity_profile_verification_unavailable');}finally{store.transaction=originalTransaction;}
  });
  await t.test('real projection corruption and storage failure never mint',async()=>{
   await store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=(SELECT id FROM harbor.requests LIMIT 1)');await assert.rejects(createCapacityProfileBinding(options),/dataset_unverified/);
   const original=store.all.bind(store);try{store.all=async()=>{throw Error('synthetic database failure with private context');};await assert.rejects(createCapacityProfileBinding(options),error=>error.message==='capacity_profile_verification_unavailable');}finally{store.all=original;}
  });
 }finally{await periodFixture?.destroy();await formalFixture?.destroy();await fixture?.destroy();await admin.close();await laboratory.close();}
});
