import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {periodicApplicationEncryption,taskPeriodicApplicationKey,periodicApplicationKeyDigest,periodicApplicationKey,periodicApplicationKeyBinding,verifyPeriodicApplicationKeyBinding,loadPeriodicApplicationKey} from '../postgres-periodic-application-key.mjs';

test('application encryption requires an independent key and exact version/digest metadata',()=>{
  const archive=randomBytes(32),application=randomBytes(32);let loaded;
  try{
    assert.throws(()=>taskPeriodicApplicationKey(archive,'test-v3',archive),{code:'periodic_application_independent_key_required'});
    assert.throws(()=>taskPeriodicApplicationKey(application,'',archive),{code:'periodic_application_key_invalid'});
    loaded=taskPeriodicApplicationKey(application,'test-v3',archive);assert.notEqual(loaded.applicationKey,application);assert.deepEqual(loaded.applicationKey,application);
    const binding=periodicApplicationKeyBinding(loaded);assert.equal(binding.applicationKeyDigest,periodicApplicationKeyDigest(application));verifyPeriodicApplicationKeyBinding(binding,loaded);
    for(const changed of [{...binding,applicationKeyVersion:'other'}, {...binding,applicationKeyDigest:'0'.repeat(64)}])assert.throws(()=>verifyPeriodicApplicationKeyBinding(changed,loaded),{code:'periodic_application_key_binding'});
    assert.throws(()=>periodicApplicationKey({closed:true,...loaded}),{code:'backup_context_closed'});assert.throws(()=>periodicApplicationKey({}),{code:'periodic_application_key_required'});
  }finally{archive.fill(0);application.fill(0);loaded?.applicationKey.fill(0);}
});

test('root application key descriptors are exact and an unprivileged loader cannot read arbitrary key files',async()=>{
  const descriptor={version:1,keyFile:'/private/application.key',keyVersion:'preview-application-v1',keyDigest:'a'.repeat(64)};assert.deepEqual(periodicApplicationEncryption(descriptor),descriptor);assert.equal(periodicApplicationEncryption(undefined),null);
  for(const changed of [{...descriptor,version:2},{...descriptor,keyFile:'relative.key'},{...descriptor,keyDigest:'bad'},{...descriptor,unknown:true}])assert.throws(()=>periodicApplicationEncryption(changed),{code:'periodic_application_encryption_invalid'});
  if(process.getuid()!==0)await assert.rejects(loadPeriodicApplicationKey(descriptor,randomBytes(32)),{code:'periodic_application_root_key_required'});
  assert.deepEqual(await loadPeriodicApplicationKey(undefined,randomBytes(32)),{});
});
