import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {generateKeyPairSync,X509Certificate,randomUUID} from 'node:crypto';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {remoteHash} from '../tools/capacity/remote-protocol.mjs';
import {createCapacityRootRemoteControlComponent,queryCapacityRootRemoteReleaseComponent,capacityRootRemoteReleaseBinding,queryCapacityRootRemoteRelease} from '../tools/capacity/remote-root-control.mjs';

test('actual mTLS release-control component binds fresh nonce, signature, identity and known physical release state without minting root capability',{timeout:30000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-root-control-component-')),cert=await syntheticCertificates(directory),keys=generateKeyPairSync('ed25519'),leaseId=randomUUID(),states=new Map([[leaseId,{known:true,resourcesReleased:false,cleanupUnknown:false}]]);let server;try{
  server=await createCapacityRootRemoteControlComponent({materials:{...cert.server,ca:cert.ca},peerCertificateHash:remoteHash(new X509Certificate(cert.client.cert).raw),signingKey:keys.privateKey,readStatus:id=>states.get(id)??{known:false,resourcesReleased:false}});assert.equal(server.formalQualified,false);
  const query=publicKey=>queryCapacityRootRemoteReleaseComponent({origin:server.origin,materials:{...cert.client,ca:cert.ca},expectedPeerHash:remoteHash(new X509Certificate(cert.server.cert).raw),publicKey:publicKey??keys.publicKey,leaseId}),first=await query();assert.equal(first.body.resourcesReleased,false);assert.equal(first.body.known,true);assert.equal(first.formalQualified,false);assert.equal(capacityRootRemoteReleaseBinding(first),null);
  states.set(leaseId,{known:true,resourcesReleased:true,cleanupUnknown:false});const second=await query();assert.equal(second.body.resourcesReleased,true);assert.notEqual(first.nonce,second.nonce);assert.equal(capacityRootRemoteReleaseBinding({...second}),null);
  states.set(leaseId,{known:true,resourcesReleased:false,cleanupUnknown:true});assert.equal((await query()).body.cleanupUnknown,true);await assert.rejects(query(generateKeyPairSync('ed25519').publicKey),/unknown/);
  await assert.rejects(queryCapacityRootRemoteRelease({approval:{...server,publicKey:keys.publicKey},leaseId}),/not_owned/);
 }finally{await server?.close();await rm(directory,{recursive:true,force:true});}
});
