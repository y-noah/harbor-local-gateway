import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {generateKeyPairSync,X509Certificate,randomUUID,sign,verify} from 'node:crypto';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {remoteHash} from '../tools/capacity/remote-protocol.mjs';
import {createCapacityRemoteEndpoint} from '../tools/capacity/remote-endpoint.mjs';
import {connectCapacityRemoteTransport,capacityRemoteTransportBinding,capacityRemoteAttestationBinding} from '../tools/capacity/remote-transport.mjs';
import {capacityRemoteTranscriptBytes} from '../tools/capacity/remote-root-approval.mjs';
import {inspectCapacityRootRemoteAttestation} from '../tools/capacity/remote-factory-lease.mjs';

test('actual control session carries fresh signed attestation challenges but pure mTLS and DTO copies cannot acquire root ownership',{timeout:30000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-root-attestation-component-')),cert=await syntheticCertificates(directory),keys=generateKeyPairSync('ed25519'),runId=randomUUID();let endpoint,transport,calls=0;const material=side=>({caFile:join(directory,'ca.pem'),certFile:join(directory,side+'.pem'),keyFile:join(directory,side+'.key'),peerCertificateHash:remoteHash(new X509Certificate(cert[side==='server'?'client':'server'].cert).raw)});try{
  endpoint=await createCapacityRemoteEndpoint({runId,tls:material('server'),agents:[{id:'probe',origin:'https://127.0.0.1:1',tls:{caFile:join(directory,'ca.pem')},routes:[{method:'GET',path:'/ready'}]}],attest:async context=>{calls++;const body={schema:'component-only-not-root',...context};return {body,signature:sign(null,capacityRemoteTranscriptBytes(body),keys.privateKey).toString('base64')};}});transport=await connectCapacityRemoteTransport({origin:endpoint.origin,runId,tls:material('client')});const owner=capacityRemoteTransportBinding(transport);assert.equal(owner.attestationSupported,true);assert.equal(owner.initialAttestation.body.nonce,owner.initialChallenge);assert.equal(calls,1);assert.equal(Object.isFrozen(owner.initialAttestation.body),true);assert.throws(()=>{owner.initialAttestation.body.nonce='mutated';},TypeError);
  const handle=await transport.attestation(),row=capacityRemoteAttestationBinding(handle);assert.equal(row.transport,transport);assert.equal(Object.isFrozen(row.value.body),true);assert.throws(()=>{row.value.body.nonce='mutated';},TypeError);assert.equal(row.value.body.nonce,row.nonce);assert.equal(row.value.body.runId,owner.runId);assert.equal(row.value.body.session,owner.session);assert.equal(row.value.body.clockEpoch,owner.clockEpoch);assert.equal(verify(null,capacityRemoteTranscriptBytes(row.value.body),keys.publicKey,Buffer.from(row.value.signature,'base64')),true);assert.equal(capacityRemoteAttestationBinding({...handle,...row}),null);await assert.rejects(inspectCapacityRootRemoteAttestation({approval:{publicKey:keys.publicKey},transport,receipt:handle}),/not_owned/);
  await transport.close();assert.equal(capacityRemoteAttestationBinding(handle),null);
 }finally{transport?.disconnect();await endpoint?.close();await rm(directory,{recursive:true,force:true});}
});

test('oversized object graph in a fresh attestation closes real mTLS transport and rejects the waiting request',{timeout:15000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-attestation-budget-')),cert=await syntheticCertificates(directory),runId=randomUUID();let endpoint,transport,calls=0;
 const material=side=>({caFile:join(directory,'ca.pem'),certFile:join(directory,side+'.pem'),keyFile:join(directory,side+'.key'),peerCertificateHash:remoteHash(new X509Certificate(cert[side==='server'?'client':'server'].cert).raw)});
 try{endpoint=await createCapacityRemoteEndpoint({runId,tls:material('server'),agents:[{id:'probe',origin:'https://127.0.0.1:1',tls:{caFile:join(directory,'ca.pem')},routes:[{method:'GET',path:'/ready'}]}],attest:async context=>++calls===1?{body:context}:{body:{items:Array.from({length:4100},()=>({}))}}});
  transport=await connectCapacityRemoteTransport({origin:endpoint.origin,runId,tls:material('client')});
  let timer;try{await assert.rejects(Promise.race([transport.attestation(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('test_waiter_did_not_settle')),2000);})]),error=>error.code==='capacity_remote_delivery_unknown');}finally{clearTimeout(timer);}
  assert.equal(transport.status().closed,true);assert.equal(transport.status().pending,0);assert.equal(calls,2);
 }finally{transport?.disconnect();await endpoint?.close();await rm(directory,{recursive:true,force:true});}
});
