import test from 'node:test';
import assert from 'node:assert/strict';
import {storageOperatorCandidateOrigins} from '../storage-operator-context.mjs';
import {networkConfig,acceptsRequest} from '../network.mjs';

test('operator preserves the existing HTTP public origin alongside the separate HTTPS device origin',()=>{
  for(const publicOrigin of ['http://gateway.example.invalid:8092','https://gateway.example.invalid','http://[::1]:8092']){
    const origins=storageOperatorCandidateOrigins({publicOrigin,deviceOrigin:'https://gateway.example.invalid:8093'});
    assert.ok(Object.isFrozen(origins));const network=networkConfig({HARBOR_PUBLIC_ORIGINS:origins.publicOrigin+','+origins.deviceOrigin});
    assert.equal(acceptsRequest({host:new URL(publicOrigin).host,origin:publicOrigin},network),true);
    assert.equal(acceptsRequest({host:'gateway.example.invalid:8093',origin:origins.deviceOrigin},network),true);
    assert.equal(acceptsRequest({host:new URL(publicOrigin).host,origin:'https://unapproved.example.invalid'},network),false);
  }
});

test('operator still requires HTTPS device authentication and canonical credential-free single origins',()=>{
  const valid={publicOrigin:'http://gateway.example.invalid:8092',deviceOrigin:'https://gateway.example.invalid:8093'};
  for(const name of ['publicOrigin','deviceOrigin'])for(const value of [undefined,null,12,'','ftp://gateway.example.invalid','https://user:password@gateway.example.invalid','https://gateway.example.invalid/path','https://gateway.example.invalid/','https://gateway.example.invalid?query=1','https://gateway.example.invalid#fragment','https://gateway.example.invalid,https://other.example.invalid','https://gateway.example.invalid,','https://gateway.example.invalid\n']){
    assert.throws(()=>storageOperatorCandidateOrigins({...valid,[name]:value}),{code:'storage_operator_candidate_origin'});
  }
  assert.throws(()=>storageOperatorCandidateOrigins({...valid,deviceOrigin:'http://gateway.example.invalid:8093'}),{code:'storage_operator_candidate_origin'});
});
