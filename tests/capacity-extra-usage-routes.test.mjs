import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityWireSignature} from '../tools/capacity/gateway-probe.mjs';
import {capacityObservedApiClass} from '../tools/capacity/observation-adapter.mjs';

const account='11111111-1111-4111-8111-111111111111',job='22222222-2222-4222-8222-222222222222';
test('extra-use management endpoints retain a canonical privacy-safe signature and real API workload class',()=>{
  const prefix='/api/admin/extra-usage',owner='/api/admin/accounts/'+account+'/extra-usage';
  for(const [method,path,expected] of [
    ['GET',prefix+'/policy','detail'],['PATCH',prefix+'/policy','configuration'],
    ['GET',prefix+'/coverage?page=2&q=private-fixture','list'],['GET',prefix+'/batches','list'],
    ['POST',prefix+'/policy/preview','batchPreview'],['POST',prefix+'/batches/preview','batchPreview'],
    ['GET',prefix+'/batches/'+job,'task'],['POST',prefix+'/batches/'+job+'/execute','task'],['POST',prefix+'/batches/'+job+'/retry','task'],
    ['GET',owner,'detail'],['PATCH',owner,'configuration'],['POST',owner+'/manual','configuration'],['POST',owner+'/reset-card-used','configuration'],
    ['GET',owner+'/records?before='+job,'list'],['POST',owner+'/refresh','task'],['GET',owner+'/refresh/'+job,'task'],
  ]){
    const signature=capacityWireSignature(method,path);
    assert.equal(capacityObservedApiClass({method,route:path}),expected,path);
    assert.doesNotMatch(signature,/private-fixture|11111111|22222222|:other-/);
    assert.equal(capacityWireSignature(method,signature.slice(method.length+1)),signature);
  }
});

test('unknown extra routes and query text cannot become an accepted measured endpoint',()=>{
  for(const [method,path] of [['GET','/api/admin/extra-usage/batches/'+job+'/execute'],['POST','/api/admin/extra-usage/policy'],['GET','/api/admin/accounts/'+account+'/extra-usage/manual'],['GET','/api/admin/extra-usage/private-fixture']]){
    assert.equal(capacityObservedApiClass({method,route:path}),'unclassified');
    assert.doesNotMatch(capacityWireSignature(method,path),/private-fixture/);
  }
});
