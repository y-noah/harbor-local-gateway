import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityWireSignature} from '../tools/capacity/gateway-probe.mjs';
import {capacityOperationCanonical} from '../tools/capacity/scenarios.mjs';
import {capacityObservedApiClass} from '../tools/capacity/observation-adapter.mjs';

test('actual member-type route parameters normalize built-in, UUID and other controller-legal ids alike',()=>{
 for(const id of ['usage-a','usage-f','123e4567-e89b-12d3-a456-426614174000','Company-Type-42','x'.repeat(80)])for(const [method,suffix] of [['GET','/templates'],['PATCH','']]){const path='/api/admin/member-types/'+id+suffix,expected=method+' /api/admin/member-types/:id'+suffix;assert.equal(capacityWireSignature(method,path),expected);assert.equal(capacityWireSignature(method,expected.slice(method.length+1)),expected);assert.equal(capacityOperationCanonical({method,path,canonicalHttp:expected}),expected);assert.equal(capacityObservedApiClass({method,route:path}),'configuration');}
 assert.equal(capacityOperationCanonical({method:'GET',path:'/api/admin/member-types/{type_id}/templates',canonicalHttp:'GET /api/admin/member-types/:id/templates'}),'GET /api/admin/member-types/:id/templates');
});

test('type-id normalization is confined to the controller route and method boundaries',()=>{
 for(const [method,path] of [['GET','/api/admin/member-types'],['GET','/api/admin/member-types/templates'],['GET','/api/admin/type-statistics']])assert.equal(capacityWireSignature(method,path),method+' '+path);
 for(const [method,path] of [['GET','/api/admin/member-types/Company-Type-42'],['POST','/api/admin/member-types/Company-Type-42/templates'],['GET','/api/admin/member-types/Company_Type/templates'],['GET','/api/admin/member-types/Company-Type-42/templates/extra'],['PATCH','/api/admin/not-member-types/Company-Type-42']])assert.match(capacityWireSignature(method,path),new RegExp('^'+method+' /api/:other-[a-f0-9]{16}$'));
 assert.equal(capacityWireSignature('GET','/api/admin/member-types/templates'),'GET /api/admin/member-types/templates');
});
