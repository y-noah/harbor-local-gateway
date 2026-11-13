import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,integration} from './device-fixture.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';

test('设备目录和员工Key查询按稳定设备聚合、保留证书状态并同时执行双向Key范围',integration,async t=>fixture(t,async({store,devices,first,enroll,approve,call})=>{
 const one=await enroll(first.memberId,'合成设备一'),two=await enroll(first.memberId,'合成设备二');const client=await approve(one);await approve(two);await call(first.secret,client);await call(first.secret,client);await call(first.secret);
 const queries=createPostgresMemberQueries(store),member=await queries.detail(first.memberId),keys=await queries.keys(memberQuery(new URLSearchParams(),{keys:true}),{memberId:first.memberId});
 assert.equal(member.registered_devices,2);assert.equal(member.used_devices,1);assert.equal(member.unverified_requests,1);assert.equal(member.access_version,0);assert.equal(member.device_mode,'compatible');assert.equal(member.device_policy_version,0);
 assert.equal(keys.rows[0].registered_devices,2);assert.equal(keys.rows[0].used_devices,1);assert.equal(keys.rows[0].unverified_requests,1);assert.equal(keys.rows[0].device_ids,null);assert.equal(keys.rows[0].access_version,0);assert.equal(keys.rows[0].device_policy_version,0);
 const selected=await queries.selection(memberQuery(new URLSearchParams()));assert.equal(selected.members.find(row=>row.id===first.memberId).access_version,0);
 let listed=await devices.list(new URLSearchParams({member_id:first.memberId,page_size:'20'}));assert.equal(listed.pagination.total,2);assert.equal(listed.rows.length,2);assert.equal(listed.pagination.pageSize,20);assert.equal(listed.summary.used_devices,1);assert.equal(listed.summary.unverified_requests,1);assert.equal(listed.summary.registered_devices,2);assert.equal(listed.member.device_policy_version,0);assert.equal(listed.rows[0].certificate_valid,true);assert.ok(listed.rows[0].certificate_expires>Date.now());assert.equal(listed.rows[0].allowed_key_ids,null);
 await devices.saveKeyPolicy(first.keyId,{device_ids:[one.registration.device_id],expected_version:0});listed=await devices.list(new URLSearchParams({key_id:first.keyId}));assert.equal(listed.rows.length,1);assert.equal(listed.rows[0].id,one.registration.device_id);
 await store.run('UPDATE harbor.employee_devices SET allowed_key_ids=$1::jsonb WHERE id=$2','[]',one.registration.device_id);listed=await devices.list(new URLSearchParams({key_id:first.keyId}));assert.equal(listed.rows.length,0);assert.equal((await queries.keys(memberQuery(new URLSearchParams(),{keys:true}),{memberId:first.memberId})).rows[0].registered_devices,0);
 for(const size of ['1','25','101'])await assert.rejects(devices.list(new URLSearchParams({page_size:size})),{status:400});
 for(const size of ['50','100'])assert.equal((await devices.list(new URLSearchParams({page_size:size}))).pagination.pageSize,Number(size));
 await assert.rejects(devices.list(new URLSearchParams({member_id:'missing'})),{status:404});
}));
