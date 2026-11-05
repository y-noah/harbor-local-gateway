import test from 'node:test';
import assert from 'node:assert/strict';
import {createInferenceBudget} from '../inference-budget.mjs';
const take=(budget,memberId='member',accountId='account',extra={})=>budget.acquire({memberId,accountId,...extra});
const empty=budget=>{for(const name of ['active','cliActive','members','accounts'])assert.equal(budget.status()[name],0,name);};
for(const [name,options,blocked,allowed] of [
 ['global',{maxConcurrent:2},['other','other'],null],
 ['member',{maxConcurrent:4,maxPerMember:2},['member','other'],['other','other']],
 ['account',{maxConcurrent:4,maxPerAccount:2},['other','account'],['other','other']]
])test('inference '+name+' capacity is shared and idempotently released',()=>{
 const budget=createInferenceBudget(options),releases=[take(budget),take(budget)];assert.ok(releases.every(Boolean));assert.equal(take(budget,...blocked),null);if(allowed){const release=take(budget,...allowed);assert.ok(release);release();}releases[0]();releases[0]();const next=take(budget,...blocked);assert.ok(next);next();releases[1]();empty(budget);assert.equal(budget.status().rejected,1);
});
for(const [name,options,blocked,allowed] of [
 ['global',{maxCli:1},['other','other'],null],
 ['member',{maxCli:3,maxCliPerMember:1},['member','other'],['other','other']],
 ['account',{maxCli:3,maxCliPerAccount:1},['other','account'],['other','other']]
])test('inference CLI '+name+' budget does not block network execution',()=>{
 const budget=createInferenceBudget(options),first=take(budget,'member','account',{cli:true});assert.ok(first);assert.equal(take(budget,...blocked,{cli:true}),null);const network=take(budget,...blocked);assert.ok(network);if(allowed){const next=take(budget,...allowed,{cli:true});assert.ok(next);next();}first();network();empty(budget);
});
test('inference idle requirement applies on admission only and release listeners cannot break cleanup',()=>{
 const budget=createInferenceBudget(),network=take(budget);assert.equal(take(budget,'other','account',{cli:true,requireIdle:true}),null);let calls=0;budget.onRelease(()=>{throw Error('synthetic listener');});const off=budget.onRelease(()=>calls++);network();assert.equal(calls,1);off();const cli=take(budget,'other','account',{cli:true,requireIdle:true});assert.ok(cli);const later=take(budget);assert.ok(later);cli();later();assert.equal(calls,1);empty(budget);assert.doesNotMatch(JSON.stringify(budget.status()),/member"|account"|other/);
});
test('inference validates server limits and trusted owner fields',()=>{
 for(const options of [{maxConcurrent:0},{maxConcurrent:1.5},{maxConcurrent:4097},{maxPerMember:121},{maxPerAccount:121},{maxCli:121},{maxCliPerMember:5},{maxCliPerAccount:5},{maxCli:NaN}])assert.throws(()=>createInferenceBudget(options),{code:'inference_configuration'});
 const budget=createInferenceBudget();for(const extra of [{memberId:''},{accountId:1},{cli:'true'},{requireIdle:1}])assert.throws(()=>take(budget,'member','account',extra),{code:'inference_configuration'});empty(budget);
});
