import test from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {createCliProcessBudget} from '../cli-process-budget.mjs';
const wait=async predicate=>{const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'CLI lifecycle timeout');await new Promise(resolve=>setTimeout(resolve,5));}};
const child=(budget,owner='synthetic',context={})=>budget.spawn(owner,process.execPath,['-e',"process.on('SIGTERM',()=>{});console.log('ready');setInterval(()=>{},1000)"],{stdio:['ignore','pipe','pipe']},context);
const clear=budget=>{for(const field of ['inUse','running','maintenance','accounts'])assert.equal(budget.status()[field],0,field);};
test('CLI budget retains capacity after caller completion until actual process close and escalates ignored TERM',{timeout:5000},async()=>{
 const budget=createCliProcessBudget({maxConcurrent:1,killGraceMs:50}),first=child(budget);try{await once(first.stdout,'data');first.kill();assert.equal(budget.status().inUse,1);assert.throws(()=>child(budget),{code:'cli_capacity_busy',usageKnown:true,notDispatched:true});await once(first,'close');clear(budget);const second=child(budget);await once(second.stdout,'data');second.kill('SIGKILL');await once(second,'close');clear(budget);}finally{await budget.close();}
});
test('CLI reservations enforce account and maintenance capacity while preserving a model slot',{timeout:5000},async()=>{
 const budget=createCliProcessBudget({maxConcurrent:3,maxPerAccount:1,maxMaintenance:1}),held=budget.reserve('maintenance-owner');try{assert.throws(()=>budget.reserve('other'),{code:'cli_capacity_busy'});assert.throws(()=>budget.reserve('maintenance-owner',{kind:'model'}),{code:'cli_capacity_busy'});const model=child(budget,'model-owner',{kind:'model'});await once(model.stdout,'data');held.release();held.release();assert.equal(budget.status().inUse,1);model.kill('SIGKILL');await once(model,'close');clear(budget);assert.doesNotMatch(JSON.stringify(budget.status()),/maintenance-owner|model-owner/);}finally{held.release();await budget.close();}
});
test('CLI abort cancels unused reservations and active children, close prevents new work',{timeout:5000},async()=>{
 const budget=createCliProcessBudget({killGraceMs:20}),unused=new AbortController(),lease=budget.reserve('unused',{signal:unused.signal});unused.abort();await lease.done;clear(budget);assert.throws(()=>lease.spawn(process.execPath,[]),{code:'service_stopping'});const active=new AbortController(),running=child(budget,'active',{signal:active.signal});await once(running.stdout,'data');active.abort();assert.equal(budget.status().inUse,1);await once(running,'close');clear(budget);await budget.close();assert.throws(()=>child(budget),{code:'service_stopping'});
});
test('CLI failed spawn releases on close and synchronous spawn errors release immediately',{timeout:5000},async()=>{
 const budget=createCliProcessBudget(),bad=budget.spawn('synthetic','/no-such-harbor-synthetic-binary',[],{});await new Promise(resolve=>bad.once('close',resolve));clear(budget);await budget.close();const thrown=createCliProcessBudget({}, {spawnProcess:()=>{throw Error('synthetic spawn');}});assert.throws(()=>child(thrown),/synthetic spawn/);clear(thrown);await thrown.close();
});
test('CLI exit kills descendants retaining inherited pipes before returning the slot',{timeout:5000},async()=>{
 const budget=createCliProcessBudget({killGraceMs:20});const running=budget.spawn('synthetic',process.execPath,['-e',"const {spawn}=require('node:child_process');const c=spawn(process.execPath,['-e',\"process.on('SIGTERM',()=>{});setInterval(()=>{},1000)\"],{stdio:'inherit'});console.log('ready');setTimeout(()=>process.exit(0),100)"],{stdio:['ignore','pipe','pipe']});try{await once(running.stdout,'data');assert.equal(budget.status().inUse,1);await once(running,'close');clear(budget);}finally{await budget.close();}
});
test('CLI budget validates resource configuration, ownership, classes and already-cancelled work',()=>{
 for(const value of [{maxConcurrent:0},{maxConcurrent:257},{maxPerAccount:9},{maxMaintenance:9},{killGraceMs:9},{killGraceMs:30001}])assert.throws(()=>createCliProcessBudget(value),{code:'cli_configuration'});const budget=createCliProcessBudget();for(const [owner,options] of [['',{}],['x',{kind:'untrusted'}],[1,{}]])assert.throws(()=>budget.reserve(owner,options),{code:'cli_configuration'});assert.throws(()=>budget.reserve('x',{signal:AbortSignal.abort()}),{code:'request_cancelled'});clear(budget);
});

test('CLI version uses one real process for concurrent reads and cached success has no further spawn',{timeout:5000},async()=>{
 const {installedVersion}=await import('../responses.mjs');const budget=createCliProcessBudget({maxConcurrent:1});let calls=0;const binary='synthetic-version-'+Date.now(),spawnProcess=(_binary,_args,options)=>{calls++;return budget.spawn('version',process.execPath,['-e',"setTimeout(()=>console.log('codex-cli 0.160.1'),30)"],options);};try{const values=await Promise.all(Array.from({length:5},()=>installedVersion(binary,{spawnProcess})));assert.deepEqual(values,Array(5).fill('0.160.1'));assert.equal(calls,1);clear(budget);assert.equal(await installedVersion(binary,{spawnProcess}),'0.160.1');assert.equal(calls,1);}finally{await budget.close();}
});
