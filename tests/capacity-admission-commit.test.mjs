import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {createGatewayProbe} from '../tools/capacity/gateway-probe.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};

test('admission facts follow real outer commits, savepoint rollback and independent async branches',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target,probe;
  try{
    target=await createTarget({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,employeeCount:2});const store=target.store,events=[],server=new EventEmitter(),original={observe:store.observe,transaction:store.transaction},members=await Promise.all(target.employeeActors.map(actor=>store.authenticate(actor.secret)));let action;
    server.on('request',(_req,res)=>{void action().then(()=>{res.statusCode=200;},()=>{res.statusCode=500;}).finally(()=>res.emit('finish'));});probe=createGatewayProbe({sink:event=>events.push(event)});probe.attach({server,store,sessionHandoffs:createSessionRequestHandoff(),status:()=>({})});
    const invoke=async(trace,operation)=>{action=operation;const req=Object.assign(new EventEmitter(),{method:'POST',url:'/v1/responses',headers:{'x-capacity-trace':trace}}),res=Object.assign(new EventEmitter(),{statusCode:200}),done=new Promise(resolve=>res.once('finish',resolve));server.emit('request',req,res);await done;return events.filter(event=>event.trace===trace);};
    const observe=member=>store.observe(member,'127.0.0.1','synthetic-commit-boundary','gpt-6.1-sol',{at:Date.now()});
    let rolledBack;const rollback=await invoke('outer-rollback',()=>store.transaction(async()=>{rolledBack=await observe(members[0]);assert.equal(probe.status().pendingAdmissions,1);assert.equal(events.filter(event=>event.kind==='gateway-admitted'&&event.trace==='outer-rollback').length,0);throw Error('synthetic rollback');}));
    assert.equal(rollback.filter(event=>event.kind==='gateway-admitted').length,0);assert.equal(rollback.filter(event=>event.kind==='gateway-admission-unconfirmed').length,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',rolledBack)).n,0);
    let committed;const commit=await invoke('outer-commit',()=>store.transaction(async()=>{committed=await store.transaction(()=>observe(members[0]));assert.equal(events.filter(event=>event.kind==='gateway-admitted'&&event.trace==='outer-commit').length,0);}));
    assert.equal(commit.filter(event=>event.kind==='gateway-admitted').length,1);assert.equal(commit.find(event=>event.kind==='gateway-admitted').requestId,committed);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',committed)).n,1);
    let innerRollback,outerCommit;const savepoint=await invoke('savepoint-rollback',()=>store.transaction(async()=>{try{await store.transaction(async()=>{innerRollback=await observe(members[0]);throw Error('synthetic savepoint rollback');});}catch{}outerCommit=await observe(members[0]);}));
    assert.deepEqual(savepoint.filter(event=>event.kind==='gateway-admitted').map(event=>event.requestId),[outerCommit]);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',innerRollback)).n,0);
    const hold=deferred(),entered=deferred();let a,b;const concurrent=await invoke('parallel-transactions',async()=>{const rejected=store.transaction(async()=>{a=await observe(members[0]);entered.resolve();await hold.promise;throw Error('synthetic concurrent rollback');}).catch(()=>{});await entered.promise;await store.transaction(async()=>{b=await observe(members[1]);});hold.resolve();await rejected;});
    assert.deepEqual(concurrent.filter(event=>event.kind==='gateway-admitted').map(event=>event.requestId),[b]);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',a)).n,0);assert.equal(probe.status().pendingAdmissions,0);assert.equal(probe.status().dropped,0);
    await probe.close();assert.strictEqual(store.observe,original.observe);assert.strictEqual(store.transaction,original.transaction);probe=null;
  }finally{await probe?.close();await target?.close();}
});

test('admission budget exhaustion marks missing evidence without rolling back actual writes',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target,probe;
  try{
    target=await createTarget({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,employeeCount:1});const store=target.store,member=await store.authenticate(target.employeeActors[0].secret),server=new EventEmitter(),events=[],ids=[],done=deferred();
    server.on('request',(_req,res)=>{void store.transaction(async()=>{for(let i=0;i<2;i++)ids.push(await store.observe(member,'127.0.0.1','synthetic-budget','gpt-6.1-sol',{at:Date.now()}));}).then(()=>{res.statusCode=200;res.emit('finish');done.resolve();},error=>done.resolve(error));});
    probe=createGatewayProbe({maxPendingAdmissions:1,sink:event=>events.push(event)});probe.attach({server,store,sessionHandoffs:createSessionRequestHandoff(),status:()=>({})});server.emit('request',Object.assign(new EventEmitter(),{method:'POST',url:'/v1/responses',headers:{'x-capacity-trace':'admission-budget'}}),Object.assign(new EventEmitter(),{statusCode:200}));assert.equal(await done.promise,undefined);
    assert.equal(ids.length,2);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=ANY($1::text[])',ids)).n,2);assert.equal(events.filter(event=>event.kind==='gateway-admitted').length,1);assert.equal(probe.status().dropped,1);assert.equal(probe.status().pendingAdmissions,0);
  }finally{await probe?.close();await target?.close();}
});

test('a rejected transaction acknowledgement is not falsely asserted to prove rollback',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target,probe,native;
  try{
    target=await createTarget({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,employeeCount:1});const store=target.store,member=await store.authenticate(target.employeeActors[0].secret),server=new EventEmitter(),events=[],done=deferred();native=store.transaction;let lost=true,id;
    store.transaction=function(...args){const outer=!this.context.getStore();return native.apply(this,args).then(result=>{if(outer&&lost){lost=false;throw Error('synthetic commit acknowledgement lost');}return result;});};
    server.on('request',(_req,res)=>{void store.transaction(async()=>{id=await store.observe(member,'127.0.0.1','synthetic-commit-ack','gpt-6.1-sol',{at:Date.now()});}).then(()=>{res.statusCode=200;},()=>{res.statusCode=500;}).finally(()=>{res.emit('finish');done.resolve();});});
    probe=createGatewayProbe({sink:event=>events.push(event)});probe.attach({server,store,sessionHandoffs:createSessionRequestHandoff(),status:()=>({})});server.emit('request',Object.assign(new EventEmitter(),{method:'POST',url:'/v1/responses',headers:{'x-capacity-trace':'uncertain-commit'}}),Object.assign(new EventEmitter(),{statusCode:200}));await done.promise;
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',id)).n,1);assert.equal(events.filter(event=>event.kind==='gateway-admitted').length,0);assert.deepEqual(events.filter(event=>event.kind==='gateway-admission-unconfirmed').map(event=>event.requestId),[id]);assert.equal(probe.status().unconfirmedAdmissions,1);assert.equal(probe.status().pendingAdmissions,0);
  }finally{try{await probe?.close();}finally{if(target&&native)target.store.transaction=native;await target?.close();}}
});
