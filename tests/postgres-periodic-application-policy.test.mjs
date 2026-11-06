import test from 'node:test';
import assert from 'node:assert/strict';
import {periodicApplicationPolicy,loadPeriodicApplicationPolicy} from '../postgres-periodic-application-policy.mjs';

const configuration=()=>({version:1,enabled:true,database:{name:'harbor_private',oid:'16384'},materialRoots:[{id:'data',directory:'/srv/private/data'},{id:'runtime',directory:'/srv/private/runtime'},{id:'secrets',directory:'/srv/private/secrets'}],publicOrigin:'http://127.0.0.1:18094',deviceOrigin:'https://127.0.0.1:18095',maxBytes:1073741824,maxFiles:100000});

test('periodic application backup is opt-in and preserves an explicitly disabled complete configuration',()=>{
  assert.deepEqual(periodicApplicationPolicy({version:1,enabled:false}),{version:1,enabled:false});
  const input=configuration(),policy=periodicApplicationPolicy(input);
  input.database.name='changed';input.materialRoots[0].directory='/other';
  assert.equal(policy.database.name,'harbor_private');assert.equal(policy.materialRoots[0].directory,'/srv/private/data');
  assert.ok(Object.isFrozen(policy)&&Object.isFrozen(policy.database)&&Object.isFrozen(policy.materialRoots)&&policy.materialRoots.every(Object.isFrozen));
  const disabled=periodicApplicationPolicy({...configuration(),enabled:false});
  assert.equal(disabled.enabled,false);assert.deepEqual(disabled.materialRoots,configuration().materialRoots);
});

test('periodic application policy rejects ambiguous coverage, broad roots and unbounded capture',()=>{
  const changes=[
    {enabled:'true'},{version:2},{unknown:true},{maxBytes:0},{maxBytes:Number.MAX_SAFE_INTEGER+1},{maxFiles:1000001},
    {database:{name:'harbor_private',oid:'4294967296'}},{database:{name:'harbor_private',oid:'016384'}},
    {database:{name:'harbor_private',oid:'16384',connectionString:'not accepted'}},
    {materialRoots:configuration().materialRoots.slice(0,2)},
    {materialRoots:[...configuration().materialRoots.slice(0,2),{id:'data',directory:'/another'}]},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='secrets'?{...row,directory:'/srv/private/data/nested'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='runtime'?{...row,databasePrefix:'/srv/private/data/nested'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='runtime'?{...row,databasePrefix:'/app/../runtime'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='secrets'?{...row,directory:'/etc'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='secrets'?{...row,directory:'/'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='secrets'?{...row,directory:'/srv/private/../secrets'}:row)},
    {materialRoots:configuration().materialRoots.map(row=>row.id==='secrets'?{...row,exclude:['device']}:row)},
  ];
  for(const change of changes)assert.throws(()=>periodicApplicationPolicy({...configuration(),...change}),{code:'periodic_application_policy_invalid'});
  for(const input of [null,[],{version:1,enabled:true},{version:1,enabled:false,maxBytes:1}])assert.throws(()=>periodicApplicationPolicy(input),{code:'periodic_application_policy_invalid'});
});

test('container database path prefixes are explicit and preserved independently from host capture roots',()=>{
  const input=configuration();input.materialRoots[1].databasePrefix='/app/runtime';
  assert.equal(periodicApplicationPolicy(input).materialRoots[1].databasePrefix,'/app/runtime');
  assert.equal(periodicApplicationPolicy(input).materialRoots[1].directory,'/srv/private/runtime');
});

test('periodic recovery endpoints require exact origins and a separate TLS device entry',()=>{
  for(const change of [{deviceOrigin:'http://127.0.0.1:18095'},{deviceOrigin:'https://user:password@localhost:18095'},{deviceOrigin:'https://localhost:18095/path'},{deviceOrigin:'https://localhost:18095?key=x'},{publicOrigin:'http://localhost:18094/'},{publicOrigin:'file:///tmp/app'},{publicOrigin:'https://localhost:18095',deviceOrigin:'https://localhost:18095'}])assert.throws(()=>periodicApplicationPolicy({...configuration(),...change}),{code:'periodic_application_policy_invalid'});
});

test('the fixed application profile loader cannot be used by an unprivileged caller',{skip:process.platform!=='linux'||process.getuid()===0?'需普通Linux用户，不创建宿主/etc配置':false},async()=>{
  await assert.rejects(loadPeriodicApplicationPolicy(),{code:'periodic_application_root_profile_required'});
});
