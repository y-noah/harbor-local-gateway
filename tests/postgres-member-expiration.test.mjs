import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;

test('员工到期摘要只统计当前页未删除Key，区分永久、已过期及最近到期，不扫描请求正文',{skip:!binDirectory?'requires private PG18':false,timeout:30000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory});let app;
 try{
  const setup=new PostgresStore({connection:lab.connection});try{await setup.migrate();}finally{await setup.close();}
  app=await createPostgresGateway({env:{},database:{connection:lab.connection},dataDir:join(lab.directory,'expiry-app'),runtimeDir:join(lab.directory,'expiry-runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-expiry-admin')},recording:{enabled:false}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password:'synthetic-expiry-admin'})});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
  const admin=async(path,method='GET',body)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});assert.ok(r.ok,method+' '+path+' HTTP '+r.status);return r.json();};
  const create=async(name,expires=null)=>admin('members','POST',{name,expires,quota:null}),key=async(memberId,expires)=>admin('keys','POST',{memberId,expires}),at=Date.now(),past=at-3600000,soon=at+3600000,later=at+7200000;
  const mixed=await create('合成混合期限');await key(mixed.memberId,past);await key(mixed.memberId,soon);await key(mixed.memberId,later);const removed=await key(mixed.memberId,0);await admin('keys/'+removed.keyId,'DELETE');
  const permanent=await create('合成永久期限');await key(permanent.memberId,null);const expired=await create('合成过期期限',0),empty=await create('合成无Key');await admin('keys/'+empty.keyId,'DELETE');
  const all=app.queryStore.all.bind(app.queryStore),get=app.queryStore.get.bind(app.queryStore);let forbidden=0;
  const check=sql=>{if(/\bharbor\.(requests|request_prices|conversation_events)\b/.test(sql)){forbidden++;throw Error('expiry metadata must not scan usage or body')}};
  app.queryStore.all=(sql,...args)=>{check(sql);return all(sql,...args)};app.queryStore.get=(sql,...args)=>{check(sql);return get(sql,...args)};
  let metadata;try{metadata=await admin('members?view=list&page_size=20');assert.equal(forbidden,0)}finally{app.queryStore.all=all;app.queryStore.get=get}
  const find=id=>metadata.rows.find(row=>row.id===id);assert.equal(find(mixed.memberId).key_count,4);assert.deepEqual(find(mixed.memberId).key_expiration,{permanent:1,expired:1,next:soon,last_expired:past});assert.deepEqual(find(permanent.memberId).key_expiration,{permanent:2,expired:0,next:null,last_expired:null});assert.deepEqual(find(expired.memberId).key_expiration,{permanent:0,expired:1,next:null,last_expired:0});assert.equal(find(empty.memberId).key_count,0);assert.deepEqual(find(empty.memberId).key_expiration,{permanent:0,expired:0,next:null,last_expired:null});
  const full=await admin('members?page_size=20');for(const row of full.rows)assert.deepEqual(row.key_expiration,find(row.id).key_expiration);
  const one=await admin('members?view=list&page_size=1');assert.equal(one.rows.length,1);assert.ok(one.rows[0].key_expiration);assert.equal(JSON.stringify(metadata).includes(mixed.secret),false);assert.equal(JSON.stringify(metadata).includes(removed.secret),false);
  const policy=await admin('keys/'+mixed.keyId+'/policy');assert.deepEqual(policy.allowed_models,null);assert.equal(policy.usage_limits.enabled,false);assert.equal(JSON.stringify(policy).includes(mixed.secret),false);assert.deepEqual(Object.keys(policy).sort(),['allowed_models','usage_limits']);
  assert.equal((await fetch(origin+'/api/admin/keys/'+mixed.keyId+'/policy')).status,401);
  assert.equal((await fetch(origin+'/api/admin/keys/'+removed.keyId+'/policy',{headers:{cookie}})).status,404);
 }finally{await app?.close();await lab.close();}
});
