import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createGateway} from '../server.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {passwordHash} from '../auth.mjs';

const integration={skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需自有隔离PG，不访问现网':false,timeout:30000};
const password='synthetic-overview-login',at=Date.parse('2026-11-13T03:00:00+08:00');
test('SQLite旧入口忽略可选首屏字段，继续返回原登录协议',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-legacy-login-overview-'));let app;
 try{
  app=createGateway({dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{codexBinary:process.execPath,adminPasswordHash:passwordHash(password)}});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));
  const response=await fetch('http://127.0.0.1:'+app.server.address().port+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password,includeOverview:true})});
  assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true});assert.match(response.headers.get('set-cookie'),/HttpOnly/);
 }finally{await app?.close();await rm(directory,{recursive:true,force:true});}
});
async function fixture(run){
 let lab,setup,app;const directory=await mkdtemp(join(tmpdir(),'harbor-login-overview-'));
 try{
  lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
  setup=new PostgresStore({connection:lab.connection});await setup.migrate();
  await setup.query("INSERT INTO harbor.members(id,name,created) VALUES('synthetic-private-member','不能出现在首屏的员工',0)");
  app=await createPostgresGateway({env:{},database:{connection:lab.connection},dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),clock:()=>at,config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash(password),reserveTokens:100},recording:{enabled:false}});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const call=async(path,{body,cookie}={})=>{const response=await fetch(origin+path,{method:body===undefined?'GET':'POST',headers:{...(cookie?{cookie}:{}),...(body===undefined?{}:{'content-type':'application/json'})},...(body===undefined?{}:{body:JSON.stringify(body)})});return {response,value:await response.json()};};
  await run({app,call});
 }finally{await app?.close();await setup?.close();await lab?.close();await rm(directory,{recursive:true,force:true});}
}

test('成功登录可复用同份轻量总览，默认和关闭选项保持旧响应',integration,()=>fixture(async({call})=>{
 for(const includeOverview of [undefined,false]){
  const result=await call('/api/auth/login',{body:{password,...(includeOverview===undefined?{}:{includeOverview})}});assert.equal(result.response.status,200);assert.deepEqual(result.value,{ok:true});
 }
 const result=await call('/api/auth/login',{body:{password,includeOverview:true}});assert.equal(result.response.status,200);assert.match(result.response.headers.get('set-cookie'),/HttpOnly/);
 const cookie=result.response.headers.get('set-cookie').split(';')[0],normal=await call('/api/admin/state?view=overview&people=none',{cookie});
 assert.equal(normal.response.status,200);assert.deepEqual(result.value.initialState,normal.value);assert.equal(result.value.initialState.view,'overview');assert.equal(result.value.initialState.memberCount,1);
 for(const field of ['members','keys','sessions','requests','audit'])assert.deepEqual(result.value.initialState[field],[]);
 assert.equal(JSON.stringify(result.value).includes('不能出现在首屏的员工'),false);
}));

test('错误密码与非法首屏选项不泄露总览，首屏查询故障不使已通过的鉴权失效',integration,()=>fixture(async({app,call})=>{
 const rejected=await call('/api/auth/login',{body:{password:'synthetic-wrong',includeOverview:true}});assert.equal(rejected.response.status,401);assert.equal(rejected.value.initialState,undefined);assert.equal(rejected.response.headers.get('set-cookie'),null);
 for(const includeOverview of ['true',1,null]){const result=await call('/api/auth/login',{body:{password,includeOverview}});assert.equal(result.response.status,400);assert.equal(result.value.initialState,undefined);assert.equal(result.response.headers.get('set-cookie'),null);}
 const original=app.queryStore.transaction;app.queryStore.transaction=async()=>{throw Error('synthetic state unavailable');};
 let result;try{result=await call('/api/auth/login',{body:{password,includeOverview:true}});}finally{app.queryStore.transaction=original;}
 assert.equal(result.response.status,200);assert.deepEqual(result.value,{ok:true});const cookie=result.response.headers.get('set-cookie').split(';')[0];assert.equal((await call('/api/admin/state?view=overview&people=none',{cookie})).response.status,200);
 assert.equal((await call('/api/admin/state?view=overview&people=none')).response.status,401);
}));
