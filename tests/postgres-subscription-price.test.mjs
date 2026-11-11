import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
import {subscriptionMonthlyUsd} from '../postgres-subscription-price.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('订阅月费接受两位小数或明确未知，拒绝无效精度及数值',()=>{
 for(const value of [20,100,200,500,199.99,null])assert.equal(subscriptionMonthlyUsd(value),value);
 for(const value of [undefined,'200',0,-1,NaN,Infinity,20.001,Number.MAX_VALUE])assert.throws(()=>subscriptionMonthlyUsd(value),{code:'subscription_price_invalid'});
});
test('真实HTTP订阅月费原子保存、版本冲突及鉴权，保留容量和路由配置',{skip:!binDirectory?'requires private PG18':false,timeout:30000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory});let app;
 try{
  const setup=new PostgresStore({connection:lab.connection});try{await setup.migrate();}finally{await setup.close();}
  app=await createPostgresGateway({env:{},database:{connection:lab.connection},dataDir:join(lab.directory,'price-app'),runtimeDir:join(lab.directory,'price-runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-price-admin')},recording:{enabled:false}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password:'synthetic-price-admin'})});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
  const call=async(path,method='GET',body,status=200,authenticated=true)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{...(authenticated?{cookie}:{}),'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});assert.equal(r.status,status,method+' '+path);return r.json();};
  const created=await call('accounts','POST',{name:'合成订阅月费',kind:'codex',auth_mode:'subscription',subscription_monthly_usd:200},201),path='accounts/'+created.id+'/subscription';
  assert.deepEqual(await call(path),{monthly_usd:200,version:1});await call(path,'GET',undefined,401,false);await call(path,'PATCH',{monthly_usd:20,expected_version:1},401,false);
  const before=await app.store.get('SELECT * FROM harbor.accounts WHERE id=$1',created.id),selection=await app.store.get('SELECT * FROM harbor.account_package_selections WHERE account_id=$1',created.id);
  const changed=await Promise.all([100,500].map(monthly_usd=>fetch(origin+'/api/admin/'+path,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({monthly_usd,expected_version:1})})));assert.deepEqual(changed.map(response=>response.status).sort(),[200,409]);const saved=await call(path);assert.equal(saved.version,2);assert.ok([100,500].includes(saved.monthly_usd));
  await call(path,'PATCH',{monthly_usd:1.001,expected_version:2},400);assert.deepEqual(await call(path),saved);
  assert.deepEqual(await app.store.get('SELECT * FROM harbor.accounts WHERE id=$1',created.id),before);assert.deepEqual(await app.store.get('SELECT * FROM harbor.account_package_selections WHERE account_id=$1',created.id),selection);
  const legacy=await call('accounts','POST',{name:'合成兼容订阅',kind:'codex',auth_mode:'subscription'},201);assert.deepEqual(await call('accounts/'+legacy.id+'/subscription'),{monthly_usd:null,version:0});
  await call('accounts','POST',{name:'合成拒绝API月费',kind:'codex',auth_mode:'openai_api',subscription_monthly_usd:200},400);assert.equal((await app.store.get('SELECT COUNT(*) total FROM harbor.accounts')).total,2);
  const run=app.store.run.bind(app.store);app.store.run=(sql,...args)=>{if(sql==='INSERT INTO harbor.audit VALUES($1,$2,$3,$4)'&&args[2]==='account.subscription.price')throw Error('synthetic audit failure');return run(sql,...args);};
  try{await call(path,'PATCH',{monthly_usd:20,expected_version:2},500);}finally{app.store.run=run;}assert.deepEqual(await call(path),saved);
  await call('accounts/'+created.id,'DELETE');await call(path,'GET',undefined,404);
 }finally{await app?.close();await lab.close();}
});
