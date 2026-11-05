import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import crypto from 'node:crypto';
import {syncBuiltinESMExports} from 'node:module';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const bounded=async(promise,reason)=>{let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(reason)),5000);})]);}finally{clearTimeout(timer);}};
test('actual PostgreSQL HTTP login awaits crypto, keeps independent sessions and releases disconnected requests',{skip:!binDirectory?'requires private PG18':false,timeout:30000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),originalScrypt=crypto.scrypt;let app;
 try{
  const setup=new PostgresStore({connection:lab.connection});try{await setup.migrate();}finally{await setup.close();}
  app=await createPostgresGateway({env:{},database:{connection:lab.connection},dataDir:join(lab.directory,'auth-app'),runtimeDir:join(lab.directory,'auth-runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-pg-login')},recording:{enabled:false}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port,post=(password='synthetic-pg-login',signal)=>fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password}),signal});
  const responses=await Promise.all(Array.from({length:5},()=>post()));assert.ok(responses.every(row=>row.status===200));const cookies=responses.map(row=>row.headers.get('set-cookie').split(';')[0]);assert.equal(new Set(cookies).size,5);for(const cookie of cookies)assert.deepEqual(await(await fetch(origin+'/api/auth/session',{headers:{cookie}})).json(),{authenticated:true});
  let startedResolve,finishedResolve,finished=false;const started=new Promise(resolve=>{startedResolve=resolve;}),done=new Promise(resolve=>{finishedResolve=resolve;});
  // Observe the actual worker call; do not replace its KDF or result. Waiting
  // for this boundary prevents an early fetch abort from testing only parsing.
  crypto.scrypt=function(...args){const callback=args.at(-1);args[args.length-1]=function(...values){finished=true;try{return callback.apply(this,values);}finally{finishedResolve();}};const result=originalScrypt.apply(this,args);startedResolve();return result;};syncBuiltinESMExports();
  const aborted=new AbortController(),pending=post('synthetic-pg-login',aborted.signal);pending.catch(()=>{});await bounded(started,'actual KDF did not start');assert.equal(finished,false);aborted.abort();await assert.rejects(pending,{name:'AbortError'});await bounded(done,'actual KDF did not finish');crypto.scrypt=originalScrypt;syncBuiltinESMExports();const until=Date.now()+5000;while(app.status().requestBodies.leases){assert.ok(Date.now()<until,'disconnected login body lease not released');await delay(5);}assert.equal((await fetch(origin+'/health')).status,200);assert.equal((await post()).status,200);
  for(let i=0;i<5;i++)assert.equal((await post('wrong')).status,401);assert.equal((await post()).status,429);
 }finally{crypto.scrypt=originalScrypt;syncBuiltinESMExports();await app?.close();await lab.close();}
});
