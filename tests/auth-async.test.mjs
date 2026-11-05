import test from 'node:test';
import assert from 'node:assert/strict';
import {setImmediate as immediate} from 'node:timers/promises';
import {createAdminAuth,passwordHash} from '../auth.mjs';
const response=()=>{const headers={};return {headers,setHeader:(name,value)=>{headers[name]=value;}};};
const cookie=res=>res.headers['Set-Cookie'].split(';')[0];
test('the real async scrypt login yields the event loop and keeps legacy synchronous sessions',async()=>{
  const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-async')}),legacy=response();assert.equal(auth.login('synthetic-async',legacy),undefined);assert.equal(auth.authenticated({headers:{cookie:cookie(legacy)}}),true);
  let settled=false;const current=response(),pending=auth.loginAsync('synthetic-async',current).finally(()=>{settled=true;});await immediate();assert.equal(settled,false);await pending;assert.equal(auth.authenticated({headers:{cookie:cookie(current)}}),true);assert.notEqual(cookie(current),cookie(legacy));
});
test('concurrent accepted logins stay ordered and five failures cannot be bypassed by an already queued valid password',async()=>{
  let at=1000;const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-correct')},{clock:()=>at}),responses=Array.from({length:6},response),outcomes=await Promise.allSettled(responses.map((res,index)=>auth.loginAsync(index===5?'synthetic-correct':'wrong',res)));
  assert.deepEqual(outcomes.map(row=>row.reason?.code),['password_incorrect','password_incorrect','password_incorrect','password_incorrect','password_incorrect','login_limited']);assert.ok(responses.every(res=>!res.headers['Set-Cookie']));
  at+=60000;const retried=response();await auth.loginAsync('synthetic-correct',retried);assert.equal(auth.authenticated({headers:{cookie:cookie(retried)}}),true);
});
test('a synchronous caller cannot make an in-flight old result bypass lockout',async()=>{
  const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-correct')}),res=response(),pending=auth.loginAsync('synthetic-correct',res);pending.catch(()=>{});await Promise.resolve();for(let i=0;i<5;i++)assert.throws(()=>auth.login('wrong',response()),{code:'password_incorrect'});await assert.rejects(pending,{code:'login_limited'});assert.equal(res.headers['Set-Cookie'],undefined);
});
test('changed credentials and KDF errors cannot issue a cookie or poison later accepted work',async()=>{
  const config={adminPasswordHash:passwordHash('synthetic-old')},auth=createAdminAuth(config),res=response(),pending=auth.loginAsync('synthetic-old',res);pending.catch(()=>{});await Promise.resolve();config.adminPasswordHash=passwordHash('synthetic-new');await assert.rejects(pending,{code:'login_configuration_changed'});assert.equal(res.headers['Set-Cookie'],undefined);
  config.adminPasswordHash={salt:{invalid:true},digest:'00'.repeat(32)};await assert.rejects(auth.loginAsync('synthetic-new',response()),{code:'login_unavailable'});config.adminPasswordHash=passwordHash('synthetic-new');const valid=response();await auth.loginAsync('synthetic-new',valid);assert.equal(auth.authenticated({headers:{cookie:cookie(valid)}}),true);
});
test('pending crypto work is bounded by the existing session budget without losing distinct successful sessions',{timeout:20000},async()=>{
  const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-bounded')}),responses=Array.from({length:100},response),pending=responses.map(res=>auth.loginAsync('synthetic-bounded',res));await assert.rejects(auth.loginAsync('synthetic-bounded',response()),{code:'login_busy'});await Promise.all(pending);assert.equal(new Set(responses.map(cookie)).size,100);assert.ok(responses.every(res=>auth.authenticated({headers:{cookie:cookie(res)}})));const next=response();await auth.loginAsync('synthetic-bounded',next);assert.equal(auth.authenticated({headers:{cookie:cookie(responses[0])}}),false);assert.equal(auth.authenticated({headers:{cookie:cookie(next)}}),true);
});
