import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {join,resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {setTimeout as sleep} from 'node:timers/promises';
import {DatabaseSync} from 'node:sqlite';
import {createGateway} from '../server.mjs';

const scratch=resolve(fileURLToPath(new URL('../../../work/',import.meta.url)),'stability-tests');
mkdirSync(scratch,{recursive:true});
const limits=()=>({rateLimits:{primary:{usedPercent:0,resetsAt:Math.floor(Date.now()/1000)+18000}}});
async function waitFor(predicate,label,timeout=5000){
  const end=Date.now()+timeout;
  while(!predicate()){if(Date.now()>=end)throw Error('Timed out waiting for '+label);await sleep(5);}
}
async function deadline(promise,label,timeout=10000){
  let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Timed out: '+label)),timeout);})]);}finally{clearTimeout(timer);}
}
function result(args,thread='stable-thread'){
  return {text:'答复',threadId:args.sessionId||thread,input:30,output:2,cached:10,usageKnown:true,cumulativeUsage:{input:args.previousUsage.input+30,output:args.previousUsage.output+2,cached:args.previousUsage.cached+10}};
}
async function fixture(t,{executor,rateReader=async()=>limits(),accounts=3}={}){
  const dir=mkdtempSync(join(scratch,'case-'));
  const app=createGateway({dataDir:join(dir,'data'),runtimeDir:join(dir,'runtime'),config:{adminToken:'stability-admin',codexBinary:'unused',reserveTokens:100},checker:async()=>true,rateReader,executor});
  for(let i=0;i<accounts;i++)app.store.run('INSERT INTO accounts(id,name,home,state,token_budget) VALUES(?,?,?,?,?)','a'+i,'Account '+i,'fake-'+i,'ready',10000000);
  await deadline(new Promise(r=>app.server.listen(0,'127.0.0.1',r)),'listen');
  const base='http://127.0.0.1:'+app.server.address().port;
  t.after(async()=>{await deadline(app.close(),'fixture shutdown');const target=resolve(dir);assert.ok(target.startsWith(scratch+sep));rmSync(target,{recursive:true,force:true});});
  async function post(path,body,secret='stability-admin',signal){
    const r=await fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+secret},body:JSON.stringify(body),signal:signal?AbortSignal.any([signal,AbortSignal.timeout(15000)]):AbortSignal.timeout(15000)});
    return {status:r.status,body:await r.json()};
  }
  const chat=(member,session,signal)=>post('/v1/chat/completions',{messages:[{role:'user',content:'下一轮'}],...(session?{session_id:session}:{})},member.secret,signal);
  return {...app,post,chat,member:(name='Person')=>app.store.createMember(name,null,null)};
}

test('three accounts sustain five independent 120-turn sessions with exact accounting', {timeout:60000},async t=>{
  const running=new Map(),maxima=new Map(),threadHomes=new Map();let sequence=0;
  const app=await fixture(t,{executor:async args=>{
    const count=(running.get(args.home)||0)+1;running.set(args.home,count);maxima.set(args.home,Math.max(maxima.get(args.home)||0,count));
    const thread=args.sessionId||'long-thread-'+(++sequence);
    if(threadHomes.has(thread))assert.equal(threadHomes.get(thread),args.home);else threadHomes.set(thread,args.home);
    try{await sleep(2);return result(args,thread);}finally{running.set(args.home,running.get(args.home)-1);}
  }});
  const people=Array.from({length:5},(_,i)=>app.member('Person '+i)),sessions=[];
  for(const m of people){const r=await app.chat(m);assert.equal(r.status,200);sessions.push(r.body.session_id);}
  for(let turn=1;turn<120;turn++){
    const responses=await Promise.all(people.map((m,i)=>app.chat(m,sessions[i])));
    responses.forEach((r,i)=>{assert.equal(r.status,200);assert.equal(r.body.session_id,sessions[i]);assert.equal(r.body.usage.total_tokens,32);});
  }
  assert.equal(app.store.get('SELECT COUNT(*) n FROM requests').n,600);
  for(const [table,column] of [['members','used'],['keys','used'],['accounts','budget_used']])assert.equal(app.store.get('SELECT SUM('+column+') n FROM '+table).n,19200);
  assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);
  assert.equal(threadHomes.size,5);assert.equal(maxima.size,3);for(const max of maxima.values())assert.equal(max,1);
  for(const s of app.store.all('SELECT * FROM sessions')){assert.equal(s.message_count,240);assert.equal(s.cum_input,3600);assert.equal(s.cum_output,240);assert.equal(s.cum_cached,1200);assert.equal(s.state,'active');}
});

test('disconnecting five callers cancels queue, preserves only running uncertainty and permits recovery',{timeout:30000},async t=>{
  let waiting=true,active=0,aborts=0;
  const app=await fixture(t,{executor:async args=>{
    active++;try{
      if(waiting)await new Promise((_,reject)=>{const abort=()=>{aborts++;reject(Object.assign(Error('intentional disconnect'),{usageKnown:false,threadId:'cancelled-thread'}));};args.signal.addEventListener('abort',abort,{once:true});if(args.signal.aborted)abort();});
      return result(args);
    }finally{active--;}
  }});
  const people=Array.from({length:5},()=>app.member()),controllers=people.map(()=>new AbortController());
  const calls=people.map((m,i)=>app.chat(m,null,controllers[i].signal).catch(()=>null));
  await waitFor(()=>app.store.get("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").n===5,'five admitted requests');
  assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status='running'").n,3);
  controllers.forEach(c=>c.abort());await deadline(Promise.all(calls),'disconnect clients');
  await waitFor(()=>active===0&&app.store.get("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").n===0,'disconnect settlement');
  assert.equal(aborts,3);assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status='cancelled' AND reserved=0").n,2);
  for(const row of app.store.all("SELECT * FROM requests WHERE status='unknown'")){assert.equal(row.reserved,100);assert.equal((await app.post('/api/admin/requests/'+row.id+'/settle',{tokens:0})).status,200);}
  assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);
  waiting=false;for(const m of people)assert.equal((await app.chat(m)).status,200);
});

test('shutdown settles three running and two queued requests without stranded HTTP clients',{timeout:30000},async t=>{
  let active=0,aborts=0;
  const app=await fixture(t,{executor:async args=>{active++;try{await new Promise((_,reject)=>{const abort=()=>{aborts++;reject(Object.assign(Error('intentional shutdown'),{usageKnown:false}));};args.signal.addEventListener('abort',abort,{once:true});if(args.signal.aborted)abort();});}finally{active--;}}});
  const calls=Array.from({length:5},()=>app.chat(app.member()).catch(()=>null));
  await waitFor(()=>app.store.get("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").n===5,'shutdown work admitted');
  await deadline(app.close(),'graceful shutdown');await deadline(Promise.all(calls),'all shutdown HTTP responses');
  assert.equal(active,0);assert.equal(aborts,3);
  const db=new DatabaseSync(join(app.data,'gateway.sqlite'),{readOnly:true});
  try{assert.equal(db.prepare("SELECT COUNT(*) n FROM requests WHERE status='unknown' AND reserved=100").get().n,3);assert.equal(db.prepare("SELECT COUNT(*) n FROM requests WHERE status='cancelled' AND reserved=0").get().n,2);assert.equal(db.prepare("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").get().n,0);}finally{db.close();}
});

test('a continuation waiting on quota reloads the session cursor after the preceding turn finishes',{timeout:30000},async t=>{
  let holdQuota=false,quotaStarted=false,releaseQuota,releaseTurn,turn=0;const cursors=[];
  const app=await fixture(t,{accounts:1,rateReader:async()=>{if(holdQuota){quotaStarted=true;await deadline(new Promise(r=>releaseQuota=r),'held quota');}return limits();},executor:async args=>{cursors.push({...args.previousUsage});turn++;if(turn===2)await deadline(new Promise(r=>releaseTurn=r),'held preceding turn');return result(args,'cursor-thread');}});
  const m=app.member(),first=await app.chat(m);assert.equal(first.status,200);
  const second=app.chat(m,first.body.session_id);await waitFor(()=>!!releaseTurn,'second turn running');
  holdQuota=true;app.store.run('UPDATE accounts SET quota_json=NULL');
  const third=app.chat(m,first.body.session_id);await waitFor(()=>quotaStarted,'third turn waiting for quota');
  releaseTurn();assert.equal((await second).status,200);holdQuota=false;releaseQuota();
  assert.equal((await third).status,200);assert.deepEqual(cursors.map(x=>x.input),[0,30,60]);
  const s=app.store.get('SELECT * FROM sessions WHERE id=?',first.body.session_id);assert.equal(s.message_count,6);assert.equal(s.cum_input,90);assert.equal(s.cum_output,6);
  assert.equal(app.store.get('SELECT used FROM members WHERE id=?',m.memberId).used,96);
});

test('a failed quota refresh releases concurrent waiters and later recovery restores official quota',{timeout:30000},async t=>{
  let failing=true;const app=await fixture(t,{rateReader:async()=>{await sleep(25);if(failing)throw Error('simulated quota timeout');return limits();},executor:async args=>{await sleep(5);return result(args);}});
  const responses=await deadline(Promise.all(Array.from({length:5},()=>app.chat(app.member()))),'quota timeout fallback');responses.forEach(r=>assert.equal(r.status,200));
  assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").n,0);
  // Wait for post-response quota reads, then explicitly verify successful recovery.
  await sleep(50);failing=false;assert.equal((await app.post('/api/admin/quotas/refresh',{})).status,200);
  for(const a of app.store.all('SELECT quota_json,quota_error FROM accounts')){assert.equal(a.quota_error,null);assert.equal(JSON.parse(a.quota_json).remaining,100);}
  assert.equal((await app.chat(app.member())).status,200);
});
