import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';import {randomUUID} from 'node:crypto';
import {Store,usageWindow} from '../store.mjs';import {createGateway} from '../server.mjs';import {createResponsesTransport} from '../responses.mjs';
const sol='gpt-6.1-sol',astra='gpt-6-astra';
async function setup(t){
 const root=mkdtempSync(join(tmpdir(),'harbor-member-limit-'));let at=Date.now()+1000,hold=null,known=true;const calls=[];
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic-upstream',accountId:'fixture'}),version:async()=>'fixture',fetchImpl:async(url,options)=>{
  const body=JSON.parse(options.body);calls.push(body);if(hold)await hold();
  return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'fixture',model:body.model,status:'completed',output:[],...(known?{usage:{input_tokens:1000000,output_tokens:0}}:{})}})+'\n\n',{headers:{'Content-Type':'text/event-stream'}});
 }});
 const a=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:'unused'},clock:()=>at,responsesTransport:transport,rateReader:async()=>null});
 a.store.run("INSERT INTO accounts(id,name,home,state) VALUES('a','fixture','unused','ready')");await new Promise(r=>a.server.listen(0,'127.0.0.1',r));t.after(async()=>{await a.close();rmSync(root,{recursive:true,force:true});});
 const base='http://127.0.0.1:'+a.server.address().port,member=a.store.createMember('Fixture',null,null),key2=a.store.createKey(member.memberId);
 const call=async(path,method='GET',body,secret='fixture-admin')=>{const r=await fetch(base+path,{method,headers:{Authorization:'Bearer '+secret,'Content-Type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});return {status:r.status,body:await r.json()};};
 const configure=(rules,enabled=true,person=member)=>call('/api/admin/members/'+person.memberId+'/usage-limits','PATCH',{enabled,rules,expected_version:a.store.memberLimits.configuration(person.memberId).version});
 const send=(model=sol,person=member,extra={})=>call('/v1/responses','POST',{model,input:'synthetic',stream:false,...extra},person.secret);
 const seed=(model,ended,input=1000000,extra={})=>{const id=a.store.observe(a.store.authenticate(member.secret),'synthetic','fixture',model);a.store.finish(id,{input,output:0,cached:0,usageKnown:!extra.unknown,status:extra.unknown?'unknown':'ok'});a.store.run('UPDATE requests SET started=?,ended=? WHERE id=?',ended-1,ended,id);return id;};
 return {...a,base,member,key2,call,configure,send,seed,calls,now:()=>at,advance:ms=>at+=ms,hold:fn=>hold=fn,known:value=>known=value};
}
const rule=(model=sol,hours=24,limit_usd='2.00')=>({model,hours,limit_usd});
test('administrators configure a per-member switch with exact amounts, strict rules and optimistic conflicts', {timeout:10000},async t=>{
 const a=await setup(t),path='/api/admin/members/'+a.member.memberId+'/usage-limits';assert.equal((await a.call(path)).body.enabled,false);
 assert.equal((await a.call(path,'PATCH',{enabled:true,rules:[rule()],expected_version:0},a.member.secret)).status,401);
 for(const rules of [[],[rule(sol,25)], [rule(sol,'24')],[rule('codex')],[rule(sol,24,'-1')],[rule(sol,24,'0.001')],[rule(sol,24,1)],[rule(),rule()],[{...rule(),override:true}]])assert.equal((await a.configure(rules)).status,400);
 const saved=await a.configure([rule(sol,24,'02.0'),rule(sol,168,'10')]);assert.equal(saved.status,200);assert.equal(saved.body.rules[0].limit_usd,'2.00');assert.equal(saved.body.rules[1].limit_usd,'10.00');
 assert.equal((await a.call(path,'PATCH',{enabled:false,expected_version:0})).status,409);
 assert.equal((await a.call(path,'PATCH',{enabled:false,expected_version:1})).status,200);assert.equal(a.store.memberLimits.configuration(a.member.memberId).rules.length,2);
 const history=(await a.call('/api/admin/history?action=member.limits')).body;assert.equal(history.rows[0].detail.member,'Fixture');
 const before=a.store.memberLimits.configuration(a.member.memberId),audit=a.store.audit.bind(a.store);a.store.audit=()=>{throw Error('synthetic transaction failure');};assert.throws(()=>a.store.memberLimits.save(a.member.memberId,{enabled:true,expected_version:before.version}),/transaction/);a.store.audit=audit;assert.deepEqual(a.store.memberLimits.configuration(a.member.memberId),before);
});
test('all Keys for the same employee share limits, while other employees and unconfigured models stay independent',{timeout:10000},async t=>{
 const a=await setup(t);await a.configure([rule()]);assert.equal((await a.send()).status,200);assert.equal((await a.send(sol,a.key2)).status,429);assert.equal(a.calls.length,1);
 assert.equal((await a.send(astra)).status,200);const other=a.store.createMember('Other',null,null);assert.equal((await a.send(sol,other)).status,200);
 const own=(await a.call('/api/me','GET',undefined,a.key2.secret)).body.usage_limits;assert.equal(own.usage[0].used_usd,'2');
 await a.configure([rule()],false);assert.equal((await a.send(sol,a.key2)).status,200);
});
test('rolling windows restore at the exact boundary and seven-day limits include archived calendar-day rows',{timeout:10000},async t=>{
 const a=await setup(t),at=a.now(),w=usageWindow(at);await a.configure([rule(sol,24,'10'),rule(sol,168,'2')]);
 const oldTime=(at-168*3600000+w.start)/2,id=a.seed(sol,Math.floor(oldTime));assert.ok(oldTime<w.start);
 assert.equal((await a.call('/api/admin/requests/'+id)).status,404);const status=a.store.memberLimits.status(a.member.memberId);assert.equal(status.usage[0].used_usd,'0');assert.equal(status.usage[1].used_usd,'2');assert.equal((await a.send()).status,429);
 a.store.run('UPDATE requests SET ended=? WHERE id=?',at-168*3600000,id);assert.equal(a.store.memberLimits.status(a.member.memberId).usage[1].used_usd,'0');assert.equal((await a.send()).status,200);
 a.advance(24*3600000+1000);assert.equal(a.store.memberLimits.status(a.member.memberId).usage[0].used_usd,'0');
});
test('limits use immutable unrounded amounts, cached prices and manual reconciliation instead of display-rounded or current prices',{timeout:10000},async t=>{
 const a=await setup(t);await a.configure([rule(sol,24,'0.01')]);const id=a.seed(sol,a.now(),1);a.store.run("UPDATE requests SET manual_cost_atoms='9999999999' WHERE id=?",id);
 assert.doesNotThrow(()=>a.store.memberLimits.check(a.member.memberId,sol));a.store.run("UPDATE requests SET manual_cost_atoms='10000000000' WHERE id=?",id);assert.throws(()=>a.store.memberLimits.check(a.member.memberId,sol),e=>e.code==='member_model_limit_exceeded');
 await a.configure([rule(sol,24,'3')]);a.store.run('UPDATE requests SET manual_cost_atoms=NULL,input=1000000 WHERE id=?',id);
 const pricing=(await a.call('/api/admin/pricing')).body;await a.call('/api/admin/pricing','PATCH',{expectedVersion:pricing.settings.version,manualRates:{[sol]:{input:'100',cached:'10',output:'100',source:'synthetic rate change'}}});assert.equal(a.store.memberLimits.status(a.member.memberId).usage[0].used_usd,'2');
});
test('unknown usage blocks relevant models until reconciled; unknown aliases and implicit tool models cannot bypass limits',{timeout:10000},async t=>{
 const a=await setup(t);await a.configure([rule()]);const id=a.seed(sol,a.now(),0,{unknown:true});assert.equal((await a.send()).body.error.type,'member_model_usage_unknown');assert.equal(a.calls.length,0);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'1.00'})).status,200);assert.equal(a.store.memberLimits.status(a.member.memberId).usage[0].used_usd,'1');
 for(const [model,extra] of [['unknown-alias',{}],[sol,{tools:[{type:'image_generation'}]}],[sol,{tools:[{type:'namespace',tools:[{type:'image_generation'}]}]}]])assert.ok([403,409].includes((await a.send(model,a.member,extra)).status));
 a.seed('unknown-alias',a.now(),1,{unknown:true});assert.equal((await a.send()).status,409);assert.equal((await a.send(astra)).status,200);
});
test('concurrent requests admitted below a limit complete, then subsequent requests are rejected',{timeout:10000},async t=>{
 const a=await setup(t);await a.configure([rule()]);let release;const held=new Promise(r=>release=r);a.hold(()=>held);const first=a.send(),second=a.send(sol,a.key2);
 const until=Date.now()+3000;while(a.calls.length<2){assert.ok(Date.now()<until);await new Promise(r=>setTimeout(r,5));}
 release();assert.equal((await first).status,200);assert.equal((await second).status,200);assert.equal(a.store.memberLimits.status(a.member.memberId).usage[0].used_usd,'4');assert.equal((await a.send()).status,429);assert.equal(a.calls.length,2);
});
test('old sessions, fixed headers and spoofed employee/config fields cannot bypass the current policy',{timeout:10000},async t=>{
 const a=await setup(t),thread=randomUUID();assert.equal((await a.send(sol,a.member,{prompt_cache_key:thread})).status,200);await a.configure([rule(sol,24,'0')]);
 assert.equal((await a.send(sol,a.key2,{prompt_cache_key:thread,member_id:'other',usage_limits:{enabled:false}})).status,429);assert.equal(a.calls.length,1);
 assert.equal((await a.call('/v1/images/generations','POST',{model:sol,prompt:'synthetic'},a.member.secret)).status,429);
});
test('rules survive restart and disabling does not delete rules or request history',t=>{
 const dir=mkdtempSync(join(tmpdir(),'harbor-limits-store-')),file=join(dir,'db.sqlite');let s=new Store(file);const m=s.createMember('Fixture',null,null);s.memberLimits.save(m.memberId,{enabled:true,rules:[rule()],expected_version:0});s.close();s=new Store(file);t.after(()=>{s.close();rmSync(dir,{recursive:true,force:true});});assert.equal(s.memberLimits.configuration(m.memberId).enabled,true);s.memberLimits.save(m.memberId,{enabled:false,expected_version:1});assert.deepEqual(s.memberLimits.configuration(m.memberId).rules,[rule()]);
});
test('R1 alias rules cannot be newly saved and existing rules enforce the same canonical model as admission',{timeout:10000},async t=>{
 const a=await setup(t),path='/api/admin/members/'+a.member.memberId+'/usage-limits',pricing=(await a.call('/api/admin/pricing')).body;
 assert.equal((await a.call('/api/admin/pricing','PATCH',{expectedVersion:pricing.settings.version,aliases:[{name:'sol-fixed',target:sol,from:a.now()-86400000,to:null,source:'synthetic alias evidence'}]})).status,200);
 const invalid=await a.configure([rule('sol-fixed',24,'0')]);assert.equal(invalid.status,400);assert.match(invalid.body.error.message,/gpt-6.1-sol/);
 a.store.run('UPDATE members SET usage_limits=? WHERE id=?',JSON.stringify({enabled:true,rules:[rule('sol-fixed',24,'0.00')]}),a.member.memberId);
 const status=a.store.memberLimits.status(a.member.memberId);assert.equal(status.usage[0].effective_model,sol);assert.equal(status.usage[0].blocked,true);
 assert.equal((await a.send(sol)).status,429);assert.equal((await a.send('sol-fixed')).status,429);assert.equal(a.calls.length,0);
 a.store.run('UPDATE members SET usage_limits=? WHERE id=?',JSON.stringify({enabled:true,rules:[rule('sol-fixed',24,'2.00')]}),a.member.memberId);a.seed('sol-fixed',a.now());assert.equal(a.store.memberLimits.status(a.member.memberId).usage[0].used_usd,'2');assert.equal((await a.send(sol)).status,429);
 const current=(await a.call('/api/admin/pricing')).body;await a.call('/api/admin/pricing','PATCH',{expectedVersion:current.settings.version,aliases:[]});assert.equal((await a.send(astra)).body.error.type,'member_limits_invalid');
 assert.equal((await a.call(path,'PATCH',{enabled:false,expected_version:0})).status,200);assert.equal((await a.send(sol)).status,200);
 assert.equal((await a.configure([rule(sol,24,'0')])).status,200);assert.equal((await a.send(sol)).status,429);
});
test('unattributed manual amounts pause configured model limits without charging the same amount to every model',{timeout:10000},async t=>{
 const a=await setup(t);await a.configure([rule(sol,24,'20'),rule(astra,24,'20')]);const id=a.seed('unknown-alias',a.now(),0,{unknown:true});
 await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'0.50'});
 const status=a.store.memberLimits.status(a.member.memberId);assert.ok(status.usage.every(row=>row.used_usd==='0'&&row.unresolved===1&&row.blocked));assert.equal((await a.send(sol)).status,409);assert.equal((await a.send(astra)).status,409);
 a.store.run("UPDATE requests SET manual_cost_atoms='0' WHERE id=?",id);assert.ok(a.store.memberLimits.status(a.member.memberId).usage.every(row=>row.unresolved===0));
});
