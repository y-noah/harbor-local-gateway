import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Store,usageWindow} from '../store.mjs';
import {requestsQuery,requestsReader} from '../observability.mjs';
import {createQueryService} from '../query-service.mjs';
import {createGateway} from '../server.mjs';

function fixture(t){
  const root=mkdtempSync(join(tmpdir(),'harbor-query-')),filename=join(root,'db.sqlite'),store=new Store(filename),at=Date.now();
  const member=store.createMember('分页合成员工',null,null),person=store.authenticate(member.secret);
  t.after(()=>{store.close();rmSync(root,{recursive:true,force:true});});
  const add=(n,overrides={})=>{
    const id=store.observe(person,'fixture','fixture','gpt-6.1-sol');store.finish(id,{input:n,output:2,cached:Math.min(n,1)});
    store.run('UPDATE requests SET started=?,ended=? WHERE id=?',at-1000,at,id);
    for(const [key,value] of Object.entries(overrides)){assert.ok(['started','ended','status','usage_unknown','model','manual_cost_atoms','price_version'].includes(key));store.run('UPDATE requests SET '+key+'=? WHERE id=?',value,id);}
    return id;
  };
  return {store,filename,at,member,person,add};
}
const query=values=>requestsQuery(new URLSearchParams(values));

test('list view reads only the requested page and does not run a window quote or summary',t=>{
  const {store,at,add}=fixture(t);for(let i=0;i<130;i++)add(i);
  const all=store.all.bind(store),reads=[];store.all=(sql,...args)=>{const rows=all(sql,...args);if(sql.includes('FROM requests'))reads.push({sql,n:rows.length});return rows;};
  store.pricing.rawQuote=()=>{throw Error('list must not aggregate');};
  const result=requestsReader(store,query({view:'list',page:'3'}),at);
  assert.equal(result.rows.length,25);assert.equal(result.pagination.total,130);assert.equal(result.pagination.page,3);assert.equal(result.summary,undefined);
  assert.equal(reads.length,1);assert.equal(reads[0].n,25);assert.match(reads[0].sql,/LIMIT \? OFFSET \?/);
});

test('cursor handles tied times and late/backdated inserts including SQLite rowid reuse',t=>{
  const {store,at,add}=fixture(t),ids=Array.from({length:77},(_,i)=>add(i));
  const first=requestsReader(store,query({view:'list'}),at),seen=first.rows.map(r=>r.id);
  const removed=ids.at(-1);store.run('DELETE FROM requests WHERE id=?',removed);
  const late=add(900,{started:at-2000});
  let cursor=first.pagination.nextCursor,page=2;
  while(cursor){const result=requestsReader(store,query({view:'list',page:String(page++),snapshot:first.snapshot,cursor}),at);seen.push(...result.rows.map(r=>r.id));cursor=result.pagination.nextCursor;}
  assert.ok(!seen.includes(late));assert.equal(seen.length,new Set(seen).size);
  for(const id of ids)if(id!==removed||first.rows.some(r=>r.id===id))assert.ok(seen.includes(id));
});

test('snapshots cannot cross filters, malformed cursors or Beijing calendar windows',t=>{
  const {store,at,add}=fixture(t);add(5);const first=requestsReader(store,query({view:'list'}),at);
  for(const values of [{snapshot:'bad'},{snapshot:first.snapshot,model:'different'},{snapshot:first.snapshot,cursor:'bad'}])assert.throws(()=>requestsReader(store,query({view:'list',...values}),at),{status:400});
  assert.throws(()=>requestsReader(store,query({view:'list',snapshot:first.snapshot}),usageWindow(at).end),{status:400});
  for(const raw of ['view=wrong','cursor=abc','snapshot=1&snapshot=2'])assert.throws(()=>requestsQuery(new URLSearchParams(raw)),{status:400});
});

test('R1 removing already-read pages cannot hide unread cursor results',t=>{
  const {store,at,add}=fixture(t);for(let i=0;i<100;i++)add(i);
  let result=requestsReader(store,query({view:'list'}),at),page=2;const snapshot=result.snapshot,read=new Set(result.rows.map(r=>r.id));
  for(const id of read)store.run('DELETE FROM requests WHERE id=?',id);
  const seen=[];
  while(result.pagination.nextCursor){
    result=requestsReader(store,query({view:'list',page:String(page),snapshot,cursor:result.pagination.nextCursor}),at);
    assert.equal(result.pagination.page,page++);seen.push(...result.rows.map(r=>r.id));
  }
  assert.equal(seen.length,75);assert.equal(new Set(seen).size,75);assert.equal(result.pagination.page,4);
  assert.ok(seen.every(id=>!read.has(id)));
});

test('single-pass aggregate exactly matches immutable pricing with manual, missing and pending records',t=>{
  const {store,at,add,person}=fixture(t);add(1000000);add(1000000,{model:'gpt-6-astra'});add(0,{status:'unknown',usage_unknown:1});add(0,{status:'reconciled',manual_cost_atoms:'12345678901234567'});add(5,{price_version:'missing'});
  const pending=store.observe(person,'fixture','fixture','gpt-6.1-sol');store.run('UPDATE requests SET started=? WHERE id=?',at,pending);
  const result=requestsReader(store,query({view:'summary'}),at),window=usageWindow(at),rows=store.all('SELECT * FROM requests WHERE COALESCE(ended,started)>=? AND COALESCE(ended,started)<?',window.start,window.end);
  assert.deepEqual(result.summary.standard,store.pricing.summary(rows,at));assert.equal(result.summary.requests,6);assert.equal(result.summary.unmetered,1);assert.equal(result.summary.unpriced,1);assert.equal(result.summary.active,1);
  assert.equal(result.byMember[0].standard.total_exact,result.summary.standard.total_exact);
});

test('read-only background statistics match foreground, invalidate on writes, and close cleanly',async t=>{
  const {store,filename,at,add}=fixture(t);for(let i=0;i<120;i++)add(i);
  const service=createQueryService(filename);t.after(()=>service.close());const q=query({view:'summary'});
  let yielded=false;const timer=setTimeout(()=>yielded=true,0);
  const actual=await service.statistics(q,at);clearTimeout(timer);assert.equal(yielded,true);assert.equal(service.pending(),0);
  assert.deepEqual(actual,requestsReader(store,q,at));
  const missing=add(3,{price_version:'missing-version',started:at-999});
  const listQuery=query({view:'list'}),backgroundList=await service.list(listQuery,at);
  assert.deepEqual(backgroundList,requestsReader(store,listQuery,at));assert.equal(backgroundList.rows.find(r=>r.id===missing).standard.state,'unpriced');assert.equal(backgroundList.rows.find(r=>r.id===missing).standard.total,null);
  add(1000000);const next=await service.statistics(q,at);assert.equal(next.summary.requests,122);assert.notEqual(actual.watermark,next.watermark);
  await service.close();await assert.rejects(service.statistics(q,at),{status:503});
  const full=createQueryService(filename,{maxPending:0});await assert.rejects(full.statistics(q,at),{status:503});await full.close();
});

test('real management API splits list and statistics while retaining authentication and legacy shape',async t=>{
  const root=mkdtempSync(join(tmpdir(),'harbor-query-api-')),app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-query-admin'}});
  await new Promise(r=>app.server.listen(0,'127.0.0.1',r));t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
  const member=app.store.createMember('API合成员工',null,null),person=app.store.authenticate(member.secret),id=app.store.observe(person,'fixture','fixture','gpt-6.1-sol');app.store.finish(id,{input:1000000});
  const base='http://127.0.0.1:'+app.server.address().port,headers={Authorization:'Bearer fixture-query-admin'};
  for(const view of ['list','summary'])assert.equal((await fetch(base+'/api/admin/requests?view='+view,{headers:{Authorization:'Bearer '+member.secret}})).status,401);
  const list=await (await fetch(base+'/api/admin/requests?view=list',{headers})).json();assert.equal(list.rows.length,1);assert.equal(list.summary,undefined);
  const stats=await (await fetch(base+'/api/admin/requests?view=summary&snapshot='+list.snapshot,{headers})).json();assert.equal(stats.summary.standard.total_exact,'2');assert.equal(stats.watermark,list.watermark);
  const legacy=await (await fetch(base+'/api/admin/requests',{headers})).json();assert.equal(legacy.rows[0].id,id);assert.equal(legacy.summary.requests,1);
  assert.equal((await fetch(base+'/health')).status,200);
});
