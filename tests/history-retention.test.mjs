import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';import {tmpdir} from 'node:os';import {join} from 'node:path';
import {Store,usageWindow} from '../store.mjs';
import {historyReader,historyQuery,requestObservation} from '../observability.mjs';
import {createGateway} from '../server.mjs';
import {createBalancer} from '../balancing.mjs';
const query=values=>historyQuery(new URLSearchParams(values));
test('history windows clamp forged ranges, hide old direct IDs and retain all audit and batch rows',t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-history-window-')),s=new Store(join(root,'db.sqlite')),at=Date.now(),w=usageWindow(at);t.after(()=>{s.close();rmSync(root,{recursive:true,force:true});});
 s.run('DELETE FROM audit');for(const [id,time] of [['old',w.start-1],['start',w.start],['now',at],['future',w.end]]){s.run('INSERT INTO audit VALUES(?,?,?,?)',id,time,id+'.event','{}');s.run('INSERT INTO balance_runs VALUES(?,?,?,?,?)','2026-01-'+({old:'01',start:'02',now:'03',future:'04'})[id],time,time,'completed','{}');}
 const reader=historyReader(s,{clock:()=>at});
 for(const kind of ['audit','nightly']){
  assert.equal(reader.list(query({kind})).pagination.total,2);assert.equal(reader.list(query({kind,start:'0',end:String(w.end+86400000)})).pagination.total,2);
  assert.equal(reader.list(query({kind,start:'0',end:String(w.start)})).pagination.total,0);assert.equal(reader.list(query({kind,start:String(w.end)})).pagination.total,0);
  assert.equal(reader.list(query({kind,page:'999'})).rows.length,2);
 }
 assert.deepEqual(reader.list(query({})).actions,['now.event','start.event']);
 assert.throws(()=>reader.nightly('2026-01-01'),{status:404});assert.throws(()=>reader.nightly('2026-01-04'),{status:404});assert.equal(reader.nightly('2026-01-02').started,w.start);
 assert.equal(s.all('SELECT * FROM audit').length,4);assert.equal(s.all('SELECT * FROM balance_runs').length,4);
 const advanced=historyReader(s,{clock:()=>w.end});assert.equal(advanced.list(query({kind:'audit'})).rows.some(x=>x.id==='start'),false);
 s.run('DELETE FROM balance_runs WHERE started>=?',w.start);const balancer=createBalancer({store:s,clock:()=>at});assert.equal(balancer.status().lastRun,null);
});
test('archived raw usage, unknown flags, manual amounts and fixed price versions survive reads and restart',t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-retain-')),file=join(root,'db.sqlite');let s=new Store(file);t.after(()=>{s.close();rmSync(root,{recursive:true,force:true});});
 const m=s.createMember('synthetic',null,null),r=s.observe(s.authenticate(m.secret),'synthetic','fixture','gpt-6.1-sol');s.finish(r,{input:200,output:30,cached:50});
 const w=usageWindow(Date.now());s.run('UPDATE requests SET started=?,ended=?,usage_unknown=1,manual_cost_atoms=? WHERE id=?',w.start-2,w.start-1,'1234567890123',r);const original=s.get('SELECT * FROM requests WHERE id=?',r);
 s.snapshot();s.pruneUsage();assert.throws(()=>requestObservation(s,r),{status:404});assert.deepEqual(s.get('SELECT * FROM requests WHERE id=?',r),original);
 s.close();s=new Store(file);s.snapshot();assert.deepEqual(s.get('SELECT * FROM requests WHERE id=?',r),original);assert.ok(s.get('SELECT id FROM pricing_versions WHERE id=?',original.price_version));
});
test('HTTP detail, history, state and export cannot reveal archived requests or batches but SQLite retains them',async t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-retention-http-')),a=createGateway({dataDir:root+'/data',runtimeDir:root+'/runtime',config:{adminToken:'synthetic-admin',codexBinary:'unused'},rateReader:async()=>null});await new Promise(r=>a.server.listen(0,'127.0.0.1',r));t.after(async()=>{await a.close();rmSync(root,{recursive:true,force:true});});
 const base='http://127.0.0.1:'+a.server.address().port,headers={Authorization:'Bearer synthetic-admin'},w=usageWindow(Date.now());
 const m=a.store.createMember('synthetic',null,null),r=a.store.observe(a.store.authenticate(m.secret),'archived-ip-marker','fixture','archived-model-marker');a.store.finish(r,{input:777,output:8,cached:9});a.store.run('UPDATE requests SET started=?,ended=? WHERE id=?',w.start-2,w.start-1,r);a.store.run("INSERT INTO balance_runs VALUES('2020-01-01',?,?,'completed','{}')",w.start-2,w.start-1);a.store.run("INSERT INTO audit VALUES('archived-event',?,'archived.event','{}')",w.start-1);
 for(const path of ['/api/admin/requests/'+r,'/api/admin/history/nightly/2020-01-01'])assert.equal((await fetch(base+path,{headers})).status,404);
 for(const path of ['/api/admin/state','/api/admin/requests','/api/admin/export','/api/admin/history?start=0','/api/admin/history?kind=nightly&start=0','/api/admin/pricing']){const res=await fetch(base+path,{headers});assert.equal(res.status,200);assert.doesNotMatch(await res.text(),/archived-ip-marker|archived-model-marker|archived-event|2020-01-01/);}
 assert.equal(a.store.get('SELECT input FROM requests WHERE id=?',r).input,777);assert.equal(a.store.get('SELECT COUNT(*) n FROM balance_runs').n,1);
});
