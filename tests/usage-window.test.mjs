import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {Store,usageWindow} from '../store.mjs';

function fixture(t){const root=mkdtempSync(join(tmpdir(),'harbor-seven-test-')),file=join(root,'db.sqlite'),encryptionKey=randomBytes(32);let store=new Store(file,{encryptionKey});t.after(()=>{store.close();rmSync(root,{recursive:true,force:true});});return {get store(){return store;},restart(){store.close();store=new Store(file,{encryptionKey});return store;}};}
function record(s,m,ended,tokens,status='ok',missing=0){const r=s.observe(s.authenticate(m.secret),'fixture-ip','fixture-agent','fixture');s.run('UPDATE requests SET account_id=?,started=?,ended=?,status=?,input=?,output=?,cached=?,usage_unknown=? WHERE id=?','account',ended-10,ended,status,tokens,2,1,missing,r);return r;}
test('seven Beijing calendar days cross midnight and Monday without weekly resets',()=>{
 const sunday=Date.parse('2026-10-11T23:59:59+08:00'),w=usageWindow(sunday);
 assert.equal(w.start,Date.parse('2026-10-05T00:00:00+08:00'));assert.equal(w.end,Date.parse('2026-10-12T00:00:00+08:00'));
 assert.equal(usageWindow(w.end).start,w.start+86400000);assert.equal(usageWindow(w.end-1).start,w.start);
});
test('all aggregate scopes use the same seven days and physically remove expired token statistics',t=>{
 const f=fixture(t),s=f.store,m=s.createMember('Fixture',null,null),at=Date.now(),w=usageWindow(at);
 s.run("INSERT INTO accounts(id,name,home,state) VALUES('account','Fixture','unused','ready')");
 const old=record(s,m,w.start-1,9000),first=record(s,m,w.start,10),current=record(s,m,at,20,'unknown',1);
 record(s,m,w.end,8000);s.prunedStart=null;
 const snapshot=s.snapshot(at);
 assert.equal(snapshot.totals.tokens,34);assert.equal(snapshot.totals.requests,2);assert.equal(snapshot.totals.success,1);assert.equal(snapshot.totals.unmetered,1);
 for(const x of [snapshot.keys[0],snapshot.members[0]]){assert.equal(x.used,34);assert.equal(x.unmetered,1);assert.equal(x.ips,1);}
 assert.equal(snapshot.accounts[0].budget_used,34);assert.equal(snapshot.daily.reduce((n,d)=>n+d.requests,0),2);assert.equal(snapshot.daily.reduce((n,d)=>n+d.tokens,0),34);
 assert.equal(s.get('SELECT input+output+cached+usage_unknown n FROM requests WHERE id=?',old).n,0);
 assert.equal(snapshot.requests.length,2);assert.equal(s.usageFor('key_id',m.keyId,at).used,34);
 // The boundary-day row expires on the next Beijing day; other rows remain.
 s.snapshot(w.end);assert.equal(s.get('SELECT input+output n FROM requests WHERE id=?',first).n,0);assert.equal(s.get('SELECT input+output n FROM requests WHERE id=?',current).n,22);
});
test('encryption survives restart, old hashes backfill only after valid authentication, ciphertext is row-bound',t=>{
 const f=fixture(t),s=f.store,m=s.createMember('Fixture',null,null),other=s.createKey(m.memberId);
 const encrypted=s.get('SELECT secret_cipher FROM keys WHERE id=?',m.keyId).secret_cipher;
 assert.ok(encrypted);assert.ok(!encrypted.includes(m.secret));assert.equal(s.snapshot().keys.find(k=>k.id===m.keyId).full_key,m.secret);
 s.run('UPDATE keys SET secret_cipher=NULL WHERE id=?',m.keyId);assert.equal(s.snapshot().keys.find(k=>k.id===m.keyId).full_key,null);
 s.run('UPDATE keys SET enabled=0 WHERE id=?',m.keyId);assert.throws(()=>s.authenticate(m.secret),/停用/);assert.equal(s.get('SELECT secret_cipher FROM keys WHERE id=?',m.keyId).secret_cipher,null);
 s.run('UPDATE keys SET enabled=1 WHERE id=?',m.keyId);s.authenticate(m.secret);assert.equal(f.restart().snapshot().keys.find(k=>k.id===m.keyId).full_key,m.secret);
 f.store.run('UPDATE keys SET secret_cipher=? WHERE id=?',encrypted,other.keyId);assert.equal(f.store.snapshot().keys.find(k=>k.id===other.keyId).full_key,null);
 assert.ok(!JSON.stringify(f.store.snapshot()).includes('secret_cipher'));
});
test('known and missing usage remain distinct and repeated finish cannot double count',t=>{
 const f=fixture(t),s=f.store,m=s.createMember('Fixture',null,null),person=s.authenticate(m.secret);
 const a=s.observe(person,'fixture','fixture','fixture'),b=s.observe(person,'fixture','fixture','fixture');
 s.finish(a,{input:3,output:2,usageKnown:true});s.finish(b,{usageKnown:false});
 assert.equal(s.snapshot().keys[0].used,5);assert.equal(s.snapshot().keys[0].unmetered,1);assert.throws(()=>s.finish(a,{input:10}),/already settled/);
 assert.equal(f.restart().snapshot().keys[0].unmetered,1);
});
