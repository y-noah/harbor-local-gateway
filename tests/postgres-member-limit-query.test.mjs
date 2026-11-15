import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {Store} from '../store.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createMemberLimits} from '../member-limits.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:20000};
const options=(database='postgres')=>{const value=new URL(url);value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:2};};

test('PG限额先按历史模型和规则窗口筛选，未知模型仍保守阻断且与SQLite一致',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_limit_query_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-limit-query-'));let store,legacy;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
  legacy=new Store(join(directory,'legacy.sqlite'),{encryptionKey:randomBytes(32)});const member=legacy.createMember('合成限额查询员工',null,null),at=Date.now(),sol='gpt-6.1-sol',astra='gpt-6-astra';
  for(const [table,id] of [['members',member.memberId],['keys',member.keyId]]){const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',id),names=Object.keys(row);await store.run('INSERT INTO harbor.'+table+'('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...names.map(k=>row[k]));}
  legacy.pricing.configure({expectedVersion:0,aliases:[{name:'historical-sol',target:sol,from:at-100000,to:null,source:'synthetic'}]});const active=legacy.pricing.current(),book=legacy.get('SELECT * FROM pricing_versions WHERE id=?',active);
  await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT DO NOTHING',book.id,book.created,book.book);await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active}));
  const limits=createPostgresMemberLimits(store,{clock:()=>at}),original=createMemberLimits(legacy,{clock:()=>at}),rules=[{model:sol,hours:24,limit_usd:'20.00'},{model:sol,hours:168,limit_usd:'40.00'}];await limits.save(member.memberId,{enabled:true,rules,expected_version:0});original.save(member.memberId,{enabled:true,rules,expected_version:0});
  const columns=new Set((await store.all("SELECT column_name FROM information_schema.columns WHERE table_schema='harbor' AND table_name='requests'")).map(r=>r.column_name));
  const sync=async id=>{const row=legacy.get('SELECT * FROM requests WHERE id=?',id),names=Object.keys(row).filter(k=>columns.has(k));await store.run('DELETE FROM harbor.requests WHERE id=$1',id);await store.run('INSERT INTO harbor.requests('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...names.map(k=>row[k]));await store.projectPrice(row,at);};
  const seed=async(model,ended,extra={})=>{const id=legacy.observe(legacy.authenticate(member.secret),'synthetic','synthetic',model);if(ended!==null)legacy.finish(id,{input:100,output:2,cached:0,usageKnown:true});const changes={started:at-1000,ended,...extra};legacy.run('UPDATE requests SET '+Object.keys(changes).map(k=>k+'=?').join(',')+' WHERE id=?',...Object.values(changes),id);await sync(id);return id;};
  for(let i=0;i<50;i++)await seed(astra,at);
  await seed(astra,null);await seed(sol,at);await seed('historical-sol',at);await seed(sol,null);await seed(sol,at-168*3600000);
  const unknown=await seed('unknown-model',at,{manual_cost_atoms:'1000000000000',status:'reconciled'});await seed('unknown-zero',at,{input:0,output:0,cached:0,price_version:'missing-synthetic'});
  let captured;const query=store.query.bind(store);store.query=(sql,args)=>{if(sql.startsWith('WITH rules AS'))captured={sql,args};return query(sql,args);};
  const actual=await limits.status(member.memberId,at);assert.deepEqual(actual,original.status(member.memberId,at));assert.ok(actual.usage.every(r=>r.unresolved===1));await assert.rejects(limits.check(member.memberId,sol,{at}),{code:'member_model_usage_unknown'});
  const plan=(await query('EXPLAIN (ANALYZE,FORMAT JSON) '+captured.sql,captured.args)).rows[0]['QUERY PLAN'][0],counts={};
  const visit=node=>{if(['CTE source','CTE relevant'].includes(node['Subplan Name']))counts[node['Subplan Name']]=node['Actual Rows'];for(const child of node.Plans??[])visit(child);};visit(plan.Plan);assert.ok(counts['CTE source']>50);assert.ok(counts['CTE relevant']<10);
  legacy.run("UPDATE requests SET manual_cost_atoms='0' WHERE id=?",unknown);await sync(unknown);assert.deepEqual(await limits.status(member.memberId,at),original.status(member.memberId,at));await limits.check(member.memberId,sol,{at});
 }finally{await store?.close();legacy?.close();await admin.query('DROP DATABASE IF EXISTS '+database);await admin.close();await rm(directory,{recursive:true,force:true});}
});
