import test from 'node:test';
import assert from 'node:assert/strict';
import {Store} from '../store.mjs';
import {stateQuery} from '../postgres-state-query.mjs';
import {requestsQuery} from '../observability.mjs';
import {typeStatisticsQuery} from '../postgres-type-statistics.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';
import {memberQuery} from '../postgres-member-query.mjs';

test('管理分页默认20，保留旧客户端显式页大小和原有上限',()=>{
 const cases=[[stateQuery,'session_page_size','sessionPageSize'],[requestsQuery,'page_size','pageSize'],[typeStatisticsQuery,'page_size','pageSize'],[recordingQuery,'limit','limit'],[memberQuery,'page_size','pageSize']];
 for(const [parse,param,field] of cases){
  assert.equal(parse(new URLSearchParams())[field],20,param);
  for(const size of [10,20,25,50,100])assert.equal(parse(new URLSearchParams({[param]:String(size)}))[field],size,param);
  for(const size of [0,101])assert.throws(()=>parse(new URLSearchParams({[param]:String(size)})),{status:400},param);
 }
});

test('兼容存储会话默认20条且末页正确，显式25仍保留',t=>{
 const store=new Store(':memory:');t.after(()=>store.close());const at=Date.now(),member=store.createMember('合成分页员工',null,null);store.run("INSERT INTO accounts(id,name,home,state) VALUES('synthetic-pagination-account','合成分页账号','unused','ready')");
 for(let i=0;i<21;i++)store.run("INSERT INTO sessions(id,member_id,account_id,protocol,created,updated) VALUES(?,?,'synthetic-pagination-account','responses',?,?)",'synthetic-session-'+i,member.memberId,at,at);
 const first=store.snapshot(at),last=store.snapshot(at,{sessionPage:2}),legacy=store.snapshot(at,{sessionPageSize:25});
 assert.equal(first.sessions.length,20);assert.equal(first.sessionsPagination.pageSize,20);assert.equal(first.sessionsPagination.totalPages,2);assert.equal(last.sessions.length,1);assert.equal(last.sessionsPagination.page,2);assert.equal(legacy.sessions.length,21);assert.equal(legacy.sessionsPagination.pageSize,25);assert.equal(legacy.sessionsPagination.totalPages,1);
});
