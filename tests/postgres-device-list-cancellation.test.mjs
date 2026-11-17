import test from 'node:test';
import assert from 'node:assert/strict';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {withLegacyDepartmentSchema} from './department-legacy-query-fixture.mjs';

function gate(){let resolve;return {promise:new Promise(done=>resolve=done),resolve:()=>resolve()};}
function fixture(blocked,{queued=true}={}){
 const entered=gate(),release=gate(),events=[];let memberReads=0;
 const step=async(name,value)=>{events.push(name);if(name===blocked){entered.resolve();await release.promise;}return value;};
 const store=withLegacyDepartmentSchema({
  transaction:async (fn,options)=>{assert.deepEqual(options,{level:'REPEATABLE READ',readOnly:true});events.push('begin');try{const value=await fn();events.push('commit');return value;}catch(error){events.push('rollback');throw error;}},
  get:async sql=>{
   if(sql.startsWith('SELECT COUNT(*) n FROM harbor.employee_devices'))return step('count',{n:1});
   if(sql.startsWith('SELECT COUNT(DISTINCT r.device_id)'))return step('summary',{used_devices:1,unverified_requests:0});
   if(sql.startsWith('SELECT member_id,device_ids FROM harbor.keys'))return step('key',{member_id:'m',device_ids:null});
   if(sql.startsWith('SELECT id,enabled,device_mode,device_policy_version'))return step(++memberReads===1?'owner':'key-owner',{id:'m',enabled:1,device_mode:'compatible',device_policy_version:0});
   throw Error('unexpected get '+sql);
  },
  all:async sql=>{assert.match(sql,/SELECT d\.\*,m\.name member_name/);return step('rows',[{id:'device',member_id:'m',name:'合成设备',state:'approved',member_name:'合成员工',request_count:1}]);}
 });
 const queue=queued?createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}):null,service=createPostgresDeviceAccess(store,{queryQueue:queue??undefined});
 return {entered,release,events,queue,service};
}

test('设备列表计数执行中取消，不继续行查询和汇总且保留真实队列槽位',{timeout:3000},async()=>{
 const f=fixture('count'),controller=new AbortController(),reason=Error('synthetic dialog closed');let settled=false;
 const read=f.service.list(new URLSearchParams(),{signal:controller.signal}),observed=read.then(value=>{settled=true;return {value};},error=>{settled=true;return {error};});
 try{await f.entered.promise;controller.abort(reason);assert.equal(settled,false);assert.equal(f.queue.status().active,1);f.release.resolve();const result=await observed;assert.equal(result.error,reason,JSON.stringify(f.events));assert.deepEqual(f.events,['begin','count','rollback']);assert.equal(f.queue.status().active,0);}
 finally{f.release.resolve();await observed;await f.queue.close();}
});

test('设备列表各已入场阶段结束后检查取消，null原因按既有499取消语义返回',{timeout:3000},async t=>{
 const sequence=['owner','key','key-owner','count','rows','summary'];
 for(const blocked of sequence)await t.test(blocked,async()=>{
  const f=fixture(blocked),controller=new AbortController(),reason=blocked==='summary'?null:Error('synthetic cancel '+blocked);let settled=false;
  const work=f.service.list(new URLSearchParams({member_id:'m',key_id:'k'}),{signal:controller.signal}),observed=work.then(value=>{settled=true;return {value};},error=>{settled=true;return {error};});
  try{await f.entered.promise;controller.abort(reason);assert.equal(settled,false);assert.equal(f.queue.status().active,1);f.release.resolve();const result=await observed;assert.equal(Object.hasOwn(result,'error'),true);if(reason===null){assert.equal(result.error.status,499);assert.equal(result.error.code,'request_cancelled');}else assert.equal(result.error,reason);assert.deepEqual(f.events,['begin',...sequence.slice(0,sequence.indexOf(blocked)+1),'rollback']);assert.equal(f.queue.status().active,0);}
  finally{f.release.resolve();await observed;await f.queue.close();}
 });
});

test('无队列设备读取也在阶段边界取消，未取消读取保留完整结果和只读事务',{timeout:3000},async()=>{
 const f=fixture('rows',{queued:false}),controller=new AbortController(),reason=Error('synthetic no queue');
 const observed=f.service.list(new URLSearchParams(),{signal:controller.signal}).then(value=>({value}),error=>({error}));
 await f.entered.promise;controller.abort(reason);f.release.resolve();assert.equal((await observed).error,reason);assert.deepEqual(f.events,['begin','count','rows','rollback']);
 const success=fixture(null),result=await success.service.list(new URLSearchParams());assert.equal(result.rows.length,1);assert.equal(result.summary.registered_devices,1);assert.equal(result.summary.used_devices,1);assert.deepEqual(success.events,['begin','count','rows','summary','commit']);await success.queue.close();
});

test('设备列表预取消或排队时取消不执行列表SQL，其他请求仍正常完成',{timeout:3000},async()=>{
 const f=fixture('count'),pre=new AbortController();pre.abort();await assert.rejects(f.service.list(new URLSearchParams(),{signal:pre.signal}),{name:'AbortError'});assert.deepEqual(f.events,[]);
 const first=f.service.list(new URLSearchParams());await f.entered.promise;const controller=new AbortController(),reason=Error('synthetic queued close'),cancelled=assert.rejects(f.service.list(new URLSearchParams(),{signal:controller.signal}),error=>error===reason);controller.abort(reason);await cancelled;assert.deepEqual(f.events,['begin','count']);assert.equal(f.queue.status().active,1);f.release.resolve();assert.equal((await first).rows.length,1);await f.queue.close();
});

test('启用部门后在列表权限读取期间取消，不再启动员工或后续SQL',{skip:!process.env.HARBOR_TEST_DATABASE_URL?'需独立真实PG':false,timeout:30000},async()=>{
 const {randomUUID}=await import('node:crypto'),{PostgresStore}=await import('../postgres-store.mjs'),{migrateDepartmentIsolation,createPostgresDepartments}=await import('../postgres-departments.mjs'),{createDepartmentScopeIssuer}=await import('../department-access.mjs');
 const name='harbor_device_cancel_'+randomUUID().replaceAll('-',''),url=new URL(process.env.HARBOR_TEST_DATABASE_URL),options=database=>{const u=new URL(url);if(database)u.pathname='/'+database;return {connectionString:u.href,poolSize:2,allowPlaintextLocalNetwork:true};},admin=new PostgresStore(options());let store,created=false;
 try{
  await admin.query('CREATE DATABASE '+name);created=true;store=new PostgresStore(options(name));await store.migrate();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),departments=createPostgresDepartments(store,{issuer}),root=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),department=await departments.createDepartment(root,{name:'合成取消验证部门'}),issued=await departments.issueManagementKey(root,department.id,{label:'合成权限'}),scope=await departments.authenticateManagementKey(issued.secret);
  await store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES('m','合成员工',0,$1)",department.id);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES('k','m','synthetic-digest','synthetic',0,$1)",department.id);
  const originalGet=store.get.bind(store),originalAll=store.all.bind(store);
  for(const stage of [1,2,3]){
   const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}),entered=gate(),release=gate(),controller=new AbortController(),reason=Error('synthetic permission cancel '+stage);let permissions=0,memberReads=0,keyReads=0,countReads=0,rowReads=0,settled=false;
   store.get=async(sql,...args)=>{if(sql.startsWith('SELECT id,enabled,device_mode,device_policy_version'))memberReads++;if(sql.startsWith('SELECT member_id,device_ids'))keyReads++;if(sql.startsWith('SELECT COUNT(*) n FROM harbor.employee_devices'))countReads++;const value=await originalGet(sql,...args);if(queue.status().active&&sql.startsWith('SELECT d.id FROM harbor.departments d JOIN harbor.department_admin_keys')&&++permissions===stage){entered.resolve();await release.promise;}return value;};
   store.all=async(sql,...args)=>{if(sql.startsWith('SELECT d.*,m.name member_name'))rowReads++;return originalAll(sql,...args);};
   const service=createPostgresDeviceAccess(store,{queryQueue:queue}),work=service.list(new URLSearchParams({member_id:'m',key_id:'k'}),{scope,signal:controller.signal}),observed=work.then(value=>{settled=true;return {value};},error=>{settled=true;return {error};});
   try{await entered.promise;controller.abort(reason);assert.equal(settled,false);assert.equal(queue.status().active,1);const before={memberReads,keyReads,countReads,rowReads};release.resolve();const result=await observed;assert.equal(result.error,reason);assert.deepEqual({memberReads,keyReads,countReads,rowReads},before);assert.equal(memberReads,stage===3?1:0);assert.equal(countReads,0);assert.equal(rowReads,0);assert.equal(queue.status().active,0);assert.equal((await originalGet('SELECT 1 n')).n,1);}
   finally{release.resolve();await observed;await queue.close();store.get=originalGet;store.all=originalAll;}
  }
 }finally{await store?.close();if(created)await admin.query('DROP DATABASE '+name);await admin.close();}
});
