import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {mkdtemp,readdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {captureRequestContext} from '../recording-request-context.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需要独占私有PG':false},at=Date.parse('2026-11-09T12:00:00+08:00');
function options(name='postgres'){const value=new URL(url);value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true};}
async function fixture(run,configuration={}){
 const admin=new PostgresStore(options()),name='harbor_context_'+randomUUID().replaceAll('-','');let store;const key=randomBytes(32),clock={now:at};
 try{
  await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");store=new PostgresStore(options(name));await store.migrate();await store.initializePricing();
  // Deliberately explicit: core remains schema 22; this is not a production
  // migration, compatibility bridge or approval to activate this feature.
  await store.query(await readFile(new URL('../migrations/023-recording-context.sql',import.meta.url),'utf8'));
  const members=[];for(let i=0;i<2;i++){const member=randomUUID(),keyId=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成上下文员工'+i,at);await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$3,$4)',keyId,member,'synthetic-context-'+keyId,at);members.push({id:member,key_id:keyId});}
  const account=randomUUID();await store.run('INSERT INTO harbor.accounts(id,name,home) VALUES($1,$2,$1)',account,'合成账号');
  const sessions=[];for(const owner of [members[0],members[0],members[1]]){const id=randomUUID();await store.run('INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,$3,$4,$4)',id,owner.id,account,at);sessions.push(id);}
  const recordings=new RecordingStore(store,{encryptionKey:key,clock:()=>clock.now}),context=new RecordingContextStore(store,{encryptionKey:key,clock:()=>clock.now,...configuration});
  const request=async({member=0,session=sessions[member===0?0:2],expires=at+10000,start=true}={})=>{const id=await store.observe(members[member],'synthetic','synthetic','gpt-6.1-sol',{at:clock.now});await store.run('UPDATE harbor.requests SET session_id=$1 WHERE id=$2',session,id);if(start){await recordings.start(id);await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',expires,id);}return id;};
  const read=async(id,repository=context)=>{let after={component:0,part:0},parts=[],state;for(let guard=0;guard<1000;guard++){const page=await repository.read(id,{after,limit:2});state=page.state;parts.push(...page.parts);if(!page.hasMore)break;assert.notDeepEqual(page.next,after);after=page.next;}
   const groups=new Map();for(const part of parts){if(!groups.has(part.component))groups.set(part.component,{kind:part.kind,id:part.objectId,text:''});groups.get(part.component).text+=part.text;}return {state,components:[...groups.values()].map(value=>({kind:value.kind,objectId:value.id,content:JSON.parse(value.text)})),parts};};
  const gc=async()=>{let result;const total={snapshots:0,objects:0};for(let i=0;i<100;i++){result=await context.gc({limit:2});total.snapshots+=result.snapshots;total.objects+=result.objects;if(result.cycleComplete)return total;}throw Error('GC did not complete');};
  await run({store,name,key,clock,context,recordings,request,read,gc,members,sessions});
 }finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.close();key.fill(0);}
}
const components=[{kind:'instruction',content:{text:'合成系统说明'}},{kind:'tool_schema',content:{name:'synthetic_tool',parameters:{type:'object',properties:{path:{type:'string'}}}}},{kind:'history',content:{role:'user',text:'合成历史问题'}}];

test('真实PG：请求上下文过滤后加密持久化，重启读回顺序、重复问题及附件元数据',integration,()=>fixture(async({store,key,context,request,read,clock})=>{
 const payload={instructions:'合成说明',tools:[{type:'function',name:'read',parameters:{type:'object'}}],input:[{role:'user',content:[{type:'input_text',text:'Authorization: Bearer '},{type:'input_text',text:'synthetic-context-secret'}]},{role:'user',content:'重复提问'},{role:'user',content:'重复提问'},{role:'user',content:[{type:'input_file',filename:'synthetic.txt',file_data:'synthetic-private-binary'}]}]};
 const captured=captureRequestContext(payload);assert.equal(captured.state,'complete');
 const first=await request(),second=await request();await context.writeSnapshot(first,captured.components);await context.writeSnapshot(second,captured.components);
 const restarted=new RecordingContextStore(store,{encryptionKey:key,clock:()=>clock.now}),restored=await read(second,restarted);
 assert.deepEqual(restored.components.map(({kind,content})=>({kind,content})),captured.components);
 const serialized=JSON.stringify(restored);assert.ok(!serialized.includes('synthetic-context-secret'));assert.ok(!serialized.includes('synthetic-private-binary'));assert.ok(serialized.includes('synthetic.txt'));
 assert.equal(restored.components.filter(row=>row.content.value?.content==='重复提问').length,2);
 assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,captured.components.length);
 for(const row of await store.all('SELECT content_cipher FROM harbor.recording_context_parts'))assert.ok(!row.content_cipher.includes(Buffer.from('重复提问')));
}));

test('真实PG：采集不完整事实进入不可变快照，成功写入不提升为完整',integration,()=>fixture(async({context,request,read})=>{
 const view=captureRequestContext({instructions:'可见内容',input:[{type:'future_item'}]}),coverage={state:view.state,reasons:view.reasons,opaqueReasoningExcluded:view.opaqueReasoningExcluded},id=await request();
 const receipt=await context.writeSnapshot(id,view.components,{coverage});assert.equal(receipt.state,'partial');assert.equal(receipt.reason,'capture_incomplete');assert.deepEqual(receipt.coverage,coverage);
 assert.deepEqual((await read(id)).state,'partial');assert.deepEqual((await context.read(id)).coverage,coverage);
 assert.deepEqual(await context.writeSnapshot(id,view.components,{coverage}),receipt);
 await assert.rejects(context.writeSnapshot(id,view.components),{code:'recording_context_conflict'});
 const clipped=captureRequestContext({input:'中文'.repeat(1000)},{maxBytes:256}),clippedCoverage={state:clipped.state,reasons:clipped.reasons,opaqueReasoningExcluded:clipped.opaqueReasoningExcluded};
 assert.equal((await context.writeSnapshot(await request(),clipped.components,{coverage:clippedCoverage})).state,'truncated');
 for(const invalid of [{state:'complete',reasons:['unsupported_input'],opaqueReasoningExcluded:0},{state:'incomplete',reasons:[],opaqueReasoningExcluded:0},{state:'incomplete',reasons:['arbitrary-private-text'],opaqueReasoningExcluded:0},{state:'complete',reasons:[],opaqueReasoningExcluded:-1}])await assert.rejects(context.writeSnapshot(await request(),[],{coverage:invalid}),{status:400});
}));

test('真实PG：上下文共用加密持久缓冲，丢提交确认后重启幂等恢复且schema未丢失',integration,()=>fixture(async({store,key,context,request,read})=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-context-buffer-')),repository=new RecordingStore(store,{encryptionKey:key,contextRepository:context});let buffer,restored;
 try{
  assert.throws(()=>new RecordingStore(store,{encryptionKey:key,contextRepository:{store:{},writeSnapshot(){}}}),{code:'recording_configuration'});
  const id=await request({start:false}),view=captureRequestContext({instructions:'合成基线',tools:[{type:'function',name:'f',parameters:{type:'object',properties:{api_key:{type:'string',default:'synthetic-private-default'}}}}],input:'合成问题'}),coverage={state:view.state,reasons:view.reasons,opaqueReasoningExcluded:view.opaqueReasoningExcluded};
  let loseAck=true;buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicCommands:async commands=>{const receipt=await repository.writeAtomicCommands(commands);if(loseAck){loseAck=false;throw Error('synthetic lost commit acknowledgement');}return receipt;}});
  assert.equal(buffer.start(id,'known'),true);assert.equal(buffer.contextSnapshot(id,view.components,coverage),true);assert.equal(buffer.finish(id,{state:'complete'}),true);
  await buffer.flush();assert.equal(buffer.status().files,3);assert.equal(buffer.status().queued,0);assert.equal((await context.read(id)).state,'complete');
  for(const name of await readdir(directory)){const cipher=await readFile(join(directory,name));for(const text of ['合成基线','synthetic-private-default','api_key'])assert.ok(!cipher.includes(Buffer.from(text)));}
  const writeContext=repository.contextSnapshot.bind(repository);repository.contextSnapshot=async()=>{throw Error('synthetic context outage until restart');};
  await buffer.close();assert.ok(buffer.status().files>=2);buffer=null;repository.contextSnapshot=writeContext;
  restored=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicCommands:commands=>repository.writeAtomicCommands(commands)});await restored.flush();assert.equal(restored.status().files,0);assert.deepEqual(await readdir(directory),[]);
  const result=await read(id);assert.deepEqual(result.components.map(({kind,content})=>({kind,content})),view.components);assert.equal(result.components[1].content.value.parameters.properties.api_key.type,'string');assert.ok(!JSON.stringify(result).includes('synthetic-private-default'));
  const offRepository=new RecordingStore(store,{encryptionKey:key}),off=await RecordingBuffer.create(offRepository,{directory,flushMs:10000});try{assert.equal(off.contextSnapshot(id,[],coverage),false);assert.equal(off.status().queued,0);}finally{await off.close();}
 }finally{await buffer?.close();await restored?.close();await rm(directory,{recursive:true,force:true});}
}));

test('真实PG：首次基线在提交前故障时保留于加密spool，重启后才标记已持久化',integration,()=>fixture(async({store,key,context,request,read})=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-context-first-baseline-')),repository=new RecordingStore(store,{encryptionKey:key,contextRepository:context}),writeContext=repository.contextSnapshot.bind(repository);let buffer,restored;
 try{
  const id=await request({start:false}),view=captureRequestContext({instructions:'首次唯一基线',input:'合成提问'}),coverage={state:view.state,reasons:view.reasons,opaqueReasoningExcluded:view.opaqueReasoningExcluded};
  repository.contextSnapshot=async()=>{throw Error('synthetic pre-commit context outage');};
  buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicCommands:commands=>repository.writeAtomicCommands(commands)});buffer.start(id,'known');buffer.contextSnapshot(id,view.components,coverage);buffer.finish(id,{state:'complete'});await buffer.flush();
  assert.equal(await store.get('SELECT request_id FROM harbor.recording_context_snapshots WHERE request_id=$1',id),undefined);assert.equal(await store.get('SELECT request_id FROM harbor.recordings WHERE request_id=$1',id),undefined,'根事务中的start必须一起回滚');assert.equal(buffer.status().files,3);
  await buffer.close();assert.ok(buffer.status().files>=2);buffer=null;repository.contextSnapshot=writeContext;
  restored=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicCommands:commands=>repository.writeAtomicCommands(commands)});await restored.flush();assert.equal(restored.status().files,0);assert.deepEqual((await read(id)).components.map(({kind,content})=>({kind,content})),view.components);assert.equal((await store.get('SELECT recording_state FROM harbor.requests WHERE id=$1',id)).recording_state,'complete');
 }finally{await buffer?.close();await restored?.close();await rm(directory,{recursive:true,force:true});}
}));

test('真实PG：恢复时来源已过期只确认丢弃上下文命令，不复活正文或阻塞FIFO',integration,()=>fixture(async({store,key,context,request,clock})=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-context-expired-buffer-')),repository=new RecordingStore(store,{encryptionKey:key,contextRepository:context,clock:()=>clock.now});let buffer;
 try{
  const id=await request({expires:clock.now+10}),view=captureRequestContext({input:'到期前收到的合成内容'}),coverage={state:view.state,reasons:view.reasons,opaqueReasoningExcluded:view.opaqueReasoningExcluded};
  buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicCommands:commands=>repository.writeAtomicCommands(commands)});assert.equal(buffer.contextSnapshot(id,view.components,coverage),true);clock.now+=100;await buffer.flush();assert.equal(buffer.status().queued,0);assert.equal(buffer.status().files,0);assert.equal(buffer.status().failures,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.recording_context_snapshots WHERE request_id=$1',id)).n,0);assert.equal((await context.read(id)).state,'expired');
 }finally{await buffer?.close();await rm(directory,{recursive:true,force:true});}
}));

test('真实PG：显式独立DDL保留核心22；首次写入读取、跨请求复用、重复出现和重启',integration,()=>fixture(async({store,key,context,request,read,clock})=>{
 assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.schema_migrations')).count,22);assert.equal(await store.verifySchema(),true);
 const first=await request(),second=await request();assert.equal((await context.writeSnapshot(first,[...components,components[0]])).state,'complete');
 const initial=await read(first);assert.deepEqual(initial.components.map(({kind,content})=>({kind,content})),[...components,components[0]]);assert.equal(initial.components[0].objectId,initial.components[3].objectId);
 await context.writeSnapshot(second,components);assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,3);
 const restarted=new RecordingContextStore(store,{encryptionKey:key,clock:()=>clock.now});assert.deepEqual((await read(second,restarted)).components,initial.components.slice(0,3));
 assert.deepEqual(await context.writeSnapshot(first,[...components,components[0]]),await restarted.writeSnapshot(first,[...components,components[0]]));
 await assert.rejects(context.writeSnapshot(first,components),{code:'recording_context_conflict'});
}));

test('真实PG：作用域只来自请求事实，不跨员工、不同真实会话或无会话请求去重',integration,()=>fixture(async({store,context,request,read,sessions})=>{
 const ids=[await request(),await request({session:sessions[1]}),await request({member:1}),await request({session:null}),await request({session:null})];
 for(const id of ids)await context.writeSnapshot(id,[components[0]]);
 assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,5);assert.equal((await store.get('SELECT COUNT(DISTINCT fingerprint) count FROM harbor.recording_context_objects')).count,5);
 const objectIds=[];for(const id of ids)objectIds.push((await read(id)).components[0].objectId);assert.equal(new Set(objectIds).size,5);
 await assert.rejects(context.writeSnapshot(await request(),[{...components[0],member_id:'forged'}]),{status:400});
 const forged=await request({session:sessions[2]});await assert.rejects(context.writeSnapshot(forged,components),{code:'recording_context_scope'});
 await assert.rejects(context.writeSnapshot(await request({start:false}),components),{code:'recording_context_missing'});
}));

test('真实PG：脱敏发生在分块前，UTF8有界分页、顺序和加密落库正确',integration,()=>fixture(async({store,context,request,read})=>{
 const id=await request(),text='中文🙂'.repeat(500)+'\nAuthorization: Bearer synthetic-access-value\n'+('hg_'+'z'.repeat(40));
 await context.writeSnapshot(id,[{kind:'history',content:{text,secret:'synthetic-secret',encrypted_content:'opaque'}}]);
 const restored=await read(id);assert.ok(restored.parts.length>1);assert.ok(restored.parts.every(part=>Buffer.byteLength(part.text)<=256));assert.ok(!restored.components[0].content.text.includes('synthetic-access-value'));assert.ok(!restored.components[0].content.text.includes('hg_'));assert.ok(!JSON.stringify(restored.components[0].content).includes('synthetic-secret'));assert.ok(!restored.components[0].content.text.includes('�'));
 const rows=await store.all('SELECT content_cipher FROM harbor.recording_context_parts');for(const row of rows){assert.ok(!row.content_cipher.includes(Buffer.from('synthetic-access-value')));assert.ok(!row.content_cipher.includes(Buffer.from('中文')));}
},{partBytes:256,maxReadBytes:512}));

test('真实PG：注入半写故障整体回滚；残缺对象不能复用或假complete',integration,()=>fixture(async({store,context,request,read})=>{
 const id=await request(),run=store.run.bind(store);let calls=0;store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.recording_context_parts')&&++calls===2)throw Error('synthetic chunk failure');return run(sql,...args);};
 const input=[{kind:'history',content:{text:'分块🙂'.repeat(200)}}];await assert.rejects(context.writeSnapshot(id,input),/synthetic chunk failure/);store.run=run;
 assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,0);assert.equal((await context.read(id)).state,'unavailable');
 await context.writeSnapshot(id,input);const object=(await read(id)).components[0].objectId;await store.run('DELETE FROM harbor.recording_context_parts WHERE object_id=$1 AND ordinal=2',object);
 await assert.rejects(context.read(id),{code:'recording_context_incomplete'});await assert.rejects(context.writeSnapshot(await request(),input),{code:'recording_context_incomplete'});
},{partBytes:256}));

test('真实PG：旧请求过期不延寿，后续引用仍可读；过期/删除来源后GC回收对象',integration,()=>fixture(async({store,context,clock,request,read,gc})=>{
 const old=await request({expires:at+100}),recent=await request({expires:at+1000});await context.writeSnapshot(old,components);await context.writeSnapshot(recent,components);clock.now=at+200;
 assert.equal((await context.read(old)).state,'expired');assert.equal((await read(recent)).state,'complete');assert.equal((await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',old)).expires_at,at+100);
 const first=await gc();assert.equal(first.snapshots,1);assert.equal(first.objects,0);assert.equal((await read(recent)).components.length,3);
 await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',at+150,recent);assert.equal((await context.read(recent)).state,'expired');
 const final=await gc();assert.equal(final.snapshots,1);assert.equal(final.objects,3);assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_parts')).count,0);
 await assert.rejects(context.writeSnapshot(old,components),{code:'recording_context_expired'});
}));

test('真实PG：并发同作用域只持久化一个对象，独立出现顺序不丢',integration,()=>fixture(async({store,key,context,clock,request,read})=>{
 const other=new PostgresStore(options((await store.get('SELECT current_database() name')).name)),repository=new RecordingContextStore(other,{encryptionKey:key,clock:()=>clock.now});
 try{const a=await request(),b=await request();await Promise.all([context.writeSnapshot(a,[components[0],components[0]]),repository.writeSnapshot(b,[components[0]])]);assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,1);assert.equal((await read(a)).components.length,2);assert.equal((await read(b)).components.length,1);}finally{await other.close();}
}));

test('真实PG：加密AAD拒绝换块和错误密钥；调用方不能改变已存作用域',integration,()=>fixture(async({store,key,context,clock,request,read})=>{
 const id=await request();await context.writeSnapshot(id,components);const objects=(await read(id)).components;
 const first=await store.get('SELECT content_cipher FROM harbor.recording_context_parts WHERE object_id=$1 AND ordinal=1',objects[0].objectId);await store.run('UPDATE harbor.recording_context_parts SET content_cipher=$1 WHERE object_id=$2 AND ordinal=1',first.content_cipher,objects[1].objectId);
 await assert.rejects(read(id),{code:'recording_context_unreadable'});
 const wrong=new RecordingContextStore(store,{encryptionKey:randomBytes(32),clock:()=>clock.now});await assert.rejects(wrong.read(id),{code:'recording_context_scope'});
 await store.run('UPDATE harbor.requests SET session_id=NULL WHERE id=$1',id);await assert.rejects(context.read(id),{code:'recording_context_scope'});
}));

test('真实PG：显式容量/数量截断，空快照不假缺失且可GC',integration,()=>fixture(async({store,context,request,clock,gc})=>{
 const id=await request();const receipt=await context.writeSnapshot(id,[...components,...components]);assert.equal(receipt.state,'truncated');assert.equal(receipt.reason,'size_limit');assert.equal(receipt.seen,6);assert.equal(receipt.components,2);
 const empty=await request();assert.equal((await context.writeSnapshot(empty,[])).state,'complete');assert.equal((await context.read(empty)).state,'complete');
 clock.now=at+20000;await gc();assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_snapshots')).count,0);
},{maxComponents:2}));


test('真实PG：字节限额保留可用前缀并明确截断，不伪称完整或丢成空快照',integration,()=>fixture(async({context,request,read})=>{
 const id=await request(),result=await context.writeSnapshot(id,[{kind:'history',content:{text:'合成可见正文'.repeat(1000)}}]);
 assert.equal(result.state,'truncated');assert.ok(result.bytes<=256);assert.equal(result.components,1);
 const restored=await read(id);assert.equal(restored.components.length,1);assert.ok(restored.components[0].content.text.startsWith('合成可见正文'));assert.equal(restored.parts[0].truncated,true);
},{maxSnapshotBytes:256,partBytes:256,maxReadBytes:512}));

test('真实PG：GC与新增引用串行，回收旧来源不破坏并发新引用',integration,()=>fixture(async({store,key,context,clock,request,read})=>{
 const old=await request({expires:at+10}),recent=await request({expires:at+1000});await context.writeSnapshot(old,components);clock.now=at+20;
 const other=new PostgresStore(options((await store.get('SELECT current_database() name')).name)),writer=new RecordingContextStore(other,{encryptionKey:key,clock:()=>clock.now});
 try{await Promise.all([context.gc(),writer.writeSnapshot(recent,components)]);assert.equal((await read(recent)).components.length,3);assert.equal((await context.read(old)).state,'expired');assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,3);}finally{await other.close();}
}));

test('真实PG：有限GC轮询跳过活作用域，清除较后作用域及删除来源遗留',integration,()=>fixture(async({store,context,clock,request,sessions,gc})=>{
 const live=await request(),expired=await request({session:sessions[1],expires:at+10}),removed=await request({member:1});
 for(const id of [live,expired,removed])await context.writeSnapshot(id,[components[0]]);await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',removed);clock.now=at+20;
 await gc();assert.equal((await store.get('SELECT COUNT(*) count FROM harbor.recording_context_objects')).count,1);assert.equal((await context.read(live)).state,'complete');
}));


test('真实PG：大量不同组件的分页验证只需固定数量查询，空尾页不逐对象查询',integration,()=>fixture(async({store,context,request})=>{
 const id=await request();await context.writeSnapshot(id,Array.from({length:200},(_,index)=>({kind:'history',content:{text:'合成不同消息'+index}})));
 const query=store.query.bind(store);let calls=0;store.query=(...args)=>{calls++;return query(...args);};
 try{assert.equal((await context.read(id,{limit:2})).parts.length,2);assert.ok(calls<=5,'分页查询次数必须固定有界');calls=0;assert.equal((await context.read(id,{after:{component:1000,part:0},limit:2})).parts.length,0);assert.ok(calls<=5,'空尾页不能逐对象验证');}finally{store.query=query;}
}));

test('真实PG：大组件分页仅提取预算内密文，不预读全部正文再截取',integration,()=>fixture(async({store,context,request,read})=>{
 const id=await request();await context.writeSnapshot(id,[{kind:'tool_schema',content:{text:'合成大块说明'.repeat(1000)}}]);
 const all=store.all.bind(store);let fetched=0;store.all=async(sql,...args)=>{const rows=await all(sql,...args);for(const row of rows)if(Buffer.isBuffer(row.content_cipher))fetched+=row.content_cipher.length;return rows;};
 try{const page=await context.read(id,{limit:100});assert.ok(page.hasMore);assert.ok(fetched<=1024);assert.ok(page.parts.length>0);const expected=await read(id);assert.equal(expected.components[0].content.text,'合成大块说明'.repeat(1000));}finally{store.all=all;}
},{partBytes:256,maxReadBytes:1024}));
