import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {hash} from '../store.mjs';
import {passwordHash} from '../auth.mjs';
const configuration={skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需独占PG18':false,timeout:60000};
async function fixture(t){
 const lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB}),root=await mkdtemp(join(tmpdir(),'harbor-context-bridge-')),store=new PostgresStore({connection:lab.connection});let app;
 t.after(async()=>{await app?.close();await store.close();await lab.close();await rm(root,{recursive:true,force:true});});
 const open=async()=>{const probe=async()=>[{total:1000000000000,free:900000000000}];probe.status=()=>({source:'controlled_test_measurement'});app=await createPostgresGateway({database:{connection:lab.connection},dataDir:root,runtimeDir:join(root,'runtime'),env:{},config:{port:43127,codexBinary:'synthetic',adminPasswordHash:passwordHash('synthetic')},rateReader:async()=>{throw Error('synthetic unavailable');},recording:{enabled:true,storage:{probe}},pricing:{fetchImpl:async()=>{throw Error('external access forbidden in fixture');}}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));return 'http://127.0.0.1:'+app.server.address().port;};
 const close=async()=>{await app.close();app=undefined;};
 return {lab,root,store,open,close};
}
test('兼容桥接默认保留22，显式启用23后原Key和核心网关仍可启动且上下文未丢失',configuration,async t=>{
 const f=await fixture(t);assert.deepEqual(await f.store.migrate(),{version:22,changed:true});await f.store.initializePricing();assert.equal(await f.store.verifySchema(),true);
 assert.equal((await f.store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name,null);
 const member=randomUUID(),key=randomUUID(),secret='hg_synthetic_schema_bridge_'+randomUUID(),at=Date.now();
 await f.store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成桥接员工',at);
 await f.store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,$5)',key,member,hash(secret),'hg_synthetic',at);
 let origin=await f.open();assert.equal((await fetch(origin+'/ready')).status,200);assert.equal((await fetch(origin+'/api/me',{headers:{authorization:'Bearer '+secret}})).status,200);await f.close();
 assert.deepEqual(await f.store.migrate(),{version:22,changed:false});assert.deepEqual(await f.store.migrate({recordingContext:true}),{version:23,changed:true});assert.equal(await f.store.verifySchema(),true);
 const recordKey=await readFile(join(f.root,'recording-encryption.key')),recordings=new RecordingStore(f.store,{encryptionKey:recordKey}),contexts=new RecordingContextStore(f.store,{encryptionKey:recordKey});
 const request=await f.store.observe({id:member,key_id:key},'synthetic','synthetic','gpt-6.1-sol');await f.store.finish(request,{input:10,output:2,cached:0});await recordings.start(request);
 await contexts.writeSnapshot(request,[{kind:'instruction',content:'合成不可丢失的可见指令'}]);const before=await f.store.get('SELECT used FROM harbor.members WHERE id=$1',member);
 origin=await f.open();assert.equal((await fetch(origin+'/ready')).status,200);assert.equal((await fetch(origin+'/api/me',{headers:{authorization:'Bearer '+secret}})).status,200);await f.close();
 assert.deepEqual(await f.store.get('SELECT used FROM harbor.members WHERE id=$1',member),before);assert.deepEqual(await f.store.migrate(),{version:23,changed:false});
 const result=await new RecordingContextStore(f.store,{encryptionKey:recordKey}).read(request);assert.equal(result.state,'complete');assert.match(result.parts.map(part=>part.text).join(''),/合成不可丢失的可见指令/);
});
test('桥接只接受确定的23校验和，未知25和迁移序列缺口均拒绝',configuration,async t=>{
 const {store}=await fixture(t);await store.migrate({recordingContext:true});const expected=(await store.get('SELECT checksum FROM harbor.schema_migrations WHERE version=23')).checksum;
 await store.run('UPDATE harbor.schema_migrations SET checksum=$1 WHERE version=23','0'.repeat(64));await assert.rejects(store.verifySchema(),{code:'database_migration_conflict'});await assert.rejects(store.migrate(),{code:'database_migration_conflict'});
 await store.run('UPDATE harbor.schema_migrations SET checksum=$1 WHERE version=23',expected);await store.migrate({recordingContextIndex:true});await store.run('INSERT INTO harbor.schema_migrations VALUES(25,$1,$2)','f'.repeat(64),Date.now());await assert.rejects(store.verifySchema(),{code:'database_migration_required'});await assert.rejects(store.migrate(),{code:'database_migration_required'});
 await store.run('DELETE FROM harbor.schema_migrations WHERE version=25');await store.run('DELETE FROM harbor.schema_migrations WHERE version=10');await assert.rejects(store.verifySchema(),{code:'database_migration_conflict'});await assert.rejects(store.migrate(),{code:'database_migration_required'});
});
test('23应用失败整体回滚，默认迁移和旧事实不变，非法启用选项拒绝',configuration,async t=>{
 const {store}=await fixture(t);await store.migrate();const before=await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),query=store.query.bind(store);let injected=false;
 store.query=async(sql,args)=>{const result=await query(sql,args);if(sql.includes('CREATE TABLE harbor.recording_context_objects')&&!injected){injected=true;throw Error('synthetic migration failure');}return result;};
 await assert.rejects(store.migrate({recordingContext:true}),/synthetic migration failure/);store.query=query;assert.equal(injected,true);assert.deepEqual(await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),before);assert.equal((await store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name,null);assert.equal(await store.verifySchema(),true);
 for(const options of [null,[],true,{recordingContext:'true'},{recordingContextIndex:'true'},{recordingContextIndex:null},{recordingContext:false,recordingContextIndex:true},{unexpected:true}])await assert.rejects(store.migrate(options),{code:'database_configuration'});
});

test('显式索引迁移保留既有上下文、核心鉴权和派生任务，关闭选项不降级',configuration,async t=>{
 const f=await fixture(t);await f.store.migrate({recordingContext:true});await f.store.initializePricing();
 const member=randomUUID(),key=randomUUID(),secret='hg_synthetic_derived_'+randomUUID(),at=Date.now();
 await f.store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成索引员工',at);
 await f.store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,$5)',key,member,hash(secret),'hg_synthetic',at);
 let origin=await f.open();await f.close();
 const recordKey=await readFile(join(f.root,'recording-encryption.key')),recordings=new RecordingStore(f.store,{encryptionKey:recordKey}),contexts=new RecordingContextStore(f.store,{encryptionKey:recordKey});
 const write=async text=>{const request=await f.store.observe({id:member,key_id:key},'synthetic','synthetic','gpt-6.1-sol');await f.store.finish(request,{input:10,output:2,cached:0});await recordings.start(request);await contexts.writeSnapshot(request,[{kind:'instruction',content:text}]);return request;};
 const request=await write('迁移之前的上下文');
 assert.deepEqual(await f.store.migrate(),{version:23,changed:false});
 assert.deepEqual(await f.store.migrate({recordingContextIndex:true}),{version:24,changed:true});
 assert.equal(await f.store.verifySchema(),true);
 assert.equal((await f.store.get('SELECT COUNT(*) count FROM harbor.recording_context_work')).count,1);
 await write('迁移之后的上下文');
 assert.equal((await f.store.get('SELECT COUNT(*) count FROM harbor.recording_context_work')).count,2);
 assert.deepEqual(await f.store.migrate({recordingContext:false,recordingContextIndex:false}),{version:24,changed:false});
 origin=await f.open();assert.equal((await fetch(origin+'/ready')).status,200);assert.equal((await fetch(origin+'/api/me',{headers:{authorization:'Bearer '+secret}})).status,200);await f.close();
 assert.match((await contexts.read(request)).parts.map(part=>part.text).join(''),/迁移之前的上下文/);
 const expected=(await f.store.get('SELECT checksum FROM harbor.schema_migrations WHERE version=24')).checksum;
 await f.store.run('UPDATE harbor.schema_migrations SET checksum=$1 WHERE version=24','0'.repeat(64));
 await assert.rejects(f.store.verifySchema(),{code:'database_migration_conflict'});await assert.rejects(f.store.migrate(),{code:'database_migration_conflict'});
 await f.store.run('UPDATE harbor.schema_migrations SET checksum=$1 WHERE version=24',expected);assert.equal(await f.store.verifySchema(),true);
});
test('24迁移中途失败完整回滚，保留23且可安全重试',configuration,async t=>{
 const {store}=await fixture(t);await store.migrate({recordingContext:true});const before=await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),query=store.query.bind(store);let injected=false;
 store.query=async(sql,args)=>{const result=await query(sql,args);if(sql.includes('CREATE TABLE harbor.recording_context_work')&&!injected){injected=true;throw Error('synthetic derived migration failure');}return result;};
 await assert.rejects(store.migrate({recordingContextIndex:true}),/synthetic derived migration failure/);store.query=query;assert.equal(injected,true);
 assert.deepEqual(await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),before);assert.equal((await store.get("SELECT to_regclass('harbor.recording_context_work') name")).name,null);
 assert.equal(await store.verifySchema(),true);assert.deepEqual(await store.migrate(),{version:23,changed:false});assert.deepEqual(await store.migrate({recordingContextIndex:true}),{version:24,changed:true});
});
