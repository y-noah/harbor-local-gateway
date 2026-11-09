import test from 'node:test';import assert from 'node:assert/strict';import {randomUUID} from 'node:crypto';import {execFile} from 'node:child_process';import {promisify} from 'node:util';
import {PostgresStore} from '../postgres-store.mjs';import {migratePostgres,postgresMigrationArguments} from '../postgres-migrate.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,configuration={skip:!url?'需独占测试PG':false};
const options=name=>{const value=new URL(url);value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true};};
async function fixture(t){const root=new PostgresStore(options('postgres')),name='harbor_migrate_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(options(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});return {store,options:options(name)};}
test('真实PG：迁移入口默认22、显式23/24且普通调用不会降级',configuration,async t=>{
 const f=await fixture(t);assert.deepEqual(await migratePostgres(f.options),{version:22,changed:true});await f.store.initializePricing();assert.equal((await f.store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name,null);
 await f.store.run("INSERT INTO harbor.members(id,name,created) VALUES('synthetic','合成迁移保留员工',1)");assert.deepEqual(await migratePostgres(f.options,postgresMigrationArguments(['--recording-context=storage'])),{version:23,changed:true});assert.deepEqual(await migratePostgres(f.options),{version:23,changed:false});assert.equal((await f.store.get("SELECT to_regclass('harbor.recording_context_work') name")).name,null);
 assert.deepEqual(await migratePostgres(f.options,postgresMigrationArguments(['--recording-context=index'])),{version:24,changed:true});assert.deepEqual(await migratePostgres(f.options),{version:24,changed:false});assert.equal((await f.store.get("SELECT name FROM harbor.members WHERE id='synthetic'")).name,'合成迁移保留员工');assert.equal(await f.store.verifySchema(),true);
});
test('真实PG：显式索引迁移失败整笔回滚，原库和既有数据不被覆盖',configuration,async t=>{
 const f=await fixture(t);await migratePostgres(f.options);await f.store.query('CREATE TABLE harbor.recording_context_work(synthetic_collision INTEGER)');
 await assert.rejects(migratePostgres(f.options,postgresMigrationArguments(['--recording-context=index'])));assert.equal((await f.store.get('SELECT MAX(version) n FROM harbor.schema_migrations')).n,22);assert.equal((await f.store.get("SELECT to_regclass('harbor.recording_context_objects') name")).name,null);await f.store.query('DROP TABLE harbor.recording_context_work');assert.deepEqual(await migratePostgres(f.options,postgresMigrationArguments(['--recording-context=index'])),{version:24,changed:true});
});
const localUrl=url&&['127.0.0.1','localhost','[::1]'].includes(new URL(url).hostname);
test('真实本机PG：CLI进程执行显式迁移，非法参数在连接前拒绝且不泄漏地址',{skip:!localUrl?'CLI真实进程需本机私有PG；跨主机TLS不降级':false},async t=>{
 const f=await fixture(t),run=promisify(execFile),script=new URL('../postgres-migrate.mjs',import.meta.url).pathname,env={...process.env,HARBOR_DATABASE_URL:f.options.connectionString};
 for(const [args,version]of [[[],22],[['--recording-context=storage'],23],[['--recording-context=index'],24],[[],24]]){const {stdout,stderr}=await run(process.execPath,[script,...args],{env,timeout:15000,maxBuffer:4096});assert.equal(JSON.parse(stdout).version,version);assert.equal(stderr,'');}
 await assert.rejects(run(process.execPath,[script,'--recording-context=invalid'],{env:{...env,HARBOR_DATABASE_URL:'not-a-database-url-synthetic'},timeout:5000,maxBuffer:4096}),error=>error.code===1&&error.stderr.includes('迁移参数无效')&&!error.stderr.includes('not-a-database-url'));
 assert.equal((await f.store.get('SELECT MAX(version) n FROM harbor.schema_migrations')).n,24);
});
