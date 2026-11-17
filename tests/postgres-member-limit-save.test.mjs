import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL;
const connection=name=>{const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true};};
test('PG限额保存返回已提交配置，统计读失败不把成功写入误报失败',{skip:!url?'需独占真实PG':false,timeout:30000},async t=>{
 const root=new PostgresStore(connection()),database='harbor_limit_save_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+database);const store=new PostgresStore(connection(database));let admin;
 t.after(async()=>{await admin?.close();await store.close();await root.query('DROP DATABASE '+database);await root.close();});
 await store.migrate();await store.initializePricing();const id=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',id,'合成限额保存',Date.now());
 let reads=0;const readStore=new Proxy(store,{get(target,property){if(property==='transaction')return async()=>{reads++;throw Error('synthetic statistics timeout');};const value=Reflect.get(target,property,target);return typeof value==='function'?value.bind(target):value;}});
 admin=createPostgresMemberAdmin(store,{readStore,encryptionKey:randomBytes(32)});const path='/api/admin/members/'+id+'/usage-limits',limits=createPostgresMemberLimits(store),rules=[{model:'gpt-6.1-sol',hours:24,limit_usd:'50.00'}];
 const enabled=await admin.handle(path,'PATCH',{enabled:true,rules,expected_version:0});assert.equal(enabled.status,200);assert.deepEqual(enabled.value,{enabled:true,rules,version:1,currency:'USD',usage_pending:true});assert.equal(reads,0);
 await assert.rejects(admin.handle(path,'GET',{},new URLSearchParams()),/synthetic statistics timeout/);assert.equal(reads,1);
 const disabled=await admin.handle(path,'PATCH',{enabled:false,expected_version:1});assert.equal(disabled.status,200);assert.deepEqual(disabled.value,{enabled:false,rules,version:2,currency:'USD',usage_pending:true});assert.equal(reads,1);assert.deepEqual(await limits.configuration(id),{enabled:false,rules,version:2});
 await assert.rejects(admin.handle(path,'PATCH',{enabled:true,expected_version:1}),{status:409,code:'member_limits_conflict'});assert.equal((await limits.configuration(id)).enabled,false);
 const audit=await store.all("SELECT action FROM harbor.audit WHERE action='member.limits'");assert.equal(audit.length,2);
});
