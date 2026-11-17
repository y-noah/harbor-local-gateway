import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,readdir,readFile,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {hash,usageWindow} from '../store.mjs';
import {postgresRequestObservation} from '../postgres-observability.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需真实隔离PG':false,timeout:30000};
const options=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};};
const diagnostic=(changes={})=>({version:1,reason:'usage_received',terminal:'completed',interruption:'none',usage_known:true,...changes});
async function fixture(t){
 const admin=new PostgresStore(options()),database='harbor_metering_diag_'+randomUUID().replaceAll('-','');let created=false,store;
 t.after(async()=>{await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();});
 await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
 const member=randomUUID(),key=randomUUID(),at=Date.now();await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成诊断员工',$2)",member,at);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,'synthetic',$4)",key,member,hash(randomUUID()),at);
 return {store,member,key,at,observe:(started=at-1000)=>store.observe({id:member,key_id:key},'synthetic-ip','synthetic-client','gpt-6.1-sol',{at:started})};
}

test('PG计量诊断随首次结算落库，非法诊断不阻断且以最终数值事实修正',{...integration},async t=>{
 const {store,observe,at,member}=await fixture(t);
 const cases=[
  {result:{input:4,output:2,cached:1,meteringDiagnostic:diagnostic()},expected:diagnostic(),known:true,used:6},
  {result:{input:1,output:2},expected:null,known:true,used:3},
  {result:{input:2,output:1,meteringDiagnostic:{...diagnostic(),secret:'NEVER-PERSIST'}},expected:null,known:true,used:3},
  {result:{input:1,output:2,cached:2,meteringDiagnostic:diagnostic()},expected:diagnostic({reason:'settlement_usage_invalid',usage_known:false}),known:false,used:0},
  {result:{input:0,output:0,usageKnown:false,meteringDiagnostic:diagnostic()},expected:diagnostic({reason:'settlement_uncertain',usage_known:false}),known:false,used:0}
 ];
 for(const item of cases){const id=await observe();await store.finish(id,item.result,at);const row=await store.get('SELECT phase_history,usage_unknown FROM harbor.requests WHERE id=$1',id),phases=JSON.parse(row.phase_history);assert.deepEqual(phases.at(-1).metering??null,item.expected);assert.equal(row.usage_unknown,item.known?0:1);assert.equal(row.phase_history.includes('NEVER-PERSIST'),false);assert.deepEqual((await postgresRequestObservation(store,id,at)).meteringDiagnostic,item.expected);const before=row.phase_history;assert.deepEqual(await store.finish(id,{input:999,output:999,meteringDiagnostic:diagnostic({reason:'not_dispatched',terminal:'none'})},at+1),{settled:false});assert.equal((await store.get('SELECT phase_history FROM harbor.requests WHERE id=$1',id)).phase_history,before);}
 assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member)).used,cases.reduce((sum,item)=>sum+item.used,0));
});

test('PG加密计量spool保留诊断与旧包兼容，重启及重复回放不二次结算',{...integration},async t=>{
 const {store,observe,at,member}=await fixture(t),directory=await mkdtemp(join(tmpdir(),'harbor-metering-diag-')),encryptionKey=randomBytes(32);let unavailable=true,recovery;
 t.after(async()=>{await recovery?.close();await rm(directory,{recursive:true,force:true});});
 const proxy={finish:(...args)=>{if(unavailable)throw Error('synthetic unavailable');return store.finish(...args);}},open=()=>MeteringRecovery.create(proxy,{encryptionKey,directory,flushMs:10000});recovery=await open();
 const valid=await observe(),legacy=await observe(),invalid=await observe();
 assert.equal(recovery.settle(valid,{input:4,output:1,meteringDiagnostic:diagnostic()},at),true);assert.equal(recovery.settle(legacy,{input:3,output:1},at),true);assert.equal(recovery.settle(invalid,{input:-1,output:2,meteringDiagnostic:diagnostic()},at),true);await recovery.flush();
 const files=await Promise.all((await readdir(directory)).map(async name=>({name,bytes:await readFile(join(directory,name))})));assert.equal(files.length,3);
 for(const file of files){assert.equal(file.bytes.includes(Buffer.from('usage_received')),false);const saved=recovery.buffer.repository.open(file.bytes,file.name);if(saved.requestId===legacy)assert.equal(saved.result.meteringDiagnostic,undefined);if(saved.requestId===invalid)assert.equal(saved.result.meteringDiagnostic.reason,'settlement_usage_invalid');}
 await recovery.close();unavailable=false;recovery=await open();await recovery.flush();assert.equal(recovery.status().files,0);assert.deepEqual((await postgresRequestObservation(store,valid,at)).meteringDiagnostic,diagnostic());assert.equal((await postgresRequestObservation(store,legacy,at)).meteringDiagnostic,null);assert.equal((await postgresRequestObservation(store,invalid,at)).meteringDiagnostic.reason,'settlement_usage_invalid');assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member)).used,9);
 const histories=await store.all('SELECT id,phase_history FROM harbor.requests ORDER BY id');await recovery.close();for(const file of files)await writeFile(join(directory,file.name),file.bytes,{mode:0o600});recovery=await open();await recovery.flush();assert.equal(recovery.status().files,0);assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member)).used,9);assert.deepEqual(await store.all('SELECT id,phase_history FROM harbor.requests ORDER BY id'),histories);
});

test('PG计量诊断详情保持部门隔离与近7天可见边界',{...integration},async t=>{
 const {store,observe,at}=await fixture(t),window=usageWindow(at),visible=await observe(),expired=await observe(window.start-2);await store.finish(visible,{input:1,output:1,meteringDiagnostic:diagnostic()},at);await store.finish(expired,{input:1,output:1,meteringDiagnostic:diagnostic()},window.start-1);await migrateDepartmentIsolation(store,{apply:true});
 await store.run("INSERT INTO harbor.departments(id,name,created,updated) VALUES('other-department','合成其他部门',$1,$1)",at);const issuer=createDepartmentScopeIssuer(),own=issuer.superAdmin({departmentId:'department-default',credentialId:'synthetic-admin',credentialVersion:0}),other=issuer.departmentAdmin({departmentId:'other-department',credentialId:'synthetic-other',credentialVersion:0});
 assert.deepEqual((await postgresRequestObservation(store,visible,at,{scope:own})).meteringDiagnostic,diagnostic());await assert.rejects(postgresRequestObservation(store,visible,at,{scope:other}),{status:404});await assert.rejects(postgresRequestObservation(store,visible,at,{scope:{...own}}),{status:403});await assert.rejects(postgresRequestObservation(store,expired,at,{scope:own}),{status:404});assert.equal(JSON.parse((await store.get('SELECT phase_history FROM harbor.requests WHERE id=$1',expired)).phase_history).at(-1).metering.reason,'usage_received');
});
