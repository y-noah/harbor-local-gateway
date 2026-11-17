import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {passwordHash} from '../auth.mjs';
import {hash,encryptStoredKey,revealStoredKey} from '../store.mjs';
import {createPostgresApiAccountScope,decorateApiAccountScope,apiAccountScopeAllowed} from '../postgres-api-account-scope.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {collectConfiguration,writeConfigurationBundle,readConfigurationBundle} from '../configuration-backup.mjs';
import {restoreConfiguration} from '../configuration-restore.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需隔离PG':false,timeout:30000};
const json=value=>typeof value==='string'?JSON.parse(value):value;
const connection=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,database:name||'postgres'},poolSize:1};const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:1};};
for(const departmental of [false,true])test('轻量配置备份恢复专用API范围及权重，不放入inherit池 '+(departmental?'部门模式':'兼容模式'),integration,async t=>{
 const root=new PostgresStore(connection()),names=['source','target'].map(label=>'scope_backup_'+label+'_'+randomUUID().replaceAll('-','')),directory=await mkdtemp(join(tmpdir(),'harbor-api-scope-backup-'));let source,target;
 t.after(async()=>{await source?.close();await target?.close();for(const name of names)await root.query('DROP DATABASE IF EXISTS '+name);await root.close();await rm(directory,{recursive:true,force:true});});for(const name of names)await root.query('CREATE DATABASE '+name);source=new PostgresStore(connection(names[0]));target=new PostgresStore(connection(names[1]));for(const store of [source,target])await store.migrate();await source.initializePricing();
 const dirs={};for(const name of ['source','target','runtime','bundle','journal']){dirs[name]=join(directory,name);await mkdir(dirs[name],{mode:0o700});}const key=randomBytes(32),backup=randomBytes(32);await writeFile(join(dirs.source,'key-encryption.key'),key,{mode:0o600});await writeFile(join(dirs.source,'settings.json'),JSON.stringify({port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-backup')}),{mode:0o600});
 const explicit=randomUUID(),shared=randomUUID(),secret='sk-synthetic-'+randomUUID();for(const id of [explicit,shared])await source.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,api_weight,api_secret_cipher,api_secret_digest,state) VALUES($1,'合成备份API',$2,'codex','openai_api',$3,$4,$5,'ready')",id,dirs.source,id===shared?100:0,encryptStoredKey(key,secret,'upstream:'+id),hash(secret));
 const service=createPostgresApiAccountScope(source);await service.save(explicit,{api_usage_scope:'explicit_api',api_weight:100,expected_api_scope_version:0});
 if(departmental)for(const store of [source,target])await migrateDepartmentIsolation(store,{apply:true});
 const bundle=await collectConfiguration(source,{dataDir:dirs.source,runtimeDir:dirs.runtime});assert.equal(Object.hasOwn(bundle.tables,'requests'),false);assert.equal(bundle.tables.accounts.find(a=>a.id===explicit).api_weight,0);const table=departmental?'department_settings':'gateway_settings',saved=bundle.tables[table].find(row=>row.name==='api_account_scope');assert.ok(saved,'范围配置须进入白名单备份');assert.equal(json(saved.value).accounts[explicit].api_weight,100);await writeConfigurationBundle(dirs.bundle,bundle,backup);assert.equal((await readFile(join(dirs.bundle,'configuration.enc'))).includes(Buffer.from(secret)),false);const loaded=await readConfigurationBundle(dirs.bundle,backup);await restoreConfiguration(target,loaded,{dataDir:dirs.target,runtimeDir:dirs.runtime,journalDir:dirs.journal,backupKey:backup});
 const restored=await target.all('SELECT * FROM harbor.accounts ORDER BY id'),setting=await target.get('SELECT value FROM harbor.'+table+" WHERE name='api_account_scope'"),configuration=json(setting.value);assert.deepEqual(configuration,json(saved.value));assert.equal(restored.find(row=>row.id===explicit).api_weight,0);assert.equal(restored.find(row=>row.id===shared).api_weight,100);for(const row of restored)assert.equal(revealStoredKey(key,{id:'upstream:'+row.id,digest:row.api_secret_digest,secret_cipher:row.api_secret_cipher}),secret);
 const ordinary=decorateApiAccountScope(restored,{configuration,policy:'inherit',allocation:true}),dedicated=decorateApiAccountScope(restored,{configuration,policy:'api_only',allocation:true});assert.equal(ordinary.find(row=>row.id===explicit).api_weight,0);assert.equal(apiAccountScopeAllowed(ordinary.find(row=>row.id===explicit),'inherit'),false);assert.equal(dedicated.find(row=>row.id===explicit).api_weight,100);assert.equal(apiAccountScopeAllowed(dedicated.find(row=>row.id===explicit),'api_only'),true);assert.equal((await target.get('SELECT count(*)::int n FROM harbor.requests')).n,0);
 // Restore deliberately marks upstream state unknown. Simulate an independent successful
 // credential check only after asserting the restored configuration itself.
 await target.run("UPDATE harbor.accounts SET state='ready'");const scope=departmental?createDepartmentScopeIssuer().superAdmin({departmentId:'department-default',credentialId:'synthetic-backup-admin',credentialVersion:0}):undefined,router=createPostgresRouter(target);const inherited=await router.catalogCandidates({policy:'inherit',scope}),explicitPool=await router.catalogCandidates({policy:'api_only',scope});assert.deepEqual(inherited.candidates.map(row=>row.account.id),[shared]);assert.deepEqual(new Set(explicitPool.candidates.map(row=>row.account.id)),new Set([shared,explicit]));
});
