import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {recordingDatabaseIdentity} from '../postgres-storage-report.mjs';
import {consumptionPolicyContract} from '../ci/postgres-preflight.mjs';
const root=fileURLToPath(new URL('../',import.meta.url));
const query=JSON.parse(execFileSync('/usr/bin/python3',['-I','-B','-c',"import ast,json,pathlib;tree=ast.parse(pathlib.Path('ci/postgres-upgrade-deploy.py').read_text());print(json.dumps(next(ast.literal_eval(node.value) for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='SOURCE_CONSUMPTION_QUERY' for target in node.targets))))"],{cwd:root,encoding:'utf8'}));
const enabled=!!process.env.HARBOR_CAPACITY_PG_BIN,options={skip:!enabled?'需私有PG18':false};let lab;
before(async()=>{if(enabled)lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});});after(async()=>{await lab?.close();});
async function fixture(t,{schema=true}={}){
 const admin=new PostgresStore({connection:lab.connection}),database='hcug_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+database);const store=new PostgresStore({connection:{...lab.connection,database}});t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+database);await admin.close();});await store.query('CREATE SCHEMA harbor; CREATE TABLE harbor.keys(id text PRIMARY KEY,enabled integer NOT NULL DEFAULT 1,removed_at bigint)');if(schema)await store.query("ALTER TABLE harbor.keys ADD COLUMN consumption_policy text NOT NULL DEFAULT 'inherit', ADD COLUMN consumption_policy_version bigint NOT NULL DEFAULT 0");await store.query("INSERT INTO harbor.keys(id) VALUES('synthetic-key')");
 const read=()=>store.transaction(async()=>{assert.equal((await store.get('SHOW transaction_read_only')).transaction_read_only,'on');return store.get(query);},{level:'REPEATABLE READ',readOnly:true});return {store,read,database};
}
test('candidate preflight declares supported consumption contract',()=>assert.equal(consumptionPolicyContract,'v1'));
test('legacy columns absent preserve readonly inheritance',options,async t=>{const f=await fixture(t,{schema:false});const before=await f.store.all('SELECT * FROM harbor.keys');assert.deepEqual(await f.read(),{explicit:false,invalid:false});assert.deepEqual(await f.store.all('SELECT * FROM harbor.keys'),before);});
test('installed inheritance metadata permits compatible old combination',options,async t=>{const f=await fixture(t);assert.deepEqual(await f.read(),{explicit:false,invalid:false});});
test('disabled explicit Key still prevents downgrade',options,async t=>{const f=await fixture(t);await f.store.query("UPDATE harbor.keys SET enabled=0,consumption_policy='api_only'");assert.deepEqual(await f.read(),{explicit:true,invalid:false});});
test('only genuinely removed explicit Keys are ignored',options,async t=>{const f=await fixture(t);await f.store.query("UPDATE harbor.keys SET removed_at=1,consumption_policy='subscription_only'");assert.deepEqual(await f.read(),{explicit:false,invalid:false});});
test('unknown policies or unsafe versions fail closed',options,async t=>{const f=await fixture(t);for(const [policy,version] of [['unknown',0],['inherit',-1],['inherit','9007199254740992']]){await f.store.run('UPDATE harbor.keys SET consumption_policy=$1,consumption_policy_version=$2',policy,version);assert.equal((await f.read()).invalid,true);}});
test('partial metadata and non-inheritance defaults are not silently accepted',options,async t=>{const f=await fixture(t,{schema:false});await f.store.query("ALTER TABLE harbor.keys ADD COLUMN consumption_policy text NOT NULL DEFAULT 'inherit'");assert.equal((await f.read()).invalid,true);await f.store.query('ALTER TABLE harbor.keys ADD COLUMN consumption_policy_version bigint NOT NULL DEFAULT 0');assert.equal((await f.read()).invalid,false);await f.store.query("ALTER TABLE harbor.keys ALTER COLUMN consumption_policy SET DEFAULT 'api_only'");assert.equal((await f.read()).invalid,true);});
test('actual bound source program reports verified database digest and booleans without records',options,async t=>{
 const f=await fixture(t,{schema:false});const program=JSON.parse(execFileSync('/usr/bin/python3',['-I','-B','-c',"import importlib.util,json;from types import SimpleNamespace;spec=importlib.util.spec_from_file_location('f','tests/test_postgres_consumption_upgrade_guard.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);t,g,w,e,a,p,c=m.fixture();captured=[];a['docker']=lambda *args,**kw:(captured.append(args[5]) or SimpleNamespace(stdout=json.dumps({'contract':'harbor-consumption-legacy-key-snapshot-v1','database_digest':t['databaseDigest'],'explicit':False,'invalid':False})));m.upgrade.source_consumption_snapshot(a,t,e,g);print(json.dumps(captured[0]))"],{cwd:root,encoding:'utf8'}));
 const address=new URL('postgresql://127.0.0.1/');address.username=lab.connection.user;address.password=lab.connection.password;address.port=String(lab.connection.port);address.pathname='/'+f.database;
 const result=JSON.parse(execFileSync(process.execPath,['--input-type=module','-e',program.replaceAll("'/app/","'"+root)],{cwd:root,env:{...process.env,HARBOR_DATABASE_URL:address.href},encoding:'utf8',timeout:10000}));
 const identity=await recordingDatabaseIdentity(f.store);assert.deepEqual(result,{contract:'harbor-consumption-legacy-key-snapshot-v1',database_digest:createHash('sha256').update(identity.cluster+'\0'+identity.database).digest('hex'),explicit:false,invalid:false});assert.deepEqual(Object.keys(result).sort(),['contract','database_digest','explicit','invalid']);
});
