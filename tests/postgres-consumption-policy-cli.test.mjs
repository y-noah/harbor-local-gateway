import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {consumptionPolicyArguments,runConsumptionPolicyCommand} from '../postgres-consumption-policy-cli.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,real={skip:!url?'需独占私有PG':false,timeout:30000},exec=promisify(execFile),script=new URL('../postgres-consumption-policy-cli.mjs',import.meta.url).pathname;
function options(database='postgres'){const target=new URL(url);target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:2,connectionTimeoutMs:500,statementTimeoutMs:5000};}
async function fixture(t,{migrationOptions={},activationContract='operator'}={}){
 const admin=new PostgresStore(options()),name='harbor_policy_cli_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+name);const store=new PostgresStore(options(name));
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();});
 await store.migrate(migrationOptions);const role=await createUsageRollupRuntime(store,{activationContract});return {store,name,role};
}
async function snapshot(store){return {migrations:await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),columns:await store.all("SELECT attrelid::regclass::text relation,attname,attnum FROM pg_attribute WHERE attrelid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass]) AND attnum>0 AND NOT attisdropped ORDER BY attrelid,attnum")};}
async function ownConnections(store){return (await store.get('SELECT count(*) n FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND pid<>pg_backend_pid()')).n;}
async function noOtherConnections(store){const deadline=Date.now()+2000;while(await ownConnections(store)){assert.ok(Date.now()<deadline,'CLI leaked a database pool');await new Promise(resolve=>setTimeout(resolve,10));}}

test('CLI defaults to verify and rejects duplicate, unknown, cross-action and unsafe role arguments',()=>{
 assert.deepEqual(consumptionPolicyArguments([]),{action:'verify'});assert.deepEqual(consumptionPolicyArguments(['verify']),{action:'verify'});assert.deepEqual(consumptionPolicyArguments(['verify','--require-installed']),{action:'verify',requireInstalled:true});assert.deepEqual(consumptionPolicyArguments(['install']),{action:'install'});assert.deepEqual(consumptionPolicyArguments(['install','--runtime-role=harbor_runtime']),{action:'install',runtimeRole:'harbor_runtime'});
 for(const args of [null,{},[1],['drop'],['disable'],['rollback'],['--require-installed'],['verify','--require-installed','--require-installed'],['verify','--runtime-role=role'],['verify','--require-installed=true'],['install','--require-installed'],['install','--runtime-role='],['install','--runtime-role=role','--runtime-role=role'],['install','--runtime-role=Uppercase'],['install','--runtime-role=bad;drop'],['install','--runtime-role='+('x'.repeat(64))],['install','--runtime-role','role'],['verify','--url=synthetic']])assert.throws(()=>consumptionPolicyArguments(args),{code:'database_configuration'});
});
test('invalid arguments fail before connection and CLI errors never expose URL or password',async()=>{
 await assert.rejects(runConsumptionPolicyCommand(['drop'],{connectionString:'synthetic-unreadable-url'}),{code:'database_configuration'});
 const secret='synthetic-password-never-log',bad='postgresql://user:'+secret+'@127.0.0.1:not-a-port/secret-db';
 for(const args of [['drop'],['verify']])await assert.rejects(exec(process.execPath,[script,...args],{env:{...process.env,HARBOR_DATABASE_URL:bad},timeout:5000}),error=>{
  assert.equal(error.code,1);const output=error.stdout+error.stderr;assert.equal(output.includes(secret),false);assert.equal(output.includes(bad),false);assert.equal(error.stdout,'');const value=JSON.parse(error.stderr);assert.ok(['database_configuration','consumption_policy_command_failed'].includes(value.error.code));assert.equal(Object.hasOwn(value.error,'stack'),false);return true;
 });
});
test('real PG default verify is read-only; require-installed fails without installing or leaking pool',real,async t=>{
 const f=await fixture(t),before=await snapshot(f.store);assert.deepEqual(await runConsumptionPolicyCommand([],options(f.name)),{action:'verify',installed:false});assert.deepEqual(await snapshot(f.store),before);await noOtherConnections(f.store);
 await assert.rejects(runConsumptionPolicyCommand(['verify','--require-installed'],options(f.name)),{code:'database_migration_required'});assert.deepEqual(await snapshot(f.store),before);await noOtherConnections(f.store);
 const run=await exec(process.execPath,[script],{env:{...process.env,HARBOR_DATABASE_URL:options(f.name).connectionString},timeout:10000});assert.deepEqual(JSON.parse(run.stdout),{action:'verify',installed:false});assert.equal(run.stderr,'');await noOtherConnections(f.store);
});
test('real PG install verifies extension, repeats idempotently and preserves core22/optional23/24',real,async t=>{
 for(const [version,migrationOptions] of [[22,{}],[23,{recordingContext:true}],[24,{recordingContextIndex:true}]]){
  const f=await fixture(t,{migrationOptions}),core=await f.store.all('SELECT * FROM harbor.schema_migrations ORDER BY version');assert.equal(core.at(-1).version,version);
  assert.deepEqual(await runConsumptionPolicyCommand(['install'],options(f.name)),{action:'install',installed:true,changed:true});assert.deepEqual(await runConsumptionPolicyCommand(['verify','--require-installed'],options(f.name)),{action:'verify',installed:true});
  const before=await snapshot(f.store);assert.deepEqual(await runConsumptionPolicyCommand(['install'],options(f.name)),{action:'install',installed:true,changed:false});assert.deepEqual(await snapshot(f.store),before);assert.deepEqual(await f.store.all('SELECT * FROM harbor.schema_migrations ORDER BY version'),core);await noOtherConnections(f.store);
 }
});
test('real restricted runtime verifies only and cannot install even an already installed extension',real,async t=>{
 const f=await fixture(t),password=randomBytes(24).toString('hex');await f.store.query('ALTER ROLE "'+f.role+'" LOGIN PASSWORD \''+password+'\'');
 const runtime=new URL(options(f.name).connectionString);runtime.username=f.role;runtime.password=password;const runtimeOptions={...options(f.name),connectionString:runtime.href};
 assert.deepEqual(await runConsumptionPolicyCommand(['verify'],runtimeOptions),{action:'verify',installed:false});
 await assert.rejects(runConsumptionPolicyCommand(['install'],runtimeOptions),{code:'consumption_migration_owner_invalid'});assert.equal((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='schema.key-consumption-policy'")),undefined);
 await runConsumptionPolicyCommand(['install'],options(f.name));const before=await snapshot(f.store);assert.deepEqual(await runConsumptionPolicyCommand(['verify','--require-installed'],runtimeOptions),{action:'verify',installed:true});await assert.rejects(runConsumptionPolicyCommand(['install'],runtimeOptions),{code:'consumption_migration_owner_invalid'});assert.deepEqual(await snapshot(f.store),before);
 await assert.rejects(exec(process.execPath,[script,'install'],{env:{...process.env,HARBOR_DATABASE_URL:runtime.href},timeout:10000}),error=>{assert.equal(error.code,1);assert.equal(error.stderr.includes(password),false);assert.equal(error.stderr.includes(runtime.href),false);assert.equal(JSON.parse(error.stderr).error.code,'consumption_migration_owner_invalid');return true;});
 assert.equal((await f.store.get('SELECT count(*) n FROM pg_stat_activity WHERE datname=current_database() AND usename=$1',f.role)).n,0);
});
test('real local-assurance install requires explicit runtime role and rolls back omitted or mismatching role',real,async t=>{
 const f=await fixture(t,{activationContract:'local-assurance'}),before=await snapshot(f.store);await assert.rejects(runConsumptionPolicyCommand(['install'],options(f.name)),{code:'consumption_runtime_invalid'});assert.deepEqual(await snapshot(f.store),before);
 await assert.rejects(runConsumptionPolicyCommand(['install','--runtime-role=missing_runtime'],options(f.name)),{code:'consumption_runtime_invalid'});assert.deepEqual(await snapshot(f.store),before);
 assert.deepEqual(await runConsumptionPolicyCommand(['install','--runtime-role='+f.role],options(f.name)),{action:'install',installed:true,changed:true});assert.deepEqual(await runConsumptionPolicyCommand(['install','--runtime-role='+f.role],options(f.name)),{action:'install',installed:true,changed:false});await noOtherConnections(f.store);
});
test('core schema verification precedes install and damaged core migrations fail closed without repair',real,async t=>{
 const f=await fixture(t);await f.store.run("UPDATE harbor.schema_migrations SET checksum='synthetic-damaged' WHERE version=22");await assert.rejects(runConsumptionPolicyCommand(['install'],options(f.name)),{code:'database_migration_conflict'});assert.equal((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='schema.key-consumption-policy'")),undefined);await noOtherConnections(f.store);
});

test('damaged installed extension is rejected by verify and install without implicit repair',real,async t=>{
 const f=await fixture(t);await runConsumptionPolicyCommand(['install'],options(f.name));await f.store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.key-consumption-policy'");const before=await snapshot(f.store);
 for(const args of [[],['verify','--require-installed'],['install']])await assert.rejects(runConsumptionPolicyCommand(args,options(f.name)),{code:'database_migration_conflict'});
 assert.deepEqual(await snapshot(f.store),before);await noOtherConnections(f.store);
});
