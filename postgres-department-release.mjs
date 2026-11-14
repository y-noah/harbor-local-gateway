import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {migrateDepartmentIsolation,verifyDepartmentIsolationAvailability,departmentIsolationContract} from './postgres-departments.mjs';
import {readUsageCascadeInput} from './postgres-usage-rollups-cli.mjs';
import {verifyQuotaSubjectAvailability,quotaSubjectContract} from './postgres-quota-subjects.mjs';

const core=['accounts','members','keys','sessions','requests','gateway_settings'];
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code:'department_release_'+code});};
const roleName=value=>typeof value==='string'&&/^[a-z_][a-z0-9_]{0,62}$/.test(value),oid=value=>typeof value==='string'&&/^[0-9]{1,20}$/.test(value),hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
async function identity(store,expected){
 need(expected&&oid(expected.databaseOid)&&oid(expected.runtimeOid)&&roleName(expected.owner)&&roleName(expected.runtime),'binding_required');
 const db=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');need(db.oid===expected.databaseOid,'database_changed');
 const roles=await store.all('SELECT oid::text oid,rolname,rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls,rolcanlogin FROM pg_roles WHERE rolname=ANY($1::text[])',[expected.owner,expected.runtime]);
 const owner=roles.find(row=>row.rolname===expected.owner),runtime=roles.find(row=>row.rolname===expected.runtime);need(owner&&runtime&&owner.oid!==runtime.oid&&runtime.oid===expected.runtimeOid,'roles_changed');
 need(!['rolsuper','rolcreatedb','rolcreaterole','rolinherit','rolreplication','rolbypassrls','rolcanlogin'].some(key=>owner[key])&&!['rolsuper','rolcreatedb','rolcreaterole','rolinherit','rolreplication','rolbypassrls'].some(key=>runtime[key]),'role_privileges');
 need(!await store.get('SELECT 1 FROM pg_auth_members WHERE member=ANY($1::oid[]) OR roleid=$2::oid LIMIT 1',[owner.oid,runtime.oid],runtime.oid),'role_membership');
 need(!await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=$1::oid AND deptype='o' AND NOT(d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",runtime.oid),'runtime_ownership');
 const tables=await store.all("SELECT c.relname,c.oid::text oid,c.relowner::text owner,c.relkind,c.relpersistence,c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relname=ANY($1::text[]) ORDER BY c.relname",core);need(tables.length===core.length&&tables.every(row=>row.owner===owner.oid&&row.relkind==='r'&&row.relpersistence==='p'&&!row.relrowsecurity),'core_identity');
 const schema=await store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version'),activation=(await store.get('SELECT to_jsonb(a) value FROM harbor_operator.activation a WHERE singleton'))?.value;need(activation&&activation.database_oid===db.oid&&(!Object.hasOwn(activation,'runtime')||activation.runtime===runtime.rolname),'activation_binding');
 const value={databaseOid:db.oid,owner:owner.rolname,ownerOid:owner.oid,runtime:runtime.rolname,runtimeOid:runtime.oid,tables:Object.fromEntries(tables.map(row=>[row.relname,row.oid])),schemaDigest:hash(schema),activationDigest:hash(activation)};
 if(expected.tables)for(const key of Object.keys(value))need(isDeepStrictEqual(value[key],expected[key]),'binding_changed');
 need((await store.get("SELECT pg_has_role(current_user,$1::oid,'SET') ok",owner.oid)).ok,'owner_authorization');return value;
}
async function installed(store){return verifyDepartmentIsolationAvailability(store);}
export async function departmentReleaseOperation(store,action,input){
 need(['plan','status','install'].includes(action)&&input&&typeof input==='object'&&!Array.isArray(input),'arguments');
 return store.transaction(async()=>{
  await store.query("SET LOCAL lock_timeout='5000ms'");await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-department-release-v1',0))");
  const binding=await identity(store,action==='plan'?input.expected:input.binding);
  if(action==='plan'){
   const w=input.runtimeWitness;need(w&&w.databaseOid===binding.databaseOid&&w.currentRoleOid===binding.runtimeOid&&oid(w.sessionRoleOid)&&oid(w.backendPid),'runtime_witness');
   const activity=await store.get('SELECT datid::text database_oid,usesysid::text session_oid FROM pg_stat_activity WHERE pid=$1::int',w.backendPid);need(activity&&activity.database_oid===binding.databaseOid&&activity.session_oid===w.sessionRoleOid&&(await store.get("SELECT pg_has_role($1::oid,$2::oid,'SET') ok",w.sessionRoleOid,binding.runtimeOid)).ok,'runtime_witness_changed');
   binding.runtimeSessionRoleOid=w.sessionRoleOid;
  }else{need(oid(input.binding.runtimeSessionRoleOid),'runtime_session_required');binding.runtimeSessionRoleOid=input.binding.runtimeSessionRoleOid;}
  const before=await installed(store);
  if(action==='install'){
   need(input.activate===true,'activation_required');await store.query('SELECT pg_stat_clear_snapshot()');
   need((await store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND usesysid=ANY($1::oid[]) AND pid<>pg_backend_pid()',[binding.runtimeOid,binding.runtimeSessionRoleOid])).n===0,'runtime_not_drained');
   await store.query('SET LOCAL ROLE "'+binding.owner+'"');
   await migrateDepartmentIsolation(store,{apply:true,runtimeRole:binding.runtime});
   need(await installed(store),'installation_unconfirmed');
  }
  return {action,contract:departmentIsolationContract,quota_subject_contract:quotaSubjectContract,quota_subject_installed:await verifyQuotaSubjectAvailability(store),binding,installed:action==='install'||before,changed:action==='install'&&!before};
 });
}
export async function runDepartmentRelease(action,database,input){const store=new PostgresStore({...database,poolSize:1,statementTimeoutMs:60000});try{await store.verifySchema();return await departmentReleaseOperation(store,action,input);}finally{await store.close();}}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{need(process.argv.length===3,'arguments');const input=await readUsageCascadeInput(process.stdin);console.log(JSON.stringify(await runDepartmentRelease(process.argv[2],{connection:input.connection},input.cascade)));}
 catch(error){console.error(JSON.stringify({ok:false,code:typeof error?.code==='string'&&/^[A-Za-z0-9_]{1,80}$/.test(error.code)?error.code:'department_release_failed'}));process.exitCode=1;}
}
