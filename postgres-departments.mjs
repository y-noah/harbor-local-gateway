import {createPostgresAudit} from './postgres-audit.mjs';
import {migrateRecordingRetention} from './recording-retention.mjs';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {AppError,id} from './store.mjs';
import {balanceSettings} from './balancing.mjs';
import {transferSchedule} from './transfer-schedule.mjs';
import {defaultDepartmentId,requireDepartmentScope,issueDepartmentManagementKey,departmentManagementKeyDigest} from './department-access.mjs';

export const departmentIsolationContract='harbor-department-isolation-v1';
export const departmentIsolationCoverage=Object.freeze({core:true,complete:false});
const actorServices=new WeakMap();
export const departmentJobFields=Object.freeze(['department_id','scope_kind','scope_identity','actor_kind','actor_id','actor_version']);
export const departmentJobKinds=Object.freeze(['worker.startup','recording.purge','account.extra.recover','routing.expiry','routing.nightly','routing.preview','member.actions','member.config','account.extra.configure','recording.index','recording.context.index','recording.export','recording.retention']);
const systemGlobalKinds=new Set(['worker.startup','recording.purge','account.extra.recover']),systemDepartmentKinds=new Set(['routing.expiry','routing.nightly','recording.index','recording.context.index']);
export const isPostgresDepartmentActorService=(service,store)=>actorServices.get(service)===store;
const setting='schema.department-isolation',migrationUrl=new URL('./migrations/department-isolation.sql',import.meta.url),verified=new WeakMap();
const scopedTables=['members','accounts','keys','sessions','requests','member_types','member_type_templates','background_jobs','recording_retention_plans','member_config_batches','member_action_batches','routing_plans','balance_runs','nightly_group_results','nightly_cycle_ready','nightly_plan_revisions','nightly_plan_applications','nightly_plan_group_results','automatic_handoff_groups','session_handoffs','account_package_templates','account_package_versions','account_package_selections','account_package_previews','extra_usage_previews','extra_usage_preview_items'];
const fail=(code='department_migration_required')=>{throw new AppError(503,'部门隔离结构未就绪，请完成受控迁移并重新启动',code);};
async function bundle(){const sql=(await Promise.all([migrationUrl,new URL('./migrations/department-account-packages.sql',import.meta.url),new URL('./migrations/department-extra-usage.sql',import.meta.url)].map(url=>readFile(url,'utf8')))).join('\n');return {sql,checksum:createHash('sha256').update(sql).digest('hex')};}
async function presence(store){
 const row=await store.get(`SELECT to_regclass('harbor.gateway_settings') IS NOT NULL settings,
  to_regclass('harbor.departments') IS NOT NULL departments,to_regclass('harbor.department_admin_keys') IS NOT NULL admin_keys,
  to_regclass('harbor.department_settings') IS NOT NULL department_settings,
  EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='harbor' AND c.relname=ANY($1::text[]) AND a.attname IN ('department_id','scope_identity') AND NOT a.attisdropped) columns`,[...scopedTables,'request_query_snapshots','audit_scopes']);
 if(!row||['settings','departments','admin_keys','department_settings','columns'].some(key=>typeof row[key]!=='boolean'))fail();
 const saved=row.settings?await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting):null;
 return {present:row.departments||row.admin_keys||row.department_settings||row.columns,marker:saved?.value??null};
}
function marker(raw,checksum){
 let value;try{value=JSON.parse(raw);}catch{fail('department_migration_conflict');}
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).sort().join(',')!=='checksum,contract,default_department_id,version'||value.contract!==departmentIsolationContract||value.version!==1||value.default_department_id!==defaultDepartmentId||value.checksum!==checksum)fail('department_migration_conflict');
 return value;
}
async function structure(store,sql){
 const rows=await store.all(`SELECT c.relname,a.attname,a.attnotnull,format_type(a.atttypid,a.atttypmod) type,
  pg_get_expr(d.adbin,d.adrelid) default_value FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
  WHERE n.nspname='harbor' AND c.relname=ANY($1::text[]) AND a.attnum>0 AND NOT a.attisdropped`,[...scopedTables,'departments','department_admin_keys','department_settings','request_query_snapshots','audit_scopes']);
 const columns=new Map(rows.map(row=>[row.relname+'.'+row.attname,row]));
 for(const table of scopedTables){const column=columns.get(table+'.department_id');if(!column||column.type!=='text'||column.attnotnull!==(table!=='background_jobs')||column.default_value!==null)fail('department_migration_conflict');}
 for(const [name,type] of Object.entries({scope_kind:'text',scope_identity:'text',actor_kind:'text',actor_id:'text',actor_version:'bigint'})){const column=columns.get('background_jobs.'+name);if(!column||column.type!==type||!column.attnotnull||column.default_value!==null)fail('department_migration_conflict');}
 for(const table of ['member_config_batches','member_action_batches','session_handoffs','account_package_previews','extra_usage_previews']){
  for(const [name,type] of Object.entries({actor_kind:'text',actor_id:'text',actor_version:'bigint'})){const column=columns.get(table+'.'+name);if(!column||column.type!==type||!column.attnotnull||column.default_value!==null)fail('department_migration_conflict');}
  if(!await store.get("SELECT 1 valid FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname='department_batch_actor_immutable' AND tgenabled='O' AND NOT tgisinternal",'harbor.'+table))fail('department_migration_conflict');
 }
 const jobTrigger=await store.get("SELECT 1 valid FROM pg_trigger WHERE tgrelid='harbor.background_jobs'::regclass AND tgname='department_job_identity_immutable' AND tgenabled='O' AND NOT tgisinternal");if(!jobTrigger)fail('department_migration_conflict');
 for(const [table,required] of Object.entries({departments:['id','name','enabled','version','created','updated'],department_admin_keys:['id','department_id','digest','prefix','label','enabled','version','created','revoked_at'],department_settings:['department_id','name','value','version'],request_query_snapshots:['department_id','scope_identity']}))for(const name of required)if(!columns.has(table+'.'+name))fail('department_migration_conflict');
 for(const name of ['audit_id','department_id','scope_kind','scope_identity','actor_kind','actor_id','actor_version','subject_kind','subject_id'])if(!columns.has('audit_scopes.'+name))fail('department_migration_conflict');
 for(const name of ['audit_id','scope_kind','scope_identity','actor_kind','actor_id','actor_version']){const column=columns.get('audit_scopes.'+name);if(!column.attnotnull||column.type!==(name==='actor_version'?'bigint':'text')||column.default_value!==null)fail('department_migration_conflict');}
 if(!await store.get("SELECT 1 FROM pg_trigger WHERE tgrelid='harbor.audit_scopes'::regclass AND tgname='audit_scope_immutable' AND tgenabled='O' AND NOT tgisinternal"))fail('department_migration_conflict');
 if(!await store.get("SELECT 1 FROM pg_trigger WHERE tgrelid='harbor.automatic_handoff_groups'::regclass AND tgname='automatic_group_identity_immutable' AND tgenabled='O' AND NOT tgisinternal"))fail('department_migration_conflict');
 for(const [table,name] of [['audit','audit_scope_required'],['audit_scopes','audit_scope_delete_guard']])if(!await store.get("SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2 AND tgenabled='O' AND tgdeferrable AND tginitdeferred AND NOT tgisinternal",'harbor.'+table,name))fail('department_migration_conflict');
 const snapshot=columns.get('request_query_snapshots.scope_identity');if(!snapshot.attnotnull||snapshot.type!=='text'||snapshot.default_value!==null||columns.get('request_query_snapshots.department_id').attnotnull)fail('department_migration_conflict');
 const expected=[...sql.matchAll(/ALTER TABLE harbor\.([a-z_]+) ADD CONSTRAINT ([a-z_]+)/g)].map(match=>match[1]+'.'+match[2]);
 for(const table of ['routing_plans','balance_runs','nightly_group_results','nightly_cycle_ready','nightly_plan_revisions','nightly_plan_applications','nightly_plan_group_results','automatic_handoff_groups','session_handoffs'])expected.push(table+'.department_'+table+'_fk');
 for(const name of ['audit_scopes_pkey','department_audit_version','department_audit_scope','department_audit_actor','department_audit_subject'])expected.push('audit_scopes.'+name);
 const constraints=await store.all(`SELECT c.relname,t.conname,t.convalidated FROM pg_constraint t JOIN pg_class c ON c.oid=t.conrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND t.conname=ANY($1::text[])`,expected.map(key=>key.split('.')[1]));
 const actual=new Set(constraints.filter(row=>row.convalidated).map(row=>row.relname+'.'+row.conname));
 if(expected.some(key=>!actual.has(key)))fail('department_migration_conflict');
 for(const name of ['audit_scope_immutable','require_audit_scope','automatic_group_identity_immutable']){
  const body=sql.match(new RegExp('CREATE FUNCTION harbor\\.'+name+'\\(\\) RETURNS trigger LANGUAGE plpgsql AS \\$\\$([\\s\\S]*?)\\$\\$'))?.[1],routine=await store.get("SELECT p.prosrc,p.prosecdef,l.lanname FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=to_regprocedure($1)",'harbor.'+name+'()');
  if(!body||!routine||routine.prosrc.trim()!==body.trim()||routine.prosecdef||routine.lanname!=='plpgsql')fail('department_migration_conflict');
 }
 const immutable=await store.all(`SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='harbor' AND t.tgname='department_identity_immutable' AND t.tgenabled='O' AND NOT t.tgisinternal`);
 if([...scopedTables,'department_admin_keys'].some(table=>!immutable.some(row=>row.relname===table)))fail('department_migration_conflict');
 const retentionBody=sql.match(/CREATE OR REPLACE FUNCTION harbor\.apply_capture_retention\(\) RETURNS trigger LANGUAGE plpgsql AS \$\$([\s\S]*?)\$\$/)?.[1],retention=await store.get("SELECT p.prosrc,p.prosecdef,l.lanname FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=to_regprocedure('harbor.apply_capture_retention()')");
 if(!retentionBody||!retention||retention.prosrc.trim()!==retentionBody.trim()||retention.prosecdef||retention.lanname!=='plpgsql')fail('department_migration_conflict');
 const captureTriggers=await store.all("SELECT t.tgname,t.tgtype,t.tgattr::text attributes FROM pg_trigger t WHERE t.tgrelid='harbor.recordings'::regclass AND t.tgfoid=to_regprocedure('harbor.apply_capture_retention()') AND t.tgenabled='O' AND NOT t.tgisinternal AND t.tgqual IS NULL"),expiry=await store.get("SELECT attnum FROM pg_attribute WHERE attrelid='harbor.recordings'::regclass AND attname='expires_at' AND NOT attisdropped");
 if(!captureTriggers.some(row=>row.tgname==='recording_capture_retention'&&row.tgtype===7&&row.attributes==='')||!captureTriggers.some(row=>row.tgname==='recording_update_retention'&&row.tgtype===19&&row.attributes===String(expiry?.attnum)))fail('department_migration_conflict');
}

// False means a confirmed never-installed database. Partial DDL and unknown
// marker versions must never turn a scoped caller back into legacy global SQL.
export async function verifyDepartmentIsolationAvailability(store){
 const observed=await presence(store);
 if(observed.marker===null){if(observed.present)fail();verified.set(store,{available:false,marker:null});return false;}
 if(!observed.present)fail('department_migration_conflict');
 const source=await bundle();marker(observed.marker,source.checksum);await structure(store,source.sql);
 verified.set(store,{available:true,marker:observed.marker});return true;
}

export async function assertDepartmentIsolationReadiness(store,initialAvailability){
 if(typeof initialAvailability!=='boolean')fail();
 const observed=await presence(store);
 if(!initialAvailability){if(observed.present||observed.marker!==null)fail('department_restart_required');return false;}
 let prior=verified.get(store);
 if(!prior?.available){if(!await verifyDepartmentIsolationAvailability(store))fail('department_restart_required');prior=verified.get(store);}
 if(!observed.present||observed.marker===null||observed.marker!==prior.marker)fail('department_restart_required');
 return true;
}

export const departmentSettingNames=Object.freeze(['balancing','routing_strategy','routing_weights','routing_auto','routing_schedule','routing_schedule.consumption_api','routing_schedule.consumption_subscription','transfer_schedule','request_handoff_policy','account_package_policy','extra_usage_policy','recording.retention']);

export async function migrateDepartmentIsolation(store,{apply=false,runtimeRole}={}){
 if(typeof apply!=='boolean'||runtimeRole!==undefined&&(typeof runtimeRole!=='string'||!/^[a-z_][a-z0-9_]{0,62}$/.test(runtimeRole)))throw new AppError(400,'部门迁移参数无效','department_migration_configuration');
 const source=await bundle();
 try{return await store.transaction(async()=>{
  await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-department-isolation-v1',0))");
  if(await verifyDepartmentIsolationAvailability(store))return {changed:false,installed:true,complete:false};
  if(!apply)return {changed:false,installed:false,complete:false};
  let runtime=runtimeRole;
  if((await store.get("SELECT to_regclass('harbor_operator.activation') relation")).relation){
   const trusted=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton'),database=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');
   if(!trusted||trusted.database_oid!==database.oid||Object.hasOwn(trusted,'runtime')&&runtime!==undefined&&runtime!==trusted.runtime)fail('department_runtime_invalid');
   runtime=trusted.runtime??runtime;if(!runtime)fail('department_runtime_required');
  }
  if(runtime!==undefined){
   if(typeof runtime!=='string'||!/^[a-z_][a-z0-9_]{0,62}$/.test(runtime))fail('department_runtime_invalid');
   const role=await store.get('SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',runtime);
   if(!role||Object.values(role).some(Boolean)||await store.get('SELECT 1 FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1) OR member=(SELECT oid FROM pg_roles WHERE rolname=$1)',runtime)||await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=(SELECT oid FROM pg_roles WHERE rolname=$1) AND deptype='o' AND NOT (d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",runtime))fail('department_runtime_invalid');
  }
  await migrateRecordingRetention(store,{apply:true});
  await store.query(source.sql);
  await store.run(`INSERT INTO harbor.department_settings(department_id,name,value,version)
   SELECT $1,name,value::jsonb,COALESCE((value::jsonb->>'version')::bigint,0) FROM harbor.gateway_settings WHERE name=ANY($2::text[])`,defaultDepartmentId,departmentSettingNames);
  await store.run("INSERT INTO harbor.department_settings(department_id,name,value,version) VALUES($1,'recording.retention','{\"days\":30,\"version\":0}'::jsonb,0) ON CONFLICT(department_id,name) DO NOTHING",defaultDepartmentId);
  if(runtime!==undefined){const quoted='"'+runtime.replaceAll('"','""')+'"';await store.query('GRANT SELECT,INSERT,UPDATE ON harbor.departments,harbor.department_admin_keys,harbor.department_settings TO '+quoted);await store.query('GRANT SELECT,INSERT,DELETE ON harbor.audit_scopes TO '+quoted);}
  const value={contract:departmentIsolationContract,version:1,checksum:source.checksum,default_department_id:defaultDepartmentId};
  await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',setting,JSON.stringify(value));
  if(!await verifyDepartmentIsolationAvailability(store))fail();
  return {changed:true,installed:true,complete:false,default_department_id:defaultDepartmentId};
 });}catch(error){
  if(error instanceof AppError)throw error;
  if(['23503','23514','23502','23505'].includes(error.code))throw new AppError(503,'部门迁移遇到已有关系冲突，未改写原归属','department_relationship_conflict');
  throw new AppError(503,'部门迁移未完成，请核对权限和结构','department_migration_failed');
 }
}

const memberTypeDefaults=Object.freeze([
 ['A','复杂研发','适合处理复杂研发任务，开展技术方案设计和疑难问题排查。'],
 ['B','日常开发','适合完成日常功能开发、问题修复和测试工作。'],
 ['C','按方案开发','适合按照明确方案完成开发任务，并处理常规调试问题。'],
 ['D','开发辅助','适合辅助理解代码、完成简单修改，以及开展测试和学习。'],
 ['E','方案与原型','适合编写方案、准备演示，并制作原型页面。'],
 ['F','文档处理','适合整理、总结和改写文档，以及提取所需信息。'],
]);
function newDepartmentSettings(){return {
 balancing:balanceSettings(),routing_strategy:{weighted:false},routing_weights:{mode:'auto',weights:{},expires:null,version:0},
 transfer_schedule:transferSchedule(),request_handoff_policy:{mode:'request-boundary',version:1},account_package_policy:{enabled:false,smoothing_hours:24,version:0},
 'recording.retention':{days:30,version:0},
};}
const bad=()=>{throw new AppError(400,'部门或管理密钥参数无效','department_input_invalid');};
const denied=()=>{throw new AppError(403,'当前身份无权访问此部门','department_forbidden');};
function object(value,fields){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(name=>!fields.includes(name)))bad();return value;}
function text(value,max){if(typeof value!=='string'||!value.trim()||value.trim().length>max||/[\x00-\x1f\x7f]/.test(value))bad();return value.trim();}
function identifier(value){if(typeof value!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,99}$/.test(value))bad();return value;}
function version(value){if(!Number.isSafeInteger(value)||value<0)bad();return value;}
const publicKeyColumns='id,department_id,prefix,label,enabled,version,created,revoked_at';

export function createPostgresDepartments(store,{issuer,clock=Date.now,authorizeSuperAdmin}={}){
 if(!issuer||typeof issuer.departmentAdmin!=='function'||typeof issuer.system!=='function'||typeof clock!=='function'||authorizeSuperAdmin!==undefined&&typeof authorizeSuperAdmin!=='function')throw new AppError(503,'部门身份签发器未配置','department_identity_configuration');
 let initialized;
 async function ready({legacy=false}={}){
  if(initialized===undefined)initialized=await verifyDepartmentIsolationAvailability(store);
  await assertDepartmentIsolationReadiness(store,initialized);
  if(!initialized&&!legacy)fail();return initialized;
 }
 const superScope=scope=>requireDepartmentScope(scope,{allowGlobal:true,roles:['super_admin']});
 function target(scope,departmentId,{superOnly=false}={}){
  const trusted=superOnly?superScope(scope):requireDepartmentScope(scope,{allowGlobal:true});identifier(departmentId);
  if(trusted.view==='department'&&trusted.department_id!==departmentId)denied();return trusted;
 }
 async function audit(scope,action,departmentId,extra={}){
  await createPostgresAudit(store,{clock}).write(scope,{action,detail:{role:scope.role,credential_id:scope.credential_id,credential_version:scope.credential_version,department_id:departmentId,...extra},subject:{kind:'department',id:departmentId}});
 }
 async function current(trusted,{lock=false}={}){
  if(trusted.role==='super_admin')return trusted;
  const row=await store.get(`SELECT k.id,k.department_id,k.version FROM harbor.department_admin_keys k JOIN harbor.departments d ON d.id=k.department_id
   WHERE k.id=$1 AND k.department_id=$2 AND k.version=$3 AND k.enabled AND k.revoked_at IS NULL AND d.enabled`+(lock?' FOR SHARE OF d,k':''),trusted.credential_id,trusted.department_id,trusted.credential_version);
  if(!row)denied();return trusted;
 }
 async function listDepartments(scope,{limit=20,after}={}){
  const trusted=superScope(scope);if(trusted.view!=='global')denied();if(![20,50,100].includes(limit))bad();await ready();
  const args=[];let seek='';if(after!==undefined){object(after,['created','id']);version(after.created);identifier(after.id);args.push(after.created,after.id);seek=' WHERE (d.created,d.id)<($1,$2)';}
  args.push(limit+1);const rows=await store.all(`SELECT d.*,(SELECT COUNT(*) FROM harbor.members m WHERE m.department_id=d.id AND m.removed_at IS NULL) members,
   (SELECT COUNT(*) FROM harbor.accounts a WHERE a.department_id=d.id AND a.removed_at IS NULL) accounts,
   (SELECT COUNT(*) FROM harbor.department_admin_keys k WHERE k.department_id=d.id AND k.enabled AND k.revoked_at IS NULL) management_keys
   FROM harbor.departments d${seek} ORDER BY d.created DESC,d.id DESC LIMIT $${args.length}`,...args);
  const selected=rows.slice(0,limit),last=selected.at(-1);return {rows:selected,next:rows.length>limit?{created:last.created,id:last.id}:null};
 }
 async function getDepartment(scope,departmentId){
  const base=requireDepartmentScope(scope,{allowGlobal:true}),selected=departmentId??base.department_id,trusted=target(base,selected);await ready();await current(trusted);
  const row=await store.get('SELECT id,name,enabled,version,created,updated FROM harbor.departments WHERE id=$1',selected);if(!row)throw new AppError(404,'部门不可用','department_not_found');return row;
 }
 async function createDepartment(scope,payload){
  const trusted=superScope(scope);if(trusted.view!=='global')denied();object(payload,['name']);const name=text(payload.name,60);await ready();
  return store.transaction(async()=>{
   const departmentId=id(),at=clock(),row=await store.get('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$2,$3,$3) RETURNING *',departmentId,name,at);
   // Take a fresh platform-default snapshot, never another department's live
   // settings, account references, credentials, quotas or configured template.
   for(const [name,value] of Object.entries(newDepartmentSettings()))await store.run('INSERT INTO harbor.department_settings(department_id,name,value,version) VALUES($1,$2,$3::jsonb,$4)',departmentId,name,JSON.stringify(value),value.version??0);
   for(const [code,typeName,description] of memberTypeDefaults){
    const typeId=id();await store.run('INSERT INTO harbor.member_types(id,department_id,code,name,description) VALUES($1,$2,$3,$4,$5)',typeId,departmentId,code,typeName,description);
    await store.run('INSERT INTO harbor.member_type_templates(department_id,type_id,version,created,allowed_models,usage_limits) VALUES($1,$2,1,$3,$4::jsonb,$5::jsonb)',departmentId,typeId,at,'null',JSON.stringify({enabled:false,rules:[]}));
    await store.run('UPDATE harbor.member_types SET default_version=1,updated=$1 WHERE department_id=$2 AND id=$3',at,departmentId,typeId);
   }
   await audit(trusted,'department.create',departmentId);return row;
  });
 }
 async function updateDepartment(scope,departmentId,payload){
  const trusted=target(scope,departmentId,{superOnly:true});object(payload,['name','enabled','expected_version']);version(payload.expected_version);
  if(payload.name!==undefined)text(payload.name,60);if(payload.enabled!==undefined&&typeof payload.enabled!=='boolean')bad();if(payload.name===undefined&&payload.enabled===undefined)bad();await ready();
  return store.transaction(async()=>{
   const row=await store.get('SELECT * FROM harbor.departments WHERE id=$1 FOR UPDATE',departmentId);if(!row)throw new AppError(404,'部门不可用','department_not_found');
   if(row.version!==payload.expected_version)throw new AppError(409,'部门已变化，请刷新后重试','department_version_conflict');
   const enabled=payload.enabled??row.enabled,name=payload.name===undefined?row.name:text(payload.name,60);
   if(enabled===row.enabled&&name===row.name)return row;
   const next=await store.get('UPDATE harbor.departments SET name=$1,enabled=$2,version=version+1,updated=$3 WHERE id=$4 RETURNING *',name,enabled,clock(),departmentId);
   // Revoke old management sessions even if their Cookie was never presented
   // during the disabled interval. Re-enabling a department never re-enables keys.
   if(row.enabled&&!enabled)await store.run('UPDATE harbor.department_admin_keys SET version=version+1 WHERE department_id=$1',departmentId);
   await audit(trusted,'department.update',departmentId,{enabled,version:next.version});return next;
  });
 }
 async function issueManagementKey(scope,departmentId,payload){
  const trusted=target(scope,departmentId,{superOnly:true});object(payload,['label']);const label=text(payload.label,120);await ready();
  return store.transaction(async()=>{
   if(!await store.get('SELECT id FROM harbor.departments WHERE id=$1 FOR SHARE',departmentId))throw new AppError(404,'部门不可用','department_not_found');
   const material=issueDepartmentManagementKey(),key=await store.get(`INSERT INTO harbor.department_admin_keys(id,department_id,digest,prefix,label,created) VALUES($1,$2,$3,$4,$5,$6) RETURNING ${publicKeyColumns}`,id(),departmentId,material.digest,material.prefix,label,clock());
   await audit(trusted,'department.management_key.issue',departmentId,{key_id:key.id});return {key,secret:material.secret};
  });
 }
 async function listManagementKeys(scope,departmentId,{limit=20,after}={}){
  target(scope,departmentId,{superOnly:true});if(![20,50,100].includes(limit))bad();await ready();
  const args=[departmentId];let seek='';if(after!==undefined){object(after,['created','id']);version(after.created);identifier(after.id);args.push(after.created,after.id);seek=' AND (created,id)<($2,$3)';}
  args.push(limit+1);const rows=await store.all(`SELECT ${publicKeyColumns} FROM harbor.department_admin_keys WHERE department_id=$1${seek} ORDER BY created DESC,id DESC LIMIT $${args.length}`,...args),selected=rows.slice(0,limit),last=selected.at(-1);
  return {rows:selected,next:rows.length>limit?{created:last.created,id:last.id}:null};
 }
 async function revokeManagementKey(scope,departmentId,keyId,payload){
  const trusted=target(scope,departmentId,{superOnly:true});identifier(keyId);object(payload,['expected_version']);version(payload.expected_version);await ready();
  return store.transaction(async()=>{
   if(!await store.get('SELECT id FROM harbor.departments WHERE id=$1 FOR SHARE',departmentId))throw new AppError(404,'部门不可用','department_not_found');
   const row=await store.get(`SELECT ${publicKeyColumns} FROM harbor.department_admin_keys WHERE department_id=$1 AND id=$2 FOR UPDATE`,departmentId,keyId);if(!row)throw new AppError(404,'管理密钥不可用','department_management_key_not_found');
   if(row.version!==payload.expected_version)throw new AppError(409,'管理密钥已变化，请刷新后重试','department_key_version_conflict');if(!row.enabled)return row;
   const next=await store.get(`UPDATE harbor.department_admin_keys SET enabled=FALSE,revoked_at=$1,version=version+1 WHERE department_id=$2 AND id=$3 RETURNING ${publicKeyColumns}`,clock(),departmentId,keyId);
   await audit(trusted,'department.management_key.revoke',departmentId,{key_id:keyId});return next;
  });
 }
 async function authenticateManagementKey(secret){
  const digest=departmentManagementKeyDigest(secret);if(digest===null)return null;if(!await ready({legacy:true}))return null;
  const row=await store.get(`SELECT k.id,k.department_id,k.version FROM harbor.department_admin_keys k JOIN harbor.departments d ON d.id=k.department_id
   WHERE k.digest=$1 AND k.enabled AND k.revoked_at IS NULL AND d.enabled`,digest);
  return row?issuer.departmentAdmin({departmentId:row.department_id,credentialId:row.id,credentialVersion:row.version}):null;
 }
 async function validateManagementSession(scope){
  let trusted;try{trusted=requireDepartmentScope(scope,{roles:['department_admin']});}catch(error){if(error.status===403)return null;throw error;}
  if(!await ready({legacy:true}))return null;
  const row=await store.get(`SELECT k.id,k.department_id,k.version FROM harbor.department_admin_keys k JOIN harbor.departments d ON d.id=k.department_id
   WHERE k.id=$1 AND k.department_id=$2 AND k.version=$3 AND k.enabled AND k.revoked_at IS NULL AND d.enabled`,trusted.credential_id,trusted.department_id,trusted.credential_version);
  return row?issuer.departmentAdmin({departmentId:row.department_id,credentialId:row.id,credentialVersion:row.version}):null;
 }
 async function readSetting(scope,departmentId,name){
  const trusted=target(scope,departmentId);if(!departmentSettingNames.includes(name))bad();await ready();await current(trusted);
  const row=await store.get('SELECT value,version FROM harbor.department_settings WHERE department_id=$1 AND name=$2',departmentId,name);return row??null;
 }
 async function authorizeActor(metadata,{lock=false}={}){
  const {department_id:departmentId,scope_kind:scopeKind,scope_identity:scopeIdentity,actor_kind:role,actor_id:credentialId,actor_version:credentialVersion,kind}=metadata;
  if(!departmentJobKinds.includes(kind)&&kind!=='session.handoff'||!Number.isSafeInteger(credentialVersion)||credentialVersion<0||typeof credentialId!=='string'||!credentialId.length||credentialId.length>200)denied();
  const global=scopeKind==='global'&&departmentId===null&&scopeIdentity==='global';
  if(!global&&(scopeKind!=='department'||typeof departmentId!=='string'||scopeIdentity!=='department:'+departmentId))denied();
  if(role==='system'){
   if(credentialVersion!==0||credentialId!=='system:'+kind&&!(credentialId==='migration-v1'&&systemGlobalKinds.has(kind))||(global?!systemGlobalKinds.has(kind):!systemDepartmentKinds.has(kind)&&!systemGlobalKinds.has(kind)))denied();
   if(!global&&!await store.get('SELECT id FROM harbor.departments WHERE id=$1 AND enabled'+(lock?' FOR SHARE':''),departmentId))denied();
   return issuer.system({purpose:kind,departmentId});
  }
  if(role!=='super_admin'&&role!=='department_admin'||global&&(role!=='super_admin'||kind!=='recording.export')||systemGlobalKinds.has(kind))denied();
  if(!global&&!await store.get('SELECT id FROM harbor.departments WHERE id=$1 AND enabled'+(lock?' FOR SHARE':''),departmentId))denied();
  if(role==='department_admin'){
   const key=await store.get('SELECT id FROM harbor.department_admin_keys WHERE id=$1 AND department_id=$2 AND version=$3 AND enabled AND revoked_at IS NULL'+(lock?' FOR SHARE':''),credentialId,departmentId,credentialVersion);if(!key)denied();
   return issuer.departmentAdmin({departmentId,credentialId,credentialVersion});
  }
  if(typeof authorizeSuperAdmin!=='function')throw new AppError(503,'后台任务尚未配置当前超管身份校验','department_super_admin_unavailable');
  const authorized=await authorizeSuperAdmin({credentialId,credentialVersion});if(!authorized)denied();
  const root=requireDepartmentScope(authorized,{allowGlobal:true,roles:['super_admin']});if(root.view!=='global'||root.credential_id!==credentialId||root.credential_version!==credentialVersion)denied();
  const currentRoot=issuer.superAdmin({credentialId,credentialVersion});return global?currentRoot:issuer.forDepartment(currentRoot,departmentId);
 }
 async function authorizeJobEnqueue(scope,kind){
  await ready();if(!store.context.getStore())throw new AppError(503,'任务授权必须位于入队事务内','database_transaction_required');
  const trusted=requireDepartmentScope(scope,{allowGlobal:true,allowSystem:true,roles:['super_admin','department_admin','system']});
  const metadata={department_id:trusted.department_id,scope_kind:trusted.view,scope_identity:trusted.view==='global'?'global':'department:'+trusted.department_id,actor_kind:trusted.role,actor_id:trusted.credential_id,actor_version:trusted.credential_version,kind};
  await authorizeActor(metadata,{lock:true});return Object.fromEntries(departmentJobFields.map(field=>[field,metadata[field]]));
 }
 async function authorizePersistedActor(job,{requireLease=true}={}){
  if(typeof requireLease!=='boolean'||!job||typeof job.id!=='string'||!job.id.length||job.id.length>200)bad();await ready();
  const row=await store.get('SELECT id,kind,'+departmentJobFields.join(',')+',status,claim_token,lease_until FROM harbor.background_jobs WHERE id=$1',job.id);
  if(!row)throw new AppError(404,'后台任务不存在','job_not_found');
  if(requireLease&&(typeof job.claim_token!=='string'||row.status!=='running'||row.claim_token!==job.claim_token||!Number.isSafeInteger(row.lease_until)||row.lease_until<=clock()))throw new AppError(409,'后台任务租约已失效','job_lease_lost');
  return authorizeActor(row);
 }
 async function authorizeHandoffActor(handoffId,{lock=false}={}){
  if(lock&&!store.context.getStore())throw new AppError(503,'交接身份锁必须位于事务内','database_transaction_required');
  identifier(handoffId);await ready();
  const row=await store.get('SELECT department_id,actor_kind,actor_id,actor_version,origin FROM harbor.session_handoffs WHERE id=$1',handoffId);
  if(!row||row.origin!=='manual')denied();
  const actor=await authorizeActor({...row,kind:'session.handoff',scope_kind:'department',scope_identity:'department:'+row.department_id},{lock});
  if(lock){const current=await store.get('SELECT department_id,actor_kind,actor_id,actor_version,origin FROM harbor.session_handoffs WHERE id=$1 FOR SHARE',handoffId);if(!current||Object.keys(row).some(key=>row[key]!==current[key]))denied();}
  return actor;
 }
 async function authorizeNightlyPlanActor(planId,{lock=false}={}){
  if(typeof planId!=='string'||!planId.length||planId.length>200)bad();
  if(lock&&!store.context.getStore())throw new AppError(503,'夜间身份锁必须位于事务内','database_transaction_required');
  await ready();
  const sql='SELECT p.department_id plan_department,j.kind,j.status,j.target,'+departmentJobFields.map(name=>'j.'+name).join(',')+' FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id WHERE p.id=$1';
  const row=await store.get(sql,planId);
  if(!row||row.kind!=='routing.nightly'||row.status!=='completed'||row.actor_kind!=='system'||row.actor_id!=='system:routing.nightly'||row.actor_version!==0||row.scope_kind!=='department'||row.plan_department!==row.department_id)denied();
  // The same actor validator used by persisted jobs, fed only an actual DB row.
  const actor=await authorizeActor(row,{lock});
  if(lock){const actual=await store.get(sql+' FOR SHARE OF p,j',planId);if(!actual||Object.keys(row).some(key=>row[key]!==actual[key]))denied();}
  return actor;
 }
 async function authorizeAutomaticGroupActor(groupId,{lock=false,finalize=false}={}){
  if(lock&&!store.context.getStore())throw new AppError(503,'交接身份锁必须位于事务内','database_transaction_required');
  identifier(groupId);await ready();
  const row=await store.get("SELECT g.department_id,g.plan_id,g.slot,j.target FROM harbor.automatic_handoff_groups g JOIN harbor.routing_plans p ON p.id=g.plan_id AND p.department_id=g.department_id JOIN harbor.background_jobs j ON j.id=p.id AND j.department_id=p.department_id WHERE g.id=$1 AND EXISTS(SELECT 1 FROM harbor.routing_plan_items i WHERE i.plan_id=p.id AND i.section='groups' AND i.ordinal=g.group_ordinal)",groupId);
  if(!row||row.slot!==row.target)denied();
  let actor;
  if(finalize){
   const actual=await store.get("SELECT j.id FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id AND j.department_id=p.department_id JOIN harbor.departments d ON d.id=p.department_id WHERE p.id=$1 AND p.department_id=$2 AND j.kind='routing.nightly' AND j.status='completed' AND j.actor_kind='system' AND j.actor_id='system:routing.nightly' AND j.actor_version=0 AND j.scope_kind='department' AND j.scope_identity='department:'||p.department_id"+(lock?' FOR SHARE OF d,p,j':''),row.plan_id,row.department_id);
   if(!actual)denied();actor=issuer.system({purpose:'routing.nightly',departmentId:row.department_id});
  }else actor=await authorizeNightlyPlanActor(row.plan_id,{lock});
  if(actor.department_id!==row.department_id)denied();
  // Group identity is immutable; decision rows are locked later, after members.
  return actor;
 }
 const service={listDepartments,getDepartment,createDepartment,updateDepartment,issueManagementKey,listManagementKeys,revokeManagementKey,authenticateManagementKey,validateManagementSession,readSetting,authorizeJobEnqueue,authorizePersistedActor,authorizeHandoffActor,authorizeNightlyPlanActor,authorizeAutomaticGroupActor};actorServices.set(service,store);return service;
}
