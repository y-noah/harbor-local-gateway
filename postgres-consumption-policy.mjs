import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {AppError,id} from './store.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {readConsumptionPolicy,updateConsumptionPolicy} from './consumption-policy.mjs';

const setting='schema.key-consumption-policy',migrationUrl=new URL('./migrations/key-consumption-policy.sql',import.meta.url),verified=new WeakSet();
const identifier=value=>'"'+value.replaceAll('"','""')+'"';
const roleName=value=>typeof value==='string'&&/^[a-z_][a-z0-9_]{0,62}$/.test(value);
const conflict=()=>new AppError(503,'Key消耗来源迁移校验不一致','database_migration_conflict');
const missing=()=>new AppError(503,'请先安装Key消耗来源迁移','database_migration_required');
const invalid=()=>{throw new AppError(400,'消耗来源记录无效','consumption_metadata_invalid');};
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const safeInteger=value=>Number.isSafeInteger(value)&&value>=0;
async function bundle(){const sql=await readFile(migrationUrl,'utf8');return {sql,checksum:createHash('sha256').update(sql).digest('hex')};}
const columns={keys:[['consumption_policy','text',true,"'inherit'::text"],['consumption_policy_version','bigint',true,'0']],requests:[['consumption_policy','text',false,null],['consumption_policy_version','bigint',false,null],['consumption_source','text',false,null],['consumption_fallback','boolean',false,null],['consumption_attempts','jsonb',false,null]]};
const functions=[['key_consumption_valid','policy text, version bigint','sql'],['consumption_attempts_valid','attempts jsonb','plpgsql'],['request_consumption_valid','policy text, version bigint, source text, fallback boolean, attempts jsonb','plpgsql']];

async function runtimeRole(store,explicit){
 if(explicit!==undefined&&!roleName(explicit))throw new AppError(400,'消耗来源运行角色无效','consumption_runtime_invalid');
 let role=explicit;
 if((await store.get("SELECT to_regclass('harbor_operator.activation') relation")).relation){
  const trusted=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton'),database=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');
  if(!trusted||trusted.database_oid!==database.oid)throw new AppError(409,'消耗来源运行角色与受信部署不一致','consumption_runtime_invalid');
  if(Object.hasOwn(trusted,'runtime')){
   if(explicit!==undefined&&explicit!==trusted.runtime)throw new AppError(409,'消耗来源运行角色与受信部署不一致','consumption_runtime_invalid');role=trusted.runtime;
  }else if(trusted.local_contract!=='local-assurance-v1'||trusted.state!=='pending'||typeof trusted.local_release_id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(trusted.local_release_id)||explicit===undefined)throw new AppError(409,'本机保障消耗来源迁移须指定已核实的运行角色','consumption_runtime_invalid');
 }
 if(!roleName(role))throw new AppError(400,'请指定受控运行角色或提供受信部署记录','consumption_runtime_required');
 const actual=await store.get('SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',role);
 if(!actual||Object.values(actual).some(Boolean)||await store.get('SELECT 1 FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1) OR member=(SELECT oid FROM pg_roles WHERE rolname=$1)',role)||await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=(SELECT oid FROM pg_roles WHERE rolname=$1) AND deptype='o' AND NOT (d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",role))throw new AppError(409,'消耗来源运行角色超出最小权限','consumption_runtime_invalid');
 if((await store.get("SELECT has_schema_privilege($1,'harbor','CREATE') OR has_schema_privilege($1,'public','CREATE') unsafe",role)).unsafe)throw new AppError(409,'消耗来源运行角色不得创建schema对象','consumption_runtime_invalid');
 return role;
}
async function grantRuntime(store,role){
 const quoted=identifier(role);
 await store.query('GRANT USAGE ON SCHEMA harbor TO '+quoted);
 for(const [table,fields] of Object.entries(columns)){
  const names=fields.map(([name])=>name).join(',');
  await store.query('GRANT SELECT ('+names+'),UPDATE ('+names+') ON harbor.'+table+' TO '+quoted);
 }
 for(const [name,args] of functions){
  const signature='harbor.'+name+'('+args.split(', ').map(value=>value.split(' ').at(-1)).join(',')+')';
  await store.query('REVOKE ALL ON FUNCTION '+signature+' FROM PUBLIC');await store.query('GRANT EXECUTE ON FUNCTION '+signature+' TO '+quoted);
 }
}
async function savedMigration(store){
 const row=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting);if(!row)return null;
 let value;try{value=JSON.parse(row.value);}catch{throw conflict();}
 if(!object(value)||Object.keys(value).some(key=>!['version','checksum','runtimeRole','ownerRole'].includes(key))||value.version!==1||!roleName(value.runtimeRole)||!roleName(value.ownerRole)||value.runtimeRole===value.ownerRole)throw conflict();
 return value;
}
export async function verifyConsumptionPolicyAvailability(store,{required=false}={}){
 if(typeof required!=='boolean')throw new TypeError('consumption_required_invalid');
 verified.delete(store);const saved=await savedMigration(store);
 if(!saved){
  const orphan=await store.get("SELECT 1 FROM pg_attribute WHERE attrelid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass]) AND attname LIKE 'consumption_%' AND attnum>0 AND NOT attisdropped LIMIT 1");
  if(orphan)throw conflict();if(required)throw missing();return false;
 }
 const source=await bundle();if(saved.checksum!==source.checksum)throw conflict();
 await verifyConsumptionPolicySchema(store,{sql:source.sql,ownerRole:saved.ownerRole,runtimeRole:saved.runtimeRole});verified.add(store);return true;
}
export async function migrateConsumptionPolicy(store,{apply=false,runtimeRole:explicit}={}){
 if(typeof apply!=='boolean')throw new TypeError('consumption_apply_invalid');
 if(!apply){await verifyConsumptionPolicyAvailability(store);return false;}
 const changed=await store.transaction(async()=>{
  await store.query('SELECT pg_advisory_xact_lock($1)',[createHash('sha256').update('harbor-postgres-migrations').digest().readInt32BE()]);
  const saved=await savedMigration(store);if(saved){if(explicit!==undefined&&explicit!==saved.runtimeRole)throw new AppError(409,'消耗来源运行角色不一致','consumption_runtime_invalid');await runtimeRole(store,saved.runtimeRole);await verifyConsumptionPolicyAvailability(store,{required:true});return false;}
  await verifyConsumptionPolicyAvailability(store);
  const role=await runtimeRole(store,explicit),owner=await store.get('SELECT current_user name');
  const tables=await store.all("SELECT c.relname,pg_get_userbyid(c.relowner) owner FROM pg_class c WHERE c.oid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass,'harbor.gateway_settings'::regclass])");
  if(!roleName(owner.name)||owner.name===role||tables.length!==3||tables.some(row=>row.owner!==owner.name))throw new AppError(409,'消耗来源迁移必须由原表owner执行','consumption_migration_owner_invalid');
  const source=await bundle();await store.query(source.sql);await grantRuntime(store,role);
  await verifyConsumptionPolicySchema(store,{sql:source.sql,ownerRole:owner.name,runtimeRole:role});
  await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',setting,JSON.stringify({version:1,checksum:source.checksum,runtimeRole:role,ownerRole:owner.name}));return true;
 });
 verified.add(store);return changed;
}
export async function verifyConsumptionPolicySchema(store,{sql,ownerRole,runtimeRole:role}={}){
 sql??=(await bundle()).sql;
 if(!roleName(ownerRole)||!roleName(role))throw conflict();
 const actual=await store.all("SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod) type,a.attnotnull,pg_get_expr(d.adbin,d.adrelid) default_value,c.relkind,c.relpersistence,pg_get_userbyid(c.relowner) owner FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE a.attrelid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass]) AND a.attname LIKE 'consumption_%' AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum");
 const expected=Object.entries(columns).flatMap(([table,fields])=>fields.map(([name,type,notnull,defaultValue])=>({relname:table,attname:name,type,attnotnull:notnull,default_value:defaultValue,relkind:'r',relpersistence:'p',owner:ownerRole})));
 if(JSON.stringify(actual)!==JSON.stringify(expected))throw conflict();
 const constraints=await store.all("SELECT c.relname,co.conname,pg_get_constraintdef(co.oid) def,co.convalidated,co.connoinherit FROM pg_constraint co JOIN pg_class c ON c.oid=co.conrelid WHERE co.conrelid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass]) AND co.conname=ANY($1::text[]) ORDER BY c.relname",['keys_consumption_policy_valid','requests_consumption_policy_valid']);
 const definitions=['CHECK (harbor.key_consumption_valid(consumption_policy, consumption_policy_version))','CHECK (harbor.request_consumption_valid(consumption_policy, consumption_policy_version, consumption_source, consumption_fallback, consumption_attempts))'];
 if(constraints.length!==2||constraints.some((row,i)=>row.relname!==['keys','requests'][i]||row.conname!==['keys_consumption_policy_valid','requests_consumption_policy_valid'][i]||row.def!==definitions[i]||!row.convalidated||row.connoinherit))throw conflict();
 const bodies=[...sql.matchAll(/CREATE FUNCTION harbor\.(\w+)\([^]*?AS \$consumption\$\n([^]*?)\n\$consumption\$;/g)];
 const routines=await store.all("SELECT p.oid::text oid,p.proname,p.prosrc,p.prosecdef,p.proisstrict,p.provolatile,p.proparallel,p.proconfig,l.lanname,pg_get_function_identity_arguments(p.oid) args,pg_get_function_result(p.oid) result,pg_get_userbyid(p.proowner) owner FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='harbor' AND p.proname=ANY($1::text[])",functions.map(([name])=>name));
 if(bodies.length!==3||routines.length!==3)throw conflict();
 for(const [name,args,language] of functions){
  const row=routines.find(value=>value.proname===name),body=bodies.find(([,n])=>n===name)?.[2];
  if(!row||row.prosrc.trim()!==body?.trim()||row.prosecdef||row.proisstrict||row.provolatile!=='i'||row.proparallel!=='u'||row.lanname!==language||row.args!==args||row.result!=='boolean'||row.owner!==ownerRole||JSON.stringify(row.proconfig)!==JSON.stringify(['search_path=pg_catalog, harbor']))throw conflict();
  const permission=await store.get("SELECT has_function_privilege($1,$2::oid,'EXECUTE') runtime,EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid=$2::oid AND a.grantee=0 AND a.privilege_type='EXECUTE') public",role,row.oid);
  if(!permission.runtime||permission.public)throw conflict();
 }
 for(const [table,fields] of Object.entries(columns))for(const [field] of fields){const permissions=await store.get("SELECT has_column_privilege($1,$2,$3,'SELECT') readable,has_column_privilege($1,$2,$3,'UPDATE') writable",role,'harbor.'+table,field);if(!permissions.readable||!permissions.writable)throw conflict();}
 return true;
}

export function normalizeConsumptionAttempts(attempts){
 if(!Array.isArray(attempts)||attempts.length>2)invalid();
 return Array.from(attempts,(item,index)=>{
  if(!object(item)||Object.keys(item).some(key=>!['source','account_id','state','usage_known','started','ended','error_code','input','output','cached','dispatched'].includes(key))||!['api','subscription'].includes(item.source)||typeof item.account_id!=='string'||!/^[a-zA-Z0-9-]{1,80}$/.test(item.account_id)||!['dispatched','rejected','completed','failed','unknown'].includes(item.state)||typeof item.usage_known!=='boolean'||!safeInteger(item.started))invalid();
  if(item.dispatched!==undefined&&typeof item.dispatched!=='boolean')invalid();
  if(item.dispatched===false&&(!['failed','rejected'].includes(item.state)||!item.usage_known||['input','output','cached'].some(field=>item[field]!==0)))invalid();
  for(const field of ['ended','input','output','cached'])if(item[field]!==undefined&&item[field]!==null&&!safeInteger(item[field]))invalid();
  if(item.ended!==undefined&&item.ended!==null&&item.ended<item.started||item.error_code!==undefined&&item.error_code!==null&&(typeof item.error_code!=='string'||!/^[a-z][a-z0-9_]{0,79}$/.test(item.error_code)))invalid();
  if(!item.usage_known&&['input','output','cached'].some(field=>item[field]!==undefined&&item[field]!==null))invalid();
  if(attempts.length===2&&(index===0&&(item.source!=='api'||item.state!=='rejected'||!item.usage_known||item.error_code!=='upstream_quota_exhausted'||item.dispatched===false)||index===1&&item.source!=='subscription'))invalid();
  return Object.fromEntries(Object.entries(item).filter(([,value])=>value!==undefined));
 });
}
export function createPostgresConsumptionPolicy(store,{clock=Date.now}={}){
 const ensure=async()=>{if(!verified.has(store))await verifyConsumptionPolicyAvailability(store,{required:true});};
 const keyIdValid=value=>{if(typeof value!=='string'||!value.length||value.length>80)invalid();};
 async function read(keyId,{memberId,lock=false}={}){
  keyIdValid(keyId);if(typeof lock!=='boolean'||lock&&!store.context.getStore())throw new AppError(503,'消耗来源锁必须位于请求事务内','database_transaction_required');
  await ensure();
  // Same member -> Key order as authenticate/settlement; no lock crosses IO.
  const owner=await store.get('SELECT member_id FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId);
  if(!owner||memberId!==undefined&&memberId!==owner.member_id)throw new AppError(404,'Key不存在或不属于该员工','key_not_found');
  const member=await store.get('SELECT id FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(lock?' FOR SHARE':''),owner.member_id);if(!member)throw new AppError(404,'员工不存在或已移除','member_not_found');
  const key=await store.get('SELECT consumption_policy,consumption_policy_version FROM harbor.keys WHERE id=$1 AND member_id=$2 AND removed_at IS NULL'+(lock?' FOR SHARE':''),keyId,owner.member_id);if(!key)throw new AppError(404,'Key不存在或已删除','key_not_found');
  return readConsumptionPolicy(key,{allowLegacy:false});
 }
 async function set(keyId,value,{scope}={}){
  keyIdValid(keyId);if(!object(value)||Object.keys(value).some(key=>!['consumption_policy','expected_consumption_policy_version'].includes(key)))invalid();
  await ensure();const departments=createAdminDepartments(store),authorization=await departments.resolve(scope,{write:true});return store.transaction(async()=>{
   await departments.current(authorization,{lock:true});
   const owner=await store.get('SELECT member_id FROM harbor.keys WHERE id=$1 AND removed_at IS NULL',keyId);if(!owner)throw new AppError(404,'Key不存在或已删除','key_not_found');
   if(authorization)departments.same(authorization,await store.get('SELECT department_id FROM harbor.members WHERE id=$1',owner.member_id));
   const member=await store.get('SELECT id FROM harbor.members WHERE id=$1 AND removed_at IS NULL FOR UPDATE',owner.member_id);if(!member)throw new AppError(404,'员工不存在或已移除','member_not_found');
   const current=await store.get('SELECT consumption_policy,consumption_policy_version FROM harbor.keys WHERE id=$1 AND member_id=$2 AND removed_at IS NULL FOR UPDATE',keyId,owner.member_id);if(!current)throw new AppError(404,'Key不存在或已删除','key_not_found');
   const next=updateConsumptionPolicy(current,value.consumption_policy,value.expected_consumption_policy_version);
   if(next.consumption_policy_version===current.consumption_policy_version)return next;
   const result=await store.run('UPDATE harbor.keys SET consumption_policy=$1,consumption_policy_version=$2 WHERE id=$3 AND member_id=$4 AND consumption_policy_version=$5',next.consumption_policy,next.consumption_policy_version,keyId,owner.member_id,current.consumption_policy_version);if(result.changes!==1)throw new AppError(409,'消耗来源已变化，请重新读取','consumption_policy_conflict');
   await createPostgresAudit(store,{clock}).write(authorization,{action:'key.consumption_policy',detail:{key:keyId,member:owner.member_id,previous:current,...next},subject:{kind:'key',id:keyId}});return next;
  });
 }
 // Internal admission hook only: caller supplies the authenticated snapshot,
 // never request JSON. Locking the Key and rechecking identity is caller-owned.
 async function recordAdmission(requestId,keyId,snapshot){
  keyIdValid(requestId);keyIdValid(keyId);const policy=readConsumptionPolicy(snapshot,{allowLegacy:false});
  if(!store.context.getStore())throw new AppError(503,'消耗来源快照必须位于受理事务内','database_transaction_required');await ensure();
  const result=await store.run('UPDATE harbor.requests r SET consumption_policy=$1,consumption_policy_version=$2,consumption_source=NULL,consumption_fallback=FALSE,consumption_attempts=\'[]\'::jsonb WHERE r.id=$3 AND r.key_id=$4 AND r.consumption_policy IS NULL AND EXISTS(SELECT 1 FROM harbor.keys k WHERE k.id=r.key_id AND k.member_id=r.member_id AND k.consumption_policy=$1 AND k.consumption_policy_version=$2 AND k.removed_at IS NULL)',policy.consumption_policy,policy.consumption_policy_version,requestId,keyId);
  if(result.changes!==1)throw new AppError(409,'请求消耗来源快照不一致','consumption_policy_conflict');return policy;
 }
 async function recordAttempts(requestId,keyId,attempts){
  keyIdValid(requestId);keyIdValid(keyId);const rows=normalizeConsumptionAttempts(attempts);await ensure();
  return store.transaction(async()=>{
   const current=await store.get('SELECT consumption_attempts FROM harbor.requests WHERE id=$1 AND key_id=$2 AND consumption_policy IS NOT NULL FOR UPDATE',requestId,keyId);if(!current)throw new AppError(409,'请求消耗来源快照不存在','consumption_policy_conflict');
   const previous=normalizeConsumptionAttempts(current.consumption_attempts);
   // A later phase may add evidence, but never erase a prior dispatch or its
   // known usage. Transport truth and account identity are caller-owned.
   if(rows.length<previous.length||previous.some((prior,index)=>{
    const next=rows[index],states={dispatched:['dispatched','rejected','completed','failed','unknown'],rejected:['rejected'],completed:['completed'],failed:['failed'],unknown:['unknown','completed','failed']};
    return ['source','account_id','started'].some(field=>prior[field]!==next[field])||prior.dispatched!==false&&next.dispatched===false||!states[prior.state].includes(next.state)||prior.usage_known&&!next.usage_known||['input','output','cached','ended','error_code'].some(field=>prior[field]!==undefined&&prior[field]!==null&&prior[field]!==next[field]);
   }))throw new AppError(409,'已保存的派发及用量证据不能覆盖','consumption_metadata_conflict');
   const sent=rows.filter(item=>item.dispatched!==false);
   await store.run('UPDATE harbor.requests SET consumption_source=$1,consumption_fallback=$2,consumption_attempts=$3::jsonb WHERE id=$4 AND key_id=$5',sent.at(-1)?.source??null,sent.length===2,JSON.stringify(rows),requestId,keyId);
  });
 }
 return {read,set,recordAdmission,recordAttempts};
}
