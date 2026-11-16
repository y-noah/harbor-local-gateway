import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {AppError,encryptStoredKey,revealStoredKey,hash} from './store.mjs';
import {departmentManagementKeyDigest} from './department-access.mjs';

const setting='schema.department-key-secrets',table='harbor.department_admin_key_secrets',migration=new URL('./migrations/department-key-secrets.sql',import.meta.url);
const roleName=value=>typeof value==='string'&&/^[a-z_][a-z0-9_]{0,62}$/.test(value);
const quoted=value=>'"'+value.replaceAll('"','""')+'"';
const conflict=()=>new AppError(503,'部门管理密钥存储结构不一致','department_key_storage_conflict');
const unavailable=()=>new AppError(503,'部门管理密钥无法解密，请核对主密钥','department_key_decryption_unavailable');
async function bundle(){const sql=await readFile(migration,'utf8');return {sql,checksum:createHash('sha256').update(sql).digest('hex')};}
async function saved(store){
 const row=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting);if(!row)return null;
 let value;try{value=JSON.parse(row.value);}catch{throw conflict();}
 if(!value||Object.keys(value).sort().join(',')!=='checksum,ownerRole,runtimeRole,version'||value.version!==1||!roleName(value.ownerRole)||!roleName(value.runtimeRole)||value.ownerRole===value.runtimeRole)throw conflict();return value;
}
async function runtimeRole(store,explicit){
 if(explicit!==undefined&&!roleName(explicit))throw new AppError(400,'部门密钥运行角色无效','department_key_runtime_invalid');
 let role=explicit;
 if((await store.get("SELECT to_regclass('harbor_operator.activation') relation")).relation){
  const trusted=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton'),database=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');
  if(!trusted||trusted.database_oid!==database.oid)throw conflict();
  if(Object.hasOwn(trusted,'runtime')){if(explicit!==undefined&&explicit!==trusted.runtime)throw conflict();role=trusted.runtime;}
  else if(trusted.local_contract!=='local-assurance-v1'||trusted.state!=='pending'||typeof trusted.local_release_id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(trusted.local_release_id)||explicit===undefined)throw conflict();
 }
 if(!roleName(role))throw new AppError(400,'请指定已核实的部门密钥运行角色','department_key_runtime_required');
 const actual=await store.get('SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',role);
 if(!actual||Object.values(actual).some(Boolean)||await store.get('SELECT 1 FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1) OR member=(SELECT oid FROM pg_roles WHERE rolname=$1)',role)||await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=(SELECT oid FROM pg_roles WHERE rolname=$1) AND deptype='o' AND NOT (d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",role)||(await store.get("SELECT has_schema_privilege($1,'harbor','CREATE') OR has_schema_privilege($1,'public','CREATE') unsafe",role)).unsafe)throw new AppError(409,'部门密钥运行角色超出最小权限','department_key_runtime_invalid');
 return role;
}
async function structure(store,marker){
 const rows=await store.all(`SELECT a.attname,format_type(a.atttypid,a.atttypmod) type,a.attnotnull,
  pg_get_expr(d.adbin,d.adrelid) default_value,c.relkind,c.relpersistence,pg_get_userbyid(c.relowner) owner
  FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
  WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`,table);
 const expected=['department_id','key_id','secret_cipher','secret_digest'].map(attname=>({attname,type:'text',attnotnull:true,default_value:null,relkind:'r',relpersistence:'p',owner:marker.ownerRole}));
 if(JSON.stringify(rows)!==JSON.stringify(expected))throw conflict();
 const constraints=await store.all("SELECT pg_get_constraintdef(oid) def,convalidated FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype<>'n'",table);
 const definitions=["CHECK (((length(secret_cipher) >= 40) AND (length(secret_cipher) <= 2048)))","CHECK ((secret_digest ~ '^[a-f0-9]{64}$'::text))",'PRIMARY KEY (department_id, key_id)','FOREIGN KEY (department_id, key_id) REFERENCES harbor.department_admin_keys(department_id, id) ON DELETE CASCADE'];
 if(constraints.length!==definitions.length||definitions.some(def=>!constraints.some(row=>row.def===def&&row.convalidated)))throw conflict();
 const acl=await store.all(`SELECT a.grantee::text grantee,a.privilege_type,a.is_grantable
  FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
  WHERE c.oid=to_regclass($1)`,table),role=await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',marker.runtimeRole),owner=await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',marker.ownerRole);
 if(!role||!owner||acl.some(row=>row.grantee!==owner.oid&&(row.grantee!==role.oid||row.is_grantable||!['SELECT','INSERT','DELETE'].includes(row.privilege_type)))||['SELECT','INSERT','DELETE'].some(privilege=>!acl.some(row=>row.grantee===role.oid&&row.privilege_type===privilege)))throw conflict();
 if(await store.get('SELECT 1 FROM pg_attribute a WHERE a.attrelid=to_regclass($1) AND a.attacl IS NOT NULL',table))throw conflict();
}
export async function verifyDepartmentKeySecretsAvailability(store,{required=false}={}){
 if(typeof required!=='boolean')throw new TypeError('department_key_required_invalid');
 const marker=await saved(store),present=(await store.get('SELECT to_regclass($1) relation',table)).relation;
 if(!marker){if(present)throw conflict();if(required)throw new AppError(503,'部门管理密钥存储未安装','department_key_storage_required');return false;}
 if(!present||marker.checksum!==(await bundle()).checksum)throw conflict();await structure(store,marker);return true;
}
export async function migrateDepartmentKeySecrets(store,{apply=false,runtimeRole:explicit}={}){
 if(typeof apply!=='boolean')throw new TypeError('department_key_apply_invalid');
 if(!apply){await verifyDepartmentKeySecretsAvailability(store);return false;}
 return store.transaction(async()=>{
  await store.query('SELECT pg_advisory_xact_lock($1)',[createHash('sha256').update('harbor-postgres-migrations').digest().readInt32BE()]);
  const existing=await saved(store);if(existing){if(explicit!==undefined&&explicit!==existing.runtimeRole)throw conflict();await runtimeRole(store,existing.runtimeRole);await verifyDepartmentKeySecretsAvailability(store,{required:true});return false;}
  await verifyDepartmentKeySecretsAvailability(store);
  const role=await runtimeRole(store,explicit),owner=await store.get('SELECT current_user name'),tables=await store.all("SELECT pg_get_userbyid(relowner) owner FROM pg_class WHERE oid=ANY(ARRAY[to_regclass('harbor.department_admin_keys'),to_regclass('harbor.gateway_settings')])");
  if(!roleName(owner.name)||owner.name===role||tables.length!==2||tables.some(row=>row.owner!==owner.name))throw new AppError(403,'部门密钥迁移须由原表owner执行','department_key_owner_invalid');
  const source=await bundle();await store.query(source.sql);await store.query('GRANT SELECT,INSERT,DELETE ON '+table+' TO '+quoted(role));
  const marker={version:1,checksum:source.checksum,ownerRole:owner.name,runtimeRole:role};await structure(store,marker);
  await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',setting,JSON.stringify(marker));return true;
 });
}
export function createDepartmentKeySecrets(store,{encryptionKey,installed}={}){
 if(installed!==undefined&&typeof installed!=='boolean')throw new TypeError('department_key_installed_invalid');
 let initialized=installed;
 async function ready(){
  if(initialized===undefined)initialized=await verifyDepartmentKeySecretsAvailability(store);
  const marker=await saved(store);if(Boolean(marker)!==initialized)throw new AppError(503,'部门密钥存储配置已变化，请重启服务','department_key_storage_restart_required');
  if(initialized)await verifyDepartmentKeySecretsAvailability(store,{required:true});return initialized;
 }
 const aad=(departmentId,keyId)=>'harbor-department-management:'+departmentId+':'+keyId;
 function requireKey(){if(!Buffer.isBuffer(encryptionKey)||encryptionKey.length!==32)throw unavailable();}
 async function save(departmentId,keyId,secret){
  if(!await ready())return false;requireKey();
  await store.run('INSERT INTO '+table+'(department_id,key_id,secret_cipher,secret_digest) VALUES($1,$2,$3,$4)',departmentId,keyId,encryptStoredKey(encryptionKey,secret,aad(departmentId,keyId)),hash(secret));return true;
 }
 async function reveal(departmentId,key){
  if(!await ready())return {available:false,reason:'storage_not_enabled'};
  const row=await store.get('SELECT secret_cipher,secret_digest FROM '+table+' WHERE department_id=$1 AND key_id=$2',departmentId,key.id);
  if(!row)return {available:false,reason:'not_stored'};requireKey();
  const secret=revealStoredKey(encryptionKey,{id:aad(departmentId,key.id),secret_cipher:row.secret_cipher,digest:row.secret_digest});
  if(!secret||departmentManagementKeyDigest(secret)!==key.digest)throw unavailable();return {available:true,secret};
 }
 async function remove(departmentId,keyId){if(await ready())await store.run('DELETE FROM '+table+' WHERE department_id=$1 AND key_id=$2',departmentId,keyId);}
 return {ready,save,reveal,remove};
}
