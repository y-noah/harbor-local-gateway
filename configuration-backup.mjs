import {createHash,createCipheriv,createDecipheriv,randomBytes,randomUUID} from 'node:crypto';
import {lstat,readFile,writeFile,link,unlink,open} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {privateDirectory,canonical,transferError,syncDirectory} from './migration-artifacts.mjs';

const fail=code=>{throw transferError('configuration_backup_'+code);};
const fields=value=>value.split(' ');
// Explicit configuration columns: history, counters and observations never enter the archive.
export const configurationTables=Object.freeze({
 pricing_versions:fields('id created book'),
 gateway_settings:fields('name value'),
 departments:fields('id name enabled version created updated'),
 department_settings:fields('department_id name value version'),
 member_types:fields('id code name description enabled version default_version updated department_id'),
 member_type_templates:fields('type_id version created allowed_models usage_limits department_id'),
 members:fields('id name quota enabled created removed_at usage_limits usage_limits_version usage_type_id usage_type_version default_models default_models_set defaults_version configuration_source access_version device_mode device_policy_version department_id'),
 keys:fields('id member_id digest prefix expires enabled created secret_cipher allowed_models model_policy_version quota removed_at model_source access_version device_ids device_policy_version consumption_policy consumption_policy_version department_id'),
 accounts:fields('id name kind enabled auth_mode api_weight api_secret_cipher api_secret_digest token_budget tokens_per_percent platform_config platform_models route_epoch quota_identity_epoch department_id'),
 department_admin_keys:fields('id department_id digest prefix label enabled version created revoked_at'),
 department_admin_key_secrets:fields('department_id key_id secret_cipher secret_digest'),
 extra_usage_policy:fields('singleton enabled version updated'),
 account_extra_usage:fields('account_id mode version budget ledger_basis ledger_unit updated department_id'),
 account_package_templates:fields('id created latest_version department_id'),
 account_package_versions:fields('template_id version created definition department_id'),
 account_package_selections:fields('account_id mode template_id template_version revision capacity_override reason recognition_version updated department_id'),
 employee_devices:fields('id member_id name system state pending_renewal hardware_state fingerprint installation_fingerprint fingerprint_source review_fingerprint review_installation_fingerprint allowed_key_ids version credential_generation public_key_sha256 csr created approved_at replaces_device_id enrollment_digest enrollment_expires department_id'),
 device_credentials:fields('fingerprint device_id generation certificate issued expires revoked department_id'),
});
const settingNames=['pricing.state','pricing.settings','pricing.official','device.instance_scope','balancing','routing_strategy','routing_weights','api_account_scope','transfer_schedule','account_package_policy','extra_usage_policy','handoff_policy'];
const allowedSetting=name=>settingNames.includes(name)||/^subscription_monthly_usd:[a-zA-Z0-9-]{1,100}$/.test(name);
const materialRole=role=>['application_key','admin_settings','device_ca_certificate','device_ca_key','device_server_certificate','device_server_key'].includes(role)||/^codex_auth:[a-zA-Z0-9-]{1,100}$/.test(role);
const primary={extra_usage_policy:['singleton'],account_extra_usage:['account_id'],gateway_settings:['name'],department_settings:['department_id','name'],member_type_templates:['type_id','version'],department_admin_key_secrets:['department_id','key_id'],account_package_versions:['template_id','version'],account_package_selections:['account_id'],device_credentials:['fingerprint']};
const digest=value=>createHash('sha256').update(value).digest('hex');
const identifier=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(value);
export async function configurationSchema(store){
 const columns=await store.all("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='harbor'");
 return Object.fromEntries(Object.entries(configurationTables).filter(([table])=>columns.some(row=>row.table_name===table)).map(([table,allowed])=>[table,allowed.filter(name=>columns.some(row=>row.table_name===table&&row.column_name===name))]));
}
function closeRows(tables){
 tables.gateway_settings=tables.gateway_settings.filter(row=>allowedSetting(row.name));for(const row of tables.gateway_settings)if(row.name==='pricing.state')row.value=JSON.stringify({active:JSON.parse(row.value).active});if(tables.department_settings)tables.department_settings=tables.department_settings.filter(row=>allowedSetting(row.name));
 const pricing=tables.gateway_settings.find(row=>row.name==='pricing.state');const active=pricing?JSON.parse(pricing.value).active:null;tables.pricing_versions=tables.pricing_versions.filter(row=>row.id===active);

 const ids=table=>new Set((tables[table]||[]).map(row=>row.id));
 const members=ids('members'),accounts=ids('accounts');
 tables.keys=tables.keys.filter(row=>members.has(row.member_id));
 if(tables.employee_devices)tables.employee_devices=tables.employee_devices.filter(row=>members.has(row.member_id));
 if(tables.employee_devices){const keys=ids('keys');for(const device of tables.employee_devices)if(device.allowed_key_ids!==null){const values=typeof device.allowed_key_ids==='string'?JSON.parse(device.allowed_key_ids):device.allowed_key_ids;device.allowed_key_ids=values.filter(id=>keys.has(id));}}
 if(tables.device_credentials){const devices=ids('employee_devices');tables.device_credentials=tables.device_credentials.filter(row=>devices.has(row.device_id));}
 if(tables.account_extra_usage)tables.account_extra_usage=tables.account_extra_usage.filter(row=>accounts.has(row.account_id));
 if(tables.account_package_selections)tables.account_package_selections=tables.account_package_selections.filter(row=>accounts.has(row.account_id));
 if(tables.member_type_templates){const required=requiredTypeVersions(tables);tables.member_type_templates=tables.member_type_templates.filter(row=>required.has(row.type_id+':'+row.version));}
 if(tables.account_package_versions){const required=requiredPackageVersions(tables);tables.account_package_versions=tables.account_package_versions.filter(row=>required.has(row.template_id+':'+row.version));}
}
function requiredTypeVersions(tables){
 const required=new Set();for(const row of tables.member_types||[])if(row.default_version!==null)required.add(row.id+':'+row.default_version);
 for(const row of [...tables.members,...tables.keys]){const configuration=typeof row.configuration_source==='string'?JSON.parse(row.configuration_source):row.configuration_source;const model=typeof row.model_source==='string'?JSON.parse(row.model_source):row.model_source;for(const source of [model,...Object.values(configuration||{})])if(source?.type_id&&source.template_version!==undefined)required.add(source.type_id+':'+source.template_version);}return required;
}
function requiredPackageVersions(tables){const required=new Set((tables.account_package_selections||[]).filter(row=>row.template_id).map(row=>row.template_id+':'+row.template_version));for(const row of tables.account_package_templates||[])required.add(row.id+':'+row.latest_version);return required;}
async function file(path,maxBytes){const handle=await open(path,(await import('node:fs')).constants.O_RDONLY|(await import('node:fs')).constants.O_NOFOLLOW);try{const info=await handle.stat();if(!info.isFile()||info.nlink!==1||info.mode&0o077||info.uid!==process.getuid()||info.size>maxBytes)fail('material_invalid');const bytes=await handle.readFile();if(bytes.length>maxBytes)fail('material_capacity');return bytes;}finally{await handle.close();}}

export async function collectConfiguration(store,{dataDir,runtimeDir,maxRows=10000,maxBytes=16*1024*1024,materialFiles={}}={}){
 return store.transaction(async()=>{
  const schema=await configurationSchema(store),tables={};
  const deferred=new Set(['pricing_versions','member_type_templates','account_package_versions']);
  const select=async(table,where='',args=[])=>{tables[table]=await store.all('SELECT '+schema[table].join(',')+' FROM harbor.'+table+where+' LIMIT $'+(args.length+1),...args,maxRows+1);if(tables[table].length>maxRows)fail('row_capacity');};
  for(const table of Object.keys(schema)){if(deferred.has(table))continue;if(['gateway_settings','department_settings'].includes(table))await select(table,' WHERE name=ANY($1::text[]) OR name ~ $2',[settingNames,'^subscription_monthly_usd:[a-zA-Z0-9-]{1,100}$']);else{const where=table==='keys'?' WHERE removed_at IS NULL AND member_id IN (SELECT id FROM harbor.members WHERE removed_at IS NULL)':table==='employee_devices'?' WHERE member_id IN (SELECT id FROM harbor.members WHERE removed_at IS NULL)':table==='device_credentials'?' WHERE device_id IN (SELECT d.id FROM harbor.employee_devices d JOIN harbor.members m ON m.id=d.member_id WHERE m.removed_at IS NULL)':['account_extra_usage','account_package_selections'].includes(table)?' WHERE account_id IN (SELECT id FROM harbor.accounts WHERE removed_at IS NULL)':['accounts','members'].includes(table)?' WHERE removed_at IS NULL':'';await select(table,where);}}
  const active=JSON.parse(tables.gateway_settings.find(row=>row.name==='pricing.state')?.value||'{}').active;
  if(schema.pricing_versions)await select('pricing_versions',' WHERE id=$1',[active??null]);
  if(schema.member_type_templates)await select('member_type_templates'," WHERE type_id || ':' || version::text=ANY($1::text[])",[[...requiredTypeVersions(tables)]]);
  if(schema.account_package_versions)await select('account_package_versions'," WHERE template_id || ':' || version::text=ANY($1::text[])",[[...requiredPackageVersions(tables)]]);
  // Preserve dependency order for atomic restore after selecting bounded current rows.
  for(const table of Object.keys(schema)){const rows=tables[table];delete tables[table];tables[table]=rows;}
  if(!tables.members||!tables.keys||!tables.accounts)fail('schema_missing');closeRows(tables);
  const sources={application_key:join(dataDir,'key-encryption.key'),...materialFiles},accountAuth={};
  try{await lstat(join(dataDir,'settings.json'));sources.admin_settings=join(dataDir,'settings.json');}catch(error){if(error.code!=='ENOENT')throw error;}
  const accounts=await store.all('SELECT id,home,kind,auth_mode FROM harbor.accounts WHERE removed_at IS NULL');
  for(const account of accounts)if(account.auth_mode==='subscription'){
   if(account.kind!=='codex')fail('unsupported_subscription');
   if(!runtimeDir||resolve(account.home)!==join(resolve(runtimeDir),'accounts',account.id))fail('account_home');
   if(!identifier(account.id))fail('account_id');const path=join(account.home,'auth.json');try{await lstat(path);sources['codex_auth:'+account.id]=path;accountAuth[account.id]='included';}catch(error){if(error.code!=='ENOENT')throw error;accountAuth[account.id]='absent';}
  }
  const materials=[],sourceDigests=new Map();let size=0;for(const [role,path] of Object.entries(sources)){if(!materialRole(role))fail('material_role');let bytes=await file(path,maxBytes);sourceDigests.set(role,digest(bytes));if(role==='admin_settings'){const value=JSON.parse(bytes.toString('utf8'));bytes=Buffer.from(JSON.stringify(Object.fromEntries(['port','codexBinary','claudeBinary','adminPasswordHash','adminCookieName'].filter(key=>Object.hasOwn(value,key)).map(key=>[key,value[key]]))));}size+=bytes.length;if(size>maxBytes)fail('material_capacity');materials.push({role,bytes:bytes.toString('base64'),sha256:digest(bytes)});}
  if(materials.find(row=>row.role==='application_key')?.bytes.length!==44)fail('application_key');
  for(const row of materials)if(digest(await file(sources[row.role],maxBytes))!==sourceDigests.get(row.role))fail('material_changed');
  const bundle={format:'harbor-configuration',version:1,created:Date.now(),schema,tables,materials,accountAuth};validateConfiguration(bundle,{maxRows,maxBytes});return bundle;
 },{level:'REPEATABLE READ',readOnly:true});
}
export function validateConfiguration(bundle,{maxRows=10000,maxBytes=16*1024*1024}={}){
 if(!bundle||bundle.format!=='harbor-configuration'||bundle.version!==1||!Number.isSafeInteger(bundle.created)||!bundle.schema||!bundle.tables||!Array.isArray(bundle.materials)||Object.keys(bundle).some(key=>!['format','version','created','schema','tables','materials','accountAuth'].includes(key)))fail('format');
 if(!['accounts','members','keys','gateway_settings'].every(table=>Array.isArray(bundle.tables[table])))fail('required_tables');
 if(canonical(Object.keys(bundle.schema).sort())!==canonical(Object.keys(bundle.tables).sort()))fail('table_set');
 for(const [table,rows] of Object.entries(bundle.tables)){const columns=bundle.schema[table];if(!configurationTables[table]||!Array.isArray(columns)||new Set(columns).size!==columns.length||columns.some(column=>!configurationTables[table].includes(column))||!Array.isArray(rows)||rows.length>maxRows)fail('table_invalid');const seen=new Set();for(const row of rows){if(!row||Array.isArray(row)||canonical(Object.keys(row).sort())!==canonical([...columns].sort()))fail('row_invalid');if(['id','member_id','account_id','department_id','type_id','key_id','template_id'].some(name=>row[name]!==undefined&&row[name]!==null&&!identifier(row[name])))fail('identifier');const key=canonical((primary[table]||['id']).map(name=>row[name]));if(seen.has(key))fail('row_duplicate');seen.add(key);}}
 let size=0;const roles=new Set();for(const material of bundle.materials){if(!material||Object.keys(material).some(key=>!['role','bytes','sha256'].includes(key))||typeof material.role!=='string'||!materialRole(material.role)||roles.has(material.role)||typeof material.bytes!=='string')fail('material_invalid');roles.add(material.role);const bytes=Buffer.from(material.bytes,'base64');if(bytes.toString('base64')!==material.bytes||digest(bytes)!==material.sha256)fail('material_digest');size+=bytes.length;}if(!roles.has('application_key')||Buffer.from(bundle.materials.find(row=>row.role==='application_key').bytes,'base64').length!==32)fail('application_key');
 if(!bundle.accountAuth||typeof bundle.accountAuth!=='object'||Array.isArray(bundle.accountAuth))fail('account_auth');
 for(const account of bundle.tables.accounts){if(account.auth_mode==='subscription'&&(!['included','absent'].includes(bundle.accountAuth[account.id])||roles.has('codex_auth:'+account.id)!==(bundle.accountAuth[account.id]==='included')))fail('account_auth');}
 const subscriptionIds=new Set(bundle.tables.accounts.filter(row=>row.auth_mode==='subscription'&&row.kind==='codex').map(row=>row.id));
 if(Object.keys(bundle.accountAuth).some(id=>!subscriptionIds.has(id))||[...roles].some(role=>role.startsWith('codex_auth:')&&!subscriptionIds.has(role.slice(11))))fail('account_auth');
 const references=(table,column,parent,{optional=false,sameDepartment=true}={})=>{const parents=new Map((bundle.tables[parent]||[]).map(row=>[row.id,row]));for(const row of bundle.tables[table]||[]){if(!Object.hasOwn(row,column))continue;if(optional&&(row[column]===null||row[column]===undefined))continue;const target=parents.get(row[column]);if(!target||sameDepartment&&row.department_id!==undefined&&target.department_id!==undefined&&row.department_id!==target.department_id)fail('reference');}};
 references('keys','member_id','members');references('employee_devices','member_id','members');references('employee_devices','replaces_device_id','employee_devices',{optional:true});references('device_credentials','device_id','employee_devices');
 for(const table of ['members','keys','accounts','member_types','member_type_templates','department_settings','department_admin_keys','department_admin_key_secrets','account_extra_usage','account_package_templates','account_package_versions','account_package_selections','employee_devices','device_credentials'])references(table,'department_id','departments',{sameDepartment:false});
 references('member_type_templates','type_id','member_types');references('members','usage_type_id','member_types',{optional:true});references('department_admin_key_secrets','key_id','department_admin_keys');
 for(const table of ['account_extra_usage','account_package_selections'])references(table,'account_id','accounts');references('account_package_versions','template_id','account_package_templates');references('account_package_selections','template_id','account_package_templates',{optional:true});
 const structured=value=>typeof value==='string'?JSON.parse(value):value;
 for(const device of bundle.tables.employee_devices||[]){for(const id of structured(device.allowed_key_ids)||[]){const key=bundle.tables.keys.find(row=>row.id===id);if(!key||key.member_id!==device.member_id)fail('device_reference');}}
 for(const key of bundle.tables.keys){for(const id of structured(key.device_ids)||[]){const device=(bundle.tables.employee_devices||[]).find(row=>row.id===id);if(!device||device.member_id!==key.member_id)fail('device_reference');}}
 if((bundle.tables.members.some(row=>row.device_mode==='enforced')||bundle.tables.keys.some(row=>row.device_ids!==null&&row.device_ids!==undefined))&&!['device_ca_certificate','device_ca_key','device_server_certificate','device_server_key'].every(role=>roles.has(role)))fail('device_material_missing');
 if(bundle.tables.gateway_settings.some(row=>!allowedSetting(row.name))||(bundle.tables.department_settings||[]).some(row=>!allowedSetting(row.name)))fail('setting_not_allowed');
 if(size>maxBytes||Buffer.byteLength(JSON.stringify(bundle))>maxBytes*2)fail('capacity');return bundle;
}
const packageMagic=Buffer.from('HBRCFG01'),maxPackageBytes=32*1024*1024;
export async function writeConfigurationBundle(directory,bundle,key){
 await privateDirectory(directory);validateConfiguration(bundle);if(!Buffer.isBuffer(key)||key.length!==32||key.equals(Buffer.from(bundle.materials.find(row=>row.role==='application_key').bytes,'base64')))fail('backup_key');
 const plain=Buffer.from(JSON.stringify(bundle));if(plain.length>maxPackageBytes-36)fail('capacity');const nonce=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,nonce);cipher.setAAD(packageMagic);const encrypted=Buffer.concat([packageMagic,nonce,cipher.update(plain),cipher.final(),cipher.getAuthTag()]);plain.fill(0);
 const path=join(directory,'configuration.enc'),temporary=join(directory,'.configuration-'+randomUUID());try{await writeFile(temporary,encrypted,{flag:'wx',mode:0o600,flush:true});await link(temporary,path);await syncDirectory(directory);}finally{await unlink(temporary).catch(error=>{if(error.code!=='ENOENT')throw error;});await syncDirectory(directory);}
 return {format:bundle.format,version:bundle.version,counts:Object.fromEntries(Object.entries(bundle.tables).map(([name,rows])=>[name,rows.length])),materials:bundle.materials.length};
}
export async function readConfigurationBundle(directory,key){
 await privateDirectory(directory);if(!Buffer.isBuffer(key)||key.length!==32)fail('backup_key');const bytes=await file(join(directory,'configuration.enc'),maxPackageBytes);if(bytes.length<36||!bytes.subarray(0,8).equals(packageMagic))fail('format');let plain;
 try{const decipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(8,20));decipher.setAAD(packageMagic);decipher.setAuthTag(bytes.subarray(-16));plain=Buffer.concat([decipher.update(bytes.subarray(20,-16)),decipher.final()]);return validateConfiguration(JSON.parse(plain.toString('utf8')));}catch(error){if(error.code?.startsWith('configuration_backup_'))throw error;fail('authentication');}finally{plain?.fill(0);}
}
