import {mkdir,lstat,readFile,writeFile,link,unlink,rm} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import {configurationSchema,validateConfiguration} from './configuration-backup.mjs';
import {canonical,privateDirectory,writeManifest,readManifest,transferError,syncDirectory} from './migration-artifacts.mjs';
import {revealStoredKey} from './store.mjs';
const fail=code=>{throw transferError('configuration_restore_'+code);};
const hash=value=>createHash('sha256').update(value).digest('hex');
const primary={extra_usage_policy:['singleton'],account_extra_usage:['account_id'],gateway_settings:['name'],department_settings:['department_id','name'],member_type_templates:['type_id','version'],department_admin_key_secrets:['department_id','key_id'],account_package_versions:['template_id','version'],account_package_selections:['account_id'],device_credentials:['fingerprint']};
const json=value=>typeof value==='string'?JSON.parse(value):value;
const migrationSetting=row=>row.name==='account_package_policy'&&canonical(json(row.value))===canonical({enabled:false,smoothing_hours:24,version:0});
const exists=async path=>{try{return await lstat(path);}catch(error){if(error.code==='ENOENT')return null;throw error;}};
function verifySecrets(bundle){const material=bundle.materials.find(row=>row.role==='application_key');if(!material)fail('application_key_missing');const key=Buffer.from(material.bytes,'base64');if(key.length!==32)fail('application_key_invalid');
 try{for(const row of bundle.tables.keys)if(row.secret_cipher&&!revealStoredKey(key,row))fail('key_cipher');for(const row of bundle.tables.accounts)if(row.api_secret_cipher&&!revealStoredKey(key,{id:'upstream:'+row.id,digest:row.api_secret_digest,secret_cipher:row.api_secret_cipher}))fail('upstream_cipher');for(const row of bundle.tables.department_admin_key_secrets||[])if(!revealStoredKey(key,{id:'harbor-department-management:'+row.department_id+':'+row.key_id,digest:row.secret_digest,secret_cipher:row.secret_cipher}))fail('department_key_cipher');}finally{key.fill(0);}}
function orderedDevices(rows){const result=[],pending=[...rows],done=new Set();while(pending.length){const index=pending.findIndex(row=>!row.replaces_device_id||done.has(row.replaces_device_id));if(index<0)fail('device_reference');const [row]=pending.splice(index,1);done.add(row.id);result.push(row);}return result;}
export async function restoreConfiguration(store,bundle,{dataDir,runtimeDir,journalDir,backupKey,beforeCommit,deviceMaterialPaths={}}={}){
 validateConfiguration(bundle);verifySecrets(bundle);await privateDirectory(dataDir);await privateDirectory(runtimeDir);await privateDirectory(journalDir);
 const journalPath=join(journalDir,'configuration-restore.enc');if(await exists(journalPath))fail('journal_exists');
 const restoreId=randomUUID(),marker='configuration.restore.'+restoreId,created=[],plans=[],materials=[];let committed=false,journalCreated=false,prepared=false;
 try{
  const mappings={application_key:join(dataDir,'key-encryption.key'),admin_settings:join(dataDir,'settings.json')};for(const [role,path] of Object.entries(deviceMaterialPaths)){if(!['device_ca_certificate','device_ca_key','device_server_certificate','device_server_key'].includes(role)||typeof path!=='string'||resolve(path)!==path)fail('device_target');mappings[role]=path;await privateDirectory((await import('node:path')).dirname(path));}
  for(const material of bundle.materials){let target=mappings[material.role];if(material.role.startsWith('codex_auth:')){const id=material.role.slice(11);if(!/^[a-zA-Z0-9-]{1,100}$/.test(id)||!bundle.tables.accounts.some(row=>row.id===id&&row.kind==='codex'&&row.auth_mode==='subscription'))fail('material_account');const accounts=join(runtimeDir,'accounts');await mkdir(accounts,{recursive:true,mode:0o700});await privateDirectory(accounts);const home=join(accounts,id);await mkdir(home,{recursive:true,mode:0o700});await privateDirectory(home);await syncDirectory(home);await syncDirectory(accounts);await syncDirectory(runtimeDir);target=join(home,'auth.json');}if(!target)fail('material_role_not_supported');if(materials.some(item=>item.target===target))fail('material_target_duplicate');materials.push({target,sha256:material.sha256});const bytes=Buffer.from(material.bytes,'base64'),current=await exists(target);if(current){if(!current.isFile()||current.isSymbolicLink()||current.nlink!==1||current.uid!==process.getuid()||current.mode&0o077||hash(await readFile(target))!==material.sha256)fail('material_conflict');continue;}const staged=join(dirname(target),'.configuration-'+restoreId+'-'+plans.length);plans.push({target,staged,sha256:material.sha256,bytes});}
  const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid');
  await writeManifest(journalPath,{database,marker,bundleDigest:hash(canonical(bundle)),format:'harbor-configuration-restore',version:1,state:'prepared',dataDir,runtimeDir,materials,files:plans.map(({target,staged,sha256})=>({target,staged,sha256}))},backupKey);journalCreated=true;
  for(const plan of plans)await writeFile(plan.staged,plan.bytes,{mode:0o600,flag:'wx',flush:true});
  await store.transaction(async()=>{
   const schema=await configurationSchema(store);if(canonical(schema)!==canonical(bundle.schema))fail('schema_mismatch');
   await store.query('LOCK TABLE '+Object.keys(schema).map(table=>'harbor.'+table).join(',')+' IN ACCESS EXCLUSIVE MODE NOWAIT');
   const peers=await store.get("SELECT count(*)::int n FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'");if(peers.n)fail('target_not_offline');
   const occupied=await store.get('SELECT (SELECT count(*) FROM harbor.members)+(SELECT count(*) FROM harbor.keys)+(SELECT count(*) FROM harbor.accounts)+(SELECT count(*) FROM harbor.requests) n');const empty=Number(occupied.n)===0;
   for(const [table,original] of Object.entries(bundle.tables)){
    const rows=table==='employee_devices'?orderedDevices(original):original;
    for(const source of rows){const columns=schema[table],pk=primary[table]||['id'],predicate=pk.map((column,i)=>column+'=$'+(i+1)).join(' AND '),current=await store.get('SELECT '+columns.join(',')+' FROM harbor.'+table+' WHERE '+predicate,...pk.map(column=>source[column]));
     if(current&&canonical(current)===canonical(source))continue;
     const seed=empty&&current&&(table==='departments'&&current.id==='department-default'&&current.version===0||table==='member_types'&&/^usage-[a-f]$/.test(current.id)&&current.version===0&&current.default_version===null||table==='extra_usage_policy'&&current.version===0&&(current.enabled===false||current.enabled===0)||table==='gateway_settings'&&migrationSetting(current)||table==='department_settings'&&current.department_id==='department-default'&&current.version===0&&(migrationSetting(current)||current.name==='extra_usage_policy'&&canonical(json(current.value))===canonical({enabled:false,version:0})));if(current&&!seed)fail('object_conflict');
     const row={...source};if(table==='member_types')row.default_version=null;if(table==='accounts'){row.home=join(runtimeDir,'accounts',row.id);row.state='unknown';}
     const names=Object.keys(row),values=names.map(name=>{const value=row[name];return table==='member_type_templates'&&name==='allowed_models'||value&&typeof value==='object'?JSON.stringify(value):value;});
     if(current)await store.run('UPDATE harbor.'+table+' SET '+names.map((name,i)=>name+'=$'+(i+1)).join(',')+' WHERE '+pk.map((name,i)=>name+'=$'+(names.length+i+1)).join(' AND '),...values,...pk.map(name=>source[name]));
     else await store.run('INSERT INTO harbor.'+table+'('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...values);
    }
    if(table==='member_type_templates')for(const type of bundle.tables.member_types)await store.run('UPDATE harbor.member_types SET default_version=$1 WHERE id=$2',type.default_version,type.id);
   }
   for(const plan of plans){await link(plan.staged,plan.target);created.push(plan);}
   for(const directory of new Set(plans.map(plan=>dirname(plan.target))))await syncDirectory(directory);
   await beforeCommit?.();await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',marker,JSON.stringify({format:'harbor-configuration-restore',digest:hash(canonical(bundle))}));prepared=true;
  });committed=true;
  await writeManifest(journalPath,{database,marker,bundleDigest:hash(canonical(bundle)),format:'harbor-configuration-restore',version:1,state:'committed',dataDir,runtimeDir,materials,files:plans.map(({target,staged,sha256})=>({target,staged,sha256})),accountVerification:'required',counts:Object.fromEntries(Object.entries(bundle.tables).map(([table,rows])=>[table,rows.length]))},backupKey,{replace:true});
  return {restored:true,accountVerification:'required',counts:Object.fromEntries(Object.entries(bundle.tables).map(([table,rows])=>[table,rows.length]))};
 }catch(error){if(!committed&&!prepared){
  for(const plan of created.reverse())if(hash(await readFile(plan.target))===plan.sha256)await unlink(plan.target);
  for(const plan of plans)await unlink(plan.staged).catch(error=>{if(error.code!=='ENOENT')throw error;});
  for(const directory of new Set(plans.map(plan=>dirname(plan.target))))await syncDirectory(directory);
  if(journalCreated){await unlink(journalPath);await syncDirectory(journalDir);}
 }throw error;
 }finally{if(committed){for(const plan of plans)await unlink(plan.staged).catch(error=>{if(error.code!=='ENOENT')throw error;});for(const directory of new Set(plans.map(plan=>dirname(plan.target))))await syncDirectory(directory);}}

}


export async function recoverConfigurationRestore(store,bundle,{dataDir,runtimeDir,journalDir,backupKey,deviceMaterialPaths={}}={}){
 validateConfiguration(bundle);await privateDirectory(dataDir);await privateDirectory(runtimeDir);await privateDirectory(journalDir);const path=join(journalDir,'configuration-restore.enc'),record=await readManifest(path,backupKey),database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid');
 if(record.format!=='harbor-configuration-restore'||record.version!==1||!['prepared','committed'].includes(record.state)||record.dataDir!==dataDir||record.runtimeDir!==runtimeDir||record.bundleDigest!==hash(canonical(bundle))||canonical(record.database)!==canonical(database)||!/^configuration\.restore\.[a-f0-9-]{36}$/.test(record.marker)||!Array.isArray(record.files)||!Array.isArray(record.materials))fail('journal_binding');
 const allowed=new Set([join(dataDir,'key-encryption.key'),join(dataDir,'settings.json'),...bundle.tables.accounts.map(row=>join(runtimeDir,'accounts',row.id,'auth.json')),...Object.values(deviceMaterialPaths)]);
 for(const item of record.materials)if(!allowed.has(item.target)||!/^[a-f0-9]{64}$/.test(item.sha256))fail('journal_path');
 for(const [index,item] of record.files.entries())if(!allowed.has(item.target)||item.staged!==join(dirname(item.target),'.configuration-'+record.marker.slice('configuration.restore.'.length)+'-'+index)||!/^[a-f0-9]{64}$/.test(item.sha256))fail('journal_path');
 return store.transaction(async()=>{
  await store.query('LOCK TABLE harbor.gateway_settings IN ACCESS EXCLUSIVE MODE NOWAIT');const peers=await store.get("SELECT count(*)::int n FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND backend_type='client backend'");if(peers.n)fail('target_not_offline');
  const marker=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',record.marker);if(marker&&JSON.parse(marker.value).digest!==hash(canonical(bundle)))fail('journal_bundle');if(!marker&&record.state==='committed')fail('committed_marker_missing');
  if(marker)for(const item of record.materials){const info=await exists(item.target);if(!info||!info.isFile()||info.isSymbolicLink()||info.mode&0o077||hash(await readFile(item.target))!==item.sha256)fail('journal_material_changed');}
  for(const item of record.files){const info=await exists(item.target);if(info&&(!info.isFile()||info.isSymbolicLink()||hash(await readFile(item.target))!==item.sha256)||marker&&!info)fail('journal_material_changed');}
  for(const item of record.files){const temporary=await exists(item.staged);if(temporary&&(!temporary.isFile()||temporary.isSymbolicLink()||temporary.nlink>2||temporary.uid!==process.getuid()||temporary.mode&0o077||marker&&hash(await readFile(item.staged))!==item.sha256))fail('staging_changed');}
  for(const item of record.files){if(!marker&&await exists(item.target))await unlink(item.target);if(await exists(item.staged))await unlink(item.staged);}
  for(const directory of new Set(record.files.map(item=>dirname(item.target))))await syncDirectory(directory);
  if(!marker){await unlink(path);await syncDirectory(journalDir);}else await writeManifest(path,{...record,state:'committed'},backupKey,{replace:true});return {state:marker?'committed':'rolled_back',accountVerification:marker?'required':'not_restored'};
 });
}
