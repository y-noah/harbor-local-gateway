import {readFile,lstat} from 'node:fs/promises';import {resolve} from 'node:path';import {fileURLToPath} from 'node:url';
import {PostgresStore} from './postgres-store.mjs';
import {collectConfiguration,writeConfigurationBundle,readConfigurationBundle} from './configuration-backup.mjs';
import {restoreConfiguration,recoverConfigurationRestore} from './configuration-restore.mjs';
const fail=code=>{throw Object.assign(Error('配置备份未完成'),{code});};
async function privateBytes(path){if(typeof path!=='string'||path!==resolve(path))fail('configuration_profile_path');const stat=await lstat(path);if(!stat.isFile()||stat.isSymbolicLink()||stat.nlink!==1||stat.uid!==process.getuid()||stat.mode&0o077||stat.size>65536)fail('configuration_profile_permissions');return readFile(path);}
export async function configurationBackupCommand(args,{env=process.env}={}){
 if(args.length!==2||!['export','inspect','restore','recover'].includes(args[0]))fail('configuration_command');const [command,path]=args,profile=JSON.parse((await privateBytes(path)).toString('utf8'));
 if(!profile||profile.version!==1||Object.keys(profile).some(key=>!['version','directory','dataDir','runtimeDir','journalDir','backupKeyFile','deviceMaterials','allowPlaintextLocalNetwork'].includes(key)))fail('configuration_profile');
 const key=await privateBytes(profile.backupKeyFile);if(key.length!==32)fail('configuration_backup_key');let store;
 try{
  if(command==='inspect'){const bundle=await readConfigurationBundle(profile.directory,key);return {format:bundle.format,version:bundle.version,counts:Object.fromEntries(Object.entries(bundle.tables).map(([table,rows])=>[table,rows.length])),materials:bundle.materials.length,historyIncluded:false};}
  if(!env.HARBOR_CONFIGURATION_DATABASE_URL)fail('configuration_database_required');store=new PostgresStore({connectionString:env.HARBOR_CONFIGURATION_DATABASE_URL,allowPlaintextLocalNetwork:profile.allowPlaintextLocalNetwork===true});
  if(command==='export'){const bundle=await collectConfiguration(store,{dataDir:profile.dataDir,runtimeDir:profile.runtimeDir,materialFiles:profile.deviceMaterials??{}});return await writeConfigurationBundle(profile.directory,bundle,key);}
  const bundle=await readConfigurationBundle(profile.directory,key);return await (command==='recover'?recoverConfigurationRestore:restoreConfiguration)(store,bundle,{dataDir:profile.dataDir,runtimeDir:profile.runtimeDir,journalDir:profile.journalDir,backupKey:key,deviceMaterialPaths:profile.deviceMaterials??{}});
 }finally{await store?.close();key.fill(0);}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))configurationBackupCommand(process.argv.slice(2)).then(result=>process.stdout.write(JSON.stringify(result)+'\n'),error=>{process.stderr.write(JSON.stringify({ok:false,code:typeof error.code==='string'&&/^[a-zA-Z0-9_]+$/.test(error.code)?error.code:'configuration_failed'})+'\n');process.exitCode=1;});
