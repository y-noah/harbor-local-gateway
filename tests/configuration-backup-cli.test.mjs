import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,chmod,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes,createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {configurationBackupCommand} from '../configuration-backup-cli.mjs';
import {writeConfigurationBundle} from '../configuration-backup.mjs';

const execute=promisify(execFile);
async function fixture(t){
 const directory=await mkdtemp(join(tmpdir(),'harbor-config-cli-'));
 t.after(()=>rm(directory,{recursive:true,force:true}));
 const key=randomBytes(32),applicationKey=randomBytes(32),keyPath=join(directory,'backup.key'),profilePath=join(directory,'profile.json');
 await writeFile(keyPath,key,{mode:0o600});
 const profile={version:1,directory:join(directory,'bundle'),backupKeyFile:keyPath};
 await mkdir(profile.directory,{mode:0o700});
 await writeFile(profilePath,JSON.stringify(profile),{mode:0o600});
 const bundle={format:'harbor-configuration',version:1,created:Date.now(),schema:{accounts:[],members:[],keys:[],gateway_settings:[]},tables:{accounts:[],members:[],keys:[],gateway_settings:[]},accountAuth:{},materials:[{role:'application_key',bytes:applicationKey.toString('base64'),sha256:createHash('sha256').update(applicationKey).digest('hex')}]};
 await writeConfigurationBundle(profile.directory,bundle,key);
 return {directory,key,applicationKey,keyPath,profilePath,profile};
}

test('inspect不需要数据库且CLI输出只含概要、不含材料或凭据',{timeout:10000},async t=>{
 const f=await fixture(t),result=await configurationBackupCommand(['inspect',f.profilePath],{env:{}});
 assert.deepEqual(result,{format:'harbor-configuration',version:1,counts:{accounts:0,members:0,keys:0,gateway_settings:0},materials:1,historyIncluded:false});
 const env={...process.env};delete env.HARBOR_CONFIGURATION_DATABASE_URL;
 const child=await execute(process.execPath,['configuration-backup-cli.mjs','inspect',f.profilePath],{cwd:new URL('..',import.meta.url),env,timeout:5000});
 assert.deepEqual(JSON.parse(child.stdout),result);assert.equal(child.stderr,'');
 for(const secret of [f.key.toString('base64'),f.applicationKey.toString('base64')])assert.equal(child.stdout.includes(secret),false);
});

test('inspect拒绝错误密钥及篡改包，保留原始文件',{timeout:10000},async t=>{
 const f=await fixture(t),path=join(f.profile.directory,'configuration.enc'),before=await readFile(path);
 await writeFile(f.keyPath,randomBytes(32));await assert.rejects(configurationBackupCommand(['inspect',f.profilePath],{env:{}}));
 assert.deepEqual(await readFile(path),before);await writeFile(f.keyPath,f.key);
 const changed=Buffer.from(before);changed[changed.length-1]^=1;await writeFile(path,changed);
 await assert.rejects(configurationBackupCommand(['inspect',f.profilePath],{env:{}}));assert.deepEqual(await readFile(path),changed);
});

test('配置与密钥拒绝宽权限及符号链接；export必须显式提供数据库',{timeout:10000},async t=>{
 const f=await fixture(t);
 await assert.rejects(configurationBackupCommand(['export',f.profilePath],{env:{}}),{code:'configuration_database_required'});
 await chmod(f.profilePath,0o644);await assert.rejects(configurationBackupCommand(['inspect',f.profilePath],{env:{}}),{code:'configuration_profile_permissions'});await chmod(f.profilePath,0o600);
 const alias=join(f.directory,'profile-link.json');await symlink(f.profilePath,alias);
 await assert.rejects(configurationBackupCommand(['inspect',alias],{env:{}}),{code:'configuration_profile_permissions'});
 await chmod(f.keyPath,0o644);await assert.rejects(configurationBackupCommand(['inspect',f.profilePath],{env:{}}),{code:'configuration_profile_permissions'});
});

test('CLI失败不打印配置内容、文件路径或原始解析异常',{timeout:10000},async t=>{
 const f=await fixture(t),marker='synthetic-private-value';await writeFile(f.profilePath,'{"secret":"'+marker+'",');
 await assert.rejects(execute(process.execPath,['configuration-backup-cli.mjs','inspect',f.profilePath],{cwd:new URL('..',import.meta.url),timeout:5000}),error=>{
  assert.equal(error.stdout,'');assert.deepEqual(JSON.parse(error.stderr),{ok:false,code:'configuration_failed'});assert.equal(error.stderr.includes(marker),false);assert.equal(error.stderr.includes(f.profilePath),false);return true;
 });
});
