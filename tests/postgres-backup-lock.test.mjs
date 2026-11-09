import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rm,writeFile,rename,symlink,link,chmod,lstat,readFile,copyFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {withBackupArchiveLock,backupCoordinationFiles} from '../postgres-backup-lock.mjs';

async function directory(t){const path=await mkdtemp(join(tmpdir(),'harbor-backup-lock-'));t.after(()=>rm(path,{recursive:true,force:true}));return path;}

test('shared locks coexist and exclusive locks conflict until the real inherited file description closes',async t=>{
 const path=await directory(t);
 await withBackupArchiveLock({directory:path},async()=>{
  await withBackupArchiveLock({directory:path},()=>true);
  await assert.rejects(withBackupArchiveLock({directory:path,exclusive:true},()=>true),{code:'backup_coordination_busy'});
 });
 await withBackupArchiveLock({directory:path,exclusive:true},async()=>{await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'backup_coordination_busy'});});
 await assert.rejects(withBackupArchiveLock({directory:path,exclusive:true},()=>{throw Error('synthetic-callback-failure');}),/synthetic-callback-failure/);
 assert.equal(await withBackupArchiveLock({directory:path,exclusive:true},()=>true),true);
 const info=await lstat(join(path,backupCoordinationFiles.lock));assert.equal(info.uid,process.getuid());assert.equal(info.mode&0o777,0o600);
});

test('a killed holder releases its lock across a real process boundary',async t=>{
 const path=await directory(t),module=new URL('../postgres-backup-lock.mjs',import.meta.url).href;
 const source=`import {withBackupArchiveLock} from ${JSON.stringify(module)};await withBackupArchiveLock({directory:process.argv[1],exclusive:true},async()=>{process.stdout.write('locked\\n');await new Promise(()=>{setInterval(()=>{},1000);});});`;
 const child=spawn(process.execPath,['--input-type=module','-e',source,path],{stdio:['ignore','pipe','pipe']});const exited=once(child,'close');
 try{
  const ready=await new Promise((resolve,reject)=>{let text='';const timer=setTimeout(()=>reject(Error('bounded lock readiness')),5000);child.stdout.on('data',chunk=>{text+=chunk;if(text.includes('locked\n')){clearTimeout(timer);resolve(true);}});child.once('error',error=>{clearTimeout(timer);reject(error);});child.once('exit',()=>{clearTimeout(timer);reject(Error('holder exited before lock'));});});
  assert.equal(ready,true);await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'backup_coordination_busy'});
 }finally{child.kill('SIGKILL');await exited;}
 assert.equal(await withBackupArchiveLock({directory:path,exclusive:true},()=>true),true);
});

test('lock replacement, hardlinks and symlinks are rejected without adopting the replacement',async t=>{
 const path=await directory(t),lock=join(path,backupCoordinationFiles.lock);
 await assert.rejects(withBackupArchiveLock({directory:path},async lease=>{await rename(lock,join(path,'original'));await writeFile(lock,'',{mode:0o600});await lease.assertPath();}),{code:'backup_coordination_lock_changed'});
 await rm(lock);await link(join(path,'original'),lock);await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'backup_coordination_lock_changed'});
 await rm(lock);await symlink(join(path,'original'),lock);await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'ELOOP'});
});

test('private directory, mode and cancellation are enforced before callbacks run',async t=>{
 const path=await directory(t);await chmod(path,0o755);await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'backup_coordination_directory'});await chmod(path,0o700);
 const alias=path+'-alias';await symlink(path,alias);t.after(()=>rm(alias,{force:true}));await assert.rejects(withBackupArchiveLock({directory:alias},()=>true),{code:'backup_coordination_directory'});
 await writeFile(join(path,backupCoordinationFiles.lock),'',{mode:0o640});await assert.rejects(withBackupArchiveLock({directory:path},()=>true),{code:'backup_coordination_lock_changed'});
 const cancellation=new AbortController(),reason=Error('synthetic-lock-cancellation');cancellation.abort(reason);await assert.rejects(withBackupArchiveLock({directory:path,signal:cancellation.signal},()=>assert.fail('cancelled callback ran')),error=>error===reason);
});

 test('备份协调身份覆盖上下文恢复依赖，单个元信息模块变化使旧身份失效',async t=>{
 const path=await directory(t),original=new URL('../postgres-backup-lock.mjs',import.meta.url),source=await readFile(original,'utf8');
 const names=[...source.matchAll(/'([a-zA-Z0-9/.-]+\.mjs)'/g)].map(match=>match[1]);
 for(const name of new Set(names)){await mkdir(join(path,name,'..'),{recursive:true,mode:0o700});await copyFile(new URL('../'+name,import.meta.url),join(path,name));}
 // Reuse the real artifact helpers without copying their unrelated dependency tree.
 await writeFile(join(path,'migration-artifacts.mjs'),'export * from '+JSON.stringify(new URL('../migration-artifacts.mjs',import.meta.url).href)+';');
 const module=await import(new URL('file://'+join(path,'postgres-backup-lock.mjs')).href),before=await module.backupCoordinationCodeDigest();
 for(const name of ['recording-request-context.mjs','recording-text-metadata.mjs','recording-attachment.mjs','recording-literal.mjs','conversation-capture.mjs']){
  const target=join(path,name),previous=await readFile(target);await writeFile(target,Buffer.concat([previous,Buffer.from('\n// synthetic identity change\n')]));assert.notEqual(await module.backupCoordinationCodeDigest(),before,name);await writeFile(target,previous);assert.equal(await module.backupCoordinationCodeDigest(),before);
 }
 });
