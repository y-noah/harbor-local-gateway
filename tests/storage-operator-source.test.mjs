import {prepareStorageOperatorMaterialOwners} from '../storage-operator.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rm,readFile,writeFile,lstat,chown} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {Store} from '../store.mjs';
import {acquireSqliteSourceLease,captureSqliteSource,inspectSqliteSourceLease,assertSqliteSourceLease} from '../sqlite-postgres-transfer.mjs';
import {openFile,readManifest,canonical} from '../migration-artifacts.mjs';
const exec=promisify(execFile);
for(const mode of ['DELETE','WAL'])test('source lease really excludes an independent SQLite writer in '+mode+' mode, preserves rows and backs up via a separate reader',{timeout:20000},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-source-lease-')),source=join(root,'source'),artifacts=join(root,'artifacts'),key=randomBytes(32);await mkdir(source,{mode:0o700});await mkdir(artifacts,{mode:0o700});let lease;
  const file=join(source,'gateway.sqlite');
  try{
    const store=new Store(file,{encryptionKey:randomBytes(32)});store.createMember('合成只读来源',null,null);store.db.exec('PRAGMA journal_mode='+mode);const original=store.all('SELECT * FROM members');store.close();
    const writer="import {DatabaseSync} from 'node:sqlite';const db=new DatabaseSync(process.argv[1]);try{db.exec(\"PRAGMA busy_timeout=10;BEGIN IMMEDIATE;UPDATE members SET name='writer accepted';COMMIT\");console.log('accepted');}catch(error){console.log('blocked:'+error.code);}finally{db.close()}";
    lease=await acquireSqliteSourceLease(source);await assertSqliteSourceLease(lease);
    const blocked=await exec(process.execPath,['--input-type=module','-e',writer,file]);assert.match(blocked.stdout,/blocked:/);assert.doesNotMatch(blocked.stdout,/accepted/);
    await captureSqliteSource({sourceDataDir:source,directory:artifacts,key,sourceLease:lease});assert.equal(await assertSqliteSourceLease(lease),true,'capture borrows rather than releasing the writer lease');
    const observed=await inspectSqliteSourceLease(lease,{directory:artifacts}),manifest=await readManifest(join(artifacts,'source-manifest.enc'),key);assert.equal(canonical(observed.content),canonical(manifest.content));
    const restored=join(root,'restored.sqlite');await openFile(join(artifacts,'source.enc'),restored,key);const backup=new DatabaseSync(restored,{readOnly:true});try{assert.deepEqual(backup.prepare('SELECT * FROM members').all().map(row=>({...row})),original.map(row=>({...row})));}finally{backup.close();}
    await lease.close();await assert.rejects(assertSqliteSourceLease(lease),{code:'transfer_source_lease_closed'});lease=null;
    const allowed=await exec(process.execPath,['--input-type=module','-e',writer,file]);assert.match(allowed.stdout,/accepted/);const changed=new DatabaseSync(file,{readOnly:true});try{assert.equal(changed.prepare('SELECT name FROM members').get().name,'writer accepted');}finally{changed.close();}
  }finally{await lease?.close();await rm(root,{recursive:true,force:true});}
});

test('forged source lease cannot stand in for a live SQLite lock',async()=>{await assert.rejects(assertSqliteSourceLease({close:async()=>{}}),{code:'transfer_source_lease_required'});});

test('root operator reads the configured UID1000 source key without changing the original owner',{skip:process.getuid?.()!==0?'requires actual root credential/layout fixture; current operator is UID1000':false,timeout:20000},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-root-source-')),source=join(root,'data'),artifacts=join(root,'artifacts'),rejected=join(root,'default-owner'),master=randomBytes(32),key=randomBytes(32);let lease;
  try{
    for(const directory of [source,artifacts,rejected])await mkdir(directory,{mode:0o700});await writeFile(join(source,'key-encryption.key'),master,{mode:0o600});await writeFile(join(source,'settings.json'),JSON.stringify({adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),{mode:0o600});const store=new Store(join(source,'gateway.sqlite'),{encryptionKey:master});store.createMember('合成root来源',null,null);store.close();for(const name of ['key-encryption.key','settings.json','gateway.sqlite'])await chown(join(source,name),1000,1000);await chown(source,1000,1000);
    await assert.rejects(captureSqliteSource({sourceDataDir:source,directory:rejected,key,fileRoots:[{id:'data',source}]}),{code:'transfer_source_key_invalid'});
    lease=await acquireSqliteSourceLease(source,{sourceOwnerUid:1000});await captureSqliteSource({sourceDataDir:source,directory:artifacts,key,fileRoots:[{id:'data',source}],sourceLease:lease});assert.equal((await lstat(join(source,'key-encryption.key'))).uid,1000);assert.deepEqual(await readFile(join(source,'key-encryption.key')),master);
  }finally{await lease?.close();await rm(root,{recursive:true,force:true});}
});

test('root operator keeps its top directory while mounted children become UID1000 and can be verified again',{skip:process.getuid?.()!==0?'requires actual root credential/layout fixture; current operator is UID1000':false},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-root-materials-'));try{await mkdir(join(root,'data'),{mode:0o700});await mkdir(join(root,'runtime'),{mode:0o700});await writeFile(join(root,'data','synthetic.key'),randomBytes(32),{mode:0o600});for(let attempt=0;attempt<2;attempt++){await prepareStorageOperatorMaterialOwners(root,['data','runtime'],1000);assert.equal((await lstat(root)).uid,0);assert.equal((await lstat(join(root,'data'))).uid,1000);assert.equal((await lstat(join(root,'data','synthetic.key'))).uid,1000);}}finally{await rm(root,{recursive:true,force:true});}
});
